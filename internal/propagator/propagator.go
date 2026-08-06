// Package propagator forwards recorded job status updates to the apps
// service's DE job callback endpoint, replacing the job-status-to-apps-adapter
// service. Unlike its predecessor it polls on an interval and claims rows with
// FOR UPDATE SKIP LOCKED, so multiple replicas can run it concurrently without
// flooding apps with duplicate callbacks.
package propagator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var log = logrus.WithFields(logrus.Fields{
	"service":   "job-status",
	"component": "propagator",
})

// JobStatusUpdate contains the data POSTed to the apps service.
type JobStatusUpdate struct {
	UUID string `json:"uuid"`
}

// Options configures a Propagator.
type Options struct {
	// PollInterval is how often the database is polled for unpropagated updates.
	PollInterval time.Duration

	// RetryBackoff is how long a claimed-but-still-unpropagated update waits
	// before it becomes eligible for another propagation attempt.
	RetryBackoff time.Duration

	// MaxRetries caps propagation attempts per row; 0 means retry forever.
	MaxRetries int64

	// BatchSize limits how many callback POSTs run concurrently.
	BatchSize int

	// HTTPTimeout bounds each callback POST to the apps service.
	HTTPTimeout time.Duration
}

// Propagator claims unpropagated job status updates and pushes them to the
// apps service, which marks them propagated once it has processed them.
type Propagator struct {
	db           *sql.DB
	callbacksURI string
	client       *http.Client
	opts         Options
	now          func() time.Time
}

// New returns a *Propagator that POSTs claimed updates to callbacksURI.
func New(db *sql.DB, callbacksURI string, opts Options) *Propagator {
	return &Propagator{
		db:           db,
		callbacksURI: callbacksURI,
		client:       &http.Client{Timeout: opts.HTTPTimeout},
		opts:         opts,
		now:          time.Now,
	}
}

// Rows are claimed by bumping their attempt bookkeeping inside a transaction;
// SKIP LOCKED keeps two replicas from claiming the same rows, and the backoff
// window keeps rows whose callback failed (or is in flight) from being
// re-claimed on every poll. The apps service resets nothing here: it marks
// rows propagated itself once the callback succeeds.
const claimQuery = `
	WITH claimed AS (
		SELECT id
		  FROM job_status_updates
		 WHERE propagated = false
		   AND ($2 = 0 OR propagation_attempts < $2)
		   AND (last_propagation_attempt IS NULL OR last_propagation_attempt < $3)
		 FOR UPDATE SKIP LOCKED
	)
	UPDATE job_status_updates AS j
	   SET propagation_attempts = j.propagation_attempts + 1,
	       last_propagation_attempt = $1
	  FROM claimed
	 WHERE j.id = claimed.id
	 RETURNING j.external_id`

// Claim marks eligible unpropagated updates as attempted and returns the
// distinct external IDs that need a callback to the apps service.
func (p *Propagator) Claim(ctx context.Context) ([]string, error) {
	nowMs := p.now().UnixMilli()

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, claimQuery, nowMs, p.opts.MaxRetries, nowMs-p.opts.RetryBackoff.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	seen := make(map[string]bool)
	var externalIDs []string
	for rows.Next() {
		var externalID string
		if err = rows.Scan(&externalID); err != nil {
			return nil, err
		}
		if !seen[externalID] {
			seen[externalID] = true
			externalIDs = append(externalIDs, externalID)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return externalIDs, nil
}

// Propagate pushes one update to the apps service.
func (p *Propagator) Propagate(ctx context.Context, uuid string) error {
	msg, err := json.Marshal(JobStatusUpdate{UUID: uuid})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.callbacksURI, bytes.NewBuffer(msg))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("bad response from %s: %s", p.callbacksURI, resp.Status)
	}

	log.Infof("propagated job status update for %s: %s", uuid, resp.Status)
	return nil
}

func (p *Propagator) sweep(ctx context.Context) {
	externalIDs, err := p.Claim(ctx)
	if err != nil {
		log.Errorf("failed to claim unpropagated job status updates; the DE database may be unreachable, will retry next poll: %s", err)
		return
	}

	if len(externalIDs) == 0 {
		return
	}
	log.Infof("claimed %d job(s) with unpropagated status updates", len(externalIDs))

	var group errgroup.Group
	group.SetLimit(p.opts.BatchSize)
	for _, externalID := range externalIDs {
		group.Go(func() error {
			if err := p.Propagate(ctx, externalID); err != nil {
				// Failed rows stay unpropagated and are retried after the backoff window.
				log.Errorf("failed to propagate status updates for %s; apps may be down or overloaded: %s", externalID, err)
			}
			return nil
		})
	}
	_ = group.Wait()
}

// Run polls for unpropagated updates until the context is canceled.
func (p *Propagator) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.sweep(ctx)
		}
	}
}
