// Package recorder consumes job status update messages from AMQP and records
// them in the DE database's job_status_updates table, ported from the
// job-status-recorder service.
package recorder

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/cyverse-de/messaging/v9"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// QueueName is the durable AMQP queue this component consumes. It must stay
// exactly "job_status_recorder" so a new deployment inherits the existing
// queue and its backlog.
const QueueName = "job_status_recorder"

// RoutingKey is the binding key for job status update messages.
const RoutingKey = messaging.UpdatesKey

// PrefetchCount is the AMQP prefetch applied to the consumer.
const PrefetchCount = 100

var log = logrus.WithFields(logrus.Fields{
	"service":   "job-status",
	"component": "recorder",
})

// Recorder inserts job status update messages into the DE database.
type Recorder struct {
	db *sql.DB
}

// New returns a *Recorder backed by the given database handle.
func New(db *sql.DB) *Recorder {
	return &Recorder{db: db}
}

func (r *Recorder) insert(ctx context.Context, status, externalID, message, sentFrom, sentFromHostname string, sentOn int64) (sql.Result, error) {
	insertStr := `
		INSERT INTO job_status_updates (
			external_id,
			message,
			status,
			sent_from,
			sent_from_hostname,
			sent_on
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6
		)`
	return r.db.ExecContext(ctx, insertStr, externalID, message, status, sentFrom, sentFromHostname, sentOn)
}

func reject(l *logrus.Entry, delivery amqp.Delivery) {
	// Requeue once; a redelivered message that fails again is dropped so a
	// malformed message can't loop forever.
	if err := delivery.Reject(!delivery.Redelivered); err != nil {
		l.Error(err)
	}
}

// HandleMessage records a single job status update delivery.
func (r *Recorder) HandleMessage(ctx context.Context, delivery amqp.Delivery) {
	start := time.Now()

	l := log.WithFields(logrus.Fields{"redelivered": delivery.Redelivered})

	update := &messaging.UpdateMessage{}

	if err := json.Unmarshal(delivery.Body, update); err != nil {
		l.Error(err)
		reject(l, delivery)
		return
	}

	if update.State == "" {
		l.Warn("State was unset, dropping update")
		reject(l, delivery)
		return
	}
	l = l.WithFields(logrus.Fields{"state": update.State})

	if update.Job == nil {
		l.Warn("Job was unset, dropping update")
		reject(l, delivery)
		return
	}

	if update.Job.InvocationID == "" {
		l.Warn("InvocationID was unset")
	}
	l = l.WithFields(logrus.Fields{"invocationID": update.Job.InvocationID})

	if update.Message == "" {
		l.Warn("Message set to empty string, setting to UNKNOWN")
		update.Message = "UNKNOWN"
	}
	l = l.WithFields(logrus.Fields{"updateMessage": update.Message})

	if update.Sender == "" {
		l.Warn("Unknown sender, setting from address to 0.0.0.0")
		update.Sender = "0.0.0.0"
	}
	l = l.WithFields(logrus.Fields{"sender": update.Sender})

	// The sent_from column is an inet, so a sender hostname has to be
	// resolved to an address before it can be recorded.
	var sentFrom string
	if parsedIP := net.ParseIP(update.Sender); parsedIP != nil {
		sentFrom = update.Sender
	} else {
		ips, err := net.LookupIP(update.Sender)
		if err != nil || len(ips) == 0 {
			l.Warnf("Provided hostname %s did not resolve to an IP, setting from address to 0.0.0.0: %+v", update.Sender, err)
			sentFrom = "0.0.0.0"
		} else {
			sentFrom = ips[0].String()
		}
	}
	l = l.WithFields(logrus.Fields{"sentFrom": sentFrom, "sentOn": update.SentOn})

	sentOn, err := strconv.ParseInt(update.SentOn, 10, 64)
	if err != nil {
		l.Errorf("Error parsing SentOn field, setting field to 0: %s", err)
		sentOn = 0
	}

	result, err := r.insert(
		ctx,
		string(update.State),
		update.Job.InvocationID,
		update.Message,
		sentFrom,
		update.Sender,
		sentOn,
	)
	if err != nil {
		l.Error(err)
		reject(l, delivery)
		return
	}

	rowCount, err := result.RowsAffected()
	if err != nil {
		l.Error(err)
		reject(l, delivery)
		return
	}

	l.Infof("Processed message and inserted %d rows in %s", rowCount, time.Since(start))

	if err := delivery.Ack(false); err != nil {
		l.Error(err)
	}
}
