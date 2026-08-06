package propagator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func newTestPropagator(t *testing.T, callbacksURI string, opts Options) (*Propagator, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was encountered when creating the mock database", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return New(db, callbacksURI, opts), mock
}

func TestClaim(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	nowMs := now.UnixMilli()
	opts := Options{RetryBackoff: time.Minute, MaxRetries: 3}

	p, mock := newTestPropagator(t, "http://apps", opts)
	p.now = func() time.Time { return now }

	rows := sqlmock.NewRows([]string{"external_id"}).
		AddRow("job-1").
		AddRow("job-2").
		AddRow("job-1") // duplicate rows for one job collapse to one callback

	mock.ExpectBegin()
	mock.ExpectQuery("WITH claimed AS").
		WithArgs(nowMs, int64(3), nowMs-time.Minute.Milliseconds()).
		WillReturnRows(rows)
	mock.ExpectCommit()

	externalIDs, err := p.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim returned an error: %s", err)
	}

	want := []string{"job-1", "job-2"}
	if len(externalIDs) != len(want) {
		t.Fatalf("Claim returned %v, want %v", externalIDs, want)
	}
	for i := range want {
		if externalIDs[i] != want[i] {
			t.Errorf("Claim returned %v, want %v", externalIDs, want)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled database expectations: %s", err)
	}
}

func TestClaimQueryError(t *testing.T) {
	p, mock := newTestPropagator(t, "http://apps", Options{RetryBackoff: time.Minute})

	mock.ExpectBegin()
	mock.ExpectQuery("WITH claimed AS").WillReturnError(errors.New("connection lost"))
	mock.ExpectRollback()

	if _, err := p.Claim(context.Background()); err == nil {
		t.Error("Claim was expected to return an error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled database expectations: %s", err)
	}
}

func TestPropagate(t *testing.T) {
	var gotBody JobStatusUpdate
	var gotContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("content-type")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p, _ := newTestPropagator(t, server.URL, Options{HTTPTimeout: time.Minute})

	if err := p.Propagate(context.Background(), "external-id-1"); err != nil {
		t.Fatalf("Propagate returned an error: %s", err)
	}
	if gotBody.UUID != "external-id-1" {
		t.Errorf("POSTed UUID = %q, want %q", gotBody.UUID, "external-id-1")
	}
	if gotContentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", gotContentType)
	}
}

func TestPropagateBadResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	p, _ := newTestPropagator(t, server.URL, Options{HTTPTimeout: time.Minute})

	if err := p.Propagate(context.Background(), "external-id-1"); err == nil {
		t.Error("Propagate was expected to return an error on a 500 response")
	}
}

func TestSweepPropagatesClaimedUpdates(t *testing.T) {
	requests := make(chan string, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var update JobStatusUpdate
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &update)
		requests <- update.UUID
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p, mock := newTestPropagator(t, server.URL, Options{
		RetryBackoff: time.Minute,
		BatchSize:    2,
		HTTPTimeout:  time.Minute,
	})

	mock.ExpectBegin()
	mock.ExpectQuery("WITH claimed AS").
		WillReturnRows(sqlmock.NewRows([]string{"external_id"}).AddRow("job-1").AddRow("job-2"))
	mock.ExpectCommit()

	p.sweep(context.Background())
	close(requests)

	got := map[string]bool{}
	for uuid := range requests {
		got[uuid] = true
	}
	if !got["job-1"] || !got["job-2"] || len(got) != 2 {
		t.Errorf("sweep POSTed %v, want job-1 and job-2", got)
	}
}
