package recorder

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/messaging/v12"
	"github.com/cyverse-de/model/v10"
	amqp "github.com/rabbitmq/amqp091-go"
)

type mockAcknowledger struct {
	acks    int
	rejects int
	requeue bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	m.acks++
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	m.rejects++
	m.requeue = requeue
	return nil
}

func updateBody(t *testing.T, state, invocationID, message, sender, sentOn string) []byte {
	t.Helper()
	u := &messaging.UpdateMessage{
		State:   messaging.JobState(state),
		Job:     &model.Job{InvocationID: invocationID},
		Message: message,
		Sender:  sender,
		SentOn:  sentOn,
	}
	body, err := json.Marshal(u)
	if err != nil {
		t.Fatalf("error marshalling delivery body: %s", err)
	}
	return body
}

func TestHandleMessage(t *testing.T) {
	now := time.Now().Unix()
	nowstr := fmt.Sprintf("%d", now)

	testCases := []struct {
		name         string
		state        string
		invocationID string
		message      string
		sender       string
		sentOn       string
		wantMessage  string
		wantSentFrom string
		wantSender   string
		wantSentOn   int64
	}{
		{
			name:  "all fields set",
			state: "Running", invocationID: "InvocationID", message: "Message", sender: "127.0.0.1", sentOn: nowstr,
			wantMessage: "Message", wantSentFrom: "127.0.0.1", wantSender: "127.0.0.1", wantSentOn: now,
		},
		{
			name:  "empty invocation ID still inserts",
			state: "Running", invocationID: "", message: "Message", sender: "127.0.0.1", sentOn: nowstr,
			wantMessage: "Message", wantSentFrom: "127.0.0.1", wantSender: "127.0.0.1", wantSentOn: now,
		},
		{
			name:  "empty message becomes UNKNOWN",
			state: "Running", invocationID: "InvocationID", message: "", sender: "127.0.0.1", sentOn: nowstr,
			wantMessage: "UNKNOWN", wantSentFrom: "127.0.0.1", wantSender: "127.0.0.1", wantSentOn: now,
		},
		{
			name:  "empty sender becomes 0.0.0.0",
			state: "Running", invocationID: "InvocationID", message: "Message", sender: "", sentOn: nowstr,
			wantMessage: "Message", wantSentFrom: "0.0.0.0", wantSender: "0.0.0.0", wantSentOn: now,
		},
		{
			name:  "unresolvable sender hostname becomes 0.0.0.0",
			state: "Running", invocationID: "InvocationID", message: "Message", sender: "no-such-host.invalid", sentOn: nowstr,
			wantMessage: "Message", wantSentFrom: "0.0.0.0", wantSender: "no-such-host.invalid", wantSentOn: now,
		},
		{
			name:  "unparseable sentOn becomes 0",
			state: "Running", invocationID: "InvocationID", message: "Message", sender: "127.0.0.1", sentOn: "",
			wantMessage: "Message", wantSentFrom: "127.0.0.1", wantSender: "127.0.0.1", wantSentOn: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("an error '%s' was encountered when creating the mock database", err)
			}
			defer func() { _ = db.Close() }()

			mock.ExpectExec("INSERT INTO job_status_updates").
				WithArgs(tc.invocationID, tc.wantMessage, tc.state, tc.wantSentFrom, tc.wantSender, tc.wantSentOn).
				WillReturnResult(sqlmock.NewResult(0, 1))

			ack := &mockAcknowledger{}
			delivery := amqp.Delivery{
				Acknowledger: ack,
				Body:         updateBody(t, tc.state, tc.invocationID, tc.message, tc.sender, tc.sentOn),
			}

			New(db).HandleMessage(context.Background(), delivery)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations inserting job_status_updates: %s", err)
			}
			if ack.acks != 1 || ack.rejects != 0 {
				t.Errorf("acks = %d, rejects = %d; want 1 ack and 0 rejects", ack.acks, ack.rejects)
			}
		})
	}
}

func TestHandleMessageRejects(t *testing.T) {
	testCases := []struct {
		name        string
		body        []byte
		redelivered bool
		insertErr   error
		wantRequeue bool
	}{
		{
			name:        "malformed JSON is rejected with requeue on first delivery",
			body:        []byte("this is not json"),
			wantRequeue: true,
		},
		{
			name:        "malformed JSON is dropped on redelivery",
			body:        []byte("this is not json"),
			redelivered: true,
			wantRequeue: false,
		},
		{
			name:        "empty state is rejected instead of left unacked",
			body:        []byte(`{"job": {}, "state": "", "message": "hi"}`),
			wantRequeue: true,
		},
		{
			name:        "missing job is rejected instead of panicking",
			body:        []byte(`{"state": "Running", "message": "hi"}`),
			wantRequeue: true,
		},
		{
			name:        "insert failure is rejected",
			body:        nil, // filled in below with a valid update
			insertErr:   fmt.Errorf("connection lost"),
			wantRequeue: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("an error '%s' was encountered when creating the mock database", err)
			}
			defer func() { _ = db.Close() }()

			body := tc.body
			if body == nil {
				body = updateBody(t, "Running", "InvocationID", "Message", "127.0.0.1", "0")
			}

			if tc.insertErr != nil {
				mock.ExpectExec("INSERT INTO job_status_updates").WillReturnError(tc.insertErr)
			}

			ack := &mockAcknowledger{}
			delivery := amqp.Delivery{
				Acknowledger: ack,
				Redelivered:  tc.redelivered,
				Body:         body,
			}

			New(db).HandleMessage(context.Background(), delivery)

			if ack.rejects != 1 || ack.acks != 0 {
				t.Errorf("acks = %d, rejects = %d; want 0 acks and 1 reject", ack.acks, ack.rejects)
			}
			if ack.requeue != tc.wantRequeue {
				t.Errorf("requeue = %v, want %v", ack.requeue, tc.wantRequeue)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled database expectations: %s", err)
			}
		})
	}
}
