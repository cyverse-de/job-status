package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cyverse-de/messaging/v12"
)

type mockPublisher struct {
	published      []*messaging.UpdateMessage
	publishErrs    []error
	publishCalls   int
	reconnectErr   error
	reconnectCalls int
}

func (m *mockPublisher) PublishJobUpdate(ctx context.Context, msg *messaging.UpdateMessage) error {
	call := m.publishCalls
	m.publishCalls++
	if call < len(m.publishErrs) && m.publishErrs[call] != nil {
		return m.publishErrs[call]
	}
	m.published = append(m.published, msg)
	return nil
}

func (m *mockPublisher) Reconnect() error {
	m.reconnectCalls++
	return m.reconnectErr
}

func (m *mockPublisher) Close() {}

const testUUID = "12345678-1234-1234-1234-123456789abc"

func TestRoutes(t *testing.T) {
	publishErr := errors.New("publish failed")

	testCases := []struct {
		name           string
		path           string
		body           string
		publishErrs    []error
		reconnectErr   error
		wantStatus     int
		wantPublished  int
		wantReconnects int
		wantState      messaging.JobState
		wantJobID      string
	}{
		{
			name:          "status update by path UUID",
			path:          "/" + testUUID + "/status",
			body:          `{"Hostname": "condor-1", "Message": "job started", "State": "running"}`,
			wantStatus:    http.StatusOK,
			wantPublished: 1,
			wantState:     messaging.RunningState,
			wantJobID:     testUUID,
		},
		{
			name:          "batch status update with UUID in body",
			path:          "/status/batch",
			body:          `{"job_uuid": "` + testUUID + `", "Hostname": "worker-2", "Message": "done", "State": "completed"}`,
			wantStatus:    http.StatusOK,
			wantPublished: 1,
			wantState:     messaging.SucceededState,
			wantJobID:     testUUID,
		},
		{
			name:       "unknown state is a 400",
			path:       "/" + testUUID + "/status",
			body:       `{"State": "levitating"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "malformed JSON is a 400",
			path:       "/" + testUUID + "/status",
			body:       `{"State": `,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:           "publish failure recovers after reconnect",
			path:           "/" + testUUID + "/status",
			body:           `{"State": "failed"}`,
			publishErrs:    []error{publishErr},
			wantStatus:     http.StatusOK,
			wantPublished:  1,
			wantReconnects: 1,
			wantState:      messaging.FailedState,
			wantJobID:      testUUID,
		},
		{
			name:           "reconnect failure is a 500",
			path:           "/" + testUUID + "/status",
			body:           `{"State": "submitted"}`,
			publishErrs:    []error{publishErr},
			reconnectErr:   errors.New("reconnect failed"),
			wantStatus:     http.StatusInternalServerError,
			wantReconnects: 1,
		},
		{
			name:           "repeated publish failure is a 500",
			path:           "/status/batch",
			body:           `{"job_uuid": "` + testUUID + `", "State": "submitted"}`,
			publishErrs:    []error{publishErr, publishErr},
			wantStatus:     http.StatusInternalServerError,
			wantReconnects: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &mockPublisher{
				publishErrs:  tc.publishErrs,
				reconnectErr: tc.reconnectErr,
			}
			router := New(publisher).Router()

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if len(publisher.published) != tc.wantPublished {
				t.Errorf("published %d messages, want %d", len(publisher.published), tc.wantPublished)
			}
			if publisher.reconnectCalls != tc.wantReconnects {
				t.Errorf("reconnect called %d times, want %d", publisher.reconnectCalls, tc.wantReconnects)
			}

			if tc.wantPublished > 0 {
				msg := publisher.published[0]
				if msg.State != tc.wantState {
					t.Errorf("published state = %q, want %q", msg.State, tc.wantState)
				}
				if msg.Job.InvocationID != tc.wantJobID {
					t.Errorf("published invocation ID = %q, want %q", msg.Job.InvocationID, tc.wantJobID)
				}
			}
		})
	}
}

func TestGetState(t *testing.T) {
	testCases := []struct {
		in      string
		want    messaging.JobState
		wantErr bool
	}{
		{in: "submitted", want: messaging.SubmittedState},
		{in: "Running", want: messaging.RunningState},
		{in: "COMPLETED", want: messaging.SucceededState},
		{in: "succeeded", want: messaging.SucceededState},
		{in: "failed", want: messaging.FailedState},
		{in: "levitating", wantErr: true},
		{in: "", wantErr: true},
	}

	for _, tc := range testCases {
		got, err := getState(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("getState(%q) expected an error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("getState(%q) returned an error: %s", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("getState(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
