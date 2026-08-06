// Package api provides the HTTP intake for job status updates, ported from
// the job-status-listener service. External callers (app-exposer,
// vice-operator, timelord, the Argo webhook sensor, and batch jobs reaching
// the /job ingress route) POST updates here, and they are published to the
// AMQP exchange for the recorder to pick up.
package api

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"strings"

	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/model/v6"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithFields(logrus.Fields{
	"service":   "job-status",
	"component": "api",
})

// MessagePost describes the structure of the job status update request body.
type MessagePost struct {
	Hostname string
	Message  string
	State    string
}

// MessagePostWithUUIDs describes the job status update sent from the batch
// analysis execution system. It differentiates between the job UUID and the
// analysis UUID and contains both in the body of the request rather than in
// the URL.
type MessagePostWithUUIDs struct {
	JobUUID  string `json:"job_uuid"`
	Hostname string
	Message  string
	State    string
}

func getState(state string) (messaging.JobState, error) {
	switch strings.ToLower(state) {
	case "submitted":
		return messaging.SubmittedState, nil
	case "running":
		return messaging.RunningState, nil
	case "completed", "succeeded":
		return messaging.SucceededState, nil
	case "failed":
		return messaging.FailedState, nil
	default:
		return "", fmt.Errorf("unknown job state: %s", state)
	}
}

// API publishes job status updates received over HTTP.
type API struct {
	publisher JobUpdatePublisher
}

// New returns an *API that publishes updates through the given publisher.
func New(publisher JobUpdatePublisher) *API {
	return &API{publisher: publisher}
}

func (a *API) update(ctx context.Context, state messaging.JobState, jobID, hostname, msg string) (*messaging.UpdateMessage, error) {
	updateMessage := &messaging.UpdateMessage{
		Job:     &model.Job{InvocationID: jobID},
		State:   state,
		Message: msg,
		Sender:  hostname,
	}

	err := a.publisher.PublishJobUpdate(ctx, updateMessage)
	if err == nil {
		log.Infof("%s (%s) [%s]: %s", jobID, state, hostname, msg)
		return updateMessage, nil
	}

	log.Errorf("failed to publish job status update; the AMQP connection may have gone stale: %s", err)

	log.Info("attempting to reestablish the messaging connection")
	err = a.publisher.Reconnect()
	if err != nil {
		log.Errorf("unable to reestablish the messaging connection: %s", err)
		return nil, err
	}

	err = a.publisher.PublishJobUpdate(ctx, updateMessage)
	if err == nil {
		log.Infof("%s (%s) [%s]: %s", jobID, state, hostname, msg)
		return updateMessage, nil
	}

	log.Errorf("failed to publish job status update again - giving up: %s", err)
	return nil, err
}

func writeError(w http.ResponseWriter, status int, err error) {
	log.Error(err)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func (a *API) postBatchStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	var updateMessage MessagePostWithUUIDs

	if err := json.NewDecoder(r.Body).Decode(&updateMessage); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	jobID := updateMessage.JobUUID

	state, err := getState(updateMessage.State)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	log.Infof("batch analysis => job id: '%s', state: '%s', hostname: '%s', message :'%s'",
		jobID,
		state,
		updateMessage.Hostname,
		updateMessage.Message,
	)

	msg, err := a.update(r.Context(), state, jobID, updateMessage.Hostname, updateMessage.Message)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	_ = json.NewEncoder(w).Encode(msg)
}

func (a *API) postUpdate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	var updateMessage MessagePost

	jobID := mux.Vars(r)["uuid"]

	if err := json.NewDecoder(r.Body).Decode(&updateMessage); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	state, err := getState(updateMessage.State)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	msg, err := a.update(r.Context(), state, jobID, updateMessage.Hostname, updateMessage.Message)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	_ = json.NewEncoder(w).Encode(msg)
}

// Router returns the HTTP router for the job status intake API.
func (a *API) Router() *mux.Router {
	r := mux.NewRouter()
	r.Handle("/debug/vars", expvar.Handler())
	r.Path("/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/status").
		Methods(http.MethodPost).HandlerFunc(a.postUpdate)
	r.Path("/status/batch").Methods(http.MethodPost).HandlerFunc(a.postBatchStatus)

	return r
}
