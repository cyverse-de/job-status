// job-status
//
// A single service combining the three stages of the DE job status pipeline
// that used to be separate services:
//
//   - api (was job-status-listener): HTTP intake for status updates, published
//     to the "de" AMQP exchange with the "jobs.updates" routing key.
//   - recorder (was job-status-recorder): consumes those messages and records
//     them in the DE database's job_status_updates table.
//   - propagator (was job-status-to-apps-adapter): polls for unpropagated
//     updates and forwards them to the apps service's DE job callback.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/dbutil"
	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/version"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/cyverse-de/job-status/internal/api"
	"github.com/cyverse-de/job-status/internal/propagator"
	"github.com/cyverse-de/job-status/internal/recorder"
)

const serviceName = "job-status"

var log = logrus.WithFields(logrus.Fields{"service": serviceName})

// mergeFlag resolves a setting that can come from either a CLI flag or the
// config file: an explicitly set flag wins, otherwise the config value is used.
func mergeFlag(cfg *viper.Viper, flagValue *string, key string) string {
	if *flagValue == "" {
		return cfg.GetString(key)
	}
	cfg.Set(key, *flagValue)
	return *flagValue
}

func main() {
	var (
		showVersion  = flag.Bool("version", false, "Print the version information")
		cfgPath      = flag.String("config", "/etc/iplant/de/job-status.yml", "The path to the config file")
		listenPort   = flag.Int("port", 60000, "The port the HTTP API listens on")
		dbURI        = flag.String("db", "", "The URI used to connect to the database (overrides db.uri)")
		amqpURI      = flag.String("amqp", "", "The URI used to connect to the AMQP broker (overrides amqp.uri)")
		amqpExchange = flag.String("exchange", "", "The AMQP exchange to connect to (overrides amqp.exchange.name)")
		amqpType     = flag.String("exchangetype", "", "The type of the AMQP exchange (overrides amqp.exchange.type)")
		pollInterval = flag.Duration("poll-interval", 5*time.Second, "How often to poll for unpropagated status updates")
		retryBackoff = flag.Duration("retry-backoff", time.Minute, "How long to wait before re-attempting propagation of an update")
		maxRetries   = flag.Int64("max-retries", 0, "The maximum number of propagation attempts per update; 0 retries forever")
		batchSize    = flag.Int("batch-size", 20, "The number of concurrent propagation requests to apps")
		httpTimeout  = flag.Duration("http-timeout", time.Minute, "The timeout for each propagation request to apps")
	)

	flag.Parse()
	logrus.SetFormatter(&logrus.JSONFormatter{})

	if *showVersion {
		version.AppVersion()
		os.Exit(0)
	}

	if *cfgPath == "" {
		log.Fatal("--config must be set.")
	}

	cfg, err := configurate.InitDefaults(*cfgPath, configurate.JobServicesDefaults)
	if err != nil {
		log.Fatal(err)
	}

	amqpURIVal := mergeFlag(cfg, amqpURI, "amqp.uri")
	exchangeVal := mergeFlag(cfg, amqpExchange, "amqp.exchange.name")
	exchangeTypeVal := mergeFlag(cfg, amqpType, "amqp.exchange.type")
	dbURIVal := mergeFlag(cfg, dbURI, "db.uri")
	callbacksURI := cfg.GetString("apps.callbacks_uri")

	// Fail fast on a config that can't work rather than limping into runtime errors.
	switch {
	case amqpURIVal == "":
		log.Fatal("amqp.uri must be set in the config or via --amqp")
	case exchangeVal == "":
		log.Fatal("amqp.exchange.name must be set in the config or via --exchange")
	case dbURIVal == "":
		log.Fatal("db.uri must be set in the config or via --db")
	case callbacksURI == "":
		log.Fatal("apps.callbacks_uri must be set in the config")
	case *pollInterval <= 0 || *retryBackoff <= 0 || *httpTimeout <= 0:
		log.Fatal("--poll-interval, --retry-backoff, and --http-timeout must all be positive")
	case *batchSize <= 0:
		log.Fatal("--batch-size must be positive")
	case *maxRetries < 0:
		log.Fatal("--max-retries must not be negative")
	}

	log.Infof("AMQP broker setting is %s", amqpURIVal)

	// The HTTP intake publishes on its own connection that it can tear down
	// and reestablish when a publish fails.
	publisher, err := api.NewDefaultJobUpdatePublisher(amqpURIVal, exchangeVal)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	log.Info("Connecting to the database...")
	connector, err := dbutil.NewDefaultConnector("1m")
	if err != nil {
		log.Fatal(err)
	}
	db, err := connector.Connect("postgres", dbURIVal)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	log.Info("Connected to the database")

	// The consumer connection is deliberately non-reconnecting: the messaging
	// library's reconnect path doesn't reestablish consumers, so a lost
	// connection exits the process and Kubernetes restarts it instead.
	consumerClient, err := messaging.NewClient(amqpURIVal, false)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerClient.Close()

	go consumerClient.Listen()

	rec := recorder.New(db)
	consumerClient.AddConsumer(
		exchangeVal,
		exchangeTypeVal,
		recorder.QueueName,
		recorder.RoutingKey,
		rec.HandleMessage,
		recorder.PrefetchCount,
	)

	prop := propagator.New(db, callbacksURI, propagator.Options{
		PollInterval: *pollInterval,
		RetryBackoff: *retryBackoff,
		MaxRetries:   *maxRetries,
		BatchSize:    *batchSize,
		HTTPTimeout:  *httpTimeout,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", *listenPort),
		Handler:           api.New(publisher).Router(),
		ReadHeaderTimeout: 30 * time.Second,
	}

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		log.Infof("HTTP API listening on %s", server.Addr)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	group.Go(func() error {
		return prop.Run(groupCtx)
	})

	group.Go(func() error {
		<-groupCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
