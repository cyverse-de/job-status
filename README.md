# job-status

A single service combining the three stages of the Discovery Environment's job
status pipeline, which used to be the separate `job-status-listener`,
`job-status-recorder`, and `job-status-to-apps-adapter` services.

## Components

- **api** — HTTP intake for job status updates. `POST /{uuid}/status` and
  `POST /status/batch` publish updates to the `de` AMQP exchange with the
  `jobs.updates` routing key. `GET /debug/vars` serves expvar output and is
  used as the health probe.
- **recorder** — consumes `jobs.updates` messages from the durable
  `job_status_recorder` queue and records them in the DE database's
  `job_status_updates` table.
- **propagator** — polls `job_status_updates` for unpropagated rows, claims
  them with `FOR UPDATE SKIP LOCKED` (so multiple replicas are safe), and
  POSTs each affected external ID to the apps service's DE job callback
  endpoint. The apps service marks rows propagated once it has processed them.

## Configuration

Reads a YAML config file (default `/etc/iplant/de/job-status.yml`):

```yaml
amqp:
  uri: amqp://user:password@rabbit:5672/de
  exchange:
    name: de
    type: topic
db:
  uri: postgresql://user:password@dedb:5432/de?sslmode=disable
apps:
  callbacks_uri: http://apps/callbacks/de-job
```

Flags:

- `--config` — path to the config file
- `--port` — HTTP listen port (default 60000)
- `--db`, `--amqp`, `--exchange`, `--exchangetype` — config overrides
- `--poll-interval` — how often to poll for unpropagated updates (default 5s)
- `--retry-backoff` — wait before re-attempting a claimed update (default 1m)
- `--max-retries` — propagation attempts per row; 0 retries forever (default 0)
- `--batch-size` — concurrent callback POSTs to apps (default 20)
- `--http-timeout` — timeout per callback POST (default 1m)
- `--version` — print version information
