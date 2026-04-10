<p align="center">
	<img src=".static/logo.png" alt="ClickSpam logo" width="400px" align="center">
	<h1 align="center">ClickSpam</h1>
</p>

# About

A program for replaying OTel data into ClickHouse and simulating concurrent user queries against it. Three independent modes can be run in any combination:

- **disk** — reads `.native`/`.native.zst` files from disk and feeds them to the insert workers
- **insert** — inserts data into ClickHouse via ch-go
- **user** — simulates concurrent users running parameterized queries against ClickHouse

Each mode is independently toggled via `enabled` in the config. You can run all three at once to load data and benchmark queries simultaneously, or run `user` alone against an already-populated table.

# Usage

Copy `example.yaml`, edit it for your environment, and enable the modes you want.

Run with Go:
```sh
go run clickspam --config my-config.yaml
```

Or build a binary first:
```sh
go build -o clickspam . && ./clickspam --config my-config.yaml
```

Or with Docker (mount your config and data):
```sh
docker build -t clickspam .
docker run -v $(pwd)/my-config.yaml:/root/my-config.yaml \
           -v $(pwd)/trace_data:/root/trace_data \
           clickspam ./clickspam --config /root/my-config.yaml
```

The config path can also be set via environment variable:
```sh
CLICKSPAM_CONFIG=my-config.yaml go run clickspam
```

By default a random UUID is generated as the run ID each time the program starts. To set a specific run ID, set `CLICKSPAM_RUN_ID`:
```sh
CLICKSPAM_RUN_ID=my-run-id go run clickspam --config my-config.yaml
```

# Preparing Data

Data must be exported from ClickHouse in Native format. Files must be named with the extension `.native` or `.native.zst`. Place them in a directory and point `disk.logs_path` or `disk.traces_path` at it.

Export logs:
```sql
SELECT * FROM otel.otel_logs LIMIT 10000000 INTO OUTFILE 'log_data/logs.native.zst' COMPRESSION 'zstd' FORMAT Native
```

Export traces:
```sql
SELECT * FROM otel.otel_traces LIMIT 10000000 INTO OUTFILE 'trace_data/traces.native.zst' COMPRESSION 'zstd' FORMAT Native
```

You can split data across multiple files — each file becomes a unit of work for the disk reader threads.

# Memory Management

ClickSpam includes two workarounds for memory growth that occurs during long runs. Both are caused by ch-go accumulating allocations over time and are addressed by periodic retirement of the relevant objects.

## Block retirement (`disk.block_retirement_uses`)

When `disk.reuse_blocks` is enabled, native blocks read from disk are recycled rather than garbage collected after each insert. This improves throughput stability by avoiding GC pressure, but ch-go has a quirk where column slice backing arrays grow each time a block is reset and refilled — memory is never returned. Over a long run this causes steady memory growth.

`block_retirement_uses` sets a limit on how many times a block can be reused before it is discarded and replaced with a fresh allocation. Setting this to a reasonable value bounds the growth without giving up the throughput benefits.

**Deriving a value:** Check your Grafana dashboard for memory growth rate and insert throughput. A block is retired after N uses regardless of size, so a lower value means more frequent fresh allocations (more GC) but tighter memory bounds. 100 is a reasonable starting point. If memory is still growing, lower it; if GC pauses are visible in throughput, raise it.

Set to `0` to disable retirement (blocks live for the program's lifetime, original behavior).

## Insert worker retirement (`insert.worker_retirement_batches`)

The ch-go encoder inside each insert worker accumulates buffer allocations over time as it encodes blocks. These buffers grow to fit the largest block seen and are never shrunk. Over many batches this causes each worker's memory footprint to drift upward.

`worker_retirement_batches` sets how many batches a worker sends before it exits and is replaced by a fresh one. Workers are staggered so they don't all restart simultaneously: each worker i gets an initial batch offset of `(i * retirement_batches) / threads`. It then counts from that offset and retires after sending exactly `retirement_batches` batches, so every worker sends the same number regardless of its position. The offsets spread retirements evenly across the retirement window, and because the offset is recalculated from the stable worker ID on each restart, the stagger is maintained for the life of the program.

**Deriving a value:** Estimate your target throughput in batches per second (throughput / `insert.batch_size`), then decide how often you want workers to recycle. For a run targeting 1M rows/s with `batch_size=100000`, that's ~10 batches/s; retiring every 100 batches means a recycle roughly every 10 seconds per worker. Lower values reduce peak memory per worker but add reconnection overhead. Higher values allow more drift.

Set to `0` to disable retirement (workers run indefinitely, original behavior).

# Grafana

A Grafana dashboard is included in `grafana.json`. Import it via Dashboards > Import in the Grafana UI. It reads metrics from the ClickHouse server configured under `metrics` in your config.

### Disk & Insert panels

![Example of disk and insert dashboard](.static/grafana_disk_and_insert_dashboard.png)


### User Query panels

![Example of user dashboard](.static/grafana_user_dashboard.png)
