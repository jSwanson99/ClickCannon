<p align="center">
	<img src=".static/logo.png" alt="otelspam logo" width="400px" align="center">
	<h1 align="center">OTelSpam</h1>
</p>

# About

A program for replaying OTel data and simulating user queries for ClickHouse.

# Usage

Copy the `example.yaml` config, and edit the values. Read the settings carefully.

Make a directory for logs or trace data.
Data should be exported from ClickHouse using `INTO OUTFILE` syntax.
Compression is optional, but you must make your file extension either `.native` or `.native.zst`.

Example data export:
```sql
SELECT * FROM otel.otel_logs LIMIT 10000000 INTO OUTFILE 'log_data/logs.native.zst' COMPRESSION 'zstd' FORMAT Native
```

Run the program:
```sh
go run otelspam --config test.yaml
```

# Grafana

A Grafana dashboard is included. It reads metrics from the configured `metrics` ClickHouse server.

### Disk & Insert panels

![Example of disk and insert dashboard](.static/grafana_disk_and_insert_dashboard.png)


### User Query panels

![Example of user dashboard](.static/grafana_user_dashboard.png)