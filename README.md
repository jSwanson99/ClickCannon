# otelspam
A program for replaying OTel data

## Usage

Build the program:
```sh
go build otelspam
```

Copy the `example.yaml` config, and edit the values.

Make a directory for logs or trace data.
Data should be exported from ClickHouse using `INTO OUTFILE` syntax.
Compression is optional, but you must make your file extension either `.native` or `.native.zst`.

Example data export:
```sql
SELECT * FROM otel.otel_logs LIMIT 10000000 INTO OUTFILE 'log_data/logs.native.zst' COMPRESSION 'zstd' FORMAT Native
```

Run the program:
```sh
./otelspam --config example.yaml
```

Logs will be in the console, or exported to a table of your choice if you have a ClickHouse URL configured.
