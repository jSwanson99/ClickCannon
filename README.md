# otelspam
A program for replaying OTel data and simulating user queries

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
go run otelspam --config test.yaml
```

Logs will be in the console, or exported to a table of your choice if you have a ClickHouse URL configured.

#### User workload

You can simulate queries too. Check the example.yaml to see how an array of queries can be configured.
I've also added experimental code for replaying a browser http archive (HAR) file.
This has some hardcoded logic, but it is intended to capture queries from HyperDX.
The timestamp on the queries is shifted as well as the database name.
The format is swapped to be `Null` since the data isn't used.
Queries are replayed at the same pace they were captured.
