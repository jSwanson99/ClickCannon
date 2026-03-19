package metrics

type Name string

const (
	// Counters

	// Disk read counters

	DiskRowsTotal              Name = "disk_rows_total"
	DiskBytesCompressedTotal   Name = "disk_bytes_compressed_total"   // raw bytes read from disk (zstd-compressed file)
	DiskBytesUncompressedTotal Name = "disk_bytes_uncompressed_total" // bytes after zstd decompression (native protocol format)

	// Insert counters

	InsertRowsTotal              Name = "insert_rows_total"
	InsertBytesUncompressedTotal Name = "insert_bytes_uncompressed_total" // InsertedBytes ProfileEvent: uncompressed data size as seen by ClickHouse
	InsertBytesCompressedTotal   Name = "insert_bytes_compressed_total"   // NetworkReceiveBytes ProfileEvent: compressed bytes received over the wire
	InsertBatchesTotal           Name = "insert_batches_total"

	// Per-worker insert counters (attribute: worker_id)
	InsertRowsWorkerTotal              Name = "insert_rows_worker_total"
	InsertBytesUncompressedWorkerTotal Name = "insert_bytes_uncompressed_worker_total"
	InsertBytesCompressedWorkerTotal   Name = "insert_bytes_compressed_worker_total"
	InsertBatchesWorkerTotal           Name = "insert_batches_worker_total"

	// Per-worker disk read counters (attribute: worker_id)
	DiskRowsWorkerTotal              Name = "disk_rows_worker_total"
	DiskBytesCompressedWorkerTotal   Name = "disk_bytes_compressed_worker_total"
	DiskBytesUncompressedWorkerTotal Name = "disk_bytes_uncompressed_worker_total"

	// Per-worker user query counters (attribute: worker_id)
	QueriesSucceededWorkerTotal Name = "queries_succeeded_worker_total"
	QueriesFailedWorkerTotal    Name = "queries_failed_worker_total"
	BlocksRetiredTotal           Name = "blocks_retired_total"
	InsertWorkersRetiredTotal    Name = "insert_workers_retired_total"

	// User query counters

	QueriesSucceededTotal Name = "queries_succeeded_total"
	QueriesFailedTotal    Name = "queries_failed_total"

	// Counters — Go runtime

	ProgramGCRunsTotal    Name = "program_gc_runs_total"
	ProgramGCPauseNsTotal Name = "program_gc_pause_ns_total"
	ProgramCPUUserNsTotal Name = "program_cpu_user_ns_total"
	ProgramCPUSysNsTotal  Name = "program_cpu_sys_ns_total"

	// Gauges

	TargetBytesPerSecond       Name = "target_bytes_per_second"
	TargetWorkerBytesPerSecond Name = "target_worker_bytes_per_second"

	ActiveReaders   Name = "active_readers"
	ActiveInserters Name = "active_inserters"
	ActiveUsers     Name = "active_users"

	BlockPoolCount    Name = "block_pool_count"
	BlockPoolCapacity Name = "block_pool_capacity"
	BlockQueueLength  Name = "block_queue_length"

	// Gauges — Go runtime

	ProgramHeapAllocBytes Name = "program_heap_alloc_bytes"
	ProgramSysBytes       Name = "program_sys_bytes"
	ProgramNumGoroutines  Name = "program_num_goroutines"
	ProgramNextGCBytes    Name = "program_next_gc_bytes"
	ProgramNumCPU         Name = "program_num_cpu"

	// Samples

	QueryLatencyMicros Name = "query_latency_micros"
)
