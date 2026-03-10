package metrics

type Name string

const (
	// Per second metrics

	ReadRowsPerSecond              Name = "read_rows_per_second"
	ReadCompressedBytesPerSecond   Name = "read_compressed_bytes_per_second"
	ReadUncompressedBytesPerSecond Name = "read_uncompressed_bytes_per_second"
	InsertRowsPerSecond            Name = "insert_rows_per_second"
	InsertBytesPerSecond           Name = "insert_bytes_per_second"
	UserQueriesPerSecond           Name = "user_queries_per_second"
	FailedUserQueriesPerSecond     Name = "failed_user_queries_per_second"

	// State counters

	TargetBytesPerSecond       Name = "target_bytes_per_second"
	TargetWorkerBytesPerSecond Name = "target_worker_bytes_per_second"

	ActiveReaders   Name = "active_readers"
	ActiveInserters Name = "active_inserters"
	ActiveUsers     Name = "active_users"

	BlockPoolCount    Name = "block_pool_count"
	BlockPoolCapacity Name = "block_pool_capacity"
	BlockQueueLength  Name = "block_queue_length"

	// Totals

	TotalRows              Name = "total_rows"
	TotalBytesCompressed   Name = "total_bytes_compressed"
	TotalBytesUncompressed Name = "total_bytes_uncompressed"

	// Individual point metrics

	QueryLatencyMicros Name = "query_latency_micros"

	// Program runtime metrics

	ProgramHeapAllocBytes   Name = "program_heap_alloc_bytes"
	ProgramSysBytes         Name = "program_sys_bytes"
	ProgramNumGoroutines    Name = "program_num_goroutines"
	ProgramNumGC            Name = "program_num_gc"
	ProgramPauseTotalNs     Name = "program_gc_pause_total_ns"
	ProgramNextGCBytes      Name = "program_next_gc_bytes"
	ProgramCPUUserNs        Name = "program_cpu_user_ns"
	ProgramCPUSysNs         Name = "program_cpu_sys_ns"
)
