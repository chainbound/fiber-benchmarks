package clickhouse

import "fmt"

func ConfirmedObservationsDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.confirmed_observations (
    tx_hash String,
    fiber_timestamp Int64,
    other_timestamp Int64,
    difference Int64,
	benchmark_id String,
	from String,
	to String,
	calldata_size Int64
) ENGINE = MergeTree()
PRIMARY KEY (tx_hash, difference)`, db)
}

func ConfirmedBlockObservationsDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.confirmed_block_observations (
    block_hash String,
    fiber_timestamp Int64,
    other_timestamp Int64,
    difference Int64,
	benchmark_id String,
	transactions_len Int64
) ENGINE = MergeTree()
PRIMARY KEY (tx_hash, difference)`, db)
}

func ObservationStatsDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.observation_stats (
	start_time DateTime64,
	end_time DateTime64,
	min Float64,
	max Float64,
	benchmark_id String,
	mean Float64,
	fiber_won Float64,
	p1 Float64,
	p5 Float64,
    p10 Float64,
	p15 Float64,
	p20 Float64,
	p25 Float64,
	p30 Float64,
	p35 Float64,
	p40 Float64,
	p45 Float64,
	p50 Float64,
	p55 Float64,
	p60 Float64,
	p65 Float64,
	p70 Float64,
	p75 Float64,
	p80 Float64,
	p85 Float64,
	p90 Float64,
	p95 Float64,
	p99 Float64
) ENGINE = MergeTree()
PRIMARY KEY (end_time)`, db)
}

func BlockObservationStatsDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.block_observation_stats (
	start_time DateTime64,
	end_time DateTime64,
	min Float64,
	max Float64,
	benchmark_id String,
	mean Float64,
	fiber_won Float64,
	p1 Float64,
	p5 Float64,
    p10 Float64,
	p15 Float64,
	p20 Float64,
	p25 Float64,
	p30 Float64,
	p35 Float64,
	p40 Float64,
	p45 Float64,
	p50 Float64,
	p55 Float64,
	p60 Float64,
	p65 Float64,
	p70 Float64,
	p75 Float64,
	p80 Float64,
	p85 Float64,
	p90 Float64,
	p95 Float64,
	p99 Float64
) ENGINE = MergeTree()
PRIMARY KEY (end_time)`, db)
}
