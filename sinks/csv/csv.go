package csv

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/chainbound/fiber-benchmarks/sinks"
	"github.com/chainbound/fiber-benchmarks/types"
)

type CsvSink struct {
	obsWriter   *csv.Writer
	statsWriter *csv.Writer
}

func NewCsvSink(fileName string, ty sinks.InitType) (*CsvSink, error) {
	var obsWriter *csv.Writer
	var statsWriter *csv.Writer

	f, err := os.Create(fileName + ".observations.csv")
	if err != nil {
		return nil, err
	}

	obsWriter = csv.NewWriter(f)
	defer obsWriter.Flush()

	f, err = os.Create(fileName + ".stats.csv")
	if err != nil {
		return nil, err
	}

	statsWriter = csv.NewWriter(f)
	defer statsWriter.Flush()

	switch ty {
	case sinks.Transactions:
		obsWriter.Write([]string{"tx_hash", "fiber_timestamp", "other_timestamp", "diff", "from", "to", "calldata_size"})
	case sinks.Blocks:
		obsWriter.Write([]string{"block_hash", "fiber_timestamp", "other_timestamp", "diff", "tx_count"})
	}

	statsWriter.Write([]string{"mean", "p50", "min", "max"})

	return &CsvSink{
		obsWriter:   obsWriter,
		statsWriter: statsWriter,
	}, nil
}

func (c *CsvSink) Close() error {
	return nil
}

func (c *CsvSink) RecordObservationRow(row *types.ConfirmedObservationRow) error {
	return c.obsWriter.Write([]string{row.TxHash, fmt.Sprint(row.FiberTimestamp), fmt.Sprint(row.OtherTimestamp), fmt.Sprint(row.Difference), row.From, row.To, fmt.Sprint(row.CallDataSize)})
}

func (c *CsvSink) RecordBlockObservationRow(row *types.BlockObservationRow) error {
	return c.obsWriter.Write([]string{row.BlockHash, fmt.Sprint(row.FiberTimestamp), fmt.Sprint(row.OtherTimestamp), fmt.Sprint(row.Difference), fmt.Sprint(row.TransactionsLen)})
}

func (c *CsvSink) RecordStats(stats *types.ObservationStatsRow) error {
	return c.statsWriter.Write([]string{fmt.Sprint(stats.Mean), fmt.Sprint(stats.P50), fmt.Sprint(stats.Min), fmt.Sprint(stats.Max)})
}

func (c *CsvSink) RecordBlockStats(stats *types.ObservationStatsRow) error {
	return c.statsWriter.Write([]string{fmt.Sprint(stats.Mean), fmt.Sprint(stats.P50), fmt.Sprint(stats.Min), fmt.Sprint(stats.Max)})
}

func (c *CsvSink) Flush() error {
	c.obsWriter.Flush()
	c.statsWriter.Flush()
	return nil
}
