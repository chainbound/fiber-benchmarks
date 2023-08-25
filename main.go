package main

import (
	"fmt"
	"os"
	"time"

	f "github.com/chainbound/fiber-go"
	"github.com/ethereum/go-ethereum/common"
	"github.com/montanaflynn/stats"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"

	"github.com/chainbound/fiber-benchmarks/log"
	"github.com/chainbound/fiber-benchmarks/sinks/clickhouse"
	"github.com/chainbound/fiber-benchmarks/sinks/csv"
	"github.com/chainbound/fiber-benchmarks/sources/bloxroute"
	"github.com/chainbound/fiber-benchmarks/sources/fiber"
	"github.com/chainbound/fiber-benchmarks/types"
)

type Sink interface {
	RecordObservationRow(result *types.ConfirmedObservationRow) error
	RecordStats(stats *types.ObservationStatsRow) error
	Flush() error
}

type config struct {
	fiberEndpoint string
	fiberKey      string
	blxrEndpoint  string
	blxrKey       string

	crossCheck    bool
	interval      time.Duration
	intervalCount int
	logMissing    bool
	logFile       string
	sink          string
	benchmarkID   string

	clickhouse clickhouse.ClickhouseConfig
}

func (c *config) validate() error {
	if c.sink != "none" && c.sink != "stdout" && c.sink != "clickhouse" && c.sink != "csv" {
		return fmt.Errorf("invalid sink: %s", c.sink)
	}

	if c.sink == "clickhouse" {
		if c.clickhouse.Endpoint == "" {
			return fmt.Errorf("clickhouse endpoint is required")
		}

		if c.clickhouse.Username == "" {
			return fmt.Errorf("clickhouse user is required")
		}

		if c.clickhouse.Password == "" {
			return fmt.Errorf("clickhouse password is required")
		}

		if c.clickhouse.DB == "" {
			return fmt.Errorf("clickhouse database is required")
		}
	}

	if c.sink == "csv" {
		if c.logFile == "" {
			return fmt.Errorf("log file is required for CSV sink")
		}
	}

	return nil
}

type TransactionSource interface {
	SubscribeTransactionObservations() chan types.Observation
}

func main() {
	var config config

	log := log.NewLogger("benchmark")

	app := &cli.App{
		Name:  "fiber-benchmark",
		Usage: "Benchmark Fiber against other data sources",
		Commands: []*cli.Command{
			{
				Name:  "transactions",
				Usage: "Benchmark transaction streams",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        "cross-check",
						Usage:       "Cross-check transaction observations with confirmed execution payloads",
						Value:       true,
						Destination: &config.crossCheck,
					},
					&cli.BoolFlag{
						Name:        "log-missing",
						Usage:       "Log transactions that are missing from the other stream",
						Value:       true,
						Destination: &config.logMissing,
					},
				},
				Action: func(c *cli.Context) error {
					if err := runTransactionBenchmark(&config); err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:  "blocks",
				Usage: "Benchmark block streams",
				Action: func(c *cli.Context) error {
					fmt.Println("Coming soon")
					return nil
				},
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "benchmark-id",
				Usage:       "Unique ID for this benchmark. Used in the sink tables.",
				Required:    true,
				Destination: &config.benchmarkID,
			},
			&cli.StringFlag{
				Name:        "fiber-endpoint",
				Usage:       "Fiber API endpoint",
				Destination: &config.fiberEndpoint,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "fiber-key",
				Usage:       "Fiber API key",
				Destination: &config.fiberKey,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "blxr-endpoint",
				Usage:       "Bloxroute API endpoint",
				Destination: &config.blxrEndpoint,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "blxr-key",
				Usage:       "Bloxroute API key",
				Destination: &config.blxrKey,
				Required:    false,
			},
			&cli.DurationFlag{
				Name:        "interval",
				Usage:       "Duration of each interval",
				Required:    true,
				Destination: &config.interval,
			},
			&cli.IntFlag{
				Name:        "interval-count",
				Usage:       "Number of intervals to run",
				Value:       1,
				Destination: &config.intervalCount,
			},
			&cli.StringFlag{
				Name:        "log-file",
				Usage:       "File to save detailed logs in case of file sink",
				Destination: &config.logFile,
			},
			&cli.StringFlag{
				Name:        "sink",
				Usage:       "Output sink. Options: 'clickhouse', 'svg', 'stdout', 'none'. Default: 'none'",
				Value:       "none",
				Destination: &config.sink,
			},
			&cli.StringFlag{
				Name:        "clickhouse-endpoint",
				Usage:       "Clickhouse endpoint",
				Destination: &config.clickhouse.Endpoint,
			},
			&cli.StringFlag{
				Name:        "clickhouse-user",
				Usage:       "Clickhouse user",
				Value:       "default",
				Destination: &config.clickhouse.Username,
			},
			&cli.StringFlag{
				Name:        "clickhouse-password",
				Usage:       "Clickhouse password",
				Destination: &config.clickhouse.Password,
			},
			&cli.StringFlag{
				Name:        "clickhouse-db",
				Usage:       "Clickhouse database",
				Destination: &config.clickhouse.DB,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err).Msg("Failed to run app, exiting")
	}
}

type TransactionBenchmarker struct {
	config *config
	logger zerolog.Logger

	fiberSource     *fiber.FiberSource
	otherSource     TransactionSource
	otherSourceName string

	sink Sink
}

// TODO: Support other sinks
func setupSink(config *config) (Sink, error) {
	switch config.sink {
	case "none":
		return nil, nil
	case "stdout":
		return nil, nil
	case "clickhouse":
		c, err := clickhouse.NewClickhouseClient(&config.clickhouse)
		if err != nil {
			return nil, err
		}
		if err := c.Init(); err != nil {
			return nil, err
		}

		return c, nil
	case "csv":
		w, err := csv.NewCsvSink(config.logFile)
		if err != nil {
			return nil, err
		}

		return w, nil
	default:
		return nil, fmt.Errorf("invalid sink: %s", config.sink)
	}
}

func runTransactionBenchmark(config *config) error {
	// - For each interval, we collect all data from both streams
	// - At the end of the interval, we print the stats and save the result
	// - At the end of the benchmark, we print the overall stats
	logger := log.NewLogger("benchmark")

	if err := config.validate(); err != nil {
		logger.Fatal().Err(err).Msg("Invalid config")
	}

	fiberSource := fiber.NewFiberSource(config.fiberEndpoint, config.fiberKey)
	if err := fiberSource.Connect(); err != nil {
		return err
	}

	var otherSource TransactionSource
	var otherSourceName string

	if config.blxrEndpoint != "" && config.blxrKey != "" {
		otherSource = bloxroute.NewBloxrouteSource(config.blxrEndpoint, config.blxrKey)
		otherSourceName = "bloxroute"
	}

	sink, err := setupSink(config)
	if err != nil {
		return err
	}

	benchmarker := &TransactionBenchmarker{
		config:          config,
		logger:          logger,
		fiberSource:     fiberSource,
		otherSource:     otherSource,
		otherSourceName: otherSourceName,
		sink:            sink,
	}

	benchmarker.Run()

	return nil
}

func (b *TransactionBenchmarker) Run() {
	var (
		fiberStream   = b.fiberSource.SubscribeTransactionObservations()
		otherStream   = b.otherSource.SubscribeTransactionObservations()
		payloadStream = b.fiberSource.SubscribeExecutionPayloads()
	)

	defer b.fiberSource.Close()
	for i := 0; i < b.config.intervalCount; i++ {
		start := time.Now()
		b.logger.Info().Int("interval", i+1).Msg("Running benchmark interval")
		fmt.Println()
		stats, err := b.runInterval(fiberStream, otherStream, payloadStream)
		if err != nil {
			b.logger.Error().Err(err).Msg("Failed to run interval")
		}
		end := time.Now()
		stats.StartTime = start
		stats.EndTime = end
		stats.BenchmarkID = b.config.benchmarkID
		b.sink.RecordStats(&stats)
		if err := b.sink.Flush(); err != nil {
			b.logger.Error().Err(err).Msg("Failed to flush sink")
		}
	}

	b.logger.Info().Msg("Benchmark complete")
}

// Runs the interval
func (b *TransactionBenchmarker) runInterval(fiberStream chan types.Observation, otherStream chan types.Observation, payloadStream chan *f.ExecutionPayload) (types.ObservationStatsRow, error) {
	// Setup
	var (
		fiberMap = make(map[common.Hash]types.Observation)
		otherMap = make(map[common.Hash]types.Observation)
		truthMap = make(map[common.Hash]struct{})
	)

	// Initialize interval timer
	timer := time.NewTimer(b.config.interval)
	end := time.Now().Add(b.config.interval)

	b.logger.Info().Int("fiber", len(fiberStream)).Int("other", len(otherStream)).Msg("Buffered observations")

loop:
	for {
		select {
		case <-timer.C:
			break loop
		case fiberObs := <-fiberStream:
			// If we're not in the tail window, continue as usual
			if _, ok := fiberMap[fiberObs.Hash]; !ok {
				fiberMap[fiberObs.Hash] = fiberObs
			} else {
				b.logger.Warn().Str("hash", fiberObs.Hash.Hex()).Str("source", "fiber").Msg("Duplicate hash during interval")
			}
		case otherObs := <-otherStream:
			if _, ok := otherMap[otherObs.Hash]; !ok {
				otherMap[otherObs.Hash] = otherObs
			} else {
				b.logger.Warn().Str("hash", otherObs.Hash.Hex()).Str("source", b.otherSourceName).Msg("Duplicate hash during interval")
			}
		case payload := <-payloadStream:
			if b.config.crossCheck {
				for _, tx := range payload.Transactions {
					truthMap[tx.Hash] = struct{}{}
				}
				if b.config.sink != "clickhouse" {
					fmt.Printf("\033[1A\033[K")
					b.logger.Info().Int("block_number", int(payload.Header.Number)).Int("amount_confirmed", len(truthMap)).Str("remaining", time.Until(end).String()).Msg("Recorded execution payload transactions")
				}
			}
		}
	}

	return b.processIntervalResults(fiberMap, otherMap, truthMap)
}

func (b *TransactionBenchmarker) processIntervalResults(fiberMap, otherMap map[common.Hash]types.Observation, truthMap map[common.Hash]struct{}) (types.ObservationStatsRow, error) {
	diffMap := make(map[common.Hash]float64, len(truthMap))
	differences := make([]float64, 0, len(truthMap))

	for hash := range truthMap {
		var (
			fiberSaw = false
			otherSaw = false
		)

		fiberObs, ok := fiberMap[hash]
		if ok {
			fiberSaw = true
		}

		otherObs, ok := otherMap[hash]
		if ok {
			otherSaw = true
		}

		fiberTs := fiberObs.Timestamp
		otherTs := otherObs.Timestamp

		switch {
		case fiberSaw && otherSaw:
			microDiff := otherTs - fiberTs
			milliDiff := float64(microDiff) / 1000
			// Both saw the transaction. Record the difference
			diffMap[hash] = milliDiff
			differences = append(differences, milliDiff)

			if b.sink != nil {
				b.sink.RecordObservationRow(&types.ConfirmedObservationRow{
					TxHash:         hash.Hex(),
					FiberTimestamp: fiberTs,
					OtherTimestamp: otherTs,
					Difference:     microDiff,
					BenchmarkID:    b.config.benchmarkID,
					From:           fiberObs.From,
					To:             fiberObs.To,
					CallDataSize:   fiberObs.CallDataSize,
				})
			}
		case fiberSaw && !otherSaw:
			// Only Fiber saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("Fiber saw transaction but %s did not", b.otherSourceName))
			}

			if b.sink != nil {
				b.sink.RecordObservationRow(&types.ConfirmedObservationRow{
					TxHash:         hash.Hex(),
					FiberTimestamp: fiberTs,
					OtherTimestamp: 0,
					Difference:     0,
					BenchmarkID:    b.config.benchmarkID,
					From:           fiberObs.From,
					To:             fiberObs.To,
					CallDataSize:   fiberObs.CallDataSize,
				})
			}
		case !fiberSaw && otherSaw:
			// Only the other source saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("%s saw transaction but fiber did not", b.otherSourceName))
			}

			if b.sink != nil {
				b.sink.RecordObservationRow(&types.ConfirmedObservationRow{
					TxHash:         hash.Hex(),
					FiberTimestamp: 0,
					OtherTimestamp: otherTs,
					Difference:     0,
					BenchmarkID:    b.config.benchmarkID,
					From:           otherObs.From,
					To:             otherObs.To,
					CallDataSize:   otherObs.CallDataSize,
				})
			}
		}
	}

	if b.config.sink != "clickhouse" {
		fmt.Println(types.MakeHistogram(differences))
		b.logger.Info().Msg(fmt.Sprintf("fiber total observations: %d", len(fiberMap)))
		b.logger.Info().Msg(fmt.Sprintf("%s total observations: %d", b.otherSourceName, len(otherMap)))
	}
	b.printStats(differences)

	return buildObservationStats(differences)
}

func buildObservationStats(differences []float64) (types.ObservationStatsRow, error) {
	empty := types.ObservationStatsRow{}

	fiberWon := float64(0)
	blxrWon := float64(0)

	for _, diff := range differences {
		if diff > 0 {
			fiberWon++
		} else {
			blxrWon++
		}
	}
	// Calculate all stats
	mean, err := stats.Mean(differences)
	if err != nil {
		return empty, err
	}

	min, err := stats.Min(differences)
	if err != nil {
		return empty, err
	}

	max, err := stats.Max(differences)
	if err != nil {
		return empty, err
	}

	p1, err := stats.Percentile(differences, 1)
	if err != nil {
		return empty, err
	}

	p5, err := stats.Percentile(differences, 5)
	if err != nil {
		return empty, err
	}

	p10, err := stats.Percentile(differences, 10)
	if err != nil {
		return empty, err
	}

	p15, err := stats.Percentile(differences, 15)
	if err != nil {
		return empty, err
	}

	p20, err := stats.Percentile(differences, 20)
	if err != nil {
		return empty, err
	}

	p25, err := stats.Percentile(differences, 25)
	if err != nil {
		return empty, err
	}

	p30, err := stats.Percentile(differences, 30)
	if err != nil {
		return empty, err
	}

	p35, err := stats.Percentile(differences, 35)
	if err != nil {
		return empty, err
	}

	p40, err := stats.Percentile(differences, 40)
	if err != nil {
		return empty, err
	}

	p45, err := stats.Percentile(differences, 45)
	if err != nil {
		return empty, err
	}

	p50, err := stats.Percentile(differences, 50)
	if err != nil {
		return empty, err
	}

	p55, err := stats.Percentile(differences, 55)
	if err != nil {
		return empty, err
	}

	p60, err := stats.Percentile(differences, 60)
	if err != nil {
		return empty, err
	}

	p65, err := stats.Percentile(differences, 65)
	if err != nil {
		return empty, err
	}

	p70, err := stats.Percentile(differences, 70)
	if err != nil {
		return empty, err
	}

	p75, err := stats.Percentile(differences, 75)
	if err != nil {
		return empty, err
	}

	p80, err := stats.Percentile(differences, 80)
	if err != nil {
		return empty, err
	}

	p85, err := stats.Percentile(differences, 85)
	if err != nil {
		return empty, err
	}

	p90, err := stats.Percentile(differences, 90)
	if err != nil {
		return empty, err
	}

	p95, err := stats.Percentile(differences, 95)
	if err != nil {
		return empty, err
	}

	p99, err := stats.Percentile(differences, 99)
	if err != nil {
		return empty, err
	}

	return types.ObservationStatsRow{
		Mean:     mean,
		P1:       p1,
		P5:       p5,
		P10:      p10,
		P15:      p15,
		P20:      p20,
		P25:      p25,
		P30:      p30,
		P35:      p35,
		P40:      p40,
		P45:      p45,
		P50:      p50,
		P55:      p55,
		P60:      p60,
		P65:      p65,
		P70:      p70,
		P75:      p75,
		P80:      p80,
		P85:      p85,
		P90:      p90,
		P95:      p95,
		P99:      p99,
		Min:      min,
		Max:      max,
		FiberWon: fiberWon / (fiberWon + blxrWon),
	}, err
}

func (b *TransactionBenchmarker) printStats(differences []float64) {
	fiberWon := float64(0)
	blxrWon := float64(0)

	for _, diff := range differences {
		if diff > 0 {
			fiberWon++
		} else {
			blxrWon++
		}
	}
	mean, err := stats.Mean(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate mean")
	}

	median, err := stats.Median(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate median")
	}

	stdev, err := stats.StandardDeviation(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate stdev")
	}

	min, err := stats.Min(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate min")
	}

	max, err := stats.Max(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate max")
	}

	b.logger.Info().Msg(fmt.Sprintf("Mean: %.4fms", mean))
	b.logger.Info().Msg(fmt.Sprintf("Median: %.4fms", median))
	b.logger.Info().Msg(fmt.Sprintf("Stdev: %.4fms", stdev))
	b.logger.Info().Msg(fmt.Sprintf("Min: %.4fms | Max: %.4fms", min, max))

	wonRatio := fiberWon / (fiberWon + blxrWon)
	b.logger.Info().Msg(fmt.Sprintf("Fiber won: %.2f%%", wonRatio*100))
}
