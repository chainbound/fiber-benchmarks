package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chainbound/fiber-benchmarks/sources/bloxroute"
	"github.com/chainbound/fiber-benchmarks/sources/fiber"
	"github.com/chainbound/fiber-benchmarks/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/montanaflynn/stats"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

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

	clickhouseEndpoint string
	clickhouseUser     string
	clickhousePassword string
	clickhouseDB       string
}

func (c *config) validate() error {
	if c.sink != "none" && c.sink != "stdout" && c.sink != "clickhouse" && c.sink != "svg" {
		return fmt.Errorf("invalid sink: %s", c.sink)
	}

	if c.sink == "clickhouse" {
		if c.clickhouseEndpoint == "" {
			return fmt.Errorf("clickhouse endpoint is required")
		}

		if c.clickhouseUser == "" {
			return fmt.Errorf("clickhouse user is required")
		}

		if c.clickhousePassword == "" {
			return fmt.Errorf("clickhouse password is required")
		}

		if c.clickhouseDB == "" {
			return fmt.Errorf("clickhouse database is required")
		}
	}

	return nil
}

type TransactionSource interface {
	SubscribeTransactionObservations() chan types.Observation
}

func main() {
	var config config

	log := NewLogger("benchmark")

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
				Usage:       "File to save detailed logs",
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
				Destination: &config.clickhouseEndpoint,
			},
			&cli.StringFlag{
				Name:        "clickhouse-user",
				Usage:       "Clickhouse user",
				Value:       "default",
				Destination: &config.clickhouseUser,
			},
			&cli.StringFlag{
				Name:        "clickhouse-password",
				Usage:       "Clickhouse password",
				Destination: &config.clickhousePassword,
			},
			&cli.StringFlag{
				Name:        "clickhouse-db",
				Usage:       "Clickhouse database",
				Destination: &config.clickhouseDB,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err).Msg("Failed to run app, exiting")
	}
}

type IntervalResult struct {
	Mean                    float64
	Median                  float64
	P10, P25, P75, P90, P99 float64
	Min, Max                float64

	// Number of transactions seen by Fiber
	FiberCount int
	// Number of transactions seen by the other source
	OtherCount int
}

type TransactionBenchmarker struct {
	config *config
	logger zerolog.Logger

	fiberSource     *fiber.FiberSource
	otherSource     TransactionSource
	otherSourceName string

	csvWriter *csv.Writer

	// Results of all intervals
	// results []IntervalResult
}

func runTransactionBenchmark(config *config) error {
	// - For each interval, we collect all data from both streams
	// - At the end of the interval, we print the stats and save the result
	// - At the end of the benchmark, we print the overall stats
	logger := NewLogger("benchmark")

	if err := config.validate(); err != nil {
		logger.Fatal().Err(err).Msg("Invalid config")
	}

	var writer *csv.Writer
	if config.logFile != "" {
		f, err := os.Create(config.logFile)
		if err != nil {
			log.Fatalln("failed to open file", err)
		}

		defer f.Close()

		writer = csv.NewWriter(f)
		defer writer.Flush()

		writer.Write([]string{"tx_hash", "fiber_timestamp", "other_timestamp", "diff"})
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

	benchmarker := &TransactionBenchmarker{
		config:          config,
		logger:          logger,
		fiberSource:     fiberSource,
		otherSource:     otherSource,
		otherSourceName: otherSourceName,
		csvWriter:       writer,
	}

	benchmarker.Run()

	return nil
}

func (b *TransactionBenchmarker) Run() {
	for i := 0; i < b.config.intervalCount; i++ {
		b.logger.Info().Int("interval", i+1).Msg("Running benchmark interval")
		fmt.Println()
		b.runInterval()
	}
}

// Runs the interval
func (b *TransactionBenchmarker) runInterval() {
	// Setup
	var (
		fiberMap = make(map[common.Hash]int64)
		otherMap = make(map[common.Hash]int64)
		truthMap = make(map[common.Hash]struct{})

		fiberStream   = b.fiberSource.SubscribeTransactionObservations()
		otherStream   = b.otherSource.SubscribeTransactionObservations()
		payloadStream = b.fiberSource.SubscribeExecutionPayloads()
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
				fiberMap[fiberObs.Hash] = fiberObs.Timestamp
			} else {
				b.logger.Warn().Str("hash", fiberObs.Hash.Hex()).Str("source", "fiber").Msg("Duplicate hash during interval")
			}
		case otherObs := <-otherStream:
			if _, ok := otherMap[otherObs.Hash]; !ok {
				otherMap[otherObs.Hash] = otherObs.Timestamp
			} else {
				b.logger.Warn().Str("hash", otherObs.Hash.Hex()).Str("source", b.otherSourceName).Msg("Duplicate hash during interval")
			}
		case payload := <-payloadStream:
			if b.config.crossCheck {
				for _, tx := range payload.Transactions {
					truthMap[tx.Hash] = struct{}{}
				}
				fmt.Printf("\033[1A\033[K")
				b.logger.Info().Int("block_number", int(payload.Header.Number)).Int("amount_confirmed", len(truthMap)).Str("remaining", time.Until(end).String()).Msg("Recorded execution payload transactions")
			}
		}
	}

	b.processIntervalResults(fiberMap, otherMap, truthMap)
}

func (b *TransactionBenchmarker) processIntervalResults(fiberMap, otherMap map[common.Hash]int64, truthMap map[common.Hash]struct{}) {
	diffMap := make(map[common.Hash]float64, len(truthMap))
	differences := make([]float64, 0, len(truthMap))

	for hash := range truthMap {
		var (
			fiberSaw = false
			otherSaw = false
		)

		fiberTs, ok := fiberMap[hash]
		if ok {
			fiberSaw = true
		}

		otherTs, ok := otherMap[hash]
		if ok {
			otherSaw = true
		}

		switch {
		case fiberSaw && otherSaw:
			microDiff := otherTs - fiberTs
			milliDiff := float64(microDiff) / 1000
			// Both saw the transaction. Record the difference
			diffMap[hash] = milliDiff
			differences = append(differences, milliDiff)

			if b.csvWriter != nil {
				b.csvWriter.Write([]string{hash.Hex(), fmt.Sprint(fiberTs), fmt.Sprint(otherTs), fmt.Sprint(microDiff)})
			}
		case fiberSaw && !otherSaw:
			// Only Fiber saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("Fiber saw transaction but %s did not", b.otherSourceName))
			}

			if b.csvWriter != nil {
				b.csvWriter.Write([]string{hash.Hex(), fmt.Sprint(fiberTs), fmt.Sprint(0), fmt.Sprint(0)})
			}
		case !fiberSaw && otherSaw:
			// Only the other source saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("%s saw transaction but fiber did not", b.otherSourceName))
			}

			if b.csvWriter != nil {
				b.csvWriter.Write([]string{hash.Hex(), fmt.Sprint(0), fmt.Sprint(otherTs), fmt.Sprint(0)})
			}
		}
	}

	fmt.Println(types.MakeHistogram(differences))
	b.logger.Info().Msg(fmt.Sprintf("fiber total observations: %d", len(fiberMap)))
	b.logger.Info().Msg(fmt.Sprintf("%s total observations: %d", b.otherSourceName, len(otherMap)))
	b.printStats(differences)

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
