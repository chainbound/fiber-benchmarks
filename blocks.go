package main

import (
	"fmt"
	"time"

	"github.com/chainbound/fiber-benchmarks/log"
	"github.com/chainbound/fiber-benchmarks/sinks"
	"github.com/chainbound/fiber-benchmarks/sources/bloxroute"
	"github.com/chainbound/fiber-benchmarks/sources/fiber"
	"github.com/chainbound/fiber-benchmarks/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/montanaflynn/stats"
	"github.com/rs/zerolog"
)

type BlockBenchmarker struct {
	config *config
	logger zerolog.Logger

	fiberSource     *fiber.FiberSource
	otherSource     BlockSource
	otherSourceName string

	sink Sink
}

func runBlockBenchmark(config *config) error {
	// - For each interval, we collect all data from both streams
	// - At the end of the interval, we print the stats and save the result
	// - At the end of the benchmark, we print the overall stats
	logger := log.NewLogger("benchmark")

	if err := config.validate(); err != nil {
		logger.Fatal().Err(err).Msg("Invalid config")
	}

	fiberSource := fiber.NewFiberSource(config.fiberEndpoints, config.fiberKey)
	if err := fiberSource.Connect(); err != nil {
		return err
	}

	var otherSource BlockSource
	var otherSourceName string

	if config.blxrEndpoint != "" && config.blxrKey != "" {
		otherSource = bloxroute.NewBloxrouteSource(config.blxrEndpoint, config.blxrKey)
		otherSourceName = "bloxroute"
	}

	sink, err := setupSink(config, sinks.Blocks)
	if err != nil {
		return err
	}

	benchmarker := &BlockBenchmarker{
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

func (b *BlockBenchmarker) Run() {
	var (
		fiberStream = b.fiberSource.SubscribeBlockObservations()
		otherStream = b.otherSource.SubscribeBlockObservations()
	)

	defer b.fiberSource.Close()
	for i := 0; i < b.config.intervalCount; i++ {
		start := time.Now()
		b.logger.Info().Int("interval", i+1).Msg("Running benchmark interval")
		stats, err := b.runInterval(fiberStream, otherStream)
		if err != nil {
			b.logger.Error().Err(err).Msg("Failed to run interval")
		}
		end := time.Now()
		stats.StartTime = start
		stats.EndTime = end
		stats.BenchmarkID = b.config.benchmarkID
		b.sink.RecordBlockStats(&stats)
		if err := b.sink.Flush(); err != nil {
			b.logger.Error().Err(err).Msg("Failed to flush sink")
		}
	}

	b.logger.Info().Msg("Benchmark complete")
}

// Runs the interval
func (b *BlockBenchmarker) runInterval(fiberStream, otherStream chan types.BlockObservation) (types.ObservationStatsRow, error) {
	// Setup
	var (
		fiberMap = make(map[common.Hash]types.BlockObservation)
		otherMap = make(map[common.Hash]types.BlockObservation)
	)

	// Initialize interval timer
	timer := time.NewTimer(b.config.interval)

	b.logger.Info().Int("fiber", len(fiberStream)).Int("other", len(otherStream)).Msg("Buffered observations")

loop:
	for {
		select {
		case <-timer.C:
			break loop
		case fiberObs := <-fiberStream:
			// If we're not in the tail window, continue as usual
			if _, ok := fiberMap[fiberObs.Hash]; !ok {
				b.logger.Info().Str("hash", fiberObs.Hash.Hex()).Msg("New Fiber block")
				fiberMap[fiberObs.Hash] = fiberObs
			} else {
				b.logger.Warn().Str("hash", fiberObs.Hash.Hex()).Str("source", "fiber").Msg("Duplicate hash during interval")
			}
		case otherObs := <-otherStream:
			if _, ok := otherMap[otherObs.Hash]; !ok {
				otherMap[otherObs.Hash] = otherObs
				b.logger.Info().Str("hash", otherObs.Hash.Hex()).Msg("New Bloxroute block")
			} else {
				b.logger.Warn().Str("hash", otherObs.Hash.Hex()).Str("source", b.otherSourceName).Msg("Duplicate hash during interval")
			}
		}
	}

	return b.processIntervalResults(fiberMap, otherMap)
}

func (b *BlockBenchmarker) processIntervalResults(fiberMap, otherMap map[common.Hash]types.BlockObservation) (types.ObservationStatsRow, error) {
	diffMap := make(map[common.Hash]float64, len(fiberMap))
	differences := make([]float64, 0, len(fiberMap))

	for hash, fiberObs := range fiberMap {
		otherSaw := false

		otherObs, ok := otherMap[hash]
		if ok {
			otherSaw = true
		}

		fiberTs := fiberObs.Timestamp
		otherTs := otherObs.Timestamp

		switch {
		case otherSaw:
			microDiff := otherTs - fiberTs
			milliDiff := float64(microDiff) / 1000
			// Both saw the transaction. Record the difference
			diffMap[hash] = milliDiff
			differences = append(differences, milliDiff)

			if b.sink != nil {
				b.sink.RecordBlockObservationRow(&types.BlockObservationRow{
					BlockHash:       hash.Hex(),
					FiberTimestamp:  fiberTs,
					OtherTimestamp:  otherTs,
					Difference:      microDiff,
					BenchmarkID:     b.config.benchmarkID,
					TransactionsLen: int64(fiberObs.TransactionsLen),
				})
			}

		case !otherSaw:
			// Only Fiber saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("Fiber saw block but %s did not", b.otherSourceName))
			}

			if b.sink != nil {
				b.sink.RecordBlockObservationRow(&types.BlockObservationRow{
					BlockHash:       hash.Hex(),
					FiberTimestamp:  fiberTs,
					OtherTimestamp:  0,
					Difference:      0,
					BenchmarkID:     b.config.benchmarkID,
					TransactionsLen: int64(fiberObs.TransactionsLen),
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

	return buildBlockObservationStats(differences)
}

func (b *BlockBenchmarker) printStats(differences []float64) {
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
