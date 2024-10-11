package main

import (
	"fmt"
	"time"

	"github.com/chainbound/fiber-benchmarks/log"
	"github.com/chainbound/fiber-benchmarks/sinks"
	"github.com/chainbound/fiber-benchmarks/sources/bloxroute"
	"github.com/chainbound/fiber-benchmarks/sources/fiber"
	"github.com/chainbound/fiber-benchmarks/types"
	f "github.com/chainbound/fiber-go"
	"github.com/ethereum/go-ethereum/common"
	"github.com/montanaflynn/stats"
	"github.com/rs/zerolog"
)

type TransactionBenchmarker struct {
	config *config
	logger zerolog.Logger

	fiberSource     *fiber.FiberSource
	otherSource     TransactionSource
	otherSourceName string

	sink Sink
}

func runTransactionBenchmark(config *config) error {
	// - For each interval, we collect all data from both streams
	// - At the end of the interval, we print the stats and save the result
	// - At the end of the benchmark, we print the overall stats
	logger := log.NewLogger("benchmark")

	if err := config.validate(); err != nil {
		logger.Fatal().Err(err).Msg("Invalid config")
	}

	var fiberSource *fiber.FiberSource
	var otherSource TransactionSource
	var otherSourceName string

	if config.fiberOnly {
		firstEndpoint := config.fiberEndpoints[0]
		fiberSource = fiber.NewFiberSource([]string{firstEndpoint}, config.fiberKey)
		if err := fiberSource.Connect(); err != nil {
			return err
		}

		otherSourceName = "fiber-2"
		secondEndpoint := config.fiberEndpoints[1]
		otherFiberSource := fiber.NewFiberSource([]string{secondEndpoint}, config.fiberKey)
		if err := otherFiberSource.Connect(); err != nil {
			return err
		}

		logger.Info().Str("fiber-1", firstEndpoint).Str("fiber-2", secondEndpoint).Msg("Running fiber-only benchmark")

		otherSource = otherFiberSource
	} else {
		fiberSource = fiber.NewFiberSource(config.fiberEndpoints, config.fiberKey)
		if err := fiberSource.Connect(); err != nil {
			return err
		}

		if config.blxrEndpoint != "" && config.blxrKey != "" {
			otherSource = bloxroute.NewBloxrouteSource(config.blxrEndpoint, config.blxrKey)
			otherSourceName = "bloxroute"
		}
	}

	sink, err := setupSink(config, sinks.Transactions)
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
		if b.sink != nil {
			b.sink.RecordStats(&stats)
			if err := b.sink.Flush(); err != nil {
				b.logger.Error().Err(err).Msg("Failed to flush sink")
			}
		}
	}

	b.logger.Info().Msg("Benchmark complete")
}

// Runs the interval
func (b *TransactionBenchmarker) runInterval(fiberStream chan types.Observation, otherStream chan types.Observation, payloadStream chan *f.Block) (types.ObservationStatsRow, error) {
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
					truthMap[tx.Hash()] = struct{}{}
				}
				if b.config.sink != "clickhouse" {
					fmt.Printf("\033[1A\033[K")
					b.logger.Info().Int("block_number", int(payload.Header.Number.Int64())).Int("amount_confirmed", len(truthMap)).Str("remaining", time.Until(end).String()).Msg("Recorded execution payload transactions")
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
