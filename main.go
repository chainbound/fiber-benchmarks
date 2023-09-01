package main

import (
	"fmt"
	"os"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/urfave/cli/v2"

	"github.com/chainbound/fiber-benchmarks/log"
	"github.com/chainbound/fiber-benchmarks/sinks"
	"github.com/chainbound/fiber-benchmarks/sinks/clickhouse"
	"github.com/chainbound/fiber-benchmarks/sinks/csv"
	"github.com/chainbound/fiber-benchmarks/types"
)

type Sink interface {
	RecordBlockObservationRow(result *types.BlockObservationRow) error
	RecordObservationRow(result *types.ConfirmedObservationRow) error
	RecordStats(stats *types.ObservationStatsRow) error
	RecordBlockStats(stats *types.ObservationStatsRow) error
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

type BlockSource interface {
	SubscribeBlockObservations() chan types.BlockObservation
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
					if err := runBlockBenchmark(&config); err != nil {
						return err
					}
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

// TODO: Support other sinks
func setupSink(config *config, ty sinks.InitType) (Sink, error) {
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
		if err := c.Init(ty); err != nil {
			return nil, err
		}

		return c, nil
	case "csv":
		w, err := csv.NewCsvSink(config.logFile, ty)
		if err != nil {
			return nil, err
		}

		return w, nil
	default:
		return nil, fmt.Errorf("invalid sink: %s", config.sink)
	}
}

func buildBlockObservationStats(differences []float64) (types.ObservationStatsRow, error) {
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
		return empty, fmt.Errorf("mean: %w", err)
	}

	min, err := stats.Min(differences)
	if err != nil {
		return empty, fmt.Errorf("min: %w", err)
	}

	max, err := stats.Max(differences)
	if err != nil {
		return empty, fmt.Errorf("max: %w", err)
	}

	p5, err := stats.Percentile(differences, 5)
	if err != nil {
		return empty, fmt.Errorf("p5: %w", err)
	}

	p25, err := stats.Percentile(differences, 25)
	if err != nil {
		return empty, fmt.Errorf("p25: %w", err)
	}

	p50, err := stats.Percentile(differences, 50)
	if err != nil {
		return empty, err
	}

	p75, err := stats.Percentile(differences, 75)
	if err != nil {
		return empty, err
	}

	p95, err := stats.Percentile(differences, 95)
	if err != nil {
		return empty, err
	}

	return types.ObservationStatsRow{
		Mean:     mean,
		P5:       p5,
		P25:      p25,
		P50:      p50,
		P75:      p75,
		P95:      p95,
		Min:      min,
		Max:      max,
		FiberWon: fiberWon / (fiberWon + blxrWon),
	}, err

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
		return empty, fmt.Errorf("mean: %w", err)
	}

	min, err := stats.Min(differences)
	if err != nil {
		return empty, fmt.Errorf("min: %w", err)
	}

	max, err := stats.Max(differences)
	if err != nil {
		return empty, fmt.Errorf("max: %w", err)
	}

	p1, err := stats.Percentile(differences, 1)
	if err != nil {
		return empty, fmt.Errorf("p1: %w", err)
	}

	p5, err := stats.Percentile(differences, 5)
	if err != nil {
		return empty, fmt.Errorf("p5: %w", err)
	}

	p10, err := stats.Percentile(differences, 10)
	if err != nil {
		return empty, fmt.Errorf("p10: %w", err)
	}

	p15, err := stats.Percentile(differences, 15)
	if err != nil {
		return empty, fmt.Errorf("p15: %w", err)
	}

	p20, err := stats.Percentile(differences, 20)
	if err != nil {
		return empty, fmt.Errorf("p20: %w", err)
	}

	p25, err := stats.Percentile(differences, 25)
	if err != nil {
		return empty, fmt.Errorf("p25: %w", err)
	}

	p30, err := stats.Percentile(differences, 30)
	if err != nil {
		return empty, fmt.Errorf("p25: %w", err)
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
