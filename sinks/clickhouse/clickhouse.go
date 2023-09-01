package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"

	"github.com/chainbound/fiber-benchmarks/log"
	"github.com/chainbound/fiber-benchmarks/sinks"
	"github.com/chainbound/fiber-benchmarks/types"
)

type ClickhouseConfig struct {
	Endpoint string
	DB       string
	Username string
	Password string
}

type ClickhouseSink struct {
	cfg    *ClickhouseConfig
	chConn driver.Conn
	log    zerolog.Logger

	// Batching of rows
	observationRowBatch driver.Batch
	statsBatch          driver.Batch

	ty sinks.InitType

	blockObservationRowBatch driver.Batch
	blockStatsBatch          driver.Batch
}

func NewClickhouseClient(cfg *ClickhouseConfig) (*ClickhouseSink, error) {
	log := log.NewLogger("clickhouse")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Endpoint},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debugf: func(format string, v ...interface{}) {
			log.Debug().Str("module", "clickhouse").Msgf(format, v)
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	if err != nil {
		return nil, err
	}

	return &ClickhouseSink{
		cfg:    cfg,
		chConn: conn,
		log:    log,
	}, nil
}

// Init creates the database and tables if they don't exist, and also prepares the batch statements
func (c *ClickhouseSink) Init(ty sinks.InitType) error {
	var err error

	c.ty = ty

	c.log.Info().Str("endpoint", c.cfg.Endpoint).Str("type", string(ty)).Msg("Setting up Clickhouse database")
	if err := c.chConn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.cfg.DB)); err != nil {
		return err
	}

	c.log.Info().Str("db", c.cfg.DB).Msg("Database created")

	switch ty {
	case sinks.Transactions:
		if err := c.chConn.Exec(context.Background(), ConfirmedObservationsDDL(c.cfg.DB)); err != nil {
			return err
		}

		if err := c.chConn.Exec(context.Background(), ObservationStatsDDL(c.cfg.DB)); err != nil {
			return err
		}

		c.log.Info().Msg("Tables created")

		for {
			c.observationRowBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.confirmed_observations", c.cfg.DB))
			if err != nil {
				c.log.Error().Err(err).Msg("preparing batch failed, retrying...")
			} else {
				break
			}
		}

		for {
			c.statsBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.observation_stats", c.cfg.DB))
			if err != nil {
				c.log.Error().Err(err).Msg("preparing batch failed, retrying...")
			} else {
				break
			}
		}

		c.log.Info().Msg("Prepared batches")
	case sinks.Blocks:
		if err := c.chConn.Exec(context.Background(), ConfirmedBlockObservationsDDL(c.cfg.DB)); err != nil {
			return err
		}

		if err := c.chConn.Exec(context.Background(), BlockObservationStatsDDL(c.cfg.DB)); err != nil {
			return err
		}

		c.log.Info().Msg("Tables created")

		for {
			c.blockObservationRowBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.confirmed_block_observations", c.cfg.DB))
			if err != nil {
				c.log.Error().Err(err).Msg("preparing batch failed, retrying...")
			} else {
				break
			}
		}

		for {
			c.blockStatsBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.block_observation_stats", c.cfg.DB))
			if err != nil {
				c.log.Error().Err(err).Msg("preparing batch failed, retrying...")
			} else {
				break
			}
		}
	}

	return nil
}

func (c *ClickhouseSink) Close() error {
	return nil
}

// These rows will be added to a batch. To flush the batch, call Flush()
func (c *ClickhouseSink) RecordObservationRow(row *types.ConfirmedObservationRow) error {
	return c.observationRowBatch.AppendStruct(row)
}

// These rows will be added to a batch. To flush the batch, call Flush()
func (c *ClickhouseSink) RecordBlockObservationRow(row *types.BlockObservationRow) error {
	return c.blockObservationRowBatch.AppendStruct(row)
}

func (c *ClickhouseSink) RecordStats(stats *types.ObservationStatsRow) error {
	return c.statsBatch.AppendStruct(stats)
}

func (c *ClickhouseSink) RecordBlockStats(stats *types.ObservationStatsRow) error {
	return c.blockStatsBatch.AppendStruct(stats)
}

// Flushes the batches concurrently. This is a blocking call that can take a while.
func (c *ClickhouseSink) Flush() error {
	var err error
	start := time.Now()

	c.log.Debug().Int("batch_size", c.observationRowBatch.Rows()).Msg("Flushing batches...")
	var wg sync.WaitGroup
	wg.Add(2)

	switch c.ty {
	case sinks.Transactions:
		go func() {
			defer wg.Done()
			// Infinite retries for now
			for {
				if c.observationRowBatch.IsSent() {
					break
				}

				if err := c.observationRowBatch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending observation batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Debug().Str("took", time.Since(start).String()).Msg("Inserted observation batch")

			// Reset batch
			for {
				c.observationRowBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.confirmed_observations", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing batch failed, retrying")
				} else {
					break
				}
			}
		}()

		go func() {
			defer wg.Done()
			// Infinite retries for now
			for {
				if c.statsBatch.IsSent() {
					break
				}

				if err := c.statsBatch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending stats batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Debug().Str("took", time.Since(start).String()).Msg("Inserted stats batch")

			// Reset batch
			for {
				c.statsBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.observation_stats", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing batch failed, retrying")
				} else {
					break
				}
			}
		}()
	case sinks.Blocks:
		go func() {
			defer wg.Done()
			// Infinite retries for now
			for {
				if c.blockObservationRowBatch.IsSent() {
					break
				}

				if err := c.blockObservationRowBatch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending observation batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Debug().Str("took", time.Since(start).String()).Msg("Inserted observation batch")

			// Reset batch
			for {
				c.blockObservationRowBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.confirmed_block_observations", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing batch failed, retrying")
				} else {
					break
				}
			}
		}()

		go func() {
			defer wg.Done()
			// Infinite retries for now
			for {
				if c.blockStatsBatch.IsSent() {
					break
				}

				if err := c.blockStatsBatch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending stats batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Debug().Str("took", time.Since(start).String()).Msg("Inserted stats batch")

			// Reset batch
			for {
				c.blockStatsBatch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.block_observation_stats", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing batch failed, retrying")
				} else {
					break
				}
			}
		}()
	}

	wg.Wait()
	c.log.Debug().Msg("Succesfully flushed batches")
	return nil
}
