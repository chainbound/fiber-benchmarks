# Fiber vs. Bloxroute Benchmarks

This repo contains a Go program to help you benchmark and compare Fiber vs. Bloxroute transaction streams.

## Requirements

- Golang: https://go.dev/doc/install

## Usage

Options:

```text
NAME:
   fiber-benchmark - Benchmark Fiber against other data sources

USAGE:
   fiber-benchmark [global options] command [command options] [arguments...]

COMMANDS:
   transactions  Benchmark transaction streams
   blocks        Benchmark block streams
   help, h       Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --fiber-endpoint     value       Fiber API endpoint
   --fiber-key          value       Fiber API key
   --blxr-endpoint      value       Bloxroute API endpoint
   --blxr-key           value       Bloxroute API key
   --interval           value       Duration of each interval (default: 0s)
   --interval-count     value       Number of intervals to run (default: 1)
   --log-file           value       File to save detailed logs
   --fiber-only                     Only benchmark between two Fiber endpoints (default: false)
   --help, -h                       show help
```

### Transactions

Example:

```bash
go run . --fiber-endpoint $FIBER_ENDPOINT --fiber-key $FIBER_KEY \
    --blxr-endpoint $BLXR_WS_ENDPOINT --blxr-key $BLXR_KEY --interval 20s --log-file benchmarks.csv transactions
```

### Blocks

WIP
