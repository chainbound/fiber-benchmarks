# Fiber vs. Bloxroute Benchmarks

This repo contains a Go program to help you benchmark and compare Fiber vs. Bloxroute transaction streams.

## Requirements
* Golang: https://go.dev/doc/install

## Usage
To run, copy `.env.example` to `.env` and fill in the necessary information. 
Then run with
```
go run .
```
The program will run for `DURATION` time, after
which it will print out the results of the benchmarks.

Example output:
```
Running benchmark for 10m0s ...
Bloxroute endpoint: wss://uk.eth.blxrbdn.com/ws
Fiber endpoint: fiberapi.io:8080


========== STATS =============
Mean difference: 174.61ms
Median difference: 2.00ms
Max difference: 335986.00ms
Min difference: -3713.00ms
Stdev: 6159.43ms

========== RESULT =============
Fiber won 56.79% of the time
```
It will also write all the individual transactions to `benchmarks.csv`, along with the difference in ms.
Positive means Fiber was faster.