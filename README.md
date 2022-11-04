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