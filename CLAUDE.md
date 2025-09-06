# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GTSDB is a Golang-based Dead Simple Timeseries Database that uses a fundamentally different approach from traditional databases. Instead of using WAL + disk blocks, it relies primarily on WAL for everything, saving IO and memory usage. The database supports both HTTP and TCP protocols for client communication.

## Architecture

### Core Components
- **main.go**: Entry point, starts both HTTP and TCP servers with graceful shutdown
- **handlers/**: HTTP and TCP request handlers with common operation logic
- **buffer/**: Data buffering and persistence layer with external/internal operations
- **fanout/**: Publisher-subscriber notification system for real-time updates
- **utils/**: Shared utilities including configuration and data directory management
- **concurrent/**: Thread-safe data structures (maps, sets, LRU cache)
- **synchronous/**: Ring buffer implementation
- **models/**: Data structure definitions

### Key Design Principles
- WAL-first approach: Write data to WAL, create indexes conditionally, read from WAL and indexes
- Dual protocol support: HTTP (port :5556) and TCP (port :5555) servers
- Real-time subscriptions via fanout mechanism
- High concurrency with custom thread-safe data structures

## Commands

### Build and Run
```bash
go run .                    # Run the database server
go build .                  # Build binary
```

### Testing
```bash
go test ./...               # Run all tests
go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1  # Run tests with coverage
make GenerateTest           # Generate test coverage report (includes HTML output)
```

### Benchmarking
```bash
go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s     # Main benchmark (50% read/write ops)
make Benchmark              # Concurrent data structures benchmarks
```

### Code Quality
```bash
golangci-lint run           # Run linter
golangci-lint run --fix     # Run linter with auto-fix
make lint                   # Run linter via Makefile
make lint-fix               # Run linter with auto-fix via Makefile
```

## Configuration

The application uses INI files for configuration (default: `gtsdb.ini`):
- `[listens]` section: TCP and HTTP listen addresses
- `[paths]` section: Data directory path
- Configuration file can be specified as command line argument: `./gtsdb custom.ini`

## API Operations

The database supports these operations via HTTP POST to /:
- `write`: Store timeseries data points
- `read`: Retrieve data with timestamp ranges or last X points
- `ids`: Get all available keys/sensors
- `subscribe`/`unsubscribe`: Real-time notifications
- `data-patch`: Bulk data insertion

## Testing Strategy

The codebase maintains high test coverage with:
- Unit tests for all major components
- Integration tests in `main_test.go`
- Benchmark tests for performance validation
- Concurrent data structure specific tests
- End-to-end HTTP/TCP handler tests