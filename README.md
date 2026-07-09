# GTSDB — Golang Dead Simple Timeseries Database

<p align="center">
  <img width="256" height="256" alt="GTSDB Hamster" src="https://github.com/user-attachments/assets/1eb70120-4748-443f-b358-0bad20b43004" />
</p>

<p align="center">
  <a href="https://github.com/abbychau/gtsdb/actions"><img alt="CI" src="https://github.com/abbychau/gtsdb/actions/workflows/go.yml/badge.svg" /></a>
  <a href="./docs/coverage-full.svg"><img alt="Coverage" src="./docs/coverage.svg" /></a>
  <a href="https://gtsdb-admin.vercel.app/"><img alt="Admin Tool" src="https://img.shields.io/badge/admin%20tool-demo-blue" /></a>
  <a href="https://github.com/abbychau/gtsdb/releases"><img alt="Release" src="https://img.shields.io/github/v/release/abbychau/gtsdb" /></a>
</p>

A simple, high-performance timeseries database for IoT and edge computing. **HTTP + TCP**, **sub-millisecond reads**, **233× faster than InfluxDB on writes**, **Gorilla compression**.

## Why GTSDB?

Traditional databases write to WAL, then copy to disk blocks, then update indexes — triple the IO. GTSDB takes a different approach:

```
Traditional:   WAL → Memory → Disk Blocks → Index
GTSDB:         WAL → Index (if needed) ─── read directly from WAL
```

- **WAL is the database.** No separate block storage. No double-write penalty.
- **Indexes are optional.** Created on-demand for fast time-range queries.
- **Result:** Less IO, less memory, simpler code, crazy fast.

### InfluxDB 2.9.1 vs GTSDB (i7-13700KF, Windows)

| Benchmark | GTSDB | InfluxDB 2.9.1 | GTSDB faster by |
|-----------|-------|----------------|-----------------|
| Write 10k sequential | **21.76 ms** | 5,070 ms | **233×** |
| Read latest data | **<1 ms** | 4.48 ms | sub-ms |
| Read 10k queries | **205 ms** | 967 ms | **4.7×** |
| Multi-Write 10k parallel | **51 ms** | 851 ms | **16.6×** |
| Storage (5,000 points) | **9.8 KB** | 78.1 KB | **7.98×** |

*10 sensors × 1,000 points each. GTSDB over TCP, InfluxDB over HTTP. See [benchmark repo](https://github.com/abbychau/gtsdb-benchmark).*

## Quick Start

```bash
# 1. Run the server
go run .

# 2. The server starts on TCP :5555 and HTTP :5556.
#    Check the logs for the auto-generated root token:
#    "Created default root user with token: abc123..."

# 3. Write data
curl -X POST http://localhost:5556/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"operation":"write","key":"sensor1","write":{"value":42.5}}'

# 4. Read latest 10 records
curl -X POST http://localhost:5556/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"operation":"read","key":"sensor1","read":{"lastx":10}}'
```

## Features

| Feature | Description |
|---------|-------------|
| **WAL-First Design** | WAL is the primary storage — no double-write, minimal IO |
| **Gorilla Compression** | Facebook's time-series algorithm — **8× smaller** files |
| **HTTP + TCP** | Dual protocol, identical JSON API |
| **Prometheus Metrics** | Built-in `/health` and `/metrics` endpoints |
| **Real-time PubSub** | Subscribe to keys, receive updates via SSE (NSQ-like) |
| **Batch Write** | Up to 10,000 points in a single call |
| **Export** | CSV or JSON export with time-range filtering |
| **Downsampling** | avg, sum, min, max, first, last, count, median (p50), p95, p99 |
| **~12 MB Memory** | Indexes on SSD, minimal RAM footprint |
| **Multi-User Auth** | Token-based authentication with namespaces |
| **Cross-Platform** | Windows, Linux, macOS — single binary |

## API Overview

### HTTP (port 5556)

All operations via `POST /` with `Authorization: Bearer <token>` header.

**Write a data point:**
```json
{
    "operation": "write",
    "key": "sensor1",
    "write": { "value": 42.5 }
}
```

**Read latest data:**
```json
{
    "operation": "read",
    "key": "sensor1",
    "read": { "lastx": 10 }
}
```

**Read with time range & downsampling:**
```json
{
    "operation": "read",
    "key": "sensor1",
    "read": {
        "start_timestamp": 1717965210,
        "end_timestamp": 1717965211,
        "downsampling": 3
    }
}
```

**Batch write (up to 10,000 points):**
```json
{
    "operation": "batch-write",
    "points": [
        { "key": "sensor1", "value": 42.5, "timestamp": 1717965210 },
        { "key": "sensor2", "value": 99.9, "timestamp": 1717965210 }
    ]
}
```

**Export as CSV:**
```json
{
    "operation": "export",
    "key": "sensor1",
    "export": { "format": "csv", "lastx": 100 }
}
```

| Operation | Description |
|-----------|-------------|
| `write` | Store a single data point |
| `batch-write` | Write up to 10,000 points across multiple keys |
| `read` / `multi-read` | Read by time range or last N records |
| `export` | Export data as CSV or JSON |
| `data-patch` | Bulk upsert (CSV or JSON array) |
| `deleteDataPoint` | Delete by value condition and time range |
| `ids` / `idswithcount` | List all keys |
| `subscribe` / `unsubscribe` | Real-time SSE notifications |
| `compact` | Compact WAL with Gorilla compression |
| `initkey` / `renamekey` / `deletekey` / `reloadkey` | Key management |
| `serverinfo` | Server diagnostics (uptime, memory, goroutines) |
| `adduser` / `resetkey` | User management (root only) |
| `flush` | Flush all data to disk |

**Health & Monitoring (no auth required):**
- `GET /health` — JSON health status
- `GET /metrics` — Prometheus metrics

### TCP (port 5555)

JSON-line protocol. Same operations as HTTP. See [TCP Protocol](docs/tcp-protocol.md).

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  TCP Client   │────▶│  TCP Server       │     │  Fanout Pub/Sub   │
│  (port 5555)  │     │  (goroutine)      │     │  (SSE push)       │
└──────────────┘     └──────┬───────────┘     └───────┬──────────┘
                            │                          │
┌──────────────┐     ┌──────▼───────────┐             │
│  HTTP Client  │────▶│  HTTP Server      │     ┌──────▼──────────┐
│  (port 5556)  │     │  (goroutine)      │────▶│  Handler Layer   │
└──────────────┘     └──────────────────┘     └──────┬──────────┘
                                                     │
                                            ┌────────▼────────┐
                                            │  Buffer Layer    │
                                            │  .aof / .idx    │
                                            │  .aof.gor       │
                                            └────────┬────────┘
                                                     │
                                            ┌────────▼────────┐
                                            │  File System     │
                                            └─────────────────┘
```

## Configuration

Default config: `gtsdb.ini`. Override with command line argument:

```bash
./gtsdb myconfig.ini
```

Example `gtsdb.ini`:
```ini
[listens]
tcp = :5555
http = :5556

[paths]
data_directory = data

[buffer]
buffer_size = 700
compaction_compression = true
```

See [Configuration Reference](docs/configuration.md) for all options.

## Benchmarks

### Main Benchmark (50% read / 50% write, TCP)

```bash
go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s
```

```
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
BenchmarkMain-24    26396    135241 ns/op    4249 B/op    5 allocs/op
```

This benchmark runs 50% read and 50% write operations to 100 different keys over a single TCP connection.

### Concurrent Data Structures

```bash
make Benchmark
```

| Operation | Performance |
|-----------|------------|
| Sequential Store | 477 ns/op |
| Sequential Load | 209 ns/op |
| Concurrent Store | 256 ns/op |
| Concurrent Load | 216 ns/op |
| Concurrent Mixed | 427 ns/op |
| Set Contains | 12.4 ns/op |

### Full Comparison

See [gtsdb-benchmark](https://github.com/abbychau/gtsdb-benchmark) for GTSDB vs InfluxDB comparison benchmarks.

## Documentation

| Document | Description |
|----------|-------------|
| [OpenAPI Specification](docs/openapi.yaml) | Complete HTTP API reference (OpenAPI 3.0) |
| [API Examples](docs/api.http) | Runnable REST Client examples for VS Code |
| [TCP Protocol](docs/tcp-protocol.md) | TCP interface protocol and examples |
| [Configuration Reference](docs/configuration.md) | All config file options |
| [Operations Guide](docs/operations.md) | Complete operations reference |
| [Cloud User Guide](GTSDB_CLOUD_GUIDE.md) | Multi-user authentication and tenancy |

## Development

```bash
# Build & run
go run .
go build .

# Tests
go test ./...
go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1
make GenerateTest          # Coverage report (HTML + SVG badge)

# Code quality
golangci-lint run
make lint-fix

# Benchmarks
make Benchmark             # Concurrent data structures
```

## License

MIT
