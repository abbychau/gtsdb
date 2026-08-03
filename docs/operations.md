# GTSDB Operations Guide

## Overview

GTSDB supports the following operations via both HTTP (POST /) and TCP protocols.

## Operations Table

| Operation | Auth Required | Key Required | Description |
|-----------|:---:|:---:|-------------|
| `write` | ✓ | ✓ | Store a single data point |
| `batch-write` | ✓ | ✗¹ | Write multiple data points across keys |
| `read` | ✓ | ✓ | Read data points by time range or last N |
| `multi-read` | ✓ | ✗² | Read data points for multiple keys |
| `export` | ✓ | ✓ | Export data as CSV or JSON |
| `data-patch` | ✓ | ✓ | Bulk insert/upsert (CSV or JSON array) |
| `deleteDataPoint` | ✓ | ✓ | Delete data points by value or time range |
| `ids` | ✓ | ✗ | List all accessible keys |
| `idswithcount` | ✓ | ✗ | List keys with data point counts |
| `initkey` | ✓ | ✓ | Initialize a new key |
| `renamekey` | ✓ | ✓ | Rename a key |
| `deletekey` | ✓ | ✓ | Delete a key and all its data |
| `reloadkey` | ✓ | ✓ | Reload a key from disk |
| `compact` | ✓ | ✓ | Compact WAL file for a key |
| `subscribe` | ✓ | ✓ | Subscribe to real-time updates (SSE) |
| `unsubscribe` | ✓ | ✓ | Unsubscribe from real-time updates |
| `flush` | ✓ | ✗ | Flush all data to disk |
| `serverinfo` | ✓ | ✗ | Get server information and metrics |

¹ `batch-write` uses `points[]` array instead of single `key`
² `multi-read` uses `keys[]` array instead of single `key`

## Administrative Operations (Root Only)

| Operation | Auth Required | Description |
|-----------|:---:|-------------|
| `adduser` | ✓ (root) | Create a new user with a generated token |
| `resetkey` | ✓ (root) | Reset a user's authentication token |

## Data Flow

### Write Path
```
Client → HTTP/TCP → Handler (validation) → StoreDataPointBuffer →
  acquireFileHandle → write to .aof → updateIndex → dataFile.Sync()
```

### Read Path
```
Client → HTTP/TCP → Handler → ReadDataPoints/ReadLastDataPoints →
  [Ring Buffer (if enabled)] → [findStartOffset via .idx] → [ReadAt from .aof] →
  [Downsampling] → Response
```

### Patch Path
```
Client → HTTP/TCP → Handler → PatchDataPoints →
  1. Fast path (append): All new points newer → append to .aof
  2. Fast path (overwrite): Single point → rewrite 16 bytes at offset
  3. Slow path (merge): Read all → merge sort → rewrite full file
```

## File Storage

### .aof files (Append-Only File)
- Binary format: each record is **16 bytes** (int64 timestamp + float64 value)
- Little-endian byte order
- Always appended to (never modified except by compaction/patch)
- Unlimited file size (compaction recommended periodically)

### .idx files (Sparse Index)
- Created every **5000 records** (configurable via `indexInterval` constant)
- Binary format: int64 timestamp + int64 byte offset
- Enables O(log n) seeks for time-range queries (scan from the last index entry ≤ start time)
- Rebuilt on compaction

### .aof.gor files (Gorilla Compressed WAL)
- Created when `compaction_compression = true` during compaction
- Blocks of 5000 points compressed with the Gorilla algorithm
- Reads prefer the compressed file and fall back to `.aof` if unavailable

### .aof.gor.idx files (Compressed WAL Index)
- Companion index for `.aof.gor` (one entry per compressed block)
- Binary format: int64 timestamp + int64 byte offset into `.aof.gor`
- Separated from `.idx` because offsets point into the compressed file, not `.aof`

## Caching

| Cache | Scope | Purpose |
|-------|-------|---------|
| Last Value | Per key | O(1) access to most recent value |
| Last Timestamp | Per key | O(1) access to most recent timestamp |
| File Handle LRU | Global | Limits open file descriptors (default: 700) |
| Ring Buffer | Per key | In-memory window (currently disabled — cacheSize = 0) |

## Security

### Multi-Tenancy
- Each user's data is isolated in `{username}/` subdirectories
- Users can only access their own data plus `root/` directory
- Root user can access all data

### Input Validation
- **Path traversal**: Keys containing `..` are rejected
- **Timestamp range**: Only timestamps between year 2000-2100 are accepted
- **Data size**: `data-patch` payload limited to 10MB
- **Batch size**: `batch-write` limited to 10,000 points

## Monitoring

- **HTTP**: `GET /health` — JSON health status (no auth)
- **HTTP**: `GET /metrics` — Prometheus metrics (no auth)
- **Operation**: `serverinfo` — Server diagnostics (auth required)
