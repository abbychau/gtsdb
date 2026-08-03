# Development Tools

Ad-hoc tools used during development and data repair. All Go files have
`//go:build ignore`, so they never compile into the main binary — run them
with `go run tools/<file>.go`.

| Tool | Purpose |
|------|---------|
| `compare_bench.go` | GTSDB (HTTP keep-alive) vs InfluxDB benchmark — write/read/batch, 10k ops each. Prints a results table and `PAGE DATA` for the homepage site. |
| `compare_bench2.go` | Same comparison, but GTSDB over **TCP** (connection reuse) instead of HTTP. Kept as a variant for transport comparison. |
| `page_bench.go` | GTSDB-only TCP benchmark (10k ops) producing the `PAGE DATA` figures for the homepage. |
| `e2e_client.go` | End-to-end client test against a running server (auth + write/read via `users.json` root token). |
| `test_concurrent_writes.go` | Concurrent write stress test (10 goroutines × 100 writes). |
| `repair.go` | Scan/fix corrupted `.aof`/`.idx` files. Commands: `scan`, `fix`, `fix --no-backup`. |
| `patch_remove_data.go` | Delete data points by value/time criteria across the data directory. |
| `remove_value_gt_0_5.go` | Remove data points with value > 0.5 (creates `.backup.<ts>` files first). |
| `check_updated_file.go` | Verify a file after a repair/update operation. |
| `patch_data.sh` | Bash script to patch specific customer keys (e.g. remove a date range). |

> These are one-off utilities; they are not covered by tests or CI.
