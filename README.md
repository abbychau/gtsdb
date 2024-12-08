# Dead Simple Timeseries Database

[![coverage](/docs/coverage.png)](./docs/coverage-full.png)

## Run / Compile

```bash
go run .
go build .
```

## Usage

### HTTP API (POST to :5556)

```json
# Write data
# POST /
{
    "operation":"write",
    "Write": {
        "ID" : "a_sensor1",
        "Value": 32242424243333333333.3333
    }
}

# Read data
# POST /
{
    "operation":"read",
    "Read": {
        "ID" : "a_sensor1",
        "start_timestamp": 1717965210,
        "end_timestamp": 1717965211,
        "downsampling": 3
    }
}
{
    "operation":"read",
    "Read": {
        "ID" : "a_sensor1",
        "lastx": 1
    }
}

# Subscribe to a key
# POST /
{
  "operation": "subscribe",
  "key": "sensor1"
}

# Unsubscribe to a key
# POST /
{
  "operation": "unsubscribe",
  "key": "sensor1"
}
```

## Performance

### Database Performance

[Benchmark](https://github.com/abbychau/gtsdb/blob/main/main_test.go#L65)

- Run: `go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s`

```
goos: windows
goarch: amd64
pkg: gtsdb
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
BenchmarkMain-24          311648             19172 ns/op            4245 B/op            5 allocs/op
PASS
ok      gtsdb   6.439s
```


Explanation:
- This benchmark does 50% read and 50% write operations to 100 different keys(devices).
- It performs 249386 operations in 6 seconds. The operations include read and write operations.


### Concurrent Internals Performance

- Run: `make benchmark`

```
goos: windows
goarch: amd64
pkg: gtsdb/concurrent
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
BenchmarkMap/Sequential_Store-24        12363549               477.2 ns/o     154 B/op           6 allocs/op
BenchmarkMap/Sequential_Load-24         44461207               209.5 ns/o       7 B/op           0 allocs/op
BenchmarkMap/Concurrent_Store-24        36248278               256.2 ns/o      56 B/op           4 allocs/op
BenchmarkMap/Concurrent_Load-24         63952998               216.3 ns/o       7 B/op           0 allocs/op
BenchmarkMap/Concurrent_Mixed-24        18266863               427.2 ns/o      94 B/op           3 allocs/op
BenchmarkSet_Add-24                     60995383               208.0 ns/o      52 B/op           0 allocs/op
BenchmarkSet_Contains-24                484518542               12.36 ns/op              0 B/op          0 allocs/op
BenchmarkSet_ConcurrentAdd-24           43510735               213.0 ns/o      37 B/op           0 allocs/op
PASS
ok      gtsdb/concurrent        147.859s
```




## Done/Todo/Feature list

- [x] Simple Indexing so to avoid full file scanning
- [x] Downsampling data
- [x] TCP Server
- [x] Tests
- [x] HTTP Server (REST API)
- [x] More Downsampling options(like sum, min, max, etc.)
- [x] Do errcheck Handling
- [x] Subscription and then streaming (cmd: `subscribe,sensor1`, `unsubscribe,sensor1`)
- [x] Buffering data in memory before writing to disk, this is to serve recent data faster and enhance write performance
- [x] 80% test coverage ([Report](./docs/coverage.html))

## Generate Test Coverage Report

```bash
go test ./... -coverprofile=docs/coverage -p 1
go tool cover -html docs/coverage -o docs/coverage.html
start .\docs\coverage.html
```

## License

MIT
