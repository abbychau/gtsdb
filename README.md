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
BenchmarkMain-24         1218360             19854 ns/op            4245 B/op          5 allocs/op
```


Explanation:
- This benchmark does 50% read and 50% write operations to 100 different keys(devices).
- It performs 249386 operations in 22.267 seconds. The operations include read and write operations.


### Concurrent Internals Performance

- Run: `go test -benchmem -run=^$ -bench ^Bench gtsdb/concurrent -benchtime=5s`

```
goos: windows
goarch: amd64
pkg: gtsdb/concurrent
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
BenchmarkHashMap/Put-24                 183965112               30.11 ns/op            4 B/op          1 allocs/op
BenchmarkHashMap/Get-24                 325839816               17.06 ns/op            0 B/op          0 allocs/op
BenchmarkHashMap/ConcurrentPut-24               76154018               101.0 ns/op             4 B/op          0 allocs/op
BenchmarkHashMap/ConcurrentGet-24               179002152               33.92 ns/op            0 B/op          0 allocs/op
BenchmarkHashMap/MixedReadWrite-24              100000000               56.78 ns/op            1 B/op          0 allocs/op
BenchmarkSet_Add-24                             83288680               135.5 ns/op            38 B/op          0 allocs/op
BenchmarkSet_Contains-24                        481914471               12.54 ns/op            0 B/op          0 allocs/op
BenchmarkSet_ConcurrentAdd-24                   43283078               163.3 ns/op            37 B/op          0 allocs/op
PASS
ok      gtsdb/concurrent        62.763s
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
