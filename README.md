# Dead Simple Timeseries Database

## Run / Compile

```bash
go run .
go build .
```

## Usage

```bash
# Start the server
./gtsdb

# Connect to the server(nc/socat/telnet/...)
# Write data
# <key>,<timestamp>,<value>
write,sensor1,1717965210,36.5
write,sensor2,1717965210,36.5
write,sensor1,1717965211,36.6
write,myhomeshit,1717965211,36.6
# Read data
# <key>,<start-timestamp>,<end-timestamp>,<downsampling>
# for downsampling, 0 or 1 means no downsampling, 2 means take average for every 2 seconds, 3 means take average for every 3 seconds and so on
read,sensor1,1717965210,1717965211,3
read,sensor2,1717965210,1717965211,3600 # 1 hour
read,myhomeshit,1717965210,1717965211,7200 # 2 hours
read,sensor1,-1 # last data point
# Output for input <sensor2,0,1817969274,3600>
# <key>,<timestamp>,<value>|<key>,<timestamp>,<value>|...\n
sensor2,1717971018,51.14|sensor2,1717974618,48.96|sensor2,1717978218,49.60|sensor2,1717981819,49.94|sensor2,1717985419,50.22|sensor2,1717989019,50.43|sensor2,1717992619,50.36|sensor2,1717996219,50.23|sensor2,1717999819,50.04|sensor2,1718003419,49.08|sensor2,1718007019,50.67|sensor2,1718010619,50.05|sensor2,1718014219,50.25|sensor2,1718017819,50.21|sensor2,1718021419,49.92|sensor2,1718025019,50.03|sensor2,1718028619,49.92|sensor2,1718032219,51.40|sensor2,1718035819,49.71|sensor2,1718039419,49.58|sensor2,1718043019,50.20|sensor2,1718046619,50.34|sensor2,1718050219,49.23|sensor2,1718053819,49.90|sensor2,1718057419,50.14|sensor2,1718061019,50.43|sensor2,1718064619,49.91|sensor2,1718068219,51.11|sensor2,1718071819,49.15|sensor2,1718075419,50.90|sensor2,1718079019,50.08|sensor2,1718082619,49.83|sensor2,1718086219,49.42|sensor2,1718089819,50.61|sensor2,1718093419,49.19|sensor2,1718097019,50.04|sensor2,1718100619,48.97|sensor2,1718104219,49.24|sensor2,1718107819,49.03|sensor2,1718111419,49.79|sensor2,1718115019,50.01|sensor2,1718118619,51.46|sensor2,1718122219,49.63|sensor2,1718125819,51.18|sensor2,1718129419,49.45|sensor2,1718133019,51.50|sensor2,1718136619,49.67|sensor2,1718140219,50.61|sensor2,1718143819,49.43|sensor2,1718147419,51.43|sensor2,1718151019,50.35|sensor2,1718154619,49.66|sensor2,1718158219,51.05|sensor2,1718161819,49.52|sensor2,1718165419,50.39|sensor2,1718169019,49.73|sensor2,1718172619,51.45|sensor2,1718176219,49.95|sensor2,1718179819,50.27|sensor2,1718183419,49.77|sensor2,1718187019,49.89|sensor2,1718190619,49.61|sensor2,1718194219,50.85|sensor2,1718197819,50.84|sensor2,1718201419,49.98|sensor2,1718205019,50.69|sensor2,1718208619,49.87|sensor2,1718212219,50.41|sensor2,1718215819,51.22|sensor2,1718219419,51.35|sensor2,1718223019,51.53|sensor2,1718226619,49.94|sensor2,1718230219,50.19|sensor2,1718233819,50.91|sensor2,1718237419,49.61|sensor2,1718241019,50.62|sensor2,1718244619,48.86|sensor2,1718248219,49.18|sensor2,1718251819,49.97|sensor2,1718255419,49.69

# Subscribe to a key
subscribe,sensor1
# Unsubscribe to a key
unsubscribe,sensor1
```

## Performance

### Database Performance

[Benchmark](https://github.com/abbychau/gtsdb/blob/main/main_test.go#L65)

```
Version: 2024 11 11
goos: windows
goarch: amd64
pkg: gtsdb
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
BenchmarkMain-24          705349             17267 ns/op
PASS
ok      gtsdb   12.466s
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
go test ./... -skip=TestMain -coverprofile=docs/coverage
go tool cover -html docs/coverage -o docs/coverage.html
```

## License

MIT
