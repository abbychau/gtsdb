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
sensor1,1717965210,36.5
sensor2,1717965210,36.5
sensor1,1717965211,36.6
myhomeshit,1717965211,36.6
# Read data
# <key>,<start-timestamp>,<end-timestamp>,<downsampling>
# for downsampling, 0 or 1 means no downsampling, 2 means take average for every 2 seconds, 3 means take average for every 3 seconds and so on
sensor1,1717965210,1717965211,3
sensor2,1717965210,1717965211,3600 # 1 hour
myhomeshit,1717965210,1717965211,7200 # 2 hours
# Output for input <sensor2,0,1817969274,3600>
# <key>,<timestamp>,<value>|<key>,<timestamp>,<value>|...\n
sensor2,1717971018,51.14|sensor2,1717974618,48.96|sensor2,1717978218,49.60|sensor2,1717981819,49.94|sensor2,1717985419,50.22|sensor2,1717989019,50.43|sensor2,1717992619,50.36|sensor2,1717996219,50.23|sensor2,1717999819,50.04|sensor2,1718003419,49.08|sensor2,1718007019,50.67|sensor2,1718010619,50.05|sensor2,1718014219,50.25|sensor2,1718017819,50.21|sensor2,1718021419,49.92|sensor2,1718025019,50.03|sensor2,1718028619,49.92|sensor2,1718032219,51.40|sensor2,1718035819,49.71|sensor2,1718039419,49.58|sensor2,1718043019,50.20|sensor2,1718046619,50.34|sensor2,1718050219,49.23|sensor2,1718053819,49.90|sensor2,1718057419,50.14|sensor2,1718061019,50.43|sensor2,1718064619,49.91|sensor2,1718068219,51.11|sensor2,1718071819,49.15|sensor2,1718075419,50.90|sensor2,1718079019,50.08|sensor2,1718082619,49.83|sensor2,1718086219,49.42|sensor2,1718089819,50.61|sensor2,1718093419,49.19|sensor2,1718097019,50.04|sensor2,1718100619,48.97|sensor2,1718104219,49.24|sensor2,1718107819,49.03|sensor2,1718111419,49.79|sensor2,1718115019,50.01|sensor2,1718118619,51.46|sensor2,1718122219,49.63|sensor2,1718125819,51.18|sensor2,1718129419,49.45|sensor2,1718133019,51.50|sensor2,1718136619,49.67|sensor2,1718140219,50.61|sensor2,1718143819,49.43|sensor2,1718147419,51.43|sensor2,1718151019,50.35|sensor2,1718154619,49.66|sensor2,1718158219,51.05|sensor2,1718161819,49.52|sensor2,1718165419,50.39|sensor2,1718169019,49.73|sensor2,1718172619,51.45|sensor2,1718176219,49.95|sensor2,1718179819,50.27|sensor2,1718183419,49.77|sensor2,1718187019,49.89|sensor2,1718190619,49.61|sensor2,1718194219,50.85|sensor2,1718197819,50.84|sensor2,1718201419,49.98|sensor2,1718205019,50.69|sensor2,1718208619,49.87|sensor2,1718212219,50.41|sensor2,1718215819,51.22|sensor2,1718219419,51.35|sensor2,1718223019,51.53|sensor2,1718226619,49.94|sensor2,1718230219,50.19|sensor2,1718233819,50.91|sensor2,1718237419,49.61|sensor2,1718241019,50.62|sensor2,1718244619,48.86|sensor2,1718248219,49.18|sensor2,1718251819,49.97|sensor2,1718255419,49.69
```

## Performance

[Benchmark](./main_test.go)

```
goos: windows
goarch: amd64
pkg: gtsdb
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
BenchmarkMain-24          249386             47291 ns/op
PASS
ok      gtsdb   22.267s
```

Explanation:
- This benchmark does 50% read and 50% write operations to 100 different keys(devices).
- The benchmark is run on a 13th Gen Intel(R) Core(TM) i7-13700KF CPU.
- It performs 249386 operations in 22.267 seconds. The operations include read and write operations.

## Todo

- [x] Simple Indexing so to avoid full file scanning
- [x] Downsampling data
- [x] TCP Server
- [x] Tests
- [ ] HTTP Server (REST API)
- [ ] More Downsampling options(like sum, min, max, etc.)

## License

MIT