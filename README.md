# Golang Dead Simple Timeseries Database

[![coverage](/docs/coverage.png)](./docs/coverage-full.png)

## Introduction

This is a simple timeseries database written in Golang. It is designed to be simple and easy to use. It is designed to be used in IoT and other applications where you need to store timeseries data.

Simple is not only in terms of usage but also in terms of a fundamentally new ways of storing data.

Let's compare it with other databases, usually, a database do these things:
1. Write data to WAL (Write Ahead Log) into disk
2. Stream IO to memory
3. Do some internal processing like indexing and caching
4. Write data to disk by blocks, fsync to disk for durability
5. Update or erase WAL cursor (maybe an offset or ID)
6. When disaster strikes, read data from disk and replay WAL to recover data
7. Read data from index and file blocks

I think in a different way. I think, if WAL is anyway needed for a production grade database, why not just use WAL for everything:
1. Write data to WAL
2. Create indexes conditionally
3. Read data from WAL and indexes

This way, we saves a lot of IO and MEMORY USAGE. That's why I call it a dead simple timeseries database. I don't scarifice durability. And reading from Index is still O(log n).

I don't want to say this is perfect. So here is the tradeoff: "GTSDB needs more file handles than other databases" because it keeps WAL open for reading and writing, but I think it's worth it, especially for weak hardwares. And more importantly, we can make a LRU of file handles when it is REALLY needed.

You can see the performance of this database in the performance section. (It is way better than you can expect by this design even it's just in Golang)

Am I going to write it in Rust/C++/Zig? Yes. I love Rust but GTSDB is still highly experimental and I want to make it more stable before I write it in Rust. I still have a lot of ideas to implement and they sometimes contradict each other. So... I'm still in Golang. Even so, I made it in very high code coverage and with some different internal and end-to-end benchmarks. If you want to use it in production, you can use it. Just make sure to checkout a git hash that is with a green tick in the CI. 


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
        "key" : "a_sensor1",
        "Value": 32242424243333333333.3333
    }
}

# Read data
# POST /
{
    "operation":"read",
    "Read": {
        "key" : "a_sensor1",
        "start_timestamp": 1717965210,
        "end_timestamp": 1717965211,
        "downsampling": 3
    }
}
{
    "operation":"read",
    "Read": {
        "key" : "a_sensor1",
        "lastx": 1
    }
}

# Get all keys
# POST /
{
    "operation":"ids"
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

# Patch data points for a key
# POST /
{
    "operation": "data-patch",
    "key": "sensor1",
    "data": "1717965210,123.45\n1717965211,123.46\n1717965212,123.47"
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
BenchmarkMain-24         6158864             19707 ns/op         4245 B/op           5 allocs/op
PASS
ok      gtsdb   141.340s
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


## Generate Test Coverage Report

```bash
make GenerateTest
```

## License

MIT
