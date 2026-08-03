GenerateTest:
	go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1
	go tool cover -html docs/coverage -o docs/coverage.html
	go run docs/coverage_badge.go docs/coverage
	start .\docs\coverage.html
submodules:
	git submodule update --init --recursive
coverage:
	go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1
	go tool cover -html docs/coverage -o docs/coverage.html
	go run docs/coverage_badge.go docs/coverage
BenchmarkTODO:
	go run main.go
	go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s
Benchmark:
	go test -benchmem -run=^$ -bench ^Bench gtsdb/concurrent -benchtime=5s
lint:
	golangci-lint run
lint-fix:
	golangci-lint run --fix
integration-test:
	go test -tags=integration -v -count=1 -timeout=60s .

build-desktop:
	go build -ldflags="-s -w" -trimpath -o "$(HOME)/Desktop/gtsdb.exe" .

build-bench:
	go build -ldflags="-s -w" -trimpath -o "$(HOME)/Desktop/gtsdb_bench.exe" ./benchmark

deploy:
	git pull
	pm2 del gtsdb-patch-remove-data
	pm2 del gtsdb
	go build .
	pm2 start pm2.config.json
	pm2 save