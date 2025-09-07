GenerateTest:
	go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1
	go tool cover -html docs/coverage -o docs/coverage.html
	php docs/cove-parse.php
	start .\docs\coverage.html
BenchmarkTODO:
	go run main.go
	go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s
Benchmark:
	go test -benchmem -run=^$ -bench ^Bench gtsdb/concurrent -benchtime=5s
lint:
	golangci-lint run
lint-fix:
	golangci-lint run --fix
deploy:
	git pull
	pm2 stop 0
	go build .
	pm2 start pm2.config.json