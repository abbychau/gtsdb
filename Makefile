GenerateTest:
	go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1
	go tool cover -html docs/coverage -o docs/coverage.html
	php docs/cove-parse.php
	start .\docs\coverage.html
Benchmark:
	go run main.go
	go test -benchmem -run=^$ -bench ^BenchmarkMain$ -benchtime=5s
lint:
	golangci-lint run