GenerateTest:
	go test ./... -skip=TestMain -coverprofile=docs/coverage -p 1
	go tool cover -html docs/coverage -o docs/coverage.html
	start .\docs\coverage.html