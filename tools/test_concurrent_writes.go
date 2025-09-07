//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"sync"
	"time"
	"gtsdb/handlers"
	"gtsdb/models"
	"gtsdb/utils"
)

func main() {
	utils.DataDir = "data"
	
	key := "concurrent_test"
	numGoroutines := 10
	writesPerGoroutine := 100
	
	fmt.Printf("Testing concurrent writes: %d goroutines, %d writes each\n", numGoroutines, writesPerGoroutine)
	
	var wg sync.WaitGroup
	startTime := time.Now().Unix()
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				writeOp := handlers.Operation{
					Operation: "write",
					Key:       key,
					Write: &handlers.WriteRequest{
						Timestamp: startTime + int64(goroutineID*1000 + j),
						Value:     float64(goroutineID) + float64(j)/100.0,
					},
				}
				handlers.HandleOperation(writeOp)
			}
		}(i)
	}
	
	wg.Wait()
	fmt.Println("All writes completed")
	
	// Read back data to check for corruption
	readOp := handlers.Operation{
		Operation: "read",
		Key:       key,
		Read: &handlers.ReadRequest{
			LastX: numGoroutines * writesPerGoroutine,
		},
	}
	
	resp := handlers.HandleOperation(readOp)
	if resp.Success {
		data := resp.Data.([]models.DataPoint)
		fmt.Printf("Successfully read %d records\n", len(data))
		
		// Check for suspicious values (1.0 indicates corruption)
		suspiciousCount := 0
		for _, dp := range data {
			if dp.Value == 1.0 {
				suspiciousCount++
			}
		}
		
		if suspiciousCount > 0 {
			fmt.Printf("⚠️  Found %d suspicious records with value=1.0 (potential corruption)\n", suspiciousCount)
		} else {
			fmt.Printf("✅ No corruption detected! All values are non-1.0\n")
		}
	} else {
		fmt.Printf("Failed to read data: %s\n", resp.Message)
	}
}