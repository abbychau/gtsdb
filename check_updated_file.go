package main

import (
	"gtsdb/handlers"
	"gtsdb/models"
	"gtsdb/utils"
)

func main() {
	utils.DataDir = "data"
	
	// Test the updated file
	readOp := handlers.Operation{
		Operation: "read",
		Key:       "vertriqe_25114_amb_temp",
		Read: &handlers.ReadRequest{
			LastX: 20,
		},
	}
	
	resp := handlers.HandleOperation(readOp)
	if resp.Success {
		data := resp.Data.([]models.DataPoint)
		utils.Log("Checking updated file - %d records:", len(data))
		
		suspiciousCount := 0
		for i, dp := range data {
			if dp.Value == 1.0 {
				suspiciousCount++
			}
			if i < 10 {
				utils.Log("  [%d]: ts=%d val=%f", i, dp.Timestamp, dp.Value)
				if dp.Value == 1.0 {
					utils.Log("    ⚠️  SUSPICIOUS: Value is 1.0")
				}
			}
		}
		
		utils.Log("Records with value=1.0: %d out of %d", suspiciousCount, len(data))
		
		// Check file size and record count
		utils.Log("File info:")
		utils.Log("  Size: 590,336 bytes")
		utils.Log("  Expected records: %d", 590336/16)
		utils.Log("  Actual records read: %d", len(data))
	}
}