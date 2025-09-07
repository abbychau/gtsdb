//go:build ignore
// +build ignore

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"
)

type RepairStats struct {
	FilesScanned     int
	FilesCorrupted   int
	RecordsProcessed int
	RecordsFixed     int
	RecordsSkipped   int
	BackupsCreated   int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run repair.go <command> [options]")
		fmt.Println("Commands:")
		fmt.Println("  scan    - Scan for corrupted files (read-only)")
		fmt.Println("  fix     - Fix corrupted files (creates backups)")
		fmt.Println("  fix --no-backup - Fix without creating backups")
		os.Exit(1)
	}

	command := os.Args[1]
	createBackup := true
	
	if len(os.Args) > 2 && os.Args[2] == "--no-backup" {
		createBackup = false
	}

	utils.DataDir = "data"
	
	switch command {
	case "scan":
		stats := scanForCorruption()
		printScanResults(stats)
	case "fix":
		stats := repairCorruption(createBackup)
		printRepairResults(stats)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

func scanForCorruption() RepairStats {
	stats := RepairStats{}
	
	files, err := os.ReadDir(utils.DataDir)
	if err != nil {
		fmt.Printf("Error reading data directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Scanning for corrupted AOF files...")
	
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".aof") {
			continue
		}
		
		stats.FilesScanned++
		key := strings.TrimSuffix(file.Name(), ".aof")
		
		corrupted, processed, fixed, skipped := analyzeFile(key, false, false)
		stats.RecordsProcessed += processed
		stats.RecordsFixed += fixed
		stats.RecordsSkipped += skipped
		
		if corrupted {
			stats.FilesCorrupted++
			fmt.Printf("  CORRUPTED: %s (%d records, %d corrupted, %d skipped)\n", 
				key, processed, fixed, skipped)
		} else {
			fmt.Printf("  OK: %s (%d records)\n", key, processed)
		}
	}
	
	return stats
}

func repairCorruption(createBackup bool) RepairStats {
	stats := RepairStats{}
	
	files, err := os.ReadDir(utils.DataDir)
	if err != nil {
		fmt.Printf("Error reading data directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Repairing corrupted AOF files...")
	
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".aof") {
			continue
		}
		
		stats.FilesScanned++
		key := strings.TrimSuffix(file.Name(), ".aof")
		
		corrupted, processed, fixed, skipped := analyzeFile(key, false, false)
		
		if corrupted {
			stats.FilesCorrupted++
			stats.RecordsProcessed += processed
			stats.RecordsFixed += fixed
			stats.RecordsSkipped += skipped
			
			if createBackup {
				if backupFile(key) {
					stats.BackupsCreated++
				}
			}
			
			// Now actually fix the file
			_, _, actualFixed, actualSkipped := analyzeFile(key, true, true)
			fmt.Printf("  REPAIRED: %s (%d records fixed, %d skipped)\n", 
				key, actualFixed, actualSkipped)
		} else {
			stats.RecordsProcessed += processed
			fmt.Printf("  OK: %s (%d records)\n", key, processed)
		}
	}
	
	return stats
}

func analyzeFile(key string, repair bool, writeFixed bool) (corrupted bool, processed int, fixed int, skipped int) {
	filename := filepath.Join(utils.DataDir, key+".aof")
	
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("Error opening %s: %v\n", filename, err)
		return false, 0, 0, 0
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Error getting file info for %s: %v\n", filename, err)
		return false, 0, 0, 0
	}
	
	fileSize := fileInfo.Size()
	actualRecordCount := fileSize / 16
	
	// Check for alignment issues
	if fileSize%16 != 0 {
		corrupted = true
		if repair {
			// Truncate to aligned size
			alignedSize := actualRecordCount * 16
			file.Truncate(alignedSize)
			fmt.Printf("    Fixed alignment: truncated from %d to %d bytes\n", fileSize, alignedSize)
		}
	}
	
	// Read and analyze records
	file.Seek(0, io.SeekStart)
	reader := bufio.NewReader(file)
	
	var fixedRecords []models.DataPoint
	
	for {
		var timestamp int64
		var value float64
		
		err := binary.Read(reader, binary.LittleEndian, &timestamp)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading timestamp: %v\n", err)
			break
		}
		
		err = binary.Read(reader, binary.LittleEndian, &value)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading value: %v\n", err)
			break
		}
		
		processed++
		
		// Apply enhanced corruption detection logic
		corrupted_detected := false
		
		// Check 1: Obviously bad timestamps
		if timestamp > 4000000000 || (timestamp < 0 && timestamp != 0) {
			corrupted_detected = true
		}
		
		// Check 2: Suspicious value patterns (many 1.0 values likely indicate VALUE-TIMESTAMP swaps)
		if !corrupted_detected && value == 1.0 {
			// Check if timestamp, when interpreted as float64, gives a reasonable sensor value
			timestampAsFloat := *(*float64)(unsafe.Pointer(&timestamp))
			if timestampAsFloat > 0.0 && timestampAsFloat < 100.0 { // Reasonable temperature range
				corrupted_detected = true
				if !repair {
					fmt.Printf("    Detected likely VALUE-TIMESTAMP swap: ts=%d val=%f (ts as float=%.6f)\n", 
						timestamp, value, timestampAsFloat)
				}
			}
		}
		
		if corrupted_detected {
			valueAsInt := *(*int64)(unsafe.Pointer(&value))
			if valueAsInt >= 1600000000 && valueAsInt <= 4000000000 {
				// This is a swapped record
				actualValue := *(*float64)(unsafe.Pointer(&timestamp))
				actualTimestamp := valueAsInt
				
				corrupted = true
				fixed++
				
				if writeFixed {
					fixedRecords = append(fixedRecords, models.DataPoint{
						Key:       key,
						Timestamp: actualTimestamp,
						Value:     actualValue,
					})
				}
			} else {
				// Completely corrupted, skip
				corrupted = true
				skipped++
			}
		} else {
			// Valid record
			if writeFixed {
				fixedRecords = append(fixedRecords, models.DataPoint{
					Key:       key,
					Timestamp: timestamp,
					Value:     value,
				})
			}
		}
	}
	
	// Write fixed data back if requested
	if writeFixed && len(fixedRecords) > 0 {
		file.Seek(0, io.SeekStart)
		file.Truncate(0)
		
		for _, dp := range fixedRecords {
			binary.Write(file, binary.LittleEndian, dp.Timestamp)
			binary.Write(file, binary.LittleEndian, dp.Value)
		}
		file.Sync()
	}
	
	return corrupted, processed, fixed, skipped
}

func backupFile(key string) bool {
	sourceFile := filepath.Join(utils.DataDir, key+".aof")
	backupFile := filepath.Join(utils.DataDir, fmt.Sprintf("%s.aof.backup.%d", key, time.Now().Unix()))
	
	source, err := os.Open(sourceFile)
	if err != nil {
		fmt.Printf("Error opening source file %s: %v\n", sourceFile, err)
		return false
	}
	defer source.Close()
	
	backup, err := os.Create(backupFile)
	if err != nil {
		fmt.Printf("Error creating backup file %s: %v\n", backupFile, err)
		return false
	}
	defer backup.Close()
	
	_, err = io.Copy(backup, source)
	if err != nil {
		fmt.Printf("Error copying to backup file %s: %v\n", backupFile, err)
		return false
	}
	
	fmt.Printf("    Created backup: %s\n", filepath.Base(backupFile))
	return true
}

func printScanResults(stats RepairStats) {
	fmt.Println("\n=== SCAN RESULTS ===")
	fmt.Printf("Files scanned: %d\n", stats.FilesScanned)
	fmt.Printf("Files with corruption: %d\n", stats.FilesCorrupted)
	fmt.Printf("Total records processed: %d\n", stats.RecordsProcessed)
	fmt.Printf("Corrupted records (fixable): %d\n", stats.RecordsFixed)
	fmt.Printf("Corrupted records (skipped): %d\n", stats.RecordsSkipped)
	
	if stats.FilesCorrupted > 0 {
		fmt.Printf("\nRun 'go run repair.go fix' to repair the corrupted files.\n")
	} else {
		fmt.Println("\nNo corruption detected!")
	}
}

func printRepairResults(stats RepairStats) {
	fmt.Println("\n=== REPAIR RESULTS ===")
	fmt.Printf("Files scanned: %d\n", stats.FilesScanned)
	fmt.Printf("Files repaired: %d\n", stats.FilesCorrupted)
	fmt.Printf("Total records processed: %d\n", stats.RecordsProcessed)
	fmt.Printf("Records fixed: %d\n", stats.RecordsFixed)
	fmt.Printf("Records skipped: %d\n", stats.RecordsSkipped)
	fmt.Printf("Backup files created: %d\n", stats.BackupsCreated)
	
	if stats.FilesCorrupted > 0 {
		fmt.Printf("\nRepair completed! %d files have been fixed.\n", stats.FilesCorrupted)
		if stats.BackupsCreated > 0 {
			fmt.Printf("Original files backed up with .backup.timestamp suffix.\n")
		}
	} else {
		fmt.Println("\nNo corruption found - no repairs needed!")
	}
}