package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	recordSize = 16 // 8 bytes for timestamp + 8 bytes for float64 value
)

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type IndexEntry struct {
	Timestamp int64
	Offset    int64
}

func main() {
	// December 8th, 2025 00:00:00 UTC to December 12th, 2025 23:59:59 UTC
	startTime := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC).Unix()
	endTime := time.Date(2025, 12, 5, 23, 59, 59, 0, time.UTC).Unix()

	seriesIDs := []string{
		// "vertriqe_25416_cttp",
		// "vertriqe_25415_cttp",
		/*
		   vertriqe_25417_cttp
		   vertriqe_25518_cttp
		   vertriqe_25519_cttp
		   vertriqe_25275_cttp
		   vertriqe_25276_cttp
		   vertriqe_25277_cttp
		   vertriqe_25412_cttp
		*/

		// "vertriqe_25417_cttp",
		// "vertriqe_25518_cttp",
		// "vertriqe_25519_cttp",
		// "vertriqe_25275_cttp",
		// "vertriqe_25276_cttp",
		// "vertriqe_25277_cttp",
		// "vertriqe_25412_cttp",

		//25253+25255+25256+25233+25257+25258

		"vertriqe_25253_cttp",
		"vertriqe_25255_cttp",
		"vertriqe_25256_cttp",
		"vertriqe_25233_cttp",
		"vertriqe_25257_cttp",
		"vertriqe_25258_cttp",
	}

	dataDir := "data"

	fmt.Printf("Removing data points from %s to %s\n",
		time.Unix(startTime, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTime, 0).Format("2006-01-02 15:04:05"))

	for _, seriesID := range seriesIDs {
		fmt.Printf("\nProcessing series: %s\n", seriesID)

		aofFile := filepath.Join(dataDir, seriesID+".aof")
		idxFile := filepath.Join(dataDir, seriesID+".idx")

		if err := patchDataFile(aofFile, idxFile, startTime, endTime); err != nil {
			fmt.Printf("Error processing %s: %v\n", seriesID, err)
		} else {
			fmt.Printf("Successfully processed %s\n", seriesID)
		}
	}
}

func patchDataFile(aofPath, idxPath string, startTime, endTime int64) error {
	// Create backup files
	backupAof := aofPath + ".backup." + fmt.Sprintf("%d", time.Now().Unix())
	backupIdx := idxPath + ".backup." + fmt.Sprintf("%d", time.Now().Unix())

	if err := copyFile(aofPath, backupAof); err != nil {
		return fmt.Errorf("failed to backup AOF file: %w", err)
	}
	fmt.Printf("  Created backup: %s\n", backupAof)

	if _, err := os.Stat(idxPath); err == nil {
		if err := copyFile(idxPath, backupIdx); err != nil {
			return fmt.Errorf("failed to backup index file: %w", err)
		}
		fmt.Printf("  Created backup: %s\n", backupIdx)
	}

	// Read all data points
	dataPoints, err := readDataPoints(aofPath)
	if err != nil {
		return fmt.Errorf("failed to read data points: %w", err)
	}

	fmt.Printf("  Total data points before: %d\n", len(dataPoints))

	// Filter out data points in the time range
	filtered := []DataPoint{}
	removedCount := 0
	for _, dp := range dataPoints {
		if dp.Timestamp >= startTime && dp.Timestamp <= endTime {
			removedCount++
		} else {
			filtered = append(filtered, dp)
		}
	}

	fmt.Printf("  Data points removed: %d\n", removedCount)
	fmt.Printf("  Data points remaining: %d\n", len(filtered))

	if removedCount == 0 {
		fmt.Printf("  No data points to remove in the specified range\n")
		// Remove backup files since no changes were made
		os.Remove(backupAof)
		if _, err := os.Stat(backupIdx); err == nil {
			os.Remove(backupIdx)
		}
		return nil
	}

	// Write filtered data points back to new file
	tempAof := aofPath + ".tmp"
	if err := writeDataPoints(tempAof, filtered); err != nil {
		return fmt.Errorf("failed to write filtered data: %w", err)
	}

	// Replace original file with new file
	if err := os.Rename(tempAof, aofPath); err != nil {
		return fmt.Errorf("failed to replace AOF file: %w", err)
	}

	// Rebuild index file
	if err := rebuildIndexFile(idxPath, filtered); err != nil {
		return fmt.Errorf("failed to rebuild index: %w", err)
	}

	return nil
}

func readDataPoints(path string) ([]DataPoint, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var dataPoints []DataPoint
	for {
		var dp DataPoint
		err := binary.Read(file, binary.LittleEndian, &dp.Timestamp)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		err = binary.Read(file, binary.LittleEndian, &dp.Value)
		if err != nil {
			return nil, err
		}

		dataPoints = append(dataPoints, dp)
	}

	return dataPoints, nil
}

func writeDataPoints(path string, dataPoints []DataPoint) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, dp := range dataPoints {
		if err := binary.Write(file, binary.LittleEndian, dp.Timestamp); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, dp.Value); err != nil {
			return err
		}
	}

	return file.Sync()
}

func rebuildIndexFile(idxPath string, dataPoints []DataPoint) error {
	// Read the old index to determine the index interval
	indexInterval := int64(1000) // Default value

	if oldIndices, err := readIndexEntries(idxPath); err == nil && len(oldIndices) > 1 {
		// Calculate interval from existing index
		// The interval is approximately the number of data points between index entries
		// We'll use the default of 1000 for now
	}

	file, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(0)
	for i, dp := range dataPoints {
		if int64(i)%indexInterval == 0 && i > 0 {
			// Write index entry
			if err := binary.Write(file, binary.LittleEndian, dp.Timestamp); err != nil {
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
				return err
			}
		}
		offset += recordSize
	}

	return file.Sync()
}

func readIndexEntries(path string) ([]IndexEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []IndexEntry
	for {
		var entry IndexEntry
		err := binary.Read(file, binary.LittleEndian, &entry.Timestamp)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		err = binary.Read(file, binary.LittleEndian, &entry.Offset)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return destFile.Sync()
}
