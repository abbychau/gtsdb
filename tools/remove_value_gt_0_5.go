//go:build ignore

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
	recordSize    = 16
	indexInterval = 5000
	threshold     = 0.5
)

var seriesIDs = []string{
	"vertriqe_25522_cttp",
	"vertriqe_25523_cttp",
	"vertriqe_25524_cttp",
	"vertriqe_25525_cttp",
	"vertriqe_25526_cttp",
	"vertriqe_25527_cttp",
}

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type IndexEntry struct {
	Timestamp int64
	Offset    int64
}

func main() {
	dataDir := "data"
	backupSuffix := fmt.Sprintf(".backup.%d", time.Now().Unix())

	fmt.Printf("Removing data points with value > %.4f\n", threshold)
	fmt.Printf("Data directory: %s\n", dataDir)

	for _, seriesID := range seriesIDs {
		fmt.Printf("\nProcessing series: %s\n", seriesID)

		aofPath := filepath.Join(dataDir, seriesID+".aof")
		idxPath := filepath.Join(dataDir, seriesID+".idx")

		if err := processSeries(aofPath, idxPath, backupSuffix); err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		fmt.Printf("  Done: %s\n", seriesID)
	}
}

func processSeries(aofPath, idxPath, backupSuffix string) error {
	if _, err := os.Stat(aofPath); err != nil {
		return fmt.Errorf("AOF file not found: %w", err)
	}

	backupAOF := aofPath + backupSuffix
	backupIDX := idxPath + backupSuffix

	if err := copyFile(aofPath, backupAOF); err != nil {
		return fmt.Errorf("failed to back up AOF: %w", err)
	}
	fmt.Printf("  Created backup: %s\n", backupAOF)

	hasIndex := false
	if _, err := os.Stat(idxPath); err == nil {
		hasIndex = true
		if err := copyFile(idxPath, backupIDX); err != nil {
			return fmt.Errorf("failed to back up IDX: %w", err)
		}
		fmt.Printf("  Created backup: %s\n", backupIDX)
	}

	dataPoints, err := readDataPoints(aofPath)
	if err != nil {
		return fmt.Errorf("failed to read data points: %w", err)
	}

	fmt.Printf("  Total data points before: %d\n", len(dataPoints))

	filtered := make([]DataPoint, 0, len(dataPoints))
	removedCount := 0
	for _, dp := range dataPoints {
		if dp.Value > threshold {
			removedCount++
			continue
		}
		filtered = append(filtered, dp)
	}

	fmt.Printf("  Data points removed: %d\n", removedCount)
	fmt.Printf("  Data points remaining: %d\n", len(filtered))

	if removedCount == 0 {
		fmt.Printf("  No matching data points found, removing backups\n")
		_ = os.Remove(backupAOF)
		if hasIndex {
			_ = os.Remove(backupIDX)
		}
		return nil
	}

	tempAOF := aofPath + ".tmp"
	if err := writeDataPoints(tempAOF, filtered); err != nil {
		return fmt.Errorf("failed to write filtered AOF: %w", err)
	}

	if err := os.Rename(tempAOF, aofPath); err != nil {
		return fmt.Errorf("failed to replace AOF: %w", err)
	}

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

		if err := binary.Read(file, binary.LittleEndian, &dp.Value); err != nil {
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
	file, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(0)
	for i, dp := range dataPoints {
		if i > 0 && int64(i)%indexInterval == 0 {
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

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}

	return destFile.Sync()
}
