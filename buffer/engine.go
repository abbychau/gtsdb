package buffer

import (
	"bufio"
	"fmt"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
	"strconv"
	"strings"
)

func readMetaCount(metaFile *os.File) int {
	_, err := metaFile.Seek(0, io.SeekStart)
	if err != nil {
		return 0
	}

	scanner := bufio.NewScanner(metaFile)
	if scanner.Scan() {
		countStr := strings.TrimPrefix(scanner.Text(), "count:")
		count, _ := strconv.Atoi(countStr)
		return count
	}

	return 0
}

func writeMetaCount(metaFile *os.File, count int) {
	_, err := metaFile.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking meta file:", err)
		return
	}

	metaFile.Truncate(0)
	metaFile.WriteString(fmt.Sprintf("count:%d\n", count))
}

func updateIndexFile(indexFile *os.File, timestamp int64, offset int64) {
	line := fmt.Sprintf("%d,%d\n", timestamp, offset)
	indexFile.WriteString(line)
}
func dataFileById(id string) *os.File {
	filename := id + ".aof"
	initLock(filename)
	rwMutex[filename].RLock()
	defer rwMutex[filename].RUnlock()

	file, ok := dataFileHandles[filename]
	if !ok {
		var err error
		file, err = os.OpenFile(utils.DataDir+"/"+filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return nil
		}
		dataFileHandles[filename] = file
	}
	return file
}
func ReadDataPoints(id string, startTime, endTime int64, downsample int, aggregation string) []models.DataPoint {

	dataPoints := readBufferedDataPoints(id, startTime, endTime)
	if len(dataPoints) == 0 {
		dataPoints = readFiledDataPoints(id, startTime, endTime)
	}

	if downsample > 1 {
		dataPoints = downsampleDataPoints(dataPoints, downsample, aggregation)
	}

	return dataPoints
}

func readFiledDataPoints(id string, startTime, endTime int64) []models.DataPoint {
	file := dataFileById(id)
	var dataPoints []models.DataPoint
	reader := bufio.NewReader(file)

	indexFilename := id + ".idx"
	indexFile, ok := indexFileHandles[indexFilename]
	if ok {
		indexReader := bufio.NewReader(indexFile)
		var offset int64

		_, err := indexFile.Seek(0, io.SeekStart)
		if err != nil {
			fmt.Println("Error seeking index file:", err)
			return nil
		}

		for {
			line, err := indexReader.ReadString('\n')
			if err != nil {
				break
			}

			parts := strings.Split(strings.TrimSpace(line), ",")
			timestamp, _ := strconv.ParseInt(parts[0], 10, 64)
			if timestamp > startTime {
				break
			}
			offset, _ = strconv.ParseInt(parts[1], 10, 64)
		}

		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			fmt.Println("Error seeking data file:", err)
			return nil
		}
	} else {
		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			fmt.Println("Error seeking data file:", err)
			return nil
		}
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading file:", err)
			return nil
		}

		parts := strings.Split(strings.TrimSpace(line), ",")
		timestamp, _ := strconv.ParseInt(parts[0], 10, 64)
		value, _ := strconv.ParseFloat(parts[1], 64)

		if timestamp > endTime {
			break
		}

		if timestamp >= startTime && timestamp <= endTime {
			dataPoints = append(dataPoints, models.DataPoint{
				ID:        id,
				Timestamp: timestamp,
				Value:     value,
			})
		}
	}

	return dataPoints
}

func ReadLastDataPoints(id string, count int) []models.DataPoint {

	dataPoints := readLastBufferedDataPoints(id, count)
	if len(dataPoints) < count {
		remaining := count - len(dataPoints)
		lastDataPoints, err := readLastFiledDataPoints(id, remaining)
		if err == nil {
			dataPoints = append(dataPoints, lastDataPoints...)
		}
	}

	return dataPoints
}

func readLastFiledDataPoints(id string, count int) ([]models.DataPoint, error) {
	file := dataFileById(id)
	reader := bufio.NewReader(file)

	// Seek from the end of the file
	_, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	var dataPoints []models.DataPoint
	var line string

	// Read lines from the end until we have the required count
	for count > 0 {
		// Move back one byte to read the previous character
		_, err = file.Seek(-2, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		// Read the character
		char, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		// If we find a newline, we have a complete line
		if char == '\n' {
			line, err = reader.ReadString('\n')
			if err != nil {
				return nil, err
			}

			parts := strings.Split(strings.TrimSpace(line), ",")
			timestamp, _ := strconv.ParseInt(parts[0], 10, 64)
			value, _ := strconv.ParseFloat(parts[1], 64)

			dataPoint := models.DataPoint{
				ID:        id,
				Timestamp: timestamp,
				Value:     value,
			}

			dataPoints = append([]models.DataPoint{dataPoint}, dataPoints...)
			count--
		}
	}

	return dataPoints, nil
}

func downsampleDataPoints(dataPoints []models.DataPoint, downsample int, aggregation string) []models.DataPoint {
	if len(dataPoints) == 0 {
		return dataPoints
	}

	var downsampled []models.DataPoint
	intervalStart := dataPoints[0].Timestamp
	intervalSum := 0.0
	intervalCount := 0
	var intervalMin, intervalMax, intervalFirst, intervalLast float64
	intervalFirst = dataPoints[0].Value

	for _, dp := range dataPoints {
		if dp.Timestamp-intervalStart >= int64(downsample) {
			// Reached the end of the current interval
			if intervalCount > 0 {
				var value float64
				switch aggregation {
				case "avg":
					value = intervalSum / float64(intervalCount)
				case "sum":
					value = intervalSum
				case "min":
					value = intervalMin
				case "max":
					value = intervalMax
				case "first":
					value = intervalFirst
				case "last":
					value = intervalLast
				default:
					value = intervalSum / float64(intervalCount)
				}
				downsampled = append(downsampled, models.DataPoint{
					ID:        dp.ID,
					Timestamp: intervalStart,
					Value:     value,
				})
			}
			// Start a new interval
			intervalStart = dp.Timestamp
			intervalSum = dp.Value
			intervalCount = 1
			intervalMin = dp.Value
			intervalMax = dp.Value
			intervalFirst = dp.Value
			intervalLast = dp.Value
		} else {
			// Accumulate values within the current interval
			intervalSum += dp.Value
			intervalCount++
			if dp.Value < intervalMin {
				intervalMin = dp.Value
			}
			if dp.Value > intervalMax {
				intervalMax = dp.Value
			}
			intervalLast = dp.Value
		}
	}

	// Process the last interval
	if intervalCount > 0 {
		var value float64
		switch aggregation {
		case "avg":
			value = intervalSum / float64(intervalCount)
		case "sum":
			value = intervalSum
		case "min":
			value = intervalMin
		case "max":
			value = intervalMax
		case "first":
			value = intervalFirst
		case "last":
			value = intervalLast
		default:
			value = intervalSum / float64(intervalCount)
		}
		downsampled = append(downsampled, models.DataPoint{
			ID:        dataPoints[len(dataPoints)-1].ID,
			Timestamp: intervalStart,
			Value:     value,
		})
	}

	return downsampled
}

func FormatDataPoints(dataPoints []models.DataPoint) string {
	var response string

	for i, dp := range dataPoints {
		response += fmt.Sprintf("%s,%d,%.2f", dp.ID, dp.Timestamp, dp.Value)
		if i < len(dataPoints)-1 {
			response += "|"
		}
	}

	response += "\n"

	return response
}
