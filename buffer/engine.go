package buffer

import (
	"bufio"
	"encoding/json"
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
		utils.Errorln("Error seeking meta file:", err)
		return
	}
	str := fmt.Sprintf("count:%d\n", count)
	//empty the file
	metaFile.Truncate(0)
	metaFile.Seek(0, io.SeekStart)
	metaFile.WriteString(str)
}

func updateIndexFile(indexFile *os.File, timestamp int64, offset int64) {
	line := fmt.Sprintf("%d,%d\n", timestamp, offset)
	indexFile.WriteString(line)
}
func dataFileById(id string) *os.File {
	filename := id + ".aof"

	file, ok := dataFileHandles.Get(filename)
	if !ok {
		var err error
		file, err = os.OpenFile(utils.DataDir+"/"+filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			utils.Error("Error opening file: %v", err)
			return nil
		}
		dataFileHandles.Set(filename, file)
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
	indexFile, ok := indexFileHandles.Get(indexFilename)
	if ok {
		indexReader := bufio.NewReader(indexFile)
		var offset int64

		_, err := indexFile.Seek(0, io.SeekStart)
		if err != nil {
			utils.Error("Error seeking index file: %v", err)
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
			utils.Error("Error seeking data file: %v", err)
			return nil
		}
	} else {
		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			utils.Error("Error seeking data file: %v", err)
			return nil
		}
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			utils.Error("Error reading file: %v", err)
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
	fmt.Println("dataPoints", dataPoints)
	fmt.Println("Count", count)
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

	//seek the last x bytes using offset
	// line width is "1731690356,3.33333330e+03" 25 + 1 (\n) = 32
	// 26 * count
	_, err := file.Seek(int64(-26*count), io.SeekEnd)
	if err != nil {
		//if the file is smaller than the offset, seek to the beginning
		file.Seek(0, io.SeekStart)
	}

	var dataPoints []models.DataPoint
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			utils.Error("Error reading file: %v", err)
			return nil, err
		}

		parts := strings.Split(strings.TrimSpace(line), ",")
		trimmedTimestamp := strings.TrimSpace(parts[0])
		timestamp, _ := strconv.ParseInt(trimmedTimestamp, 10, 64)
		trimmedValue := strings.TrimSpace(parts[1])
		value, _ := strconv.ParseFloat(trimmedValue, 64)

		dataPoints = append(dataPoints, models.DataPoint{
			ID:        id,
			Timestamp: timestamp,
			Value:     value,
		})
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

// JsonFormatDataPoints
func JsonFormatDataPoints(dataPoints []models.DataPoint) string {
	var response string
	//use json marshal to format the data points
	bytes, _ := json.Marshal(dataPoints)
	response = string(bytes)
	return response
}
