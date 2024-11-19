package buffer

import (
	"bufio"
	"fmt"
	"gtsdb/concurrent"
	"gtsdb/models"
	"gtsdb/utils"
	"io"
	"os"
	"strconv"
	"strings"
)

func storeDataPoints(dataPointId string, dataPoints []models.DataPoint) {
	dataFile := prepareFileHandles(dataPointId+".aof", dataFileHandles)
	metaFile := prepareFileHandles(dataPointId+".meta", metaFileHandles)
	indexFile := prepareFileHandles(dataPointId+".idx", indexFileHandles)
	for _, dataPoint := range dataPoints {

		line := fmt.Sprintf("%d,%.8e\n", dataPoint.Timestamp, dataPoint.Value)
		dataFile.WriteString(line)
		count := readMetaCount(metaFile)
		count++
		writeMetaCount(metaFile, count)

		if count%indexInterval == 0 {

			//end position of this file
			offset, _ := dataFile.Seek(0, io.SeekEnd)
			offset -= int64(len(line))
			updateIndexFile(indexFile, dataPoint.Timestamp, offset)
		}
	}

}
func prepareFileHandles(fileName string, handleArray *concurrent.HashMap[string, *os.File]) *os.File {

	file, ok := handleArray.Get(fileName)
	if !ok {
		var err error
		file, err = os.OpenFile(utils.DataDir+"/"+fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			file, err = os.OpenFile("../"+utils.DataDir+"/"+fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644) //this is for go test //TODO: any better way?
			if err != nil {
				panic(err)
			}
		}
		handleArray.Set(fileName, file)
	}
	return file
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

func readBufferedDataPoints(id string, startTime, endTime int64) []models.DataPoint {
	if cacheSize == 0 {
		return []models.DataPoint{}
	}

	rb, ok := idToRingBufferMap.Get(id)
	if !ok {
		return []models.DataPoint{}
	}

	var result []models.DataPoint
	for i := 0; i < rb.Size(); i++ {
		dataPoint := rb.Get(i)
		if dataPoint.Timestamp >= startTime && dataPoint.Timestamp <= endTime {
			result = append(result, dataPoint)
		}
	}
	return result
}

func readLastBufferedDataPoints(id string, count int) []models.DataPoint {
	if count == 1 && lastTimestamp[id] != 0 {
		return []models.DataPoint{{Timestamp: lastTimestamp[id], Value: lastValue[id]}}
	}

	rb, ok := idToRingBufferMap.Get(id)
	if !ok {
		return []models.DataPoint{}
	}

	if count > rb.Size() {
		count = rb.Size()
	}
	if count == 0 {
		return []models.DataPoint{}
	}

	result := make([]models.DataPoint, count)
	for i := 0; i < count; i++ {
		result[i] = rb.Get(rb.Size() - count + i)
	}
	return result
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
