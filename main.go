package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	fanout "gtsdb/fanout"
	models "gtsdb/models"
)

const indexInterval = 5000
const dataDir = "data"

var fanoutManager = fanout.NewFanout()

func main() {
	fanoutManager.Start() //this will start 2 go routines in the background

	//if dataDir does not exist, create it
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err := os.Mkdir(dataDir, 0755)
		if err != nil {
			fmt.Println("Error creating data directory:", err)
			os.Exit(1)
		}
	}
	listener, err := net.Listen("tcp", ":5555")
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Listening on :5555")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	id := rand.Intn(1000) + int(time.Now().UnixNano())
	scanner := bufio.NewScanner(conn)
	subscribingDevices := []string{}
	for scanner.Scan() {
		message := scanner.Text()

		parts := strings.Split(message, ",")
		if len(parts) == 2 {
			// Create data stream (subscribe,sensor_id)
			if parts[0] == "subscribe" {

				subscribingDevices = append(subscribingDevices, parts[1])
				if len(subscribingDevices) == 1 {
					fanoutManager.AddConsumer(id, func(msg models.DataPoint) {
						// if in the list of subscribing devices, send the message
						if slices.Contains(subscribingDevices, msg.ID) {
							conn.Write([]byte(fmt.Sprintf("%v\n", msg)))
						}

					})
				}

			} else if parts[0] == "unsubscribe" {
				for i, device := range subscribingDevices {
					if device == parts[1] {
						subscribingDevices = append(subscribingDevices[:i], subscribingDevices[i+1:]...)
						break
					}
				}
				if len(subscribingDevices) == 0 {
					fanoutManager.RemoveConsumer(id)
				}
			}

		} else if len(parts) == 3 {
			// Store data point
			id := parts[0]
			timestamp, _ := strconv.ParseInt(parts[1], 10, 64)
			value, _ := strconv.ParseFloat(parts[2], 64)

			dataPoint := models.DataPoint{
				ID:        id,
				Timestamp: timestamp,
				Value:     value,
			}

			storeDataPoint(dataPoint)
			conn.Write([]byte("Data point stored\n"))
		} else if len(parts) == 4 {
			// Read data points
			id := parts[0]
			startTime, _ := strconv.ParseInt(parts[1], 10, 64)
			endTime, _ := strconv.ParseInt(parts[2], 10, 64)
			downsample, _ := strconv.Atoi(parts[3])

			dataPoints := readDataPoints(id, startTime, endTime, downsample)
			response := formatDataPoints(dataPoints)
			conn.Write([]byte(response))
		} else {
			conn.Write([]byte("Invalid request\n"))
		}
	}
}

var dataFileHandles = make(map[string]*os.File)
var indexFileHandles = make(map[string]*os.File)
var metaFileHandles = make(map[string]*os.File)

func storeDataPoint(dataPoint models.DataPoint) {
	filename := dataPoint.ID + ".aof"
	file, ok := dataFileHandles[filename]
	if !ok {
		var err error
		file, err = os.OpenFile(dataDir+"/"+filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		dataFileHandles[filename] = file
	}

	line := fmt.Sprintf("%d,%.2f\n", dataPoint.Timestamp, dataPoint.Value)
	file.WriteString(line)

	metaFilename := dataPoint.ID + ".meta"
	metaFile, ok := metaFileHandles[metaFilename]
	if !ok {
		var err error
		metaFile, err = os.OpenFile(dataDir+"/"+metaFilename, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fmt.Println("Error opening meta file:", err)
			return
		}
		metaFileHandles[metaFilename] = metaFile
	}

	count := readMetaCount(metaFile)
	count++
	writeMetaCount(metaFile, count)

	if count%indexInterval == 0 {
		indexFilename := dataPoint.ID + ".idx"
		indexFile, ok := indexFileHandles[indexFilename]
		if !ok {
			var err error
			indexFile, err = os.OpenFile(dataDir+"/"+indexFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				fmt.Println("Error opening index file:", err)
				return
			}
			indexFileHandles[indexFilename] = indexFile
		}
		//end position of this file
		offset, _ := file.Seek(0, io.SeekEnd)
		offset -= int64(len(line))
		updateIndexFile(indexFile, dataPoint.Timestamp, offset)
	}
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

func readDataPoints(id string, startTime, endTime int64, downsample int) []models.DataPoint {
	filename := id + ".aof"
	file, ok := dataFileHandles[filename]
	if !ok {
		var err error
		file, err = os.OpenFile(dataDir+"/"+filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return nil
		}
		dataFileHandles[filename] = file
	}
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

	if downsample > 1 {
		dataPoints = downsampleDataPoints(dataPoints, downsample)
	}

	return dataPoints
}
func downsampleDataPoints(dataPoints []models.DataPoint, downsample int) []models.DataPoint {
	if len(dataPoints) == 0 {
		return dataPoints
	}

	var downsampled []models.DataPoint
	intervalStart := dataPoints[0].Timestamp
	intervalSum := 0.0
	intervalCount := 0

	for _, dp := range dataPoints {
		if dp.Timestamp-intervalStart >= int64(downsample) {
			// Reached the end of the current interval
			if intervalCount > 0 {
				avgValue := intervalSum / float64(intervalCount)
				downsampled = append(downsampled, models.DataPoint{
					ID:        dp.ID,
					Timestamp: intervalStart,
					Value:     avgValue,
				})
			}
			// Start a new interval
			intervalStart = dp.Timestamp
			intervalSum = dp.Value
			intervalCount = 1
		} else {
			// Accumulate values within the current interval
			intervalSum += dp.Value
			intervalCount++
		}
	}

	// Process the last interval
	if intervalCount > 0 {
		avgValue := intervalSum / float64(intervalCount)
		downsampled = append(downsampled, models.DataPoint{
			ID:        dataPoints[len(dataPoints)-1].ID,
			Timestamp: intervalStart,
			Value:     avgValue,
		})
	}

	return downsampled
}

func formatDataPoints(dataPoints []models.DataPoint) string {
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
