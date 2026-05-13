package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

type removeRequest struct {
	SeriesIDs            []string `json:"seriesIds"`
	StartTime            int64    `json:"startTime"`
	EndTime              int64    `json:"endTime"`
	DataPointGreaterThan *float64 `json:"dataPointGreaterThan"`
	DataPointLessThan    *float64 `json:"dataPointLessThan"`
}

type seriesResult struct {
	SeriesID       string `json:"seriesId"`
	Removed        int    `json:"removed"`
	Remaining      int    `json:"remaining"`
	Original       int    `json:"original"`
	AOFBackupPath  string `json:"aofBackupPath,omitempty"`
	IDXBackupPath  string `json:"idxBackupPath,omitempty"`
	NoChanges      bool   `json:"noChanges"`
	Error          string `json:"error,omitempty"`
	ResolvedAOF    string `json:"resolvedAof"`
	ResolvedIDX    string `json:"resolvedIdx"`
	FilterStart    int64  `json:"filterStart"`
	FilterEnd      int64  `json:"filterEnd"`
	FilterValueMin string `json:"filterValueMin"`
	FilterValueMax string `json:"filterValueMax"`
	ReloadAttempt  bool   `json:"reloadAttempt"`
	ReloadSuccess  bool   `json:"reloadSuccess"`
	ReloadMessage  string `json:"reloadMessage,omitempty"`
}

type removeResponse struct {
	DataDir string         `json:"dataDir"`
	Results []seriesResult `json:"results"`
}

func main() {
	dataDir, err := resolveDataDir("mydata/root")
	if err != nil {
		fmt.Printf("Failed to resolve data directory: %v\n", err)
		os.Exit(1)
	}

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/api/remove-data", removeDataHandler(dataDir))

	addr := ":8080"
	fmt.Printf("Patch remove data server running at http://localhost%s\n", addr)
	fmt.Printf("Using data directory: %s\n", dataDir)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	}
}

func indexHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, pageHTML)
}

func removeDataHandler(dataDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req removeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json payload", http.StatusBadRequest)
			return
		}

		if len(req.SeriesIDs) == 0 {
			http.Error(w, "seriesIds is required", http.StatusBadRequest)
			return
		}
		if req.StartTime <= 0 || req.EndTime <= 0 || req.EndTime < req.StartTime {
			http.Error(w, "invalid startTime/endTime", http.StatusBadRequest)
			return
		}

		resp := removeResponse{DataDir: dataDir}
		for _, seriesID := range req.SeriesIDs {
			seriesID = strings.TrimSpace(seriesID)
			if seriesID == "" {
				continue
			}

			aofFile := filepath.Join(dataDir, seriesID+".aof")
			idxFile := filepath.Join(dataDir, seriesID+".idx")

			result, err := patchDataFile(aofFile, idxFile, req.StartTime, req.EndTime, req.DataPointGreaterThan, req.DataPointLessThan)
			if err != nil {
				result.Error = err.Error()
			} else {
				result.ReloadAttempt = true
				ok, msg := reloadKey(seriesID)
				result.ReloadSuccess = ok
				result.ReloadMessage = msg
			}

			result.SeriesID = seriesID
			result.ResolvedAOF = aofFile
			result.ResolvedIDX = idxFile
			result.FilterStart = req.StartTime
			result.FilterEnd = req.EndTime
			if req.DataPointGreaterThan == nil {
				result.FilterValueMin = "disabled"
			} else {
				result.FilterValueMin = strconv.FormatFloat(*req.DataPointGreaterThan, 'f', -1, 64)
			}
			if req.DataPointLessThan == nil {
				result.FilterValueMax = "disabled"
			} else {
				result.FilterValueMax = strconv.FormatFloat(*req.DataPointLessThan, 'f', -1, 64)
			}

			resp.Results = append(resp.Results, result)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func patchDataFile(aofPath, idxPath string, startTime, endTime int64, dataPointGreaterThan, dataPointLessThan *float64) (seriesResult, error) {
	result := seriesResult{}

	backupAof := aofPath + ".backup." + fmt.Sprintf("%d", time.Now().Unix())
	backupIdx := idxPath + ".backup." + fmt.Sprintf("%d", time.Now().Unix())

	if err := copyFile(aofPath, backupAof); err != nil {
		return result, fmt.Errorf("failed to backup AOF file: %w", err)
	}
	result.AOFBackupPath = backupAof

	if _, err := os.Stat(idxPath); err == nil {
		if err := copyFile(idxPath, backupIdx); err != nil {
			return result, fmt.Errorf("failed to backup index file: %w", err)
		}
		result.IDXBackupPath = backupIdx
	}

	dataPoints, err := readDataPoints(aofPath)
	if err != nil {
		return result, fmt.Errorf("failed to read data points: %w", err)
	}
	result.Original = len(dataPoints)

	filtered := make([]DataPoint, 0, len(dataPoints))
	removedCount := 0
	for _, dp := range dataPoints {
		shouldRemove := dp.Timestamp >= startTime && dp.Timestamp <= endTime
		if shouldRemove && dataPointGreaterThan != nil {
			shouldRemove = dp.Value > *dataPointGreaterThan
		}
		if shouldRemove && dataPointLessThan != nil {
			shouldRemove = dp.Value < *dataPointLessThan
		}

		if shouldRemove {
			removedCount++
		} else {
			filtered = append(filtered, dp)
		}
	}

	result.Removed = removedCount
	result.Remaining = len(filtered)

	if removedCount == 0 {
		result.NoChanges = true
		_ = os.Remove(backupAof)
		if result.IDXBackupPath != "" {
			_ = os.Remove(backupIdx)
		}
		result.AOFBackupPath = ""
		result.IDXBackupPath = ""
		return result, nil
	}

	tempAof := aofPath + ".tmp"
	if err := writeDataPoints(tempAof, filtered); err != nil {
		return result, fmt.Errorf("failed to write filtered data: %w", err)
	}

	if err := os.Rename(tempAof, aofPath); err != nil {
		return result, fmt.Errorf("failed to replace AOF file: %w", err)
	}

	if err := rebuildIndexFile(idxPath, filtered); err != nil {
		return result, fmt.Errorf("failed to rebuild index: %w", err)
	}

	return result, nil
}

func reloadKey(seriesID string) (bool, string) {
	payload := map[string]interface{}{
		"operation": "reloadkey",
		"key":       seriesID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Sprintf("marshal reload payload failed: %v", err)
	}

	resp, err := http.Post("http://localhost:5556/", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return false, fmt.Sprintf("reload request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Sprintf("reload http %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	return true, strings.TrimSpace(string(respBody))
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
	indexInterval := int64(1000)

	if oldIndices, err := readIndexEntries(idxPath); err == nil && len(oldIndices) > 1 {
		_ = oldIndices
	}

	file, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(0)
	for i, dp := range dataPoints {
		if int64(i)%indexInterval == 0 && i > 0 {
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

func resolveDataDir(dataDir string) (string, error) {
	if filepath.IsAbs(dataDir) {
		if info, err := os.Stat(dataDir); err == nil && info.IsDir() {
			return dataDir, nil
		}
		return "", fmt.Errorf("absolute data directory %q does not exist", dataDir)
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}

	for dir := wd; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, dataDir)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}

	return "", fmt.Errorf("could not find %q from %q or any parent directory", dataDir, wd)
}

const pageHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Patch Remove Data</title>
  <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
  <style>
    :root {
      --bg: #f3f6f4;
      --card: #ffffff;
      --ink: #162218;
      --soft: #66756b;
      --accent: #1f7a4d;
      --accent-2: #0f5d37;
      --border: #d7e2da;
      --danger: #b52323;
      --shadow: 0 12px 30px rgba(0, 0, 0, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 10% 10%, #dcefe3 0%, transparent 35%),
        radial-gradient(circle at 85% 20%, #e8f5ed 0%, transparent 30%),
        var(--bg);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .card {
      width: min(920px, 100%);
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .head {
      padding: 20px 22px;
      background: linear-gradient(135deg, #e8f4ec, #f5fbf7);
      border-bottom: 1px solid var(--border);
    }
    .head h1 {
      margin: 0;
      font-size: 1.35rem;
      letter-spacing: 0.2px;
    }
    .head p {
      margin: 8px 0 0;
      color: var(--soft);
      font-size: 0.95rem;
    }
    .grid {
      padding: 20px 22px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }
    .field { display: grid; gap: 6px; }
    .field.full { grid-column: 1 / -1; }
    label {
      font-weight: 600;
      font-size: 0.92rem;
    }
    input, textarea {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 0.95rem;
      outline: none;
      transition: border-color 120ms ease, box-shadow 120ms ease;
      background: #fff;
    }
    textarea { min-height: 130px; resize: vertical; }
    input:focus, textarea:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(31, 122, 77, 0.15);
    }
    .actions {
      padding: 0 22px 22px;
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 16px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
      transition: transform 120ms ease, background 120ms ease;
    }
    button:hover { background: var(--accent-2); transform: translateY(-1px); }
    #status { font-size: 0.92rem; color: var(--soft); }
    #status.error { color: var(--danger); }
    #output {
      margin: 0 22px 22px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #f8fbf9;
      padding: 12px;
      font-family: "IBM Plex Mono", Consolas, monospace;
      font-size: 0.85rem;
      white-space: pre-wrap;
      max-height: 320px;
      overflow: auto;
    }
    @media (max-width: 760px) {
      .grid { grid-template-columns: 1fr; }
      .head h1 { font-size: 1.2rem; }
    }
  </style>
</head>
<body>
  <section class="card">
    <header class="head">
      <h1>Patch Remove Data</h1>
      <p>Remove data points by time range and optional value threshold.</p>
    </header>

    <div class="grid">
      <div class="field full">
        <label for="seriesIds">Series IDs (one per line)</label>
        <textarea id="seriesIds" placeholder="vertriqe_25567_cttp&#10;vertriqe_25568_cttp"></textarea>
      </div>
      <div class="field">
        <label for="startTime">Start Time (Unix seconds)</label>
        <input id="startTime" type="number" />
      </div>
      <div class="field">
        <label for="endTime">End Time (Unix seconds)</label>
        <input id="endTime" type="number" />
      </div>
      <div class="field full">
        <label for="dataPointGreaterThan">Data Point Greater Than (optional)</label>
        <input id="dataPointGreaterThan" type="number" step="any" placeholder="e.g. 0.05" />
      </div>
      <div class="field full">
        <label for="dataPointLessThan">Data Point Less Than (optional)</label>
        <input id="dataPointLessThan" type="number" step="any" placeholder="e.g. 1.25" />
      </div>
    </div>

    <div class="actions">
      <button id="runBtn" type="button">Run Patch</button>
      <span id="status"></span>
    </div>
    <pre id="output">Waiting for request...</pre>
  </section>

  <script>
    $(function () {
      function setStatus(message, isError) {
        $("#status").text(message).toggleClass("error", !!isError);
      }

      $("#runBtn").on("click", function () {
        var ids = $("#seriesIds").val().split(/\r?\n/).map(function (x) { return x.trim(); }).filter(Boolean);
        var startTime = Number($("#startTime").val());
        var endTime = Number($("#endTime").val());
        var rawGreaterThan = $("#dataPointGreaterThan").val().trim();
        var rawLessThan = $("#dataPointLessThan").val().trim();

        if (!ids.length) {
          setStatus("Please provide at least one series ID.", true);
          return;
        }
        if (!startTime || !endTime || endTime < startTime) {
          setStatus("Please provide valid start/end unix times.", true);
          return;
        }

        var payload = {
          seriesIds: ids,
          startTime: startTime,
          endTime: endTime
        };

        if (rawGreaterThan !== "") {
          payload.dataPointGreaterThan = Number(rawGreaterThan);
        }
        if (rawLessThan !== "") {
          payload.dataPointLessThan = Number(rawLessThan);
        }

        setStatus("Processing...", false);
        $("#output").text("Running patch request...");

        $.ajax({
          url: "/api/remove-data",
          method: "POST",
          contentType: "application/json",
          data: JSON.stringify(payload)
        }).done(function (res) {
          setStatus("Completed.", false);
          $("#output").text(JSON.stringify(res, null, 2));
        }).fail(function (xhr) {
          var msg = xhr.responseText || "Request failed";
          setStatus("Request failed.", true);
          $("#output").text(msg);
        });
      });
    });
  </script>
</body>
</html>
`
