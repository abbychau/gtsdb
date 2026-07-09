//go:build ignore

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type fileStats struct {
	total   int
	covered int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: go run docs/coverage_badge.go <coverprofile>")
		os.Exit(1)
	}
	stats, overallTotal, overallCovered := parseCoverProfile(os.Args[1])
	if overallTotal == 0 {
		fmt.Fprintln(os.Stderr, "No coverage data found")
		os.Exit(1)
	}
	coverage := float64(overallCovered) / float64(overallTotal) * 100

	fmt.Printf("\n=== Coverage Summary ===\n")
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := stats[k]
		fmt.Printf("  %-30s %5.1f%% (%d/%d)\n", k, float64(s.covered)/float64(s.total)*100, s.covered, s.total)
	}
	fmt.Printf("\n  Total: %.1f%% (%d/%d statements)\n\n", coverage, overallCovered, overallTotal)

	outDir := filepath.Dir(os.Args[1])
	badgePath := filepath.Join(outDir, "coverage.svg")
	generateSVGBadge(badgePath, coverage)
	fmt.Printf("Badge saved: %s\n", badgePath)

	fullPath := filepath.Join(outDir, "coverage-full.svg")
	generateSVGBarChart(fullPath, stats, coverage)
	fmt.Printf("Bar chart saved: %s\n", fullPath)
}

func parseCoverProfile(filename string) (map[string]*fileStats, int, int) {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening coverage file:", err)
		os.Exit(1)
	}
	defer f.Close()
	stats := make(map[string]*fileStats)
	totalStmts, coveredStmts := 0, 0
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}
		file := strings.Split(parts[0], ":")[0]
		stmts, _ := strconv.Atoi(parts[len(parts)-2])
		count, _ := strconv.Atoi(parts[len(parts)-1])
		displayName := shortenPath(file)
		if _, ok := stats[displayName]; !ok {
			stats[displayName] = &fileStats{}
		}
		stats[displayName].total += stmts
		if count > 0 {
			stats[displayName].covered += stmts
		}
		totalStmts += stmts
		if count > 0 {
			coveredStmts += stmts
		}
	}
	return stats, totalStmts, coveredStmts
}

func shortenPath(file string) string {
	if idx := strings.Index(file, "/"); idx != -1 {
		return file[idx+1:]
	}
	return file
}

func coverageColor(pct float64) string {
	switch {
	case pct < 50:
		return "#b54343"
	case pct < 80:
		return "#d4c43a"
	default:
		return "#4bb543"
	}
}

func generateSVGBadge(path string, coverage float64) {
	color := coverageColor(coverage)
	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="170" height="28">
  <rect width="170" height="28" rx="4" fill="#0a0a0a"/>
  <text x="12" y="19" font-family="monospace" font-size="13" fill="#fff">Coverage:</text>
  <text x="90" y="19" font-family="monospace" font-size="13" font-weight="bold" fill="%s">%.1f%%</text>
</svg>`, color, coverage)
	os.WriteFile(path, []byte(svg), 0644)
}

func generateSVGBarChart(path string, stats map[string]*fileStats, overall float64) {
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	barH, gap, topMargin := 20, 8, 40
	chartWidth, chartHeight := 750, topMargin+len(keys)*(barH+gap)+30
	maxBarW := chartWidth - 350

	var bars strings.Builder
	y := topMargin
	for _, k := range keys {
		s := stats[k]
		pct := float64(s.covered) / float64(s.total) * 100
		barW := int(pct / 100 * float64(maxBarW))
		color := coverageColor(pct)
		bars.WriteString(fmt.Sprintf(
			`  <text x="10" y="%d" font-family="monospace" font-size="12" fill="#333">%s</text>
  <rect x="220" y="%d" width="%d" height="%d" rx="2" fill="%s"/>
  <rect x="%d" y="%d" width="%d" height="%d" rx="2" fill="#eee"/>
  <text x="%d" y="%d" font-family="monospace" font-size="12" fill="#333">%.1f%%</text>
`, y+15, k, y+3, barW, barH, color, 220+barW, y+3, maxBarW-barW, barH, 220+maxBarW+10, y+15, pct))
		y += barH + gap
	}

	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d">
  <rect width="%d" height="%d" fill="#fff"/>
  <text x="10" y="25" font-family="monospace" font-size="15" font-weight="bold" fill="#333">Coverage Report: %.1f%%</text>
%s</svg>`, chartWidth, chartHeight, chartWidth, chartHeight, overall, bars.String())
	os.WriteFile(path, []byte(svg), 0644)
}
