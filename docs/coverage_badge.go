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
	generateSVGBarChart(fullPath, stats, coverage, overallCovered, overallTotal)
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
		return "#e05d44"
	case pct < 70:
		return "#dfb317"
	case pct < 80:
		return "#a4a61d"
	case pct < 90:
		return "#97ca00"
	default:
		return "#44cc11"
	}
}

func generateSVGBadge(path string, coverage float64) {
	color := coverageColor(coverage)
	label := "coverage"
	value := fmt.Sprintf("%.1f%%", coverage)

	// Measure approximate text widths (Verdana 11px, ~6.5px per char)
	labelW := len(label)*7 + 20
	valueW := len(value)*7 + 20
	totalW := labelW + valueW
	labelCenter := labelW / 2
	valueCenter := labelW + valueW/2

	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="20">
  <defs>
    <linearGradient id="s" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#fff" stop-opacity=".18"/>
      <stop offset="0.5" stop-color="#fff" stop-opacity=".06"/>
      <stop offset="1" stop-color="#000" stop-opacity=".1"/>
    </linearGradient>
    <linearGradient id="left" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#4a4a4a"/>
      <stop offset="1" stop-color="#333"/>
    </linearGradient>
    <filter id="shadow">
      <feDropShadow dx="0" dy="1" stdDeviation="0.5" flood-opacity="0.15"/>
    </filter>
  </defs>
  <clipPath id="r">
    <rect width="%d" height="20" rx="4" fill="#fff"/>
  </clipPath>
  <g clip-path="url(#r)" filter="url(#shadow)">
    <rect width="%d" height="20" fill="url(#left)"/>
    <rect x="%d" width="%d" height="20" fill="%s"/>
    <rect width="%d" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,DejaVu Sans,sans-serif" font-size="11">
    <text x="%d" y="14" fill="#000" opacity=".25">%s</text>
    <text x="%d" y="13.5">%s</text>
    <text x="%d" y="14" fill="#000" opacity=".25">%s</text>
    <text x="%d" y="13.5">%s</text>
  </g>
</svg>`, totalW, totalW, labelW, labelW, valueW, color, totalW, labelCenter, label, labelCenter, label, valueCenter, value, valueCenter, value)
	os.WriteFile(path, []byte(svg), 0644)
}

func generateSVGBarChart(path string, stats map[string]*fileStats, overall float64, covered, total int) {
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	barH, gap, topMargin := 22, 6, 48
	chartWidth, chartHeight := 750, topMargin+len(keys)*(barH+gap)+30
	maxBarW := chartWidth - 360

	var bars strings.Builder
	y := topMargin
	for _, k := range keys {
		s := stats[k]
		pct := float64(s.covered) / float64(s.total) * 100
		barW := int(pct / 100 * float64(maxBarW))
		if barW < 2 {
			barW = 2
		}
		color := coverageColor(pct)
		bars.WriteString(fmt.Sprintf(
			`  <text x="12" y="%d" font-family="Verdana,DejaVu Sans,sans-serif" font-size="12" fill="#444">%s</text>
  <rect x="240" y="%d" width="%d" height="%d" rx="3" fill="%s"/>
  <rect x="%d" y="%d" width="%d" height="%d" rx="3" fill="%s" opacity="0.15"/>
  <text x="%d" y="%d" font-family="Verdana,DejaVu Sans,sans-serif" font-size="11" fill="#555">%.1f%% (%d/%d)</text>
`, y+15, k, y+2, barW, barH, color, 240+barW, y+2, maxBarW-barW, barH, color, 240+maxBarW+8, y+15, pct, s.covered, s.total))
		y += barH + gap
	}

	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d">
  <rect width="%d" height="%d" fill="#fafafa"/>
  <text x="12" y="30" font-family="Verdana,DejaVu Sans,sans-serif" font-size="16" font-weight="bold" fill="#333">Coverage: %.1f%% (%d/%d statements)</text>
%s</svg>`, chartWidth, chartHeight, chartWidth, chartHeight, overall, covered, total, bars.String())
	os.WriteFile(path, []byte(svg), 0644)
}
