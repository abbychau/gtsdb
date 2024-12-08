<?php

class CoverageAnalyzer {
    private $files = [];
    private $totalStmts = 0;
    private $coveredStmts = 0;

    public function analyze($filename) {
        $lines = file($filename);
        array_shift($lines); // remove mode: set

        foreach ($lines as $line) {
            if (empty(trim($line))) continue;
            
            // Parse line: file:start.col,end.col statements coverage
            $parts = preg_split('/[\s]+/', trim($line));
            $file = explode(':', $parts[0])[0];
            $stmts = intval($parts[count($parts)-2]);
            $covered = intval($parts[count($parts)-1]) === 1;

            if (!isset($this->files[$file])) {
                $this->files[$file] = ['total' => 0, 'covered' => 0];
            }
            
            $this->files[$file]['total'] += $stmts;
            if ($covered) {
                $this->files[$file]['covered'] += $stmts;
            }
            
            $this->totalStmts += $stmts;
            if ($covered) {
                $this->coveredStmts += $stmts;
            }
        }
    }

    public function generateImage($width = 600, $height = 300) {
        $img = imagecreatetruecolor($width, $height);
        $white = imagecolorallocate($img, 255, 255, 255);
        $green = imagecolorallocate($img, 75, 181, 67);
        $red = imagecolorallocate($img, 181, 67, 67);
        $black = imagecolorallocate($img, 0, 0, 0);
        
        imagefill($img, 0, 0, $white);
        
        // Draw title
        $coverage = round(($this->coveredStmts / $this->totalStmts) * 100, 2);
        imagestring($img, 5, 10, 10, "Coverage Report: {$coverage}%", $black);
        
        // Draw bars
        $barHeight = 20;
        $y = 50;
        $maxBarWidth = $width - 320;
        
        foreach ($this->files as $file => $data) {
            $percent = ($data['covered'] / $data['total']) * 100;
            $barWidth = ($percent / 100) * $maxBarWidth;
            
            // Draw file name
            $txt="";
            // last 2 parts of the file name
            $parts = explode("/", $file);
            $txt = $parts[count($parts)-2] . "/" . $parts[count($parts)-1];
            

            imagestring($img, 3, 10, $y, $txt, $black);
            
            //make them int
            $barWidth = (int) $barWidth;
            $barHeight = (int) $barHeight;

            // Draw coverage bar
            imagefilledrectangle($img, 200, $y, 200 + $barWidth, $y + $barHeight, $green);
            imagefilledrectangle($img, 200 + $barWidth, $y, 200 + $maxBarWidth, $y + $barHeight, $red);
            
            // Draw percentage
            imagestring($img, 3, 200 + $maxBarWidth + 10, $y, round($percent, 1) . '%', $black);
            
            $y += $barHeight + 10;
        }
        
        imagepng($img, 'docs/coverage-full.png');
        imagedestroy($img);
    }

    public function generateImageOnlyTotalCoverage($width = 165, $height=35){
        $img = imagecreatetruecolor($width, $height);
        $background = imagecolorallocate($img, 10,10,10);
        $foreground = imagecolorallocate($img, 255,255,255);
        
        imagefill($img, 0, 0, $background);
        
        // Draw title
        $coverage = round(($this->coveredStmts / $this->totalStmts) * 100, 2);
        imagestring($img, 2, 25, 10, "Coverage:", $foreground);

        $textColor = imagecolorallocate($img, 0, 255, 0);
        if ($coverage < 50) {
            $textColor = imagecolorallocate($img, 255, 0, 0);
        } else if ($coverage < 80) {
            $textColor = imagecolorallocate($img, 255, 255, 0);
        }

        imagestring($img, 2, 100, 10, "{$coverage}%", $textColor);

        imagepng($img, 'docs/coverage.png');
        imagedestroy($img);

    }
    
    public function printTextReport() {
        foreach ($this->files as $file => $data) {
            $percent = round(($data['covered'] / $data['total']) * 100, 2);
            printf("%s: %.2f%% (%d/%d statements)\n", 
                $file, $percent, $data['covered'], $data['total']);
        }
        
        $totalPercent = round(($this->coveredStmts / $this->totalStmts) * 100, 2);
        printf("\nTotal Coverage: %.2f%% (%d/%d statements)\n",
            $totalPercent, $this->coveredStmts, $this->totalStmts);
    }
}

$analyzer = new CoverageAnalyzer();
$analyzer->analyze('docs/coverage');

// Generate text report
//$analyzer->printTextReport();

// Generate image (uncomment to output PNG instead of text)
$analyzer->generateImage();
$analyzer->generateImageOnlyTotalCoverage();

