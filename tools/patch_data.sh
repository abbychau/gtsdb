#!/bin/bash

# Script to patch data for vertriqe_25416_cttp and vertriqe_25415_cttp
# Removes data points from December 8th to 12th, 2025

echo "Data Patch Script - Remove data from Dec 8-12, 2025"
echo "Series: vertriqe_25416_cttp, vertriqe_25415_cttp"
echo ""
echo "WARNING: This will modify data files. Backups will be created automatically."
echo ""
read -p "Do you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Building and running patch tool..."
cd "$(dirname "$0")/.."

# Build the tool
go run tools/patch_remove_data.go

echo ""
echo "Done! Check the output above for results."
echo "Backup files have been created with .backup.[timestamp] extension."

pm2 restart gtsdb
echo "GTSDB service restarted."