#!/bin/bash

binary_file="/mnt/tmp/.wkube_agent/wagt"

if [ ! -f "$binary_file" ]; then
    echo "Error: Binary file not found. Please download it first."
    exit 1
fi

chmod +x "$binary_file"
echo "Executing binary..."
./"$binary_file" "bash command"
