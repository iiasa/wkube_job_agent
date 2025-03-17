#!/bin/bash

binary_url="https://testwithfastapi.s3.amazonaws.com/wagt-v0.5.4-linux-amd/wagt"
binary_file="/mnt/agent/wagt"

mkdir -p /mnt/agent

download_with_curl() {
    if command -v curl &>/dev/null; then
        curl -sSL "$binary_url" -o "$binary_file"
        return $?
    else
        return 1
    fi
}

download_with_wget() {
    if command -v wget &>/dev/null; then
        wget -q "$binary_url" -O "$binary_file"
        return $?
    else
        return 1
    fi
}

if download_with_curl; then
    echo "Wagt downloaded successfully with curl."
elif download_with_wget; then
    echo "Wagt downloaded successfully with wget."
else
    echo "Error: Neither curl nor wget is available."
    exit 1
fi

chmod +x "$binary_file"


