#!/bin/bash

# Function to display current time
display_time() {
    date +%T
}

# Display current time for 20 seconds
for ((i=0; i<16; i++)); do
    echo "Current time: $(display_time)"
    sleep 1
done

# Throw an error after 20 seconds
echo "Last time: $(display_time)"
# flush

# exit 1