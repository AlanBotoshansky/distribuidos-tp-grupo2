#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <expected_results_path> <actual_results_path>"
    exit 1
fi

EXPECTED_DIR=$1
ACTUAL_DIR=$2

if [ ! -d "$EXPECTED_DIR" ]; then
    echo -e "${RED}Expected results directory does not exist: $EXPECTED_DIR${NC}"
    exit 1
fi

if [ ! -d "$ACTUAL_DIR" ]; then
    echo -e "${RED}Actual results directory does not exist: $ACTUAL_DIR${NC}"
    exit 1
fi

for expected_file in "$EXPECTED_DIR"/*.csv; do
    filename=$(basename "$expected_file")
    actual_file="$ACTUAL_DIR/$filename"

    if [ ! -f "$actual_file" ]; then
        echo -e "${RED}Missing actual results file: $actual_file${NC}"
        continue
    fi

    expected_header=$(head -n 1 "$expected_file")
    actual_header=$(head -n 1 "$actual_file")
    
    if [ "$expected_header" != "$actual_header" ]; then
        echo -e "${RED}Headers differ in file: $filename${NC}"
        continue
    fi
    
    if diff <(tail -n +2 "$expected_file" | sort) <(tail -n +2 "$actual_file" | sort) > /dev/null; then
        echo -e "${GREEN}Files match: $filename${NC}"
    else
        echo -e "${RED}Files differ: $filename${NC}"
        diff <(tail -n +2 "$expected_file" | sort) <(tail -n +2 "$actual_file" | sort)
    fi
done