#!/bin/bash
python3 docker_compose_generator/main.py

if [ $? -eq 0 ]; then
    echo "Compose file generated successfully"
else
    echo "Failed to generate compose file"
    exit 1
fi
