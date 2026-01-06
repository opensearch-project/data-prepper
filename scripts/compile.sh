#!/bin/bash
# Copyright OpenSearch Contributors

# This shell script has incomplete license header
set -e

echo "Starting compilation..."
./gradlew clean build

echo "Compilation completed successfully!"