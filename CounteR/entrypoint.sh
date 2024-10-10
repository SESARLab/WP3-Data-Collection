#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

echo "Starting TOR ..."
service tor start

echo "Checking TOR status ..."
service tor status

echo "Starting socialConnectorSwagger ..."
python socialConnectorSwagger.py
