#!/bin/bash
set -e

# Load environment if .env exists
if [ -f .env ]; then
  export $(cat .env | grep -v '#' | xargs)
fi

echo "=============================================="
echo " Running Holdex API Payload Drift Detection   "
echo "=============================================="

# Run the API payload snapshot drift tests
go test -v -run TestAPIPayloadDrift ./test/...

echo "=============================================="
echo " Drift check passed successfully! No drift.   "
echo "=============================================="
