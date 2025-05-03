#!/bin/bash
# Test script for the aqueducts remote execution capabilities

# Ensure the executor is running
echo "Checking if executor is running..."
curl -s http://localhost:3031/health || {
  echo "Error: Cannot connect to executor at http://localhost:3031"
  echo "Please start the executor with: cargo run --bin aqueducts-executor -- --port 3031 --api-key test_key"
  exit 1
}

# Define API key (should match the one used to start the executor)
API_KEY="test_key"

# Define executor URL
EXECUTOR_URL="http://localhost:3031"

# Ensure example files exist
echo "Checking example files..."
EXAMPLE_DIR="/home/kato/Development/repos/aqueducts/examples"
PIPELINE_FILE="$EXAMPLE_DIR/aqueduct_pipeline_simple.yml"

if [ ! -f "$PIPELINE_FILE" ]; then
  echo "Error: Example pipeline file not found at $PIPELINE_FILE"
  exit 1
fi

# Testing the different CLI commands
echo "1. Testing command: status"
cargo run --bin aqueducts -- status --executor $EXECUTOR_URL --api-key $API_KEY

echo ""
echo "2. Testing command: run with local execution"
cargo run --bin aqueducts -- run --file $PIPELINE_FILE --params year=2024 --params month=jan

echo ""
echo "3. Testing command: run with remote execution"
cargo run --bin aqueducts -- run --file $PIPELINE_FILE --params year=2024 --params month=jan --executor $EXECUTOR_URL --api-key $API_KEY

echo ""
echo "Test complete."
echo "You can manually test pipeline cancellation with:"
echo "cargo run --bin aqueducts -- cancel --executor $EXECUTOR_URL --api-key $API_KEY"