#!/bin/bash

# Make the script exit on any error
set -e

echo "Building aqueducts-executor..."
# Build from the project root
(cd .. && cargo build)

echo "Starting aqueducts-executor for testing..."
# Start server with test API key in the background
AQUEDUCTS_API_KEY=test-api-key AQUEDUCTS_LOG_LEVEL=info ../target/debug/aqueducts-executor &
SERVER_PID=$!

# Give the server some time to start
echo "Waiting for server to start..."
sleep 2

# Run the hurl tests
echo "Running API tests with hurl..."
HURL_EXIT_CODE=0
hurl --test tests/api_tests.hurl || HURL_EXIT_CODE=$?

# Clean up - kill the server
echo "Stopping server..."
kill $SERVER_PID

# Exit with the same code as hurl
exit $HURL_EXIT_CODE