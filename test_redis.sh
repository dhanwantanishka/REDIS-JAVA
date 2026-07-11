#!/bin/bash
# Test script for Antigravity Redis

echo "Building project..."
mvn -q -B package -Ddir=.

echo "Starting Redis server on port 6379 (HTTP Dashboard on 8080)..."
java -jar codecrafters-redis.jar &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test function
run_test() {
    local cmd_name="$1"
    local raw_resp="$2"
    echo -n "Testing $cmd_name... "
    # Parse RESP response slightly
    if [[ "$raw_resp" == *"+PONG"* || "$raw_resp" == *"+OK"* || "$raw_resp" == *":1"* || "$raw_resp" == *"\$-1"* ]]; then
        echo -e "\e[32mPASS\e[0m (Response: $(echo -n "$raw_resp" | tr -d '\r\n'))"
    else
        echo -e "\e[31mFAIL\e[0m (Response: $(echo -n "$raw_resp" | tr -d '\r\n'))"
    fi
}

# 1. Test PING
RESP_PING=$(echo -e "*1\r\n\$4\r\nPING\r\n" | nc -w 1 localhost 6379)
run_test "PING" "$RESP_PING"

# 2. Test SET
RESP_SET=$(echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\ntest\r\n\$5\r\nhello\r\n" | nc -w 1 localhost 6379)
run_test "SET test hello" "$RESP_SET"

# 3. Test GET
RESP_GET=$(echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\ntest\r\n" | nc -w 1 localhost 6379)
if [[ "$RESP_GET" == *"$5"* && "$RESP_GET" == *"hello"* ]]; then
    echo -e "Testing GET test... \e[32mPASS\e[0m"
else
    echo -e "Testing GET test... \e[31mFAIL\e[0m (Response: $RESP_GET)"
fi

# 4. Test EXISTS
RESP_EXISTS=$(echo -e "*2\r\n\$6\r\nEXISTS\r\n\$4\r\ntest\r\n" | nc -w 1 localhost 6379)
run_test "EXISTS test" "$RESP_EXISTS"

# 5. Test DEL
RESP_DEL=$(echo -e "*2\r\n\$3\r\nDEL\r\n\$4\r\ntest\r\n" | nc -w 1 localhost 6379)
run_test "DEL test" "$RESP_DEL"

# 6. Test GET after DEL (should be nil/$-1)
RESP_GET_NIL=$(echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\ntest\r\n" | nc -w 1 localhost 6379)
run_test "GET test (after DEL)" "$RESP_GET_NIL"

# 7. Test MULTI / EXEC Transaction
RESP_TX=$(echo -e "*1\r\n\$5\r\nMULTI\r\n*3\r\n\$3\r\nSET\r\n\$2\r\ntx\r\n\$3\r\nval\r\n*2\r\n\$3\r\nGET\r\n\$2\r\ntx\r\n*1\r\n\$4\r\nEXEC\r\n" | nc -w 1 localhost 6379)
if [[ "$RESP_TX" == *"+OK"* && "$RESP_TX" == *"+QUEUED"* && "$RESP_TX" == *"*2"* && "$RESP_TX" == *"+OK"* && "$RESP_TX" == *"val"* ]]; then
    echo -e "Testing MULTI/EXEC transaction... \e[32mPASS\e[0m"
else
    echo -e "Testing MULTI/EXEC transaction... \e[31mFAIL\e[0m (Response: $RESP_TX)"
fi

# 8. Test HTTP Dashboard health
HTTP_PING=$(curl -s http://localhost:8080/api/ping)
if [[ "$HTTP_PING" == *"PONG"* ]]; then
    echo -e "Testing HTTP Dashboard API (/api/ping)... \e[32mPASS\e[0m"
else
    echo -e "Testing HTTP Dashboard API (/api/ping)... \e[31mFAIL\e[0m (Response: $HTTP_PING)"
    # Try alternate port or display error
fi

echo "Stopping Redis server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
echo "Done!"
