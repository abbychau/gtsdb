# GTSDB TCP Protocol

GTSDB supports a TCP interface (default port **5555**) for low-latency operations and real-time data subscriptions.

## Connection

Open a TCP connection to the server:

```bash
nc localhost 5555
# or via telnet
telnet localhost 5555
```

## Authentication

All operations (except `auth`) require prior authentication. Send an `auth` operation immediately after connecting:

**Request:**
```json
{"operation": "auth", "key": "<your-token>"}
```

**Response:**
```json
{"success": true, "message": "Authenticated as <username>"}
```

If authentication fails or is not provided, the server will return:

```json
{"success": false, "message": "Authentication required"}
```

## Protocol Format

The protocol uses **JSON-line** format — each request and response is a single line of JSON terminated by a newline (`\n`).

### Request Format
```json
{"operation": "<operation_name>", ...parameters...}
```

### Response Format
```json
{"success": true/false, "message": "...", "data": ...}
```

## Supported Operations

All HTTP API operations are supported over TCP. See the full [API Reference](openapi.yaml) for parameter details.

### Basic Operations

| Operation | Description |
|-----------|-------------|
| `write` | Store a data point |
| `read` | Read data points (range or last N) |
| `multi-read` | Batch read across multiple keys |
| `data-patch` | Bulk insert/upsert data |
| `deleteDataPoint` | Delete data points by value or time range |
| `ids` | List all accessible keys |
| `idswithcount` | List keys with data point counts |
| `flush` | Flush all data to disk |

### Key Management

| Operation | Description |
|-----------|-------------|
| `initkey` | Initialize a new key |
| `renamekey` | Rename a key |
| `deletekey` | Delete a key and its data |
| `reloadkey` | Reload a key from disk |
| `compact` | Compact WAL file for a key |

### Administrative (root only)

| Operation | Description |
|-----------|-------------|
| `adduser` | Create a new user |
| `resetkey` | Reset a user's authentication token |
| `serverinfo` | Get server information and metrics |

## Subscriptions

TCP connections support multiple simultaneous subscriptions.

### Subscribe

```json
{"operation": "subscribe", "key": "sensor1"}
```

Optional **historical replay** — specify a `since` timestamp to receive past data:

```json
{"operation": "subscribe", "key": "sensor1", "since": 1717965210}
```

**Response** (immediate):
```json
{"success": true, "message": "Subscribed to sensor1"}
```

**Updates** (real-time):
```json
{"success": true, "data": {"key": "sensor1", "timestamp": 1717965210, "value": 42.5}}
```

### Unsubscribe

```json
{"operation": "unsubscribe", "key": "sensor1"}
```

**Response:**
```json
{"success": true, "message": "Unsubscribed from sensor1"}
```

## Ping / Keepalive

The server sends a **ping** message every 30 seconds to keep the connection alive:

```json
{"success": true, "message": "ping"}
```

If the ping fails (connection lost), the server automatically removes all subscriptions for that connection.

## Key Prefixing

When using multi-user authentication, keys are automatically prefixed with the username.
For example, if user `alice` subscribes to `sensor1`, the server internally manages `alice/sensor1`.
Responses strip the prefix, so the client sees `sensor1`.

## Example Session

```
# Connect
→ nc localhost 5555

# Authenticate
→ {"operation": "auth", "key": "abc123def456..."}
← {"success": true, "message": "Authenticated as alice"}

# Write data
→ {"operation": "write", "key": "cpu_temp", "write": {"value": 72.5}}
← {"success": true, "message": "Data point stored"}

# Read last 5 points
→ {"operation": "read", "key": "cpu_temp", "read": {"lastx": 5}}
← {"success": true, "data": [...], "read_query_params": {"lastx": 5, "aggregation": "avg"}}

# Subscribe to real-time updates
→ {"operation": "subscribe", "key": "cpu_temp"}
← {"success": true, "message": "Subscribed to cpu_temp"}
← {"success": true, "data": {"key": "cpu_temp", "timestamp": ..., "value": 73.1}}
```
