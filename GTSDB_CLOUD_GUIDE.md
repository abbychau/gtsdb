# GTSDB Cloud User Guide

Welcome to GTSDB Cloud! This guide explains how to manage users and interact with the database using the HTTP API.

## 1. Authentication

All requests to the API require a Bearer Token in the `Authorization` header, except when the server is configured with a default `no_auth_user`.

**Header Format:**
```
Authorization: Bearer <your-token>
```

## 2. User Management

User management operations are performed by sending a POST request to the root endpoint `/` with specific operations. These commands generally require **root** privileges.

### Create a New User

To create a new user, send an `adduser` operation.

**Request:**
*   **Method:** `POST`
*   **URL:** `/`
*   **Headers:** `Authorization: Bearer <root-token>`
*   **Body:**
    ```json
    {
      "operation": "adduser",
      "key": "alice"
    }
    ```

**Response:**
```json
{
  "success": true,
  "data": {
    "name": "alice",
    "token": "a1b2c3d4..."
  }
}
```

### Reset User Token

To generate a new token for an existing user.

**Request:**
*   **Method:** `POST`
*   **URL:** `/`
*   **Headers:** `Authorization: Bearer <root-token>`
*   **Body:**
    ```json
    {
      "operation": "resetkey",
      "key": "alice"
    }
    ```

**Response:**
```json
{
  "success": true,
  "data": {
    "token": "new-token-value..."
  }
}
```

## 3. Data Operations

All data operations are isolated. Users can only access data associated with their account.

### Write Data

**Request:**
*   **Method:** `POST`
*   **URL:** `/`
*   **Headers:** `Authorization: Bearer <user-token>`
*   **Body:**
    ```json
    {
      "operation": "write",
      "key": "cpu_usage",
      "write": {
        "value": 45.5,
        "timestamp": 1638230000
      }
    }
    ```

### Read Data

**Request:**
*   **Method:** `POST`
*   **URL:** `/`
*   **Headers:** `Authorization: Bearer <user-token>`
*   **Body:**
    ```json
    {
      "operation": "read",
      "key": "cpu_usage",
      "read": {
        "lastx": 10
      }
    }
    ```
    *Or by time range:*
    ```json
    {
      "operation": "read",
      "key": "cpu_usage",
      "read": {
        "start_timestamp": 1638200000,
        "end_timestamp": 1638230000,
        "aggregation": "avg",
        "downsampling": 60
      }
    }
    ```

### List Keys

Returns only the keys belonging to the authenticated user.

**Request:**
*   **Method:** `POST`
*   **URL:** `/`
*   **Headers:** `Authorization: Bearer <user-token>`
*   **Body:**
    ```json
    {
      "operation": "ids"
    }
    ```

### Subscribe (Server-Sent Events)

**Request:**
*   **Method:** `POST`
*   **URL:** `/`
*   **Headers:** `Authorization: Bearer <user-token>`
*   **Body:**
    ```json
    {
      "operation": "subscribe",
      "key": "cpu_usage"
    }
    ```
*   **Response:** A stream of `text/event-stream` data updates.

## 4. TCP Interface

You can also connect via TCP (default port 5555).

1.  **Connect**: Open a TCP connection.
2.  **Authenticate**: Send `{"operation": "auth", "key": "<your-token>"}`.
3.  **Operate**: Send standard JSON operation objects (same as HTTP body).
