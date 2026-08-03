# GTSDB Multi-User Guide

GTSDB supports token-based authentication with per-user data namespaces. Each user's data is stored in its own `{username}/` directory and isolated from other users.

## User Model

- **Users** are identified by name and authenticated with a bearer token.
- **The `root` user** has a special role: it can create users, reset tokens, and set quotas.
- Every user's data lives under `data/{username}/`, so keys are namespaced: user `alice` writing `sensor1` actually writes `alice/sensor1` on disk.
- Users are persisted in `data/users.json`.

## Getting Started

### 1. Configure the root token

Set a fixed root token in the config file:

```ini
[auth]
root_token = your-secure-root-token
```

If `root_token` is empty, a random token is generated on first start and printed to the console (and saved in `data/users.json`).

### 2. Authenticate

**HTTP** — send the token as a Bearer header on every request:

```http
POST /
Authorization: Bearer <token>
Content-Type: application/json

{"operation": "write", "key": "sensor1", "write": {"value": 42.5}}
```

**TCP** — send an `auth` operation first:

```json
{"operation": "auth", "key": "<token>"}
```

### 3. Create users (root only)

```json
{"operation": "adduser", "key": "alice"}
```

A random token is generated and returned in the response. An empty key auto-generates a username:

```json
{"operation": "adduser"}
```

### 4. Reset a user's token (root only)

```json
{"operation": "resetkey", "key": "alice"}
```

### 5. Set a storage quota (root only)

```json
{"operation": "setquota", "key": "alice", "max_points": 1000000}
```

`max_points = 0` means unlimited (default). Writes that would exceed the quota are rejected with a `Data point storage quota exceeded` message.

## Key Namespacing

Keys are automatically namespaced to the authenticated user:

| User | Request key | Actual key on disk |
|------|-------------|--------------------|
| `alice` | `sensor1` | `alice/sensor1` |
| `alice` | `alice/sensor1` | `alice/sensor1` (already prefixed) |
| `root` | `sensor1` | `root/sensor1` |

- Responses strip the namespace prefix, so clients always see unprefixed keys.
- A user may only access keys in their own namespace (plus `root/` data on the HTTP side for tenants that share it).
- Keys containing `..` (path traversal) are rejected.

## Skip Authentication (not recommended for production)

The `no_auth_user` config option treats unauthenticated requests as a fixed user:

```ini
[auth]
no_auth_user = root
```

Useful for local development or single-tenant deployments behind a trusted network.

## Users File

`data/users.json` stores all users:

```json
[
  {
    "name": "root",
    "token": "your-secure-root-token"
  },
  {
    "name": "alice",
    "token": "abc123...",
    "max_points": 1000000
  }
]
```

> **Security note:** the file contains plaintext tokens. Protect it with filesystem permissions and do not commit it.
