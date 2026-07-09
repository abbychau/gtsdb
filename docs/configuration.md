# GTSDB Configuration Reference

GTSDB uses an INI configuration file (default: `gtsdb.ini`). You can specify a custom config file as a command-line argument:

```bash
./gtsdb myconfig.ini
```

## File Format

```ini
[listens]
tcp = :5555
http = :5556

[paths]
data = data

[auth]
no_auth_user = ""
root_token = ""

[buffer]
file_handle_lru_capacity = 700
```

## Sections

### `[listens]` — Network Listen Addresses

| Key | Default | Description |
|-----|---------|-------------|
| `tcp` | `:5555` | TCP server listen address (host:port) |
| `http` | `:5556` | HTTP server listen address (host:port) |

Both support:
- `:port` — Listen on all interfaces
- `host:port` — Listen on specific interface
- `localhost:port` — Listen on localhost only

### `[paths]` — Data Directory

| Key | Default | Description |
|-----|---------|-------------|
| `data` | `data` | Directory for storing WAL files (.aof), index files (.idx), and user data |

The data directory structure:
```
data/
├── users.json         # User credentials
├── root/              # Root user's data
│   ├── sensor1.aof    # WAL data file
│   ├── sensor1.idx    # Sparse index file
│   └── ...
└── username/          # Other users' data (isolated by folders)
    ├── sensor1.aof
    └── sensor1.idx
```

### `[auth]` — Authentication

| Key | Default | Description |
|-----|---------|-------------|
| `no_auth_user` | `""` | Skip authentication for this username (not recommended for production) |
| `root_token` | `""` | Pre-set root token. If empty, a random token is generated on first start |

### `[buffer]` — Buffer and Cache Settings

| Key | Default | Description |
|-----|---------|-------------|
| `file_handle_lru_capacity` | `700` | Maximum number of open file handles. Must be less than OS limit (typically 1024 per process on Linux with ulimit). Reduce for weak hardware. |

## Advanced Configuration (Environment Variables)

Not yet supported. All configuration must be in the INI file.

## Example Configurations

### Minimal (all defaults)
```ini
[listens]
tcp = :5555
http = :5556

[paths]
data = data
```

### Production with authentication
```ini
[listens]
tcp = 0.0.0.0:5555
http = 0.0.0.0:5556

[paths]
data = /var/lib/gtsdb

[auth]
root_token = "your-secure-root-token-here"

[buffer]
file_handle_lru_capacity = 700
```

### Development / Testing
```ini
[listens]
tcp = localhost:5555
http = localhost:5556

[paths]
data = ./testdata

[auth]
no_auth_user = root
```
