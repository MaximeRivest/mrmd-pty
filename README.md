# mrmd-pty

PTY WebSocket server for terminal blocks in MRMD documents.

## Overview

`mrmd-pty` provides persistent pseudo-terminal (PTY) sessions accessible via WebSocket. It's designed for embedding interactive terminals in MRMD markdown documents using ` ```term` code blocks.

## Features

- **Persistent sessions** - PTY sessions survive client disconnects
- **Multi-client support** - Multiple clients can view the same terminal
- **Buffer replay** - Reconnecting clients see previous output (last 64KB)
- **Virtual environment support** - Activate venvs in terminal sessions
- **Session management** - Create, list, rename, and kill terminals via REST API

## Installation

```bash
pip install mrmd-pty
```

Or with uv:

```bash
uv pip install mrmd-pty
```

## Usage

### Start the server

```bash
# Default: localhost:8765
mrmd-pty

# Custom host/port
mrmd-pty --host 0.0.0.0 --port 9000
```

### WebSocket API

Connect to `ws://localhost:8765/api/pty` with query parameters:

| Parameter | Description |
|-----------|-------------|
| `session_id` | Unique session identifier |
| `cwd` | Working directory (optional) |
| `venv` | Path to venv's python (optional) |
| `file_path` | Associated file path (optional) |

**Messages to server:**

```json
// User input
{"type": "input", "data": "ls -la\n"}

// Resize terminal
{"type": "resize", "cols": 80, "rows": 24}
```

**Messages from server:**

Raw terminal output as text.

### REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/terminals` | GET | List all terminals |
| `/api/terminals` | POST | Create terminal (pre-allocate) |
| `/api/terminals/rename` | POST | Rename a terminal |
| `/api/pty/kill` | POST | Kill a terminal session |
| `/api/terminals/kill-for-file` | POST | Kill terminals for a file |

### Programmatic usage

```python
from aiohttp import web
from mrmd_pty import setup_pty_routes, create_app

# Option 1: Use standalone app
app = create_app()
web.run_app(app, port=8765)

# Option 2: Add routes to existing app
app = web.Application()
setup_pty_routes(app)
# ... add your other routes
web.run_app(app)
```

## Integration with mrmd-editor

The `mrmd-editor` package includes client-side components for ` ```term` blocks:

- `term-widget.js` - CodeMirror widget embedding xterm.js
- `term-pty-client.js` - WebSocket client for PTY communication
- `term-block.js` - Model for terminal block state

## Architecture

```
Browser                          Server (mrmd-pty)
┌──────────────┐                ┌───────────────────┐
│  xterm.js    │◄──WebSocket───►│  PtySession       │
│  (terminal   │                │  (pty.openpty())  │
│   emulator)  │                │                   │
└──────────────┘                └───────────────────┘
       ↑                                ↑
   keystrokes                     PTY master fd
   renders output                 reads/writes
```

## License

MIT
