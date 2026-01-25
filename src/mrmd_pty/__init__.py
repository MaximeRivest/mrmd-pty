"""
mrmd-pty - PTY WebSocket server for terminal blocks

Provides persistent pseudo-terminal sessions accessible via WebSocket.
Designed for embedding interactive terminals in MRMD documents.

Features:
- Persistent PTY sessions that survive client disconnects
- Multiple clients can view the same terminal
- Buffer replay on reconnect
- Virtual environment activation support
"""

__version__ = "0.1.0"

from .server import (
    PtySession,
    TerminalMeta,
    create_app,
    setup_pty_routes,
    get_terminal_list,
    get_terminal_meta,
)

__all__ = [
    "__version__",
    "PtySession",
    "TerminalMeta",
    "create_app",
    "setup_pty_routes",
    "get_terminal_list",
    "get_terminal_meta",
]
