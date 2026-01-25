"""
CLI for mrmd-pty server.

Usage:
    mrmd-pty [--host HOST] [--port PORT]

Example:
    mrmd-pty --port 8765
"""

from __future__ import annotations

import argparse
import sys

from aiohttp import web

from .server import create_app


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="PTY WebSocket server for MRMD terminal blocks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Start server on default port (8765)
    mrmd-pty

    # Start server on custom port
    mrmd-pty --port 9000

    # Start server on all interfaces
    mrmd-pty --host 0.0.0.0 --port 8765
        """,
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to listen on (default: 8765)",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="mrmd-pty 0.1.0",
    )

    args = parser.parse_args()

    print(f"[mrmd-pty] Starting server on {args.host}:{args.port}")
    print(f"[mrmd-pty] WebSocket endpoint: ws://{args.host}:{args.port}/api/pty")
    print(f"[mrmd-pty] REST endpoints:")
    print(f"           GET  /api/terminals        - List terminals")
    print(f"           POST /api/terminals        - Create terminal")
    print(f"           POST /api/terminals/rename - Rename terminal")
    print(f"           POST /api/pty/kill         - Kill terminal")
    print()

    app = create_app()

    try:
        web.run_app(app, host=args.host, port=args.port, print=None)
    except KeyboardInterrupt:
        print("\n[mrmd-pty] Shutting down...")
        return 0
    except Exception as e:
        print(f"[mrmd-pty] Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
