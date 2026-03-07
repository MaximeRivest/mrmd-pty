"""
PTY WebSocket server for terminal blocks.

Provides WebSocket endpoints that spawn terminal sessions and connect them to
clients for full terminal emulation.

On POSIX this uses a real PTY via pty/fork.
On Windows this uses ConPTY via pywinpty.
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import signal
import struct
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from aiohttp import WSMsgType, web

if TYPE_CHECKING:
    from collections.abc import Callable

IS_WINDOWS = os.name == "nt"

if IS_WINDOWS:
    try:
        from winpty import PtyProcess  # type: ignore
    except ImportError:  # pragma: no cover - depends on Windows env
        PtyProcess = None  # type: ignore[assignment]
else:  # POSIX
    import fcntl
    import pty
    import termios


@dataclass
class TerminalMeta:
    """Metadata for a terminal session."""

    session_id: str
    name: str  # Display name
    cwd: str
    venv: str | None = None
    shell: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    file_path: str | None = None  # Associated file (for notebook terminals)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "name": self.name,
            "cwd": self.cwd,
            "venv": self.venv,
            "shell": self.shell,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "file_path": self.file_path,
        }


class PtySession:
    """Manages a single terminal session with multiple client support."""

    OUTPUT_BUFFER_SIZE = 65536  # 64KB

    def __init__(self, session_id: str):
        self.clients: list[web.WebSocketResponse] = []
        self.session_id = session_id
        self.running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._output_buffer = bytearray()
        self._clients_lock = asyncio.Lock()

        # POSIX state
        self.master_fd: int | None = None
        self.slave_fd: int | None = None
        self.pid: int | None = None

        # Windows state
        self.proc: Any | None = None
        self._reader_task: asyncio.Task | None = None

    async def start(
        self,
        shell: str | None = None,
        cwd: str | None = None,
        venv: str | None = None,
        cols: int = 80,
        rows: int = 24,
    ) -> None:
        """Start the terminal session."""
        self._loop = asyncio.get_event_loop()
        cwd = cwd or os.getcwd()

        if IS_WINDOWS:
            await self._start_windows(shell=shell, cwd=cwd, venv=venv, cols=cols, rows=rows)
        else:
            await self._start_posix(shell=shell, cwd=cwd, venv=venv)

    async def _start_posix(self, shell: str | None, cwd: str, venv: str | None) -> None:
        shell = shell or os.environ.get("SHELL", "/bin/bash")

        self.master_fd, self.slave_fd = pty.openpty()
        self.pid = os.fork()

        if self.pid == 0:
            os.close(self.master_fd)
            os.setsid()
            fcntl.ioctl(self.slave_fd, termios.TIOCSCTTY, 0)
            os.dup2(self.slave_fd, 0)
            os.dup2(self.slave_fd, 1)
            os.dup2(self.slave_fd, 2)

            if self.slave_fd > 2:
                os.close(self.slave_fd)

            try:
                os.chdir(cwd)
            except OSError:
                pass

            env = os.environ.copy()
            env["TERM"] = "xterm-256color"

            if venv:
                venv_bin = os.path.dirname(venv)
                venv_root = os.path.dirname(venv_bin)
                if os.path.isdir(venv_bin):
                    env["VIRTUAL_ENV"] = venv_root
                    env["PATH"] = venv_bin + ":" + env.get("PATH", "")
                    env.pop("PYTHONHOME", None)

            os.execvpe(shell, [shell], env)
        else:
            os.close(self.slave_fd)
            self.slave_fd = None
            self.running = True

            flags = fcntl.fcntl(self.master_fd, fcntl.F_GETFL)
            fcntl.fcntl(self.master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            self._loop.add_reader(self.master_fd, self._on_posix_read)

    async def _start_windows(
        self,
        shell: str | None,
        cwd: str,
        venv: str | None,
        cols: int,
        rows: int,
    ) -> None:
        if PtyProcess is None:
            raise RuntimeError("pywinpty is not installed")

        argv, shell_name, shell_cwd = _resolve_windows_shell(shell, cwd)
        env = os.environ.copy()
        env.setdefault("TERM", "xterm-256color")

        if venv:
            venv_bin = os.path.dirname(venv)
            venv_root = os.path.dirname(venv_bin)
            if os.path.isdir(venv_bin):
                env["VIRTUAL_ENV"] = venv_root
                env["PATH"] = venv_bin + os.pathsep + env.get("PATH", "")
                env.pop("PYTHONHOME", None)

        self.proc = PtyProcess.spawn(
            argv,
            cwd=shell_cwd,
            env=env,
            dimensions=(rows, cols),
        )
        self.running = True
        self._reader_task = asyncio.create_task(self._windows_read_loop())
        print(f"[PTY] Windows terminal started: {shell_name}")

    async def _windows_read_loop(self) -> None:
        while self.running and self.proc is not None:
            try:
                chunk = await asyncio.to_thread(self.proc.read, 65536)
            except EOFError:
                break
            except OSError as e:
                print(f"[PTY] Windows read error: {e}")
                break
            except Exception as e:
                print(f"[PTY] Windows terminal exited: {e}")
                break

            if not chunk:
                await asyncio.sleep(0.01)
                continue

            text = chunk if isinstance(chunk, str) else chunk.decode("utf-8", errors="replace")
            self._append_output(text)
            await self._send_to_ws(text)

        self._cleanup()

    def _append_output(self, text: str) -> None:
        data = text.encode("utf-8", errors="replace")
        self._output_buffer.extend(data)
        if len(self._output_buffer) > self.OUTPUT_BUFFER_SIZE:
            self._output_buffer = self._output_buffer[-self.OUTPUT_BUFFER_SIZE :]

    def _on_posix_read(self) -> None:
        if not self.running or self.master_fd is None:
            return

        try:
            data = os.read(self.master_fd, 65536)
            if data:
                text = data.decode("utf-8", errors="replace")
                self._append_output(text)
                asyncio.create_task(self._send_to_ws(text))
            else:
                self._cleanup()
        except BlockingIOError:
            pass
        except OSError as e:
            if getattr(e, "errno", None) == 5:
                self._cleanup()
            else:
                print(f"[PTY] Read error: {e}")
                self._cleanup()

    async def _send_to_ws(self, text: str) -> None:
        if not self.clients:
            return

        disconnected = []
        async with self._clients_lock:
            for ws in self.clients:
                try:
                    if not ws.closed:
                        await ws.send_str(text)
                except Exception as e:
                    print(f"[PTY] WebSocket send error: {e}")
                    disconnected.append(ws)

            for ws in disconnected:
                if ws in self.clients:
                    self.clients.remove(ws)

    async def replay_buffer(self, ws: web.WebSocketResponse) -> None:
        if self._output_buffer:
            try:
                await ws.send_str(self._output_buffer.decode("utf-8", errors="replace"))
            except Exception as e:
                print(f"[PTY] Buffer replay error: {e}")

    async def add_client(self, ws: web.WebSocketResponse) -> None:
        async with self._clients_lock:
            if ws not in self.clients:
                self.clients.append(ws)
                print(f"[PTY] Client added to {self.session_id}, total clients: {len(self.clients)}")

    async def remove_client(self, ws: web.WebSocketResponse) -> None:
        async with self._clients_lock:
            if ws in self.clients:
                self.clients.remove(ws)
                print(
                    f"[PTY] Client removed from {self.session_id}, remaining clients: {len(self.clients)}"
                )

    def _cleanup(self) -> None:
        self.running = False
        if not IS_WINDOWS and self._loop and self.master_fd is not None:
            try:
                self._loop.remove_reader(self.master_fd)
            except Exception:
                pass

    def write(self, data: str) -> None:
        if not self.running:
            return

        try:
            if IS_WINDOWS and self.proc is not None:
                self.proc.write(data)
            elif self.master_fd is not None:
                os.write(self.master_fd, data.encode("utf-8"))
        except OSError as e:
            print(f"[PTY] Write error: {e}")
        except Exception as e:
            print(f"[PTY] Write error: {e}")

    def resize(self, cols: int, rows: int) -> None:
        try:
            if IS_WINDOWS and self.proc is not None:
                self.proc.setwinsize(rows, cols)
            elif self.master_fd is not None:
                winsize = struct.pack("HHHH", rows, cols, 0, 0)
                fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, winsize)
        except OSError as e:
            print(f"[PTY] Resize error: {e}")
        except Exception as e:
            print(f"[PTY] Resize error: {e}")

    def stop(self) -> None:
        self._cleanup()

        if IS_WINDOWS:
            if self._reader_task:
                self._reader_task.cancel()
                self._reader_task = None
            if self.proc is not None:
                try:
                    self.proc.terminate(force=True)
                except Exception:
                    try:
                        self.proc.close(force=True)
                    except Exception:
                        pass
                self.proc = None
            return

        if self.pid:
            try:
                os.kill(self.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            if self._loop:
                self._loop.call_later(0.5, self._force_kill)

        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except OSError:
                pass
            self.master_fd = None

    def _force_kill(self) -> None:
        if not self.pid:
            return
        try:
            os.kill(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception:
            pass
        try:
            os.waitpid(self.pid, os.WNOHANG)
        except Exception:
            pass


# ==================== Terminal Registry ====================

_pty_sessions: dict[str, PtySession] = {}
_terminal_meta: dict[str, TerminalMeta] = {}


def _generate_terminal_name() -> str:
    existing_names = {meta.name for meta in _terminal_meta.values()}
    if "main" not in existing_names:
        return "main"
    i = 2
    while f"term-{i}" in existing_names:
        i += 1
    return f"term-{i}"


def _windows_to_wsl_path(windows_path: str | None) -> str | None:
    if not windows_path:
        return None
    norm = windows_path.replace("\\", "/")
    if len(norm) >= 3 and norm[1:3] == ":/":
        drive = norm[0].lower()
        rest = norm[3:]
        return f"/mnt/{drive}/{rest}".rstrip("/") or "/"
    return None


def _resolve_windows_shell(shell: str | None, cwd: str | None) -> tuple[list[str], str, str | None]:
    requested = (shell or os.environ.get("MRMD_PTY_SHELL") or "powershell").strip()
    lower = requested.lower()

    def which(exe: str) -> str | None:
        return shutil.which(exe)

    if lower in {"auto", "default", "powershell", "powershell.exe", "pwsh", "pwsh.exe"}:
        if which("pwsh.exe"):
            return (["pwsh.exe", "-NoLogo"], "pwsh", cwd)
        if which("powershell.exe"):
            return (["powershell.exe", "-NoLogo"], "powershell", cwd)
        return (["cmd.exe"], "cmd", cwd)

    if lower in {"cmd", "cmd.exe"}:
        return (["cmd.exe"], "cmd", cwd)

    if lower in {"wsl", "wsl.exe"}:
        argv = ["wsl.exe"]
        wsl_cwd = _windows_to_wsl_path(cwd)
        if wsl_cwd:
            argv += ["--cd", wsl_cwd]
        return (argv, "wsl", None)

    argv = shlex.split(requested, posix=False)
    if not argv:
        return (["powershell.exe", "-NoLogo"], "powershell", cwd)
    return (argv, argv[0], cwd)


def get_terminal_list() -> list[TerminalMeta]:
    stale_ids = []
    for session_id in _terminal_meta:
        session = _pty_sessions.get(session_id)
        if not session or not session.running:
            stale_ids.append(session_id)

    for session_id in stale_ids:
        del _terminal_meta[session_id]
        if session_id in _pty_sessions:
            del _pty_sessions[session_id]

    return list(_terminal_meta.values())


def get_terminal_meta(session_id: str) -> TerminalMeta | None:
    return _terminal_meta.get(session_id)


def update_terminal_activity(session_id: str) -> None:
    meta = _terminal_meta.get(session_id)
    if meta:
        meta.last_activity = datetime.now()


def rename_terminal(session_id: str, new_name: str) -> bool:
    meta = _terminal_meta.get(session_id)
    if meta:
        meta.name = new_name
        return True
    return False


async def handle_pty_websocket(request: web.Request) -> web.WebSocketResponse:
    """WebSocket handler for PTY connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session_id = request.query.get("session_id", "default")
    cwd = request.query.get("cwd")
    venv = request.query.get("venv")
    shell = request.query.get("shell")
    name = request.query.get("name")
    file_path = request.query.get("file_path")

    print(
        f"[PTY] WebSocket connection for session {session_id}, "
        f"cwd={cwd}, venv={venv}, shell={shell}"
    )

    session = _pty_sessions.get(session_id)

    if session and session.running:
        print(f"[PTY] Joining existing session {session_id}")
        await session.add_client(ws)

        if not IS_WINDOWS and session.master_fd is not None and session._loop:
            try:
                session._loop.add_reader(session.master_fd, session._on_posix_read)
            except Exception:
                pass

        await session.replay_buffer(ws)
        update_terminal_activity(session_id)
    else:
        session = PtySession(session_id)
        await session.add_client(ws)
        _pty_sessions[session_id] = session

        if session_id not in _terminal_meta:
            effective_cwd = cwd or os.getcwd()
            meta = TerminalMeta(
                session_id=session_id,
                name=name or _generate_terminal_name(),
                cwd=effective_cwd,
                venv=venv,
                shell=shell,
                file_path=file_path,
            )
            _terminal_meta[session_id] = meta

        try:
            await session.start(shell=shell, cwd=cwd, venv=venv)
        except Exception as e:
            print(f"[PTY] Failed to start session: {e}")
            await ws.send_str(f"[PTY] Failed to start terminal: {e}\r\n")
            await session.remove_client(ws)
            del _pty_sessions[session_id]
            if session_id in _terminal_meta:
                del _terminal_meta[session_id]
            await ws.close()
            return ws

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    msg_type = data.get("type")

                    if msg_type == "input":
                        session.write(data.get("data", ""))
                        update_terminal_activity(session_id)
                    elif msg_type == "resize":
                        cols = data.get("cols", 80)
                        rows = data.get("rows", 24)
                        session.resize(cols, rows)
                except json.JSONDecodeError:
                    session.write(msg.data)
                    update_terminal_activity(session_id)

            elif msg.type == WSMsgType.ERROR:
                print(f"[PTY] WebSocket error: {ws.exception()}")
                break
    except Exception as e:
        print(f"[PTY] Error: {e}")
    finally:
        await session.remove_client(ws)

        if not IS_WINDOWS and not session.clients and session._loop and session.master_fd is not None:
            try:
                session._loop.remove_reader(session.master_fd)
            except Exception:
                pass

        print(
            f"[PTY] WebSocket disconnected for session {session_id} "
            f"({len(session.clients)} clients remaining)"
        )

    return ws


async def handle_terminals_list(request: web.Request) -> web.Response:
    terminals = get_terminal_list()
    return web.json_response({"terminals": [t.to_dict() for t in terminals]})


async def handle_terminals_create(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        data = {}

    session_id = str(uuid.uuid4())[:8]
    meta = TerminalMeta(
        session_id=session_id,
        name=data.get("name") or _generate_terminal_name(),
        cwd=data.get("cwd") or os.getcwd(),
        venv=data.get("venv"),
        shell=data.get("shell"),
        file_path=data.get("file_path"),
    )
    _terminal_meta[session_id] = meta

    return web.json_response(meta.to_dict())


async def handle_terminals_rename(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        session_id = data.get("session_id")
        new_name = data.get("name")

        if not session_id or not new_name:
            return web.json_response({"error": "session_id and name required"}, status=400)

        if rename_terminal(session_id, new_name):
            return web.json_response({"success": True})
        return web.json_response({"error": "Terminal not found"}, status=404)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_pty_kill(request: web.Request) -> web.Response:
    data = await request.json()
    session_id = data.get("session_id")

    if session_id and session_id in _pty_sessions:
        session = _pty_sessions[session_id]
        session.stop()
        del _pty_sessions[session_id]
        if session_id in _terminal_meta:
            del _terminal_meta[session_id]
        print(f"[PTY] Killed session {session_id}")
        return web.json_response({"status": "ok"})

    if session_id and session_id in _terminal_meta:
        del _terminal_meta[session_id]
        return web.json_response({"status": "ok"})

    return web.json_response({"status": "not_found"}, status=404)


async def handle_terminals_kill_for_file(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        file_path = data.get("file_path")

        if not file_path:
            return web.json_response({"error": "file_path required"}, status=400)

        killed = []
        for session_id, meta in list(_terminal_meta.items()):
            if meta.file_path == file_path:
                session = _pty_sessions.get(session_id)
                if session:
                    session.stop()
                    del _pty_sessions[session_id]
                del _terminal_meta[session_id]
                killed.append(session_id)
                print(f"[PTY] Killed session {session_id} (file closed: {file_path})")

        return web.json_response({"killed": killed})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


def setup_pty_routes(app: web.Application) -> None:
    app.router.add_get("/api/pty", handle_pty_websocket)
    app.router.add_post("/api/pty/kill", handle_pty_kill)
    app.router.add_get("/api/terminals", handle_terminals_list)
    app.router.add_post("/api/terminals", handle_terminals_create)
    app.router.add_post("/api/terminals/rename", handle_terminals_rename)
    app.router.add_post("/api/terminals/kill-for-file", handle_terminals_kill_for_file)


def create_app() -> web.Application:
    app = web.Application()
    setup_pty_routes(app)
    return app
