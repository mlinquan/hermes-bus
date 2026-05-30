#!/usr/bin/env python3
"""
Hermes Message Bus Daemon (busd)

Daemon manager for hermes_bus/server.py — the new Message Bus (4-byte framing).
Manages start/stop/status/restart lifecycle.

Usage:
    busd.py start       # Start the bus server daemon
    busd.py stop        # Stop the bus server daemon
    busd.py status      # Check bus server status (PID + socket)
    busd.py restart     # Restart the bus server daemon
"""

import argparse
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

HERMES_HOME = Path(os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes")))
ROOT_HERMES_HOME = Path(os.environ.get("HERMES_BUS_ROOT", os.path.expanduser("~/.hermes")))
RUN_DIR = ROOT_HERMES_HOME / "run"
PID_PATH = RUN_DIR / "busd.pid"
SOCKET_PATH = ROOT_HERMES_HOME / "hermes-bus.sock"
LOG_PATH = RUN_DIR / "busd.log"
# Use module-based invocation so it always runs the installed hermes_bus.server
SERVER_MODULE = "hermes_bus.server"

# Log rotation: keep max 500KB
MAX_LOG_BYTES = 500_000

# Use sys.executable: it's where busd runs from and can import hermes_bus
SERVER_PYTHON = sys.executable


# ── Logging ────────────────────────────────────────────────────────────

def _log(msg: str):
    """Append a timestamped line to the busd log file."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    line = f"[{ts}] {msg}\n"
    try:
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        # Rotate if too large
        if LOG_PATH.exists() and LOG_PATH.stat().st_size > MAX_LOG_BYTES:
            rotated = LOG_PATH.read_text(encoding="utf-8")[-MAX_LOG_BYTES // 2:]
            LOG_PATH.write_text(rotated, encoding="utf-8")
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


# ── Protocol helpers (from client.py) ────────────────────────────────
from hermes_bus.client import _send_msg, _recv_msg


# ── Daemon Management ──────────────────────────────────────────────


def _is_socket_alive() -> bool:
    """Check if socket exists and server is listening.

    Uses list_endpoints query instead of register — avoid polluting
    endpoint_map with a fake 'busd-health' entry.
    """
    if not SOCKET_PATH.exists():
        return False
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        sock.connect(str(SOCKET_PATH))
        _send_msg(sock, {"type": "list_endpoints"})
        sock.settimeout(1.5)
        reply = _recv_msg(sock)
        sock.close()
        return reply is not None and reply.get("type") == "endpoints_list"
    except socket.timeout:
        _log("_is_socket_alive: connect timeout — server may be hung")
        return False
    except ConnectionRefusedError:
        _log("_is_socket_alive: connection refused — stale socket file")
        return False
    except Exception as e:
        _log(f"_is_socket_alive: {type(e).__name__}: {e}")
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def is_running() -> bool:
    """Check if busd PID is alive and server socket is responsive."""
    if not PID_PATH.exists():
        return False
    try:
        pid = int(PID_PATH.read_text().strip())
        os.kill(pid, 0)
    except (ValueError, OSError, ProcessLookupError):
        _log(f"PID file exists but process dead — cleaning up")
        PID_PATH.unlink(missing_ok=True)
        return False
    # Also verify socket
    if not _is_socket_alive():
        # PID exists but server is dead — stale PID
        _log(f"PID {pid} exists but socket unresponsive — sending SIGTERM")
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        PID_PATH.unlink(missing_ok=True)
        return False
    return True


# ── Disconnect Diagnosis ────────────────────────────────────────────

def _diagnose_disconnect() -> list[str]:
    """Analyze log and current state for likely disconnection causes.

    Returns a list of human-readable diagnostic messages.
    """
    diag: list[str] = []

    # 1. Check for multiple server processes
    try:
        result = subprocess.run(
            ["pgrep", "-f", "hermes_bus.server"],
            capture_output=True, text=True, timeout=3,
        )
        pids = [p.strip() for p in result.stdout.strip().split("\n") if p.strip()]
        if len(pids) > 1:
            diag.append(f"Found {len(pids)} server processes: {', '.join(pids)} — duplicates may cause socket contention")
        elif len(pids) == 0:
            diag.append("No server process running — server may have crashed")
    except Exception:
        pass

    # 2. Check socket file state
    if SOCKET_PATH.exists():
        st = SOCKET_PATH.stat()
        age_s = time.time() - st.st_mtime
        if age_s > 300:
            diag.append(f"socket file last modified {age_s:.0f}s ago — possibly stale")
    else:
        diag.append("socket file not found")

    # 3. Check PID vs socket consistency
    if PID_PATH.exists():
        try:
            pid = int(PID_PATH.read_text().strip())
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                diag.append(f"PID file exists but process {pid} has exited — abnormal termination (crash/kill)")
        except ValueError:
            diag.append("PID file content invalid")
    else:
        diag.append("No PID file — busd was not started via start command")

    # 4. Scan recent log for error patterns
    if LOG_PATH.exists():
        try:
            lines = LOG_PATH.read_text(encoding="utf-8").strip().split("\n")
            recent = lines[-50:]  # last 50 lines
            error_keywords: dict[str, list[str]] = {}
            for line in recent:
                lower = line.lower()
                for kw, desc in [
                    ("traceback", "Python exception traceback"),
                    ("connectionreset", "Client connection reset (ConnectionReset)"),
                    ("brokenpipe", "Client broken pipe (BrokenPipe)"),
                    ("timeout", "Timeout"),
                    ("oserror", "System-level I/O error"),
                    ("memory", "Out of memory"),
                    ("permission", "Permission error"),
                    ("file not found", "File not found"),
                    ("bind", "Port/socket bind failed"),
                    ("address already", "Address already in use"),
                    ("sigkill", "Process killed by SIGKILL"),
                    ("sigterm", "Process received SIGTERM"),
                    ("killed", "Process killed by OOM Killer or kill"),
                ]:
                    if kw in lower:
                        error_keywords.setdefault(kw, []).append(desc)

            for kw, occurrences in error_keywords.items():
                diag.append(f"Log contains '{kw}': {occurrences[0]} ({len(occurrences)} occurrences)")
        except Exception:
            pass

    if not diag:
        diag.append("No obvious anomalies detected")
    return diag


def _tail_log(n: int = 20) -> str:
    """Return the last n lines of the log file."""
    if not LOG_PATH.exists():
        return "(log file not found)"
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").strip().split("\n")
        return "\n".join(lines[-n:])
    except Exception as e:
        return f"(unable to read log: {e})"


# ── Commands ────────────────────────────────────────────────────────


def cmd_start():
    if is_running():
        pid = PID_PATH.read_text().strip()
        print(f"busd already running (pid={pid})")
        return

    # Verify module is importable
    try:
        subprocess.run(
            [SERVER_PYTHON, "-c", f"import {SERVER_MODULE}"],
            capture_output=True, timeout=5,
        ).check_returncode()
    except Exception:
        print(f"server module not found: {SERVER_MODULE}")
        _log(f"start failed: cannot import {SERVER_MODULE}")
        sys.exit(1)

    RUN_DIR.mkdir(parents=True, exist_ok=True)

    # Clean up stale socket
    if SOCKET_PATH.exists():
        if _is_socket_alive():
            _log("start: socket exists and is alive — unexpected, cleaning")
        try:
            SOCKET_PATH.unlink()
        except Exception:
            pass

    # Run server via module invocation (always uses installed package)
    log_fh = open(LOG_PATH, "a")
    proc = subprocess.Popen(
        [SERVER_PYTHON, "-m", SERVER_MODULE],
        stdout=log_fh,
        stderr=log_fh,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )

    # Write PID immediately
    PID_PATH.write_text(str(proc.pid))
    _log(f"start: server pid={proc.pid}, python={SERVER_PYTHON}, module={SERVER_MODULE}")

    # Wait for socket to appear
    for _ in range(30):
        time.sleep(0.1)
        if SOCKET_PATH.exists() and _is_socket_alive():
            log_fh.close()
            print(f"busd started (pid={proc.pid})")
            print(f"  socket: {SOCKET_PATH}")
            print(f"  log:    {LOG_PATH}")
            return

    # Timeout — read any error output and clean up
    _log("start: timed out waiting for server — checking stderr")
    log_fh.close()

    # Print last few log lines for diagnosis
    print(f"busd start timed out — server not ready")
    print(f"  Recent logs:")
    for line in _tail_log(10).split("\n"):
        print(f"    {line}")

    try:
        os.kill(proc.pid, signal.SIGTERM)
    except Exception:
        pass
    PID_PATH.unlink(missing_ok=True)
    sys.exit(1)


def cmd_stop():
    if not PID_PATH.exists():
        print("busd not running (no PID file)")
        if SOCKET_PATH.exists():
            # Check if there's actually a server running without PID
            if _is_socket_alive():
                print(f"  but socket is alive — may have been started by another process, cleaning up")
                _log("stop: socket alive without PID — manual cleanup")
            SOCKET_PATH.unlink(missing_ok=True)
        return

    pid = int(PID_PATH.read_text().strip())
    _log(f"stop: sending SIGTERM to pid={pid}")
    print(f"Stopping busd (pid={pid})...")

    # Try graceful shutdown
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        _log(f"stop: pid={pid} already gone")
        pass
    except Exception as e:
        _log(f"stop: kill failed: {type(e).__name__}: {e}")
        print(f"  kill failed: {e}")

    # Wait for exit
    exited = False
    for _ in range(15):
        try:
            os.kill(pid, 0)
            time.sleep(0.3)
        except ProcessLookupError:
            exited = True
            break

    if not exited:
        _log(f"stop: pid={pid} did not exit — sending SIGKILL")
        print(f"  process not responding to SIGTERM, sending SIGKILL...")
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass
        time.sleep(0.5)

    # Clean up
    SOCKET_PATH.unlink(missing_ok=True)
    PID_PATH.unlink(missing_ok=True)
    _log("stop: complete")
    print(f"busd stopped")


def _query_endpoints() -> dict:
    """Send list_endpoints query to the bus server. Returns {endpoint: session_id}."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(3)
    try:
        sock.connect(str(SOCKET_PATH))
        _send_msg(sock, {"type": "list_endpoints"})

        header = b""
        while len(header) < 4:
            c = sock.recv(4 - len(header))
            if not c:
                return {}
            header += c
        plen = struct.unpack(">I", header)[0]
        if plen > 10 * 1024 * 1024:
            return {}
        payload = b""
        while len(payload) < plen:
            c = sock.recv(plen - len(payload))
            if not c:
                return {}
            payload += c
        data = json.loads(payload.decode("utf-8"))
        if data.get("type") == "endpoints_list":
            return data.get("endpoints", {})
        return {}
    except socket.timeout:
        _log("_query_endpoints: timeout")
        return {}
    except Exception as e:
        _log(f"_query_endpoints: {type(e).__name__}: {e}")
        return {}
    finally:
        sock.close()


def cmd_status():
    if not PID_PATH.exists():
        print("busd not running (no PID file)")
        endpoints = {}
        if SOCKET_PATH.exists():
            # Socket exists without PID — check if alive
            if _is_socket_alive():
                print(f"  but socket is alive — may have been started by another process")
                endpoints = _query_endpoints()
            else:
                print(f"  socket file exists but unresponsive (stale file)")
        _print_endpoints(endpoints)
        print()
        _print_diagnostics()
        return

    pid = int(PID_PATH.read_text().strip())
    pid_alive = True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pid_alive = False

    sock_exists = SOCKET_PATH.exists()
    sock_alive = _is_socket_alive() if sock_exists else False

    if pid_alive and sock_alive:
        print(f"busd running (pid={pid})")
        print(f"  socket: {SOCKET_PATH} ✓")
        print(f"  log:    {LOG_PATH}")
        endpoints = _query_endpoints()
        _print_endpoints(endpoints)
    elif pid_alive and not sock_alive:
        print(f"pid={pid} exists but socket is unresponsive")
        print(f"  socket: {SOCKET_PATH} {'file exists but unusable' if sock_exists else 'not found'}")
        print()
        _print_diagnostics()
    else:
        reason = "zombie process" if not pid_alive else "unknown"
        print(f"busd not running (pid={pid} — {reason})")
        PID_PATH.unlink(missing_ok=True)
        SOCKET_PATH.unlink(missing_ok=True)
        print()
        _print_diagnostics()

    # Always show recent log tail if available
    _print_log_tail()


def cmd_restart():
    cmd_stop()
    time.sleep(0.5)
    cmd_start()


# ── Output helpers ──────────────────────────────────────────────────

def _print_endpoints(endpoints: dict):
    if endpoints:
        print(f"  Connected endpoints ({len(endpoints)}):")
        for ep, sid in sorted(endpoints.items()):
            print(f"    {ep}  (sid={sid[:8]}...)")
    else:
        print("  No connected endpoints")


def _print_diagnostics():
    diag = _diagnose_disconnect()
    print("  Disconnect diagnosis:")
    for d in diag:
        print(f"    {d}")


def _print_log_tail():
    print()
    print("  Recent logs:")
    tail = _tail_log(20)
    if tail:
        for line in tail.split("\n"):
            print(f"    {line}")
    else:
        print("    (no logs)")


# ── CLI ────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Hermes Message Bus Daemon")
    parser.add_argument(
        "command",
        choices=["start", "stop", "status", "restart"],
        help="daemon command",
    )
    args = parser.parse_args()

    commands = {
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "restart": cmd_restart,
    }
    commands[args.command]()


if __name__ == "__main__":
    main()
