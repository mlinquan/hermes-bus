#!/usr/bin/env python3
"""Hermes MessageBus — Unix Domain Socket daemon.

session_id <-> endpoint bi-directional mapping for point-to-point routing and broadcast.
Short-lived (anonymous) connections are send-only. They don't occupy endpoint_map.
Protocol: 4-byte big-endian length prefix + JSON body.

Hook configuration:
  After each message is routed, configured hook scripts run asynchronously.
  Scripts read message JSON from stdin.

  Resolution order (highest to lowest priority):
    1. ENV HERMES_BUS_HOOKS — comma-separated script paths
       e.g. export HERMES_BUS_HOOKS="/path/to/hook1.py,/path/to/hook2.py"
       Set to empty or "none" to disable all hooks.
    2. Config file hermes-bus/hooks.yaml — hooks list
    3. Default: none (route processing is handled by hermes-bus-plugin)
"""
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from typing import Any, Optional


def _get_bus_socket_path() -> str:
    root = os.environ.get("HERMES_BUS_ROOT", os.path.expanduser("~/.hermes"))
    return os.path.join(root, "hermes-bus.sock")


MAX_PAYLOAD_BYTES = 10 * 1024 * 1024  # 10MB
HEARTBEAT_INTERVAL = 60   # client heartbeat interval (seconds)
HEARTBEAT_TIMEOUT = 90    # server heartbeat timeout (seconds)
HEARTBEAT_CHECK_EVERY = 15


# ── Hook resolution ─────────────────────────────────────────────────────

def _get_home() -> str:
    return os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))


def _resolve_hook_scripts() -> list[str]:
    """Resolve hook script list.

    Priority:
      1. HERMES_BUS_HOOKS env var (comma-separated list or JSON array)
      2. hermes-bus/hooks.yaml config file
      3. Default: none (route processing is handled by hermes-bus-plugin)
    """
    home = _get_home()

    # 1. Env var
    hooks_env = os.environ.get("HERMES_BUS_HOOKS", "")
    if hooks_env:
        if hooks_env.strip().lower() in ("none", ""):
            return []
        if hooks_env.strip().startswith("["):
            # JSON array
            try:
                hooks = json.loads(hooks_env)
                if isinstance(hooks, list):
                    return [os.path.expanduser(h) for h in hooks]
            except json.JSONDecodeError:
                pass
        # Comma-separated list
        return [
            os.path.expanduser(h.strip())
            for h in hooks_env.split(",")
            if h.strip()
        ]

    # 2. hooks.yaml config file
    hooks_config = os.path.join(home, "hermes-bus", "hooks.yaml")
    if os.path.exists(hooks_config):
        try:
            hooks = _parse_hooks_yaml(hooks_config)
            if hooks is not None:
                return hooks
        except Exception:
            pass

    return []


def _parse_hooks_yaml(path: str) -> Optional[list[str]]:
    """Parse hooks.yaml config file (simple YAML parse, no third-party deps)."""
    hooks = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("- "):
                script = stripped[2:].strip().strip("'\"")
                hooks.append(os.path.expanduser(script))
    return hooks if hooks else None


class BusServer:
    """Unix Domain Socket message bus server.

    Long-lived: send register message to register endpoint, then send/receive.
    Short-lived: send message directly, no registration, no endpoint_map entry.
    """

    def __init__(self, socket_path: str = None, hook_scripts: list[str] = None):
        self.socket_path = socket_path or _get_bus_socket_path()
        self.sessions: dict[str, dict[str, Any]] = {}
        self.endpoint_map: dict[str, str] = {}
        self.server_socket: Optional[socket.socket] = None
        self.running = False
        self.lock = threading.Lock()
        # hook script list: None = auto-resolve
        self._hook_scripts = hook_scripts

    @property
    def hook_scripts(self) -> list[str]:
        """Get hook script list (lazy resolve)."""
        if self._hook_scripts is None:
            self._hook_scripts = _resolve_hook_scripts()
        return self._hook_scripts

    # ── Start / Stop ──────────────────────────────────────────────────

    def start(self):
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_socket.bind(self.socket_path)
        self.server_socket.listen(128)
        os.chmod(self.socket_path, 0o600)
        self.running = True

        # Ignore SIGPIPE (client disconnect won't kill process)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

        hooks = self.hook_scripts
        hooks_info = ", ".join(hooks) if hooks else "(none)"
        print(f"[HermesBus] listening on {self.socket_path}")
        print(f"[HermesBus] hooks: {hooks_info}")

        threading.Thread(target=self._heartbeat_check_loop, daemon=True).start()

        while self.running:
            try:
                client_sock, _ = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client, args=(client_sock,), daemon=True
                ).start()
            except Exception:
                if self.running:
                    continue
                break

    def stop(self):
        self.running = False
        # Close all client connections
        with self.lock:
            for sess in list(self.sessions.values()):
                try:
                    sess["socket"].close()
                except Exception:
                    pass
            self.sessions.clear()
            self.endpoint_map.clear()
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

    # ── Client handling ──────────────────────────────────────────────────

    def _handle_client(self, client_sock: socket.socket):
        session_id = str(uuid.uuid4())
        endpoint = None
        is_registered = False

        try:
            while self.running:
                msg = self._recv_msg(client_sock)
                if msg is None:
                    break

                msg_type = msg.get("type")

                if msg_type == "register" and not is_registered:
                    endpoint = msg["endpoint"]
                    with self.lock:
                        # Same-name endpoint: kick old connection
                        if endpoint in self.endpoint_map:
                            old_sid = self.endpoint_map[endpoint]
                            old = self.sessions.pop(old_sid, None)
                            if old:
                                try:
                                    old["socket"].close()
                                except Exception:
                                    pass
                            print(f"[HermesBus] kick old: {endpoint} (session={old_sid[:8]})")

                        self.sessions[session_id] = {
                            "endpoint": endpoint,
                            "socket": client_sock,
                            "last_ping": time.time(),
                        }
                        self.endpoint_map[endpoint] = session_id
                    is_registered = True
                    print(f"[HermesBus] register: {endpoint} (session={session_id[:8]})")
                    # Send session_id back to client
                    self._send_msg(client_sock, {
                        "type": "registered",
                        "endpoint": endpoint,
                        "session_id": session_id,
                    })

                elif msg_type == "ping":
                    self._send_msg(client_sock, {"type": "pong"})
                    if is_registered:
                        with self.lock:
                            s = self.sessions.get(session_id)
                            if s:
                                s["last_ping"] = time.time()

                elif msg_type == "message":
                    if is_registered:
                        with self.lock:
                            s = self.sessions.get(session_id)
                            if s:
                                s["last_ping"] = time.time()

                    msg.setdefault("id", str(uuid.uuid4()))
                    msg.setdefault("ts", time.time())

                    self._route_message(msg, reply_socket=None if is_registered else client_sock)

                elif msg_type == "list_endpoints":
                    """Query: return all registered endpoints + session IDs."""
                    with self.lock:
                        endpoints = {
                            sess["endpoint"]: sid
                            for sid, sess in self.sessions.items()
                        }
                    self._send_msg(client_sock, {
                        "type": "endpoints_list",
                        "endpoints": endpoints,
                    })
                    print(f"[HermesBus] list_endpoints query: {len(endpoints)} endpoint(s)")

        except Exception:
            pass
        finally:
            with self.lock:
                if session_id in self.sessions:
                    ep = self.sessions[session_id].get("endpoint")
                    del self.sessions[session_id]
                    if ep and self.endpoint_map.get(ep) == session_id:
                        del self.endpoint_map[ep]
                    print(f"[HermesBus] disconnect: {ep or '(anonymous)'} (session={session_id[:8]})")

            try:
                client_sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            client_sock.close()

    # ── Message routing ─────────────────────────────────────────────────

    def _trigger_hooks(self, msg: dict):
        """Run all configured hook scripts asynchronously, without blocking the main message loop."""
        for hook_path in self.hook_scripts:
            if not os.path.exists(hook_path):
                continue
            try:
                p = subprocess.Popen(
                    [sys.executable, hook_path],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
                p.stdin.write(json.dumps(msg).encode())
                p.stdin.close()
            except Exception:
                pass

    def _route_message(self, msg: dict, reply_socket: Optional[socket.socket] = None):
        to_ep = msg.get("to", "")
        content = json.dumps(msg, ensure_ascii=False)

        with self.lock:
            if not to_ep:
                # Broadcast: send to all registered clients
                for sess in list(self.sessions.values()):
                    try:
                        self._send_raw(sess["socket"], content)
                    except Exception:
                        continue
            elif to_ep in self.endpoint_map:
                sid = self.endpoint_map[to_ep]
                sess = self.sessions.get(sid)
                if sess:
                    try:
                        self._send_raw(sess["socket"], content)
                    except Exception:
                        pass
            elif reply_socket:
                try:
                    self._send_msg(reply_socket, {
                        "type": "error",
                        "code": "endpoint_not_found",
                        "detail": f"Endpoint '{to_ep}' is not connected",
                        "id": msg.get("id"),
                    })
                except Exception:
                    pass

        # Trigger hooks asynchronously after routing (non-blocking)
        self._trigger_hooks(msg)

    # ── Heartbeat check ────────────────────────────────────────────────

    def _heartbeat_check_loop(self):
        while self.running:
            time.sleep(HEARTBEAT_CHECK_EVERY)
            now = time.time()
            with self.lock:
                dead = [
                    sid for sid, sess in self.sessions.items()
                    if now - sess["last_ping"] > HEARTBEAT_TIMEOUT
                ]
                for sid in dead:
                    ep = self.sessions[sid].get("endpoint")
                    print(f"[HermesBus] heartbeat timeout: {ep} (session={sid[:8]})")
                    try:
                        self.sessions[sid]["socket"].close()
                    except Exception:
                        pass
                    del self.sessions[sid]
                    if ep and self.endpoint_map.get(ep) == sid:
                        del self.endpoint_map[ep]

    # ── Frame protocol ────────────────────────────────────────────────

    @staticmethod
    def _recv_msg(sock: socket.socket) -> Optional[dict]:
        """Read one 4-byte length-prefixed + JSON body message. Returns None on connection close."""
        try:
            header = b""
            while len(header) < 4:
                chunk = sock.recv(4 - len(header))
                if not chunk:
                    return None
                header += chunk
        except (ConnectionResetError, BrokenPipeError, OSError):
            return None

        payload_len = struct.unpack(">I", header)[0]
        if payload_len > MAX_PAYLOAD_BYTES:
            return None

        try:
            payload = b""
            while len(payload) < payload_len:
                chunk = sock.recv(payload_len - len(payload))
                if not chunk:
                    return None
                payload += chunk
        except (ConnectionResetError, BrokenPipeError, OSError):
            return None

        try:
            return json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    @staticmethod
    def _send_msg(sock: socket.socket, msg: dict):
        BusServer._send_raw(sock, json.dumps(msg, ensure_ascii=False))

    @staticmethod
    def _send_raw(sock: socket.socket, raw: str):
        data = raw.encode("utf-8")
        header = struct.pack(">I", len(data))
        try:
            sock.sendall(header + data)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


def run_server():
    server = BusServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n[HermesBus] shutting down...")
    finally:
        server.stop()


if __name__ == "__main__":
    run_server()


def main():
    """Entry point for pip console_scripts."""
    run_server()

