#!/usr/bin/env python3
"""Hermes MessageBus client.

BusClient — long-lived connection (CLI / Gateway):
  - Auto start/stop bus server
  - Auto reconnect
  - Heartbeat keep-alive
  - Thread-safe message queue

send_message() — short-lived static method (external agent):
  - Connect → send one message → disconnect
  - No endpoint registration, prevents endpoint_map pollution
"""
import json
import os
import queue
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


MAX_PAYLOAD_BYTES = 10 * 1024 * 1024
HEARTBEAT_INTERVAL = 60
RECONNECT_DELAY_INITIAL = 1.0
RECONNECT_DELAY_MAX = 30.0


# ── Frame protocol ───────────────────────────────────────────────────────

def _recv_msg(sock: socket.socket) -> Optional[dict]:
    try:
        header = b""
        while len(header) < 4:
            chunk = sock.recv(4 - len(header))
            if not chunk:
                return None
            header += chunk
    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError):
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
    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError):
        return None

    try:
        return json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _send_msg(sock: socket.socket, msg: dict):
    data = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    header = struct.pack(">I", len(data))
    try:
        sock.sendall(header + data)
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass


# ── Short-lived: send one message then disconnect ────────────────────────

def send_message(to: str, body: dict, socket_path: str = None, from_ep: str = "anonymous") -> bool:
    """Short-lived: connect to bus → send one message → disconnect.

    No endpoint registration, no endpoint_map entry.
    External agents (e.g. notify-hermes.py) use this interface.

    Returns:
        True means message was sent (delivery not guaranteed; target may be offline).
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(5)

    try:
        sock.connect(socket_path or _get_bus_socket_path())
    except (FileNotFoundError, ConnectionRefusedError, OSError):
        # Bus server not running, try starting
        _start_bus_server()
        time.sleep(0.3)
        try:
            sock.connect(socket_path or _get_bus_socket_path())
        except Exception:
            sock.close()
            return False

    msg = {
        "type": "message",
        "to": to,
        "from": from_ep,
        "id": str(uuid.uuid4()),
        "ts": time.time(),
        "body": body,
    }

    try:
        _send_msg(sock, msg)
        # Read receipt (optional: check for error reply)
        sock.settimeout(1.0)
        reply = _recv_msg(sock)
        if reply and reply.get("type") == "error":
            return False
        return True
    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


# ── Bus Server lifecycle management ───────────────────────────────────────

_server_process: Optional[subprocess.Popen] = None
_server_start_lock = threading.Lock()


def _start_bus_server():
    """Start bus server subprocess (if not already running)."""
    global _server_process
    socket_path = _get_bus_socket_path()

    if os.path.exists(socket_path):
        # Check if socket is actually listening
        test = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        test.settimeout(0.5)
        try:
            test.connect(socket_path)
            test.close()
            return  # server already running
        except Exception:
            # socket file is stale, clean up
            try:
                os.unlink(socket_path)
            except Exception:
                pass
        finally:
            test.close()

    with _server_start_lock:
        if os.path.exists(socket_path):
            return

        try:
            _server_process = subprocess.Popen(
                ["hermes-busd", "start"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            # Wait for server to be ready
            for _ in range(20):
                time.sleep(0.1)
                if os.path.exists(socket_path):
                    break
        except Exception:
            pass


# ── Long-lived client ─────────────────────────────────────────────────────

class BusClient:
    """Long-lived message bus client.

    CLI / Gateway use this mode:
    - Register endpoint name
    - Maintain long-lived connection + heartbeat
    - Auto reconnect on disconnect
    - Thread-safe message queue

    Usage:
        client = BusClient("cli")
        client.connect()

        # Send message
        client.send("gateway", {"text": "hello"})

        # Poll received messages
        for msg in client.poll():
            print(msg)

        client.disconnect()
    """

    def __init__(self, endpoint: str, socket_path: str = None):
        self.endpoint = endpoint
        self.socket_path = socket_path or _get_bus_socket_path()
        self._sock: Optional[socket.socket] = None
        self._running = False
        self._connected = False
        self._lock = threading.Lock()
        self._recv_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._message_queue: queue.Queue = queue.Queue()
        self._reconnect_delay = RECONNECT_DELAY_INITIAL
        self._local_id = uuid.uuid4().hex[:8]
        self._bus_session_id: Optional[str] = None  # assigned by server

    @property
    def local_id(self) -> str:
        """Client local identifier (8 hex chars)."""
        return self._local_id

    @property
    def bus_session_id(self) -> Optional[str]:
        """Bus server assigned session_id (available after successful registration)."""
        return self._bus_session_id

    # ── Connection management ───────────────────────────────────────

    def connect(self) -> bool:
        """Connect to bus server, register endpoint, start heartbeat and receive threads.

        Returns:
            True means connection succeeded.
        """
        with self._lock:
            if self._connected:
                return True

            self._running = True

            if not self._connect_and_register():
                # Try starting server then retry
                _start_bus_server()
                time.sleep(0.3)
                if not self._connect_and_register():
                    # Start reconnect thread
                    self._start_reconnect_thread()
                    return False

            self._connected = True
            self._reconnect_delay = RECONNECT_DELAY_INITIAL

            # Start receive thread
            self._recv_thread = threading.Thread(
                target=self._recv_loop, daemon=True
            )
            self._recv_thread.start()

            # Start heartbeat thread
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, daemon=True
            )
            self._heartbeat_thread.start()

            return True

    def disconnect(self):
        """Disconnect, stop all background threads."""
        with self._lock:
            self._running = False
            self._connected = False

        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    # ── Send ──────────────────────────────────────────────────────

    def send(self, to: str, body: dict) -> bool:
        """Send message to target endpoint.

        Returns:
            True means message was written to socket (delivery not guaranteed).
        """
        msg = {
            "type": "message",
            "to": to,
            "from": self.endpoint,
            "id": str(uuid.uuid4()),
            "ts": time.time(),
            "body": body,
        }

        with self._lock:
            if not self._connected or not self._sock:
                return False
            try:
                _send_msg(self._sock, msg)
                return True
            except Exception:
                return False

    # ── Receive ───────────────────────────────────────────────────

    def poll(self) -> list[dict]:
        """Non-blocking poll for received messages.

        Returns:
            List of messages received since last poll (may be empty).
        """
        msgs = []
        while True:
            try:
                msgs.append(self._message_queue.get_nowait())
            except queue.Empty:
                break
        return msgs

    # ── Internal methods ───────────────────────────────────────────

    def _connect_and_register(self) -> bool:
        """Establish TCP connection and register endpoint, wait for server to return session_id."""
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(self.socket_path)

            # Send register message
            _send_msg(sock, {
                "type": "register",
                "endpoint": self.endpoint,
            })

            # Wait for server to return registered confirmation (with session_id)
            sock.settimeout(3.0)
            reply = _recv_msg(sock)
            if reply and reply.get("type") == "registered":
                self._bus_session_id = reply.get("session_id")

            sock.settimeout(1.0)  # short timeout for non-blocking poll
            self._sock = sock
            return True
        except (FileNotFoundError, ConnectionRefusedError, OSError):
            return False

    def _start_reconnect_thread(self):
        def _reconnect_loop():
            while self._running:
                with self._lock:
                    if self._connected:
                        return

                time.sleep(self._reconnect_delay)
                if not self._running:
                    return

                if self._connect_and_register():
                    with self._lock:
                        self._connected = True
                        self._reconnect_delay = RECONNECT_DELAY_INITIAL
                    # Start receive and heartbeat threads
                    self._recv_thread = threading.Thread(
                        target=self._recv_loop, daemon=True
                    )
                    self._recv_thread.start()
                    self._heartbeat_thread = threading.Thread(
                        target=self._heartbeat_loop, daemon=True
                    )
                    self._heartbeat_thread.start()
                    return
                else:
                    self._reconnect_delay = min(
                        self._reconnect_delay * 1.5, RECONNECT_DELAY_MAX
                    )

        threading.Thread(target=_reconnect_loop, daemon=True).start()

    def _recv_loop(self):
        """Receive thread: continuously read messages from socket, put into queue."""
        while self._running:
            with self._lock:
                if not self._connected or not self._sock:
                    break
                sock = self._sock

            try:
                msg = _recv_msg(sock)
            except socket.timeout:
                continue  # timeout is normal, keep waiting

            if msg is None:
                # Connection lost
                with self._lock:
                    self._connected = False
                    self._sock = None
                try:
                    sock.close()
                except Exception:
                    pass
                # Auto reconnect
                self._start_reconnect_thread()
                break

            msg_type = msg.get("type")
            if msg_type == "pong":
                continue  # heartbeat reply, don't propagate to app layer

            self._message_queue.put(msg)

    def _heartbeat_loop(self):
        """Heartbeat thread: periodically send ping."""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                if not self._connected or not self._sock:
                    continue
                try:
                    _send_msg(self._sock, {"type": "ping"})
                except Exception:
                    self._connected = False
                    self._sock = None


# ── Auto start/stop helper ───────────────────────────────────────────────

def ensure_bus_running():
    """Ensure bus server is running (start if not)."""
    _start_bus_server()
