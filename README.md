# hermes-bus

[English](./README.md) | [中文](./README.zh.md)

<p align="center"><img src="assets/avator_default_png8.png" width="500" alt="Snow"></p>

**Role in the Hermes messaging ecosystem:** hermes-bus is the **transport layer** — a Unix Socket IPC daemon that routes JSON messages between endpoints. It is the backbone. The other two packages in the ecosystem are:

- [hermes-notify](https://github.com/mlinquan/hermes-notify) — **CLI senders** (`notify-hermes`, `notify-agent`) that inject messages into the bus or tmux sessions
- [hermes-bus-plugin](https://github.com/mlinquan/hermes-bus-plugin) — **receive-side agent plugin** that consumes bus messages and routes them to terminal output, LLM context injection, or command execution

Together: **notify → bus → plugin**. The bus itself is transport-only — it knows nothing about audio, display, LLM context, or chat platforms.

---

## Hermes Messaging Ecosystem

![Hermes Bus Ecosystem Architecture](https://raw.githubusercontent.com/mlinquan/hermes-bus-plugin/main/docs/architecture.svg)

The ecosystem has four layers:

```
Layer 1 — CLI / User Space          Layer 3 — Agent / Plugin
┌──────────────────────┐            ┌──────────────────────────┐
│ notify-hermes        │──┐         │ hermes-bus-plugin        │
│ notify-agent  ──→ tmux│  │         │  print  → terminal       │
│ (hermes-notify)      │  │         │  context → LLM injection  │
└──────────────────────┘  │         │  command → subprocess     │
                           ▼         │  channel → Gateway        │
Layer 2 — Transport      ┌──────────┐└────────────┬─────────────┘
┌──────────────────┐     │hermes-bus│              │
│ Unix Socket IPC  │────→│ message  │──────────────┘
│ JSON routing     │     │ daemon   │
│ session mgmt     │     └──────────┘              Layer 4 — Gateway
└──────────────────┘                               ┌──────────────────┐
 (hermes-bus)                                      │ Platform Adapters│
                                                   │ WeChat · Feishu  │
                                                   │ WeCom · DingTalk │
                                                   └──────────────────┘
```

| Layer | Package | Role |
|-------|---------|------|
| 1 — CLI | **hermes-notify** | Send messages into the ecosystem (`notify-hermes`, `notify-agent`) |
| 2 — Transport | **hermes-bus** | Route JSON messages between endpoints via Unix Socket |
| 3 — Plugin | **hermes-bus-plugin** | Receive-side agent plugin: print, LLM context injection, command execution, channel routing |
| 4 — Gateway | *(downstream)* | Platform adapters deliver replies to end users. **Zero agent code changes** |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HERMES_BUS_ROOT` | `~/.hermes` | Bus socket directory (`hermes-bus.sock`) and run directory (`run/`). Separate from `HERMES_HOME` so all profiles share one bus daemon. |
| `HERMES_HOME` | `~/.hermes` | Hermes config home (may be profile-scoped). Does NOT affect the bus socket location. |

### Profile Example

```bash
# Default profile (HERMES_HOME=~/.hermes)
hermes-busd start                                # socket → ~/.hermes/hermes-bus.sock
notify-hermes --to hermes-bus "hello"             # route to default profile endpoint

# Create and use a work profile
hermes profile create work

# Work profile shares the same bus daemon
hermes-busd status                               # still connected to ~/.hermes/hermes-bus.sock
notify-hermes --to work-gateway "hello"           # route to work profile's Gateway endpoint
```

> **Key design:** `HERMES_BUS_ROOT` (socket location) is separate from `HERMES_HOME` (config directory). All profiles share one `hermes-busd` daemon, but each has its own `bus-rules.yaml` and endpoint registration. Profile endpoint naming: `<profile>-gateway` (e.g., `work` → `work-gateway`).

## Install

```bash
pip install hermes-bus
```

Or from source:

```bash
git clone https://github.com/mlinquan/hermes-bus.git
cd hermes-bus && pip install -e .
```

## CLI

```bash
# Daemon management
hermes-busd start       # Start the bus daemon
hermes-busd stop        # Stop the daemon
hermes-busd status      # Check daemon + connected endpoints
hermes-busd restart     # Restart the daemon

# Foreground server (for debugging)
hermes-bus-server
```

### Restart Order

After upgrading `hermes-bus`, restart the daemon and Gateway:

```bash
# 1. Restart the bus daemon (connected endpoints will auto-reconnect)
hermes-busd restart

# 2. Restart Gateway to reload the updated hermes-bus module
# In the Gateway tmux pane: Ctrl+C, then:
hermes gateway
```

> `hermes-busd restart` = `stop` + `start`. All registered endpoints auto-reconnect (bus-plugin has built-in retry every 5 seconds).

## Python API

```python
from hermes_bus.client import BusClient, send_message

# Long-lived: register as an endpoint and receive messages
client = BusClient("my-service")
client.connect()
for msg in client.poll():
    print(msg)

# Short-lived: fire-and-forget
send_message("target-service", {"text": "hello", "type": "ack"})
```

## Architecture

```
client.py ──── Unix Socket ──── server.py
  (BusClient)                    (BusServer)
   - Register endpoint           - Session management
   - Heartbeat (60s)             - Message routing
   - Auto-reconnect              - Hook triggers
   - Thread-safe message queue
   
busd.py — Daemon manager: start / stop / status / restart
```

## Protocol

4-byte big-endian length prefix + JSON body. Max payload: 10 MB.

Long-lived connections register an endpoint name and can send/receive. Short-lived connections send one message and disconnect — no registration, no endpoint_map pollution.

## Hooks

After each message is routed, hook scripts are triggered asynchronously. Resolution priority:

1. `HERMES_BUS_HOOKS` env var (comma-separated or JSON array of script paths)
2. `hooks.yaml` config file
3. Default: none (routing handled by hermes-bus-plugin)

Each hook receives the full message JSON on stdin. Hook execution is non-blocking — the bus continues routing.

## Message Format

All bus messages use 4-byte big-endian length prefix + JSON body. Max payload: 10 MB.

### Envelope (wire format)

```
┌──────────────────┬──────────────────────────────────┐
│  4 bytes (BE)    │  JSON body (up to 10 MB)         │
│  payload length  │  UTF-8 encoded                   │
└──────────────────┴──────────────────────────────────┘
```

### Message structure

```json
{
  "type": "message",
  "to": "target-endpoint",
  "from": "sender-endpoint",
  "ts": 1716307200.123,
  "body": {
    "text": "Human-readable message content",
    "type": "task_done",
    "channel": "feishu:oc_abc123"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | yes | Message type: `message`, `register`, `ping`, `pong`, `list_endpoints` |
| `to` | string | yes (for `message`) | Target endpoint name |
| `from` | string | auto | Sender endpoint name (set by bus for registered endpoints) |
| `ts` | float | auto | Unix timestamp (set by bus on receipt) |
| `body` | object | no | Application payload — see below |
| `body.text` | string | no | Human-readable message text |
| `body.type` | string | no | Application-level type: `directive`, `ack`, `task_start`, `progress`, `task_done`, `plan_ready`, `task_error`, `need_decision` |
| `body.channel` | string | no | Reply routing token (`platform:chat_id`). Carried through the chain unmodified |

### Special message types

| `type` | Direction | Description |
|--------|-----------|-------------|
| `register` | client → server | Register as a named endpoint |
| `registered` | server → client | Registration acknowledgement with `session_id` |
| `ping` | client → server | Heartbeat (sent every 55s by long-lived connections) |
| `pong` | server → client | Heartbeat response |
| `list_endpoints` | client → server | Request connected endpoint list |
| `endpoint_list` | server → client | Response with current endpoints |
| `message` | bidirectional | Application message (routed by `to` field) |

### Message lifecycle

```
Client sends:      {"type":"message","to":"lead-agent","body":{"text":"hello","type":"ack"}}
                                                                                    │
Bus server:        Registers timestamp, resolves target endpoint, delivers          │
                                                                                    ▼
Target receives:   {"type":"message","from":"worker-alpha","to":"lead-agent",
                    "ts":1716307200.123,"body":{"text":"hello","type":"ack"}}
```

The bus adds `from` (if not set by sender) and `ts` fields during routing. The `body` object is passed through unmodified — the bus never inspects or alters application payload.
