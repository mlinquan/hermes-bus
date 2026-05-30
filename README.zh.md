# hermes-bus

[English](./README.md) | [中文](./README.zh.md)

<p align="center"><img src="assets/avator_default_png8.png" width="500" alt="Snow"></p>

**在 Hermes 消息生态系统中的角色：** hermes-bus 是 **传输层** —— 一个 Unix Socket IPC 守护进程，在端点之间路由 JSON 消息。它是整个生态的骨干。另外两个包：

- [hermes-notify](https://github.com/mlinquan/hermes-notify) — **CLI 发送器**（`notify-hermes`、`notify-agent`），将消息注入总线或 tmux 会话
- [hermes-bus-plugin](https://github.com/mlinquan/hermes-bus-plugin) — **接收端 agent 插件**，消费总线消息并路由到终端输出、LLM 上下文注入或命令执行

三者协作：**notify → bus → plugin**。总线本身是纯传输层 —— 不感知音频、显示、LLM 上下文或聊天平台。

---

## Hermes 消息生态系统

![Hermes Bus Ecosystem Architecture](https://raw.githubusercontent.com/mlinquan/hermes-bus-plugin/main/docs/architecture.svg)

生态系统分为四层：

```
第1层 — CLI / 用户空间              第3层 — Agent / 插件
┌──────────────────────┐            ┌──────────────────────────┐
│ notify-hermes        │──┐         │ hermes-bus-plugin        │
│ notify-agent  ──→ tmux│  │         │  print  → 终端输出        │
│ (hermes-notify)      │  │         │  context → LLM 注入       │
└──────────────────────┘  │         │  command → 子进程          │
                           ▼         │  channel → Gateway        │
第2层 — 传输             ┌──────────┐└────────────┬─────────────┘
┌──────────────────┐     │hermes-bus│              │
│ Unix Socket IPC  │────→│ 消息     │──────────────┘
│ JSON 路由         │     │ 守护进程  │
│ 会话管理           │     └──────────┘              第4层 — Gateway
└──────────────────┘                               ┌──────────────────┐
 (hermes-bus)                                      │ 平台适配器         │
                                                   │ WeChat · Feishu   │
                                                   │ WeCom · DingTalk  │
                                                   └──────────────────┘
```

| 层 | 包 | 角色 |
|----|-----|------|
| 1 — CLI | **hermes-notify** | 将消息发送到生态系统（`notify-hermes`、`notify-agent`） |
| 2 — 传输 | **hermes-bus** | 通过 Unix Socket 在端点之间路由 JSON 消息 |
| 3 — 插件 | **hermes-bus-plugin** | 接收端 agent 插件：终端输出、LLM 上下文注入、命令执行、channel 路由 |
| 4 — Gateway | *(下游)* | 平台适配器将回复投递给最终用户。**零 agent 代码改动** |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `HERMES_BUS_ROOT` | `~/.hermes` | 总线 socket 目录（`hermes-bus.sock`）和运行目录（`run/`）。与 `HERMES_HOME` 分离，确保所有 profile 共享一个总线守护进程。 |
| `HERMES_HOME` | `~/.hermes` | Hermes 配置主目录（可指向 profile 子目录）。不影响总线 socket 位置。 |

### Profile 多配置示例

```bash
# 默认 profile（HERMES_HOME=~/.hermes）
hermes-busd start                           # socket → ~/.hermes/hermes-bus.sock
notify-hermes --to hermes-bus "hello"        # 路由到默认 profile 的端点

# 创建 work profile
hermes profile create work

# work profile 共享同一个总线守护进程
hermes-busd status                          # 仍然连接 ~/.hermes/hermes-bus.sock
notify-hermes --to work-gateway "hello"      # 路由到 work profile 的端点
```

> **关键设计：** `HERMES_BUS_ROOT`（socket 位置）与 `HERMES_HOME`（配置目录）分离。所有 profile 共用同一个 `hermes-busd` 守护进程，但各自拥有独立的 `bus-rules.yaml` 和端点注册。消息可跨 profile 互通。端点命名规则：`<profile>-gateway`（如 `work` → `work-gateway`）。

## 安装

```bash
pip install hermes-bus
```

或从源码安装：

```bash
git clone https://github.com/mlinquan/hermes-bus.git
cd hermes-bus && pip install -e .
```

## CLI

```bash
# 守护进程管理
hermes-busd start       # 启动总线守护进程
hermes-busd stop        # 停止
hermes-busd status      # 查看状态 + 已连接端点
hermes-busd restart     # 重启

# 前台运行（调试用）
hermes-bus-server
```

### 重启顺序

升级 `hermes-bus` 包后，需重启守护进程使改动生效：

```bash
# 1. 重启总线守护进程（已注册端点会断连并自动重连）
hermes-busd restart

# 2. Gateway 需要重启以重新加载 hermes-bus 模块
# 在 Gateway tmux 窗格中 Ctrl+C 后：
hermes gateway
```

> `hermes-busd restart` = `stop` + `start`。所有已注册端点会自动重连（bus-plugin 内置自动重连机制，5 秒重试）。

## Python API

```python
from hermes_bus.client import BusClient, send_message

# 长连接：注册为端点并持续监听消息
client = BusClient("my-service")
client.connect()
for msg in client.poll():
    print(msg)

# 短连接：一次性发送
send_message("target-service", {"text": "hello", "type": "ack"})
```

## 架构

```
client.py ──── Unix Socket ──── server.py
  (BusClient)                    (BusServer)
   - 注册端点                      - 会话管理
   - 心跳 (60s)                    - 消息路由
   - 自动重连                      - Hook 触发
   - 线程安全消息队列

busd.py — 守护进程管理：start / stop / status / restart
```

## 协议

4 字节大端长度前缀 + JSON body。单条消息上限 10 MB。

长连接注册端点后可收发消息。短连接发完即断，不注册不占用端点映射表。

## Hook

消息路由后异步触发 hook 脚本。优先级：

1. `HERMES_BUS_HOOKS` 环境变量（逗号分隔或 JSON 数组）
2. `hooks.yaml` 配置文件
3. 默认：无（路由由 hermes-bus-plugin 处理）

每个 hook 从 stdin 接收完整消息 JSON，不阻塞主消息循环。

## 消息格式

所有总线消息均使用 4 字节大端长度前缀 + JSON body。最大负载：10 MB。

### 封包（线格式）

```
┌──────────────────┬──────────────────────────────────┐
│  4 bytes (BE)    │  JSON body (最大 10 MB)          │
│  负载长度          │  UTF-8 编码                      │
└──────────────────┴──────────────────────────────────┘
```

### 消息结构

```json
{
  "type": "message",
  "to": "target-endpoint",
  "from": "sender-endpoint",
  "ts": 1716307200.123,
  "body": {
    "text": "人类可读的消息内容",
    "type": "task_done",
    "channel": "feishu:oc_abc123"
  }
}
```

| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `type` | string | 是 | 消息类型：`message`、`register`、`ping`、`pong`、`list_endpoints` |
| `to` | string | 是（`message` 时） | 目标端点名称 |
| `from` | string | 自动 | 发送者端点名称（总线为已注册端点设置） |
| `ts` | float | 自动 | Unix 时间戳（总线在接收时设置） |
| `body` | object | 否 | 应用层负载 —— 见下 |
| `body.text` | string | 否 | 人类可读的消息文本 |
| `body.type` | string | 否 | 应用层类型：`directive`、`ack`、`task_start`、`progress`、`task_done`、`plan_ready`、`task_error`、`need_decision` |
| `body.channel` | string | 否 | 回复路由令牌（`platform:chat_id`）。在整个链路中原样透传 |

### 特殊消息类型

| `type` | 方向 | 说明 |
|--------|------|------|
| `register` | 客户端 → 服务端 | 注册为命名端点 |
| `registered` | 服务端 → 客户端 | 注册确认，包含 `session_id` |
| `ping` | 客户端 → 服务端 | 心跳（长连接每 55 秒发送一次） |
| `pong` | 服务端 → 客户端 | 心跳响应 |
| `list_endpoints` | 客户端 → 服务端 | 查询已连接端点列表 |
| `endpoint_list` | 服务端 → 客户端 | 返回当前端点列表 |
| `message` | 双向 | 应用消息（按 `to` 字段路由） |

### 消息生命周期

```
客户端发送：       {"type":"message","to":"lead-agent","body":{"text":"hello","type":"ack"}}
                                                                                    │
总线服务端：       记录时间戳，解析目标端点，投递                                      │
                                                                                    ▼
目标接收：         {"type":"message","from":"worker-alpha","to":"lead-agent",
                   "ts":1716307200.123,"body":{"text":"hello","type":"ack"}}
```

总线在路由过程中添加 `from`（如果发送者未设置）和 `ts` 字段。`body` 对象原样透传 —— 总线从不检查或修改应用层负载。
