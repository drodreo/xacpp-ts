# xacpp

[English](./README.md)

Agent Control Plane Protocol — TypeScript 实现。

xacpp 定义了 Agent 与对端之间的通信协议。它提供了分层架构，支持基于请求-响应的消息传递、会话管理，以及多种传输后端。

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│  Peer（协议层）                                                │
│  类型化操作 + 会话路由                                          │
├──────────────────────────────────────────────────────────────┤
│  Session（会话层）                                             │
│  独立会话上下文，直达 Transport 收发                              │
├──────────────────────────────────────────────────────────────┤
│  Transport（传输层）                                           │
│  信封装拆、id 关联、pending 匹配                                 │
├──────────────────────────────────────────────────────────────┤
│  Stdio / TCP / WebSocket                                     │
└──────────────────────────────────────────────────────────────┘
```

## 安装

```bash
npm install xacpp
```

## 快速开始

### 建立会话（发起方）

```typescript
import { XacppPeer, XacppSession, StdioTransport, XacppSessionHandler, XacppResponse } from "xacpp";

// 创建 Transport + Peer
const transport = new StdioTransport(process.stdout, process.stdin);

const peer = new XacppPeer(transport, {
  async onEstablish(transport, credentials) {
    return { sessionId: "server-session", handler: mySessionHandler };
  },
});

await peer.connect();

// 建立逻辑会话
const session = await peer.establish(null, mySessionHandler);

// 通过会话发送命令/事件
const response = await session.requestCommand("new_activity");
await session.requestEvent({ type: "think", content: "Hello!" });
```

### 处理入站请求（响应方）

```typescript
const sessionHandler: XacppSessionHandler = {
  async onCommand(command) {
    // 处理命令
    return { kind: "acknowledge" };
  },
  async onEvent(event) {
    // 处理事件
    return { kind: "acknowledge" };
  },
};
```

### TCP 传输（网络通信）

```typescript
import { SocketTransport } from "xacpp";

// 客户端
const client = SocketTransport.connectTo(8080, "127.0.0.1");

// 服务端（使用已 accept 的 socket）
const server = new SocketTransport(acceptedSocket);
```

## API

### 类型

| 类型 | 说明 |
|------|------|
| `XacppTransport` | 传输层接口（`connect`、`disconnect`、`send`、`onRequest`） |
| `XacppPeer` | 协议端点，含会话路由 |
| `XacppSession` | 逻辑会话，直达 Transport 收发 |
| `XacppSessionHandler` | 处理会话内的入站 Command/Event |
| `EstablishHandler` | 处理 Establish 握手请求 |
| `XacppCommand` | 协议命令（`establish`、`new_activity` 等） |
| `ActivityRef` | 命令/事件信封共用的活动标识引用（`{id}`） |
| `XacppEvent` | 协议事件（think、content_delta、pair_complete 等） |
| `XacppRequest` | 请求载荷（`command` 或 `event`） |
| `XacppResponse` | 响应载荷（`established`、`acknowledge`、`action` 等） |
| `XacppError` | 错误类，含机器可读错误码 |
| `PeerState` | Peer 状态枚举（`Disconnected`、`Connected`） |

### 传输实现

| 类 | 说明 |
|---|------|
| `StdioTransport` | stdin/stdout JSONL 管道 |
| `SocketTransport` | TCP socket（`net.Socket`），spawn-per-request 并发模型 |

## 线路协议

JSONL（每行一个 JSON 对象），信封结构：

```json
{"type":"request","id":"r1","payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}
{"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1"}}
```

Generic 命令携带可选的 `activity` 引用（`activity: {"id": string}`）。缺省时字段不出现在线路上；协议层不强制——校验权归命令实现方。

活动事件信封携带结构化的 `activity: {"id": string}` 引用：

```json
{"type":"request","id":"r2","payload":{"kind":"event","payload":{"activity":{"id":"act-1"},"event":{"name":"think","data":null}}}}
```

## 命令声明约定

命令声明 schema 可携带 `dispatcher` 字段，枚举值 `"bridge" | "tool"`，缺省视为 `"bridge"`：`bridge` 表示直连事件桥系统路径，`tool` 表示注入接收方模型工具面（同名 replace，对端声明优先）。声明 schema 为透传 JSON——协议库不强制类型。

## 许可证

MIT
