# xacpp

[English](./README.md)

Agent Control Plane Protocol — TypeScript 实现。

xacpp 定义了 Agent 与对端之间的通信协议。它提供了分层架构，支持基于请求-响应的消息传递、会话管理，以及多种传输后端。

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│  Peer（生命周期编排，发起方）                                    │
│  negotiate → connect → establish → disconnect                │
├──────────────────────────────────────────────────────────────┤
│  Session（通信门面 + 传输状态）                                  │
│  requestCommand / sendEvent / sendBinary；                    │
│  持有 TransferSession（ACK/NACK/RESUME/心跳），                 │
│  自行挂载到当前 transport                                       │
├──────────────────────────────────────────────────────────────┤
│  Transport（连接实体）                                         │
│  物理 IO、二进制组帧、单一 FIFO 出口                             │
├──────────────────────────────────────────────────────────────┤
│  Stdio / TCP                                                  │
└──────────────────────────────────────────────────────────────┘
```

会话的生存期长于单条连接：断连时会话立即从失效 transport 摘除传输挂载——死链上仍在途的帧被丢弃，绝不进入会话状态机——传输状态保留在会话中；重连后会话挂载到新 transport，中断的传输经 RESUME 握手恢复（裁决门控重传，见 SPEC §5）。

## 安装

```bash
npm install xacpp
```

## 快速开始

### 建立会话（发起方）

```typescript
import {
  XacppPeer, XacppSession, SocketTransport,
  NegotiateHandler, EstablishHandler, XacppSessionHandler,
} from "xacpp";

// 会话持有自己的传输状态：独立于任何连接创建，随连接的建立与断开
// 自行挂载到传输上。
const session = new XacppSession(); // TransferConfig::default()

const transport = SocketTransport.connectTo(8080, "127.0.0.1");

// Peer 编排生命周期，并在 connect 时自动挂载会话。
// capabilities 的 fetchExt 字段声明 fetch 扩展支持（SPEC §8.7），
// 省略则禁用 fetch。
const peer = new XacppPeer(
  { commands: [], produceEvents: [], acceptEvents: [] }, // capabilities
  transport,
  negotiateHandler,
  establishHandler,
  session,
);
await peer.connect();

// 建立逻辑会话（向会话填充身份/可发事件）。
await peer.establish(undefined, mySessionHandler, () => {});

// 经会话门面发送命令/事件/二进制。
const response = await session.requestCommand("new_activity");
await session.sendEvent({ type: "think", content: "Hello!" });
const txid = session.sendBinary(payload, (result) => {
  /* BinarySendResult: 恰好一次的终态 */
});
```

### 处理入站连接（响应方）

```typescript
// 响应方没有 Peer：accept 连接、创建会话、重连时恢复身份，
// 然后把会话挂载到传输上。
const session = new XacppSession();
session.bindIdentity("session-1", credentials);
session.wire(transport); // 关闭时自动卸载
session.onBinary((txid, payload) => { /* 消费二进制 */ });

transport.onRequest((_sessionId, payload) => {
  // 对入站命令/事件返回 XacppResponse
});
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
| `XacppPeer` | 生命周期编排（negotiate/connect/establish/disconnect）；connect 时自动把会话挂载到传输（发起方） |
| `XacppSession` | 通信门面（`requestCommand`/`sendEvent`/`sendBinary`）；持有传输状态、跨重连存活，`wire`/`unwire` 挂载到传输 |
| `TransferSession` | 传输事务状态机（ACK/NACK/RESUME/心跳/背压）；由 `XacppSession` 持有 |
| `TransferConfig` | 传输调优（心跳间隔、静默超时、ACK 批量、tick） |
| `XacppTransport` | 连接实体：物理 IO、二进制组帧、单一 FIFO 出口 |
| `XacppSessionHandler` | 处理会话内的入站 Command/Event |
| `NegotiateHandler` / `EstablishHandler` | 握手处理器 |
| `XacppCommand` / `XacppEvent` | 协议命令与事件 |
| `ActivityRef` | 命令/事件信封共用的活动标识引用（`{id}`） |
| `XacppResponse` | 响应载荷（`established`、`acknowledge`、`action` 等） |
| `XacppError` | 错误类，含机器可读错误码 |
| `PeerState` | Peer 状态枚举（`Disconnected`、`Connected`） |

### 传输实现

| 类 | 说明 |
|---|------|
| `StdioTransport` | stdin/stdout 二进制帧管道 |
| `SocketTransport` | TCP socket（`net.Socket`），spawn-per-request 并发模型 |

## 线路协议

组帧采用 XACPP 二进制帧协议（docs/SPEC.md §3/§4）：每个信封作为 `DATA_JSON` 帧的载荷传输。信封结构：

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

## 性能

基线由 `pnpm bench:protocol` 与 `pnpm bench:egress` 测得（TCP 回环；需 Node >= 22.2）。以下数字是后续优化工作的参照基准。

| 指标 | 基线 2026-10-14 | 复测 2026-10-23 |
|---|---|---|
| 编解码开销 vs JSONL | −38% ~ −83%（CRC + 40 B 帧头） | 该组 PASS（分块路径） |
| 请求-响应 RTT | p50 0.071 ms @ ~11.9k req/s | p50 0.070 ms |
| 64 MiB `sendBinary`（1 MiB 工作帧） | 单流 448 MiB/s | 461 MiB/s |
| 4 × 16 MiB 并发 | 399 MiB/s | 390–439 MiB/s |
| 批量传输下心跳 p50 | ≈98 ms | 178–196 ms |
| 裸 peer 有序出站 | 入队 ~196k events/s，排空 ~38k | 入队 184k，排空 35k |

## 许可证

MIT
