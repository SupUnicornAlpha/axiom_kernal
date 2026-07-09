# TypeScript SDK

这是第一版极简骨架，目标不是在 SDK 内复制 runtime，而是：

- 构建 `RunSpec`
- 注册 tool metadata
- 为后续 RemoteTransport / sidecar transport 提供薄封装入口

当前阶段故意很薄：

- 不实现 scheduler loop
- 不实现 state apply
- 不实现内核旁路执行

后续对齐项：

- 与 Rust spec 的 JSON schema 对齐
- 与 `axiom_validate/fixtures/runspec` golden fixtures 对齐
- 增加 event stream client
- 增加 child run builder
