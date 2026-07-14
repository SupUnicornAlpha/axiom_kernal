# axiom_kernal TODO

## 已完成（粗版）

- Rust workspace
- `axiom-spec`
- `axiom-core`
- `axiom-cli`
- 最小 `Run/Step/Event`
- 最小 `ShellDecision`
- 最小 `CapabilityLease`
- 最小 `ChildRun`
- JSONL EventLog

## 下一步

- 增加单元测试与 golden EventLog
- 抽出 `LocalTransport` trait
- 抽出 `SubRunTransport`
- 引入 `PolicyEngine` 接口
- 引入 `RunNamespace` 的真实校验
- 引入 `BudgetGroup` 的 token/time/cost 维度
- 实现 sidecar driver protocol
- 实现 TypeScript SDK（已建最小骨架）
- 实现 Python SDK（已建最小骨架）
- 实现 Go SDK（已完成 spec、sidecar、workspace tools 与 conformance）
