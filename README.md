# axiom_kernal

`axiom_kernal` 是 Axiom / Agent Atom 的框架实现项目，负责实现 Rust canonical runtime 与主流语言薄 SDK。

> 当前目录名沿用 `axiom_kernal`。如果后续对外发布需要统一拼写，可再决定是否迁移到 `axiom_kernel`。

## 职责

- 实现 Rust runtime core。
- 冻结并维护最小 spec。
- 实现 Kernel、EventBus、Checkpoint、Transport、CapabilityRegistry。
- 实现 ShellDecision、ReActScheduler、ChildRun、CapabilityLease。
- 后续提供 TypeScript、Python、Go、Java 薄 SDK。

## MVP 优先级

1. `Run/Step/Event` 内核语义。
2. `ShellDecision` 审计闭环。
3. `ChildRun` 沙箱合并语义。
4. `LocalTransport` 与 `SubRunTransport`。
5. JSONL EventLog 与 conformance golden cases。

## 与 axiom_validate 的关系

每个核心能力完成后，都必须在 `../axiom_validate` 增加对应验证 case。框架实现是否进入下一阶段，以验证项目的结果为准。

详细路线见 `../docs/09-dual-track-development-validation.md`。
