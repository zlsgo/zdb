# Transaction Session Optimization Design

> 目标：修复事务上下文与会话状态传播缺陷，并补充最小回归测试。

## 已确认方案
- 统一事务/Source/Replica/Migration 的会话绑定逻辑，避免丢失 idKey、Debug 等 DB 状态。
- 在事务开始时改用 BeginTx 透传 context。
- 归还 Session 到池时重置 ctx，避免复用脏上下文。
- 增加针对事务状态传播和 context 取消的回归测试。
