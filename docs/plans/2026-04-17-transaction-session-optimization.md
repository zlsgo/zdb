# Transaction Session Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 修复事务上下文与会话状态传播缺陷，并用最小回归测试锁住行为。

**Architecture:** 通过统一 `DB` 与 `Session` 的绑定入口，保证 `Transaction`、`Source`、`Replica`、`Migration` 看到一致的 `DB` 状态；事务开始改为 `BeginTx`，让调用方传入的 `context.Context` 真实参与建事务过程。测试采用包内测试直接校验私有状态传播，再补事务取消语义。

**Tech Stack:** Go, `database/sql`, SQLite test driver

---

### Task 1: 修复会话绑定与事务上下文

**Files:**
- Modify: `db.go`
- Modify: `session.go`
- Modify: `engine.go`
- Modify: `migration.go`

**Step 1: 统一 DB 会话绑定入口**

在 `db.go` 添加 `withSession`，基于当前 `DB` 浅拷贝并注入 `session`，保留 `idKey`、`Debug`、`pools` 等状态。

**Step 2: 修复事务 begin 语义**

在 `session.go` 将 `Begin` 改为 `BeginTx(s.ctx, nil)`，并在归还池对象时清空 `ctx`。

**Step 3: 收敛调用点**

让 `Transaction`、`Source`、`Replica`、`Migration` 全部走统一的 `withSession` 绑定逻辑。

### Task 2: 补充回归测试

**Files:**
- Create: `session_internal_test.go`

**Step 1: 测试事务状态传播**

验证事务回调中的 `DB` 保留 `idKey`、`Debug` 且带有非空 `session`。

**Step 2: 测试 begin 使用 context**

对已取消 `context` 调用 `Transaction`，预期返回 `context.Canceled`。

**Step 3: 测试非事务会话绑定流**

验证 `Source`、`Replica`、`Migration` 也保留相同状态。

### Task 3: 验证与模块整理

**Files:**
- Modify if needed: `go.mod`
- Modify if needed: `go.sum`

**Step 1: 运行最小测试集**

Run: `go test ./...`

**Step 2: 若模块元数据阻塞测试**

Run: `go mod tidy`

**Step 3: 重新执行测试并确认通过**

Run: `go test ./...`
