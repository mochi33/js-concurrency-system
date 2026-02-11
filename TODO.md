# TODO: js-concurrency-system 改善タスクリスト

> P0 修正（Connection Pool / Multi-task Concurrency / Execution Timeout）は実装済み。
> 全 12 E2E テスト pass 確認済み。

---

## High Priority（本番運用に必要）

### 1. Graceful Drain（優雅な停止）
- 現状: `shutdown` → 即座に close（実行中タスクが死ぬ）
- 必要: 「新規タスクを受け付けない + 実行中タスクの完了を待つ」
- API サーバーデプロイ時にデータロスを防ぐために必須

### 2. Discovery bind address の設定可能化
- 現状: `127.0.0.1` にハードコード
- 必要: `--host` フラグで設定可能にする
- リモートノード接続（マルチマシン構成）に必須
- 変更量は少ない（1行レベル）

### 3. Closed Multiplexer のメモリリーク修正
- 閉じた Multiplexer が `incomingMuxes` Set に残り続ける
- 長期運用でメモリが蓄積する問題
- クリーンアップ処理の追加が必要

---

## Medium Priority（運用性・可観測性の向上）

### 4. Discovery 再接続
- 現状: Discovery が落ちるとノードが切断され、復旧不可
- Discovery 再起動時の自動再接続が必要
- 本番稼働率に直結

### 5. 構造化ログ + Request ID トラッキング
- 現状: `console.log` のみ、トレーサビリティなし
- 必要: 全ログに `taskId` を含める
- フォーマット例: `[2024-02-11T10:00:00Z] [task=abc123] exec fibonacci on node-1`
- デバッグ効率を大幅に改善

### 6. メトリクス & Observability
- Discovery が公開すべき情報:
  - ノード数・アクティブタスク数
  - タスク完了レイテンシ (p50, p95, p99)
  - `at_capacity` 拒否率
  - spawn 待ち時間
- オートスケーリング判断に必要

---

## Low Priority（拡張機能・Nice-to-have）

### 7. Fan-out / Map-Reduce パターン
- 同じタスクを全ノードに spawn して結果を集約
- 例: `const results = await caller.fanout("search", [query], { merge: "concat" })`

### 8. Sticky Routing（Affinity）
- 同じキー（userId 等）を一貫して同じノードにルーティング
- ノードローカルキャッシュの活用が可能
- 例: `spawn("getUserProfile", [userId], { affinity: userId })`

### 9. タスク優先度
- 例: `spawn("urgent_task", [data], { priority: "high" })`
- Discovery がルーティング時に高優先タスクを優先

### 10. Pure Function のリザルトキャッシュ
- 関数を pure とマーク: `registry.register("fibonacci", fn, { pure: true, cacheTTL: 60_000 })`
- 同じ引数 → キャッシュ済み結果を即座に返却

### 11. Binary Protocol（MessagePack）
- JSON を MessagePack に置き換え
- 大きなデータ転送時に特に有効
- シリアライズ/デシリアライズの高速化 + 転送サイズ削減

---

## 完了済み (P0)

- [x] **Connection Pool** — TCP 接続を `Map<string, Multiplexer>` で再利用。同一ピアへの複数タスクが 1 接続を共有
- [x] **Multi-task Concurrency** — `idle/busy` 二値を `activeTasks` カウンタ + `maxConcurrency`（デフォルト 4）に置換
- [x] **Execution Timeout** — `SpawnOptions` の `execTimeout` 指定。Executor がタスクと `setTimeout` を race し、超過時に `ExecTimeoutError`
