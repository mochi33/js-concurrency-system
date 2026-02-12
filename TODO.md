# TODO: js-concurrency-system 改善タスクリスト

> P0 修正（Connection Pool / Multi-task Concurrency / Execution Timeout）は実装済み。
> 全 12 E2E テスト pass 確認済み。

---

## ~~High Priority（本番運用に必要）~~ → 完了済み

### 1. ~~Graceful Drain（優雅な停止）~~ ✅
- `ProcessNode.close(drainTimeout)`: draining フラグで新規タスクを拒否し、実行中タスクの完了を待機（タイムアウト付き）
- `Discovery.shutdown(drainTimeout)`: 全ピアに shutdown 送信後、drain 完了を待機
- API サーバーデプロイ時のデータロスを防止

### 2. ~~Discovery bind address の設定可能化~~ ✅
- `DiscoveryConfig.host` フィールド追加（デフォルト: `127.0.0.1`）
- `--host` CLI フラグで設定可能に
- リモートノード接続（マルチマシン構成）に対応

### 3. ~~Closed Multiplexer のメモリリーク修正~~ ✅
- `Multiplexer.onClose()` コールバック追加
- 閉じた Multiplexer を `incomingMuxes` / `connectionPool` から自動削除
- 長期運用時のメモリ蓄積問題を解決

---

## Medium Priority（運用性・可観測性の向上）

### 4. Registry の簡易記法（Record 形式）サポート
- 現状: `new Registry()` + `register()` を繰り返す冗長な記述が必要
- 必要: `export default { multiply: async (ctx, a, b) => a * b }` のような Record 形式で定義可能に
- `Registry.from()` で Registry インスタンスと `Record<string, TaskFunction>` の両方を受け付ける
- `node_main.ts` / `node.ts` の `connect()` のローディング箇所を対応
- 従来の Registry 形式も引き続き動作させる（後方互換）

### 5. メッセージのランタイム型バリデーション
- 現状: `deserialize()` が `JSON.parse() as Message` でキャストしており、ランタイムの型検証なし
- 問題点:
  - 不正な JSON がそのまま `Message` 型として通過する
  - `msg as DiscoveryMessage` / `msg as P2PMessage` の接続種別キャストも unsafe
  - switch の `default` ケースが網羅性チェック（exhaustiveness check）を無効化
  - Multiplexer が `taskId` の有無だけでルーティングし、`type` を確認しない
- 対応:
  - `deserialize()` に型ガード or スキーマバリデーション（zod 等）を導入
  - 接続種別ごとに型を分離（Discovery 接続は `DiscoveryMessage` のみ受信）
  - switch の `default` を `never` 型チェックに置換して網羅性を保証

### 6. Discovery 再接続
- 現状: Discovery が落ちるとノードが切断され、復旧不可
- Discovery 再起動時の自動再接続が必要
- 本番稼働率に直結

### 7. 構造化ログ + Request ID トラッキング
- 現状: `console.log` のみ、トレーサビリティなし
- 必要: 全ログに `taskId` を含める
- フォーマット例: `[2024-02-11T10:00:00Z] [task=abc123] exec fibonacci on node-1`
- デバッグ効率を大幅に改善

### 8. メトリクス & Observability
- Discovery が公開すべき情報:
  - ノード数・アクティブタスク数
  - タスク完了レイテンシ (p50, p95, p99)
  - `at_capacity` 拒否率
  - spawn 待ち時間
- オートスケーリング判断に必要

---

## Low Priority（拡張機能・Nice-to-have）

### 9. Fan-out / Map-Reduce パターン
- 同じタスクを全ノードに spawn して結果を集約
- 例: `const results = await caller.fanout("search", [query], { merge: "concat" })`

### 10. Sticky Routing（Affinity）
- 同じキー（userId 等）を一貫して同じノードにルーティング
- ノードローカルキャッシュの活用が可能
- 例: `spawn("getUserProfile", [userId], { affinity: userId })`

### 11. タスク優先度
- 例: `spawn("urgent_task", [data], { priority: "high" })`
- Discovery がルーティング時に高優先タスクを優先

### 12. Pure Function のリザルトキャッシュ
- 関数を pure とマーク: `registry.register("fibonacci", fn, { pure: true, cacheTTL: 60_000 })`
- 同じ引数 → キャッシュ済み結果を即座に返却

### 13. Binary Protocol（MessagePack）
- JSON を MessagePack に置き換え
- 大きなデータ転送時に特に有効
- シリアライズ/デシリアライズの高速化 + 転送サイズ削減

---

## 完了済み (P0)

- [x] **Connection Pool** — TCP 接続を `Map<string, Multiplexer>` で再利用。同一ピアへの複数タスクが 1 接続を共有
- [x] **Multi-task Concurrency** — `idle/busy` 二値を `activeTasks` カウンタ + `maxConcurrency`（デフォルト 4）に置換
- [x] **Execution Timeout** — `SpawnOptions` の `execTimeout` 指定。Executor がタスクと `setTimeout` を race し、超過時に `ExecTimeoutError`

## 完了済み (High Priority)

- [x] **Graceful Drain** — `close(drainTimeout)` で新規タスク拒否 + 実行中タスク完了待ち。Discovery も drain 対応
- [x] **Discovery bind address** — `DiscoveryConfig.host` + `--host` CLI フラグ。デフォルト `127.0.0.1`
- [x] **Multiplexer メモリリーク修正** — `onClose` コールバックで `incomingMuxes` / `connectionPool` から自動削除
