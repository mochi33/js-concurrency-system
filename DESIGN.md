# js-concurrency-system 設計書

## 1. 概要

Denoプロセスをプールし、タスクをP2Pで分散実行する並行処理システム。

- **Discovery Service**: 軽量なピア追跡サービス。メタデータのみ管理し、タスクデータは中継しない
- **全プロセスは対等**: spawn()もexec受付も全プロセスが可能。直接通信する
- **マルチタスク並行実行**: ノードあたり最大 `maxConcurrency` タスクを同時処理
- **コネクション多重化**: Multiplexerにより1本のTCP接続で複数タスクを並行処理
- **エラー後もプロセス再利用**: ヘルスチェック付き
- **TCP通信**: ローカルもリモートも同じプロトコル
- **Actorモデル**: ステートフルなActorインスタンスをリモートに生成・呼び出し
- **共有オブジェクトストア**: 大規模データをコピーせずObjectRefで参照渡し
- **構造化ログ**: コンテキストフィールド付きの構造化ログ出力
- **メトリクス収集**: Prometheus互換のメトリクスエンドポイント
- **ランタイムバリデーション**: Zodスキーマによるメッセージ検証

## 2. アーキテクチャ

```
┌──────────────────────────────────────────────────────┐
│                    Discovery Service                  │  軽量・置換可能
│                                                      │
│  ・ピア登録/一覧                                     │  ← 死んでも実行中タスクに影響なし
│  ・キャパシティ追跡 (activeTasks / maxConcurrency)    │
│  ・managedプロセスのspawn/kill                        │
│  ・メトリクス収集 & Prometheusエンドポイント          │
│  ・タスクデータは中継しない                           │
└──────┬───────────────────────────────────────────────┘
       │ TCP (登録・問い合わせのみ)
       │
  ┌────┼──────────┬────────────┐
  │    │          │            │
  ▼    ▼          ▼            ▼
┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐
│  A  │  │  B  │  │  C  │  │  D  │   各ノードが自分のTCPポートでlisten
│:4001│  │:4002│  │:4003│  │:4004│   maxConcurrency=4 (デフォルト)
└──┬──┘  └──┬──┘  └──┬──┘  └─────┘
   │        │       │
   └─ Mux ──┘       │    Multiplexer: 1 TCP接続で複数タスクを並行処理
            └─ Mux ─┘    Discoveryを経由しない
```

### 主要コンポーネント

| コンポーネント | 役割 |
|---|---|
| **Discovery** | ピア登録・キャパシティ追跡・プロセスライフサイクル管理 |
| **ProcessNode** | P2Pリスナー・タスク実行・Actor管理・ObjectStore保持 |
| **Multiplexer** | 1 TCP接続を複数タスクで共有するメッセージルーター |
| **Registry** | タスク関数・Actorクラスの登録管理 |
| **Logger** | コンテキストフィールド付き構造化ログ |
| **MetricsCollector** | カウンター・ゲージ・パーセンタイルの収集 |
| **ObjectStore** | ノードローカルなオブジェクト保持・リモートフェッチ |

### Discoveryが落ちた場合の影響

| 状況 | 影響 |
|------|------|
| 実行中タスク (A↔B直接接続) | **影響なし** — 直接接続は維持される |
| 新規spawn | **不可** — ピア一覧を取得できない |
| スケールアップ/ダウン | **停止** — Discoveryのみがmanagedプロセスを管理 |
| Discovery復旧後 | ノードが指数バックオフで自動再接続 → 正常復帰 |

## 3. プロトコル

### 3.1 フレーミング

TCP上で長さプレフィクス方式を使用。

```
┌──────────────────┬────────────────────────────┐
│  4 bytes          │  N bytes                   │
│  payload length   │  Extended JSON (UTF-8)     │
│  (BigEndian u32)  │                            │
└──────────────────┴────────────────────────────┘
```

最大メッセージサイズ: 16 MiB (超過はエラー)

### 3.2 拡張JSON (Extended JSON)

標準JSONでは表現できない型を `$type` マーカーで拡張する。

```ts
// エンコード例
Date        → { "$type": "Date",        "value": "2024-01-01T00:00:00.000Z" }
BigInt      → { "$type": "BigInt",      "value": "12345678901234567890" }
Map         → { "$type": "Map",         "entries": [[key, value], ...] }
Set         → { "$type": "Set",         "values": [value, ...] }
undefined   → { "$type": "undefined" }
Uint8Array  → { "$type": "Uint8Array",  "base64": "SGVsbG8=" }
ArrayBuffer → { "$type": "ArrayBuffer", "base64": "SGVsbG8=" }
RegExp      → { "$type": "RegExp",      "source": "\\d+", "flags": "gi" }
ObjectRef   → { "$type": "ObjectRef",   "id": "...", "size": 1024, "ownerHost": "127.0.0.1", "ownerPort": 4001 }
```

- `$type` キーを持つプレーンオブジェクトとの衝突を避けるため、ユーザーデータの `$type` キーは `$$type` にエスケープされる
- `ObjectRef` も拡張JSONとして自動的にシリアライズ・デシリアライズされる
- protocol.ts の `encode()` / `decode()` に集約。将来MessagePack等に差し替え可能

### 3.3 ランタイムバリデーション

全メッセージは受信時にZodスキーマで検証される。不正なメッセージはパースエラーとして接続を切断する。

```ts
// schemas.ts — Zodによる discriminatedUnion
const DiscoveryMessageSchema = z.discriminatedUnion("type", [
  RegisterSchema,      // type: "register"
  RegisteredSchema,    // type: "registered"
  CapacityChangeSchema, // type: "capacity_change"
  FindSchema,          // type: "find"
  FoundSchema,         // type: "found"
  // ... 他
]);

const P2PMessageSchema = z.discriminatedUnion("type", [
  ExecSchema,          // type: "exec"
  AcceptSchema,        // type: "accept"
  RejectSchema,        // type: "reject"
  // ... Actor, ObjectFetch 等含む全P2Pメッセージ
]);
```

- `deserializeDiscovery()` / `deserializeP2P()` がバリデーション付きデシリアライズを行う
- パース結果は `ParsedDiscoveryMessage` / `ParsedP2PMessage` 型として型安全に利用可能

### 3.4 Process ↔ Discovery メッセージ

```ts
// --- 登録 ---

// Process → Discovery: 起動時に自身を登録
{
  type: "register"
  processId: string         // UUID
  host: string              // 自プロセスのlistenアドレス
  port: number              // 自プロセスのlistenポート
  funcs: string[]           // 実行可能な関数名 + Actor名 一覧
  maxConcurrency: number    // 最大同時タスク数
}

// Discovery → Process: 登録受理
{
  type: "registered"
  processId: string
}

// --- キャパシティ通知 ---

// Process → Discovery: キャパシティ変更を報告
{
  type: "capacity_change"
  activeTasks: number       // 現在実行中のタスク数
  maxConcurrency: number    // 最大同時タスク数
}

// --- ピア検索 ---

// Process → Discovery: 指定関数を実行可能なピアを要求
{
  type: "find"
  func: string
  exclude?: string[]        // 除外するprocessId（リトライ時）
}

// Discovery → Process: 候補ピアを返却 (キャパシティに空きがあるピア)
{
  type: "found"
  peers: Array<{
    processId: string
    host: string
    port: number
  }>                        // 空配列 = 該当なし
}

// --- スケーリング要求 ---

// Process → Discovery: 新プロセスのspawnを要求
{
  type: "request_spawn"
  reason: "no_idle" | "overflow" // overflow = デッドロック回避用
}

// Discovery → Process: spawn結果
{
  type: "spawn_result"
  success: boolean
  processId?: string
  host?: string
  port?: number
}

// --- ヘルスチェック ---

// 双方向: Discovery ↔ Process
{ type: "heartbeat" }
{ type: "heartbeat_ack" }

// --- ライフサイクル ---

// Discovery → Process: 終了要求（スケールダウン時）
{ type: "shutdown" }

// Process → Discovery: 切断通知（graceful disconnect）
{ type: "bye" }
```

### 3.5 Process ↔ Process (P2P) メッセージ

callerがexecutorにMultiplexer経由で送受信する。1本のTCP接続で複数タスクが並行する。

```ts
// --- タスク開始 ---

// Caller → Executor: タスク実行要求
{
  type: "exec"
  taskId: string            // caller側で生成 (UUID)
  func: string
  args: unknown[]           // Extended JSONでシリアライズ
  execTimeout?: number      // 実行タイムアウト (ms)
}

// Executor → Caller: 受諾
{ type: "accept", taskId: string }

// Executor → Caller: 拒否
{
  type: "reject"
  taskId: string
  reason: "at_capacity" | "unknown_func"
}

// --- 双方向チャネル (accept後) ---

// 双方向: Caller ↔ Executor
// 中間データの送受信
{
  type: "send"
  taskId: string
  value: unknown
}

// --- 完了 ---

// Executor → Caller: 最終結果（関数のreturn値）
{
  type: "result"
  taskId: string
  value: unknown
}

// Executor → Caller: エラー（関数内でthrow）
{
  type: "error"
  taskId: string
  error: {
    message: string
    name: string
    stack?: string
  }
}

// --- キャンセル ---

// Caller → Executor: タスクキャンセル要求（協調的）
{ type: "cancel", taskId: string }
```

### 3.6 ObjectFetch メッセージ

ObjectRefを含む引数を受信したノードが、オーナーノードからデータを取得する。

```ts
// Requester → Owner: オブジェクトデータの要求
{
  type: "object_fetch"
  taskId: string
  objectId: string          // ObjectRef.id
}

// Owner → Requester: オブジェクトデータの返却
{
  type: "object_fetch_response"
  taskId: string
  objectId: string
  found: boolean
  data: unknown             // found=true のとき実データ
}
```

### 3.7 Actor メッセージ

リモートノード上にステートフルなActorインスタンスを生成・呼び出し・破棄する。

```ts
// --- 生成 ---

// Client → Host: Actor生成要求
{
  type: "actor_create"
  taskId: string
  actorName: string         // Registry に登録された Actor 名
}

// Host → Client: 生成結果
{
  type: "actor_create_result"
  taskId: string
  actorId?: string          // 成功時: 生成された Actor の UUID
  error?: SerializedError   // 失敗時: エラー情報
}

// --- メソッド呼び出し ---

// Client → Host: Actorメソッド呼び出し
{
  type: "actor_call"
  taskId: string
  actorId: string
  method: string
  args: unknown[]
}

// Host → Client: 呼び出し結果
{ type: "actor_result", taskId: string, value: unknown }

// Host → Client: 呼び出しエラー
{ type: "actor_error", taskId: string, error: SerializedError }

// --- 破棄 ---

// Client → Host: Actor破棄要求
{
  type: "actor_destroy"
  taskId: string
  actorId: string
}
```

## 4. マルチタスク並行実行

### 4.1 キャパシティモデル

各ノードは `maxConcurrency` (デフォルト: 4) で同時に処理可能なタスク数を宣言する。Discoveryはこの情報をもとにルーティングする。

```
Node A (maxConcurrency=4)
├── Task 1 (active)
├── Task 2 (active)
├── Task 3 (active)
└── (空き1スロット)         ← Discoveryが新しいタスクをルーティング可能
```

### 4.2 capacity_change 通知

ノードはタスク受諾・完了のたびにDiscoveryへキャパシティ変更を通知する。

```ts
// タスク accept 直後:
{ type: "capacity_change", activeTasks: 3, maxConcurrency: 4 }

// タスク完了後:
{ type: "capacity_change", activeTasks: 2, maxConcurrency: 4 }
```

Discoveryは `activeTasks < maxConcurrency` のノードを「キャパシティあり」と判断しfind応答に含める。

### 4.3 exec受信時の判定 (executor側)

JavaScriptのシングルスレッド性を利用し、状態チェックと変更を同期的に行う。
これによりロックなしで競合を回避できる。

```ts
// 同期コードブロック内（awaitなし）で判定・変更するため安全
function handleExecRequest(mux: Multiplexer, msg: ExecMessage): void {
  if (this.activeTasks >= this.maxConcurrency) {
    mux.writeMessage({ type: "reject", taskId: msg.taskId, reason: "at_capacity" });
    return;
  }

  const taskFn = this.registry.get(msg.func);
  if (!taskFn) {
    mux.writeMessage({ type: "reject", taskId: msg.taskId, reason: "unknown_func" });
    return;
  }

  // ↓ ここまで同期的に到達 — 他のexecが割り込む余地なし
  this.activeTasks++;
  this.executeTask(mux, msg, taskFn);  // async — ここからは非同期
}
```

## 5. Multiplexer

### 5.1 概要

Multiplexerは1本のTCP接続（FramedConnection）を複数タスクで共有するメッセージルーター。従来の「1タスク = 1 TCP接続」からの改善で、接続オーバーヘッドを削減する。

```
Node A                           Node B
┌──────────────┐                ┌──────────────┐
│ spawn("X")─┐ │   1本のTCP    │ ┌─ Task X    │
│ spawn("Y")─┤ ├═══════════════╡ ├─ Task Y    │
│ spawn("Z")─┘ │   Multiplexer │ └─ Task Z    │
└──────────────┘                └──────────────┘
```

### 5.2 メッセージルーティング

- 各メッセージには `taskId` フィールドがある
- Multiplexerは `taskId` → handler のマップで適切なタスクにディスパッチする
- 新規 `exec` メッセージなど未登録の `taskId` は `unroutedHandler` で処理される

```ts
class Multiplexer {
  registerTask(taskId: string, handler: TaskMessageHandler): void
  unregisterTask(taskId: string): void
  setUnroutedHandler(handler: UnroutedMessageHandler): void
  writeMessage(msg: Message): Promise<void>
}
```

### 5.3 コネクションプール

- caller側: `connectionPool` で `host:port` → Multiplexer のマップを保持
- executor側: 受信接続ごとにMultiplexerを作成し `incomingMuxes` で管理
- TCP切断時にMultiplexerは全登録タスクに合成エラーメッセージを配信してクリーンアップ

## 6. Actorモデル

### 6.1 概要

Actorはステートフルなオブジェクトをリモートノード上に配置し、メソッド呼び出しで操作するモデル。メールボックスパターンにより、並行呼び出しでも状態の一貫性を保証する。

### 6.2 ActorInstance (サーバー側)

```ts
class ActorInstance {
  // メールボックス: 呼び出しをキューイングし1つずつ直列実行
  private mailbox: MailboxItem[]
  private processing = false

  async call(method: string, args: unknown[]): Promise<unknown>
  destroy(): void
}
```

- メソッド呼び出しはメールボックスに追加され、FIFO順に1つずつ処理される
- 同時実行されないため、Actor内の状態は安全に変更可能
- `destroy()` で未処理のメールボックスアイテムをすべてreject

### 6.3 ActorHandle (クライアント側)

```ts
interface ActorHandle {
  readonly actorId: string
  call(method: string, ...args: unknown[]): Promise<unknown>
  destroy(): Promise<void>
}
```

### 6.4 ライフサイクル

```
Client                     Host Node
  │                            │
  │── actor_create ──────────→│  Registry から ActorClass を取得
  │                            │  new ActorClass() でインスタンス生成
  │                            │  activeTasks++ (キャパシティ消費)
  │←── actor_create_result ──│  actorId を返却
  │                            │
  │── actor_call(method) ────→│  mailbox に追加 → 直列実行
  │←── actor_result ──────────│
  │                            │
  │── actor_call(method2) ───→│  前のcallが完了してから実行
  │←── actor_result ──────────│
  │                            │
  │── actor_destroy ─────────→│  instance.destroy()
  │                            │  activeTasks-- (キャパシティ解放)
  │←── actor_result(null) ───│
```

### 6.5 使用例

```ts
// Actor クラスの定義
class Counter {
  private count = 0;

  increment(n: number): number {
    this.count += n;
    return this.count;
  }

  getCount(): number {
    return this.count;
  }
}

// Registry に登録
const registry = new Registry();
registry.registerActor("Counter", Counter);

// リモートで Actor を生成・呼び出し
const handle = await node.createActor("Counter");
await handle.call("increment", 5);   // → 5
await handle.call("increment", 3);   // → 8
await handle.call("getCount");        // → 8
await handle.destroy();
```

## 7. 共有オブジェクトストア

### 7.1 概要

大規模データ（画像、モデルパラメータ等）をノード間で効率的に共有するための仕組み。データそのものを引数に渡す代わりに `ObjectRef`（参照）を渡し、executor側が必要時にオーナーノードからフェッチする。

### 7.2 ObjectRef

```ts
class ObjectRef {
  readonly id: string          // UUID
  readonly size: number        // 推定バイトサイズ
  readonly ownerHost: string   // データ所有ノードのホスト
  readonly ownerPort: number   // データ所有ノードのポート
}
```

- 拡張JSONとして自動シリアライズされるため、spawn引数にそのまま渡せる
- シリアライズ時はデータ本体ではなくメタデータ（id, size, ownerHost, ownerPort）のみ転送

### 7.3 ObjectStore

```ts
class ObjectStore {
  put(data: unknown): ObjectRef     // データを格納しObjectRefを返す
  get(ref: ObjectRef): unknown      // ローカル格納データを取得
  has(id: string): boolean          // 存在確認
  delete(ref: ObjectRef): void      // 参照カウントを減らし、0なら削除
  cache(id: string, data: unknown): void  // リモートフェッチ結果をキャッシュ
  pin(ref: ObjectRef): void         // 参照カウントを増やす
  unpin(ref: ObjectRef): void       // 参照カウントを減らす
}
```

### 7.4 リモートフェッチフロー

```
Node A (caller)              Node B (executor)           Node A (owner)
    │                            │                           │
    │── exec(args=[ObjectRef]) →│                           │
    │                            │  ObjectRef を検出         │
    │                            │  ローカルに無い           │
    │                            │── object_fetch ─────────→│
    │                            │←─ object_fetch_response ─│
    │                            │  データをキャッシュ       │
    │                            │  タスク実行               │
    │←── result ─────────────────│                           │
```

### 7.5 使用例

```ts
// caller側: 大きなデータを ObjectRef として格納
const ref = node.put(largeDataset);

// spawn時に ObjectRef を引数として渡す（データ本体は転送されない）
const ch = node.spawn("processData", [ref]);
const result = await ch.join();
```

## 8. spawnフロー

### 8.1 正常系

```
Process A (caller)         Discovery         Process B (executor)
    │                          │                     │
    │── find(func="calc") ───→│                     │
    │                          │  activeTasks < maxConcurrency のピアを検索
    │←── found([B:4002]) ─────│                     │
    │                          │                     │
    │══ TCP接続 (or 既存Mux) ════════════════════→│  ← Multiplexer接続
    │                          │                     │
    │── exec(T1,"calc",args) ─────────────────────→│
    │                          │                     │
    │                          │←── capacity_change ─│ ← activeTasks++ を通知
    │                          │                     │
    │←── accept(T1) ──────────────────────────────│
    │                          │                     │
    │←── send(T1, progress) ──────────────────────│ ← 同じMux上でP2P通信
    │                          │                     │
    │── send(T1, data) ──────────────────────────→│ ← 同じMux上でP2P通信
    │                          │                     │
    │←── result(T1, value) ───────────────────────│ ← 関数return
    │                          │                     │
    │                          │←── capacity_change ─│ ← activeTasks-- を通知
    │                          │                     │
    │  (Mux接続は保持: 次のタスクに再利用)          │
```

### 8.2 リジェクト & リトライ

Discoveryの状態情報はベストエフォート（staleの可能性あり）。
**最終的な受け入れ判定はexecutor自身が行う。**

```
Process A               Discovery               Process B         Process C
    │                       │                        │                 │
    │── find("calc") ──────→│                        │                 │
    │←── found([B,C]) ─────│                        │                 │
    │                       │                        │                 │
    │── exec(T1) ──────────────────────────────────→│                 │
    │←── reject(at_capacity) ────────────────────────│  ← キャパシティ満杯
    │                       │                        │                 │
    │── exec(T1) ───────────────────────────────────────────────────→│
    │←── accept(T1) ────────────────────────────────────────────────│  ← 成功
    │                       │                        │                 │
```

リトライ上限: 3回。超過時はDiscoveryに `request_spawn` を送り新プロセスを要求。

### 8.3 ネストspawn

タスク実行中のノードも他ノードへのspawn()は可能。

```
Process A              Discovery           Process B           Process C
    │                      │                    │                   │
    │── exec(T1) ────────────────────────────→│                   │
    │                      │                    │                   │
    │                      │←── find("mul") ───│  ctx.spawn()      │
    │                      │── found([C]) ────→│                   │
    │                      │                    │                   │
    │                      │                    │── exec(T2) ─────→│ ← B→C Mux接続
    │                      │                    │←── accept(T2) ──│
    │                      │                    │←── result(T2) ──│
    │                      │                    │                   │
    │←── result(T1) ────────────────────────│                   │
    │                      │                    │                   │
```

## 9. Channel API

### 9.1 receive() のセマンティクス

`receive()` は `{ value, done }` を返す。Iteratorプロトコルと同じ形式。

```ts
// executor側
ctx.send(X)    → caller: { value: X, done: false }
return Y       → caller: { value: Y, done: true }
throw E        → caller: receive() が reject (Eを再throw)
```

```ts
// caller側
const ch = node.spawn("heavyCalc", [10, 20])

const r1 = await ch.receive()  // { value: { progress: 0.5 }, done: false }
const r2 = await ch.receive()  // { value: 35, done: true }
const r3 = await ch.receive()  // throws ChannelClosedError
```

### 9.2 caller側 API

```ts
interface Channel {
  /**
   * 次のメッセージを受信する。
   * - executor の ctx.send() → { value, done: false }
   * - executor の return    → { value, done: true }
   * - executor の throw     → Promise が reject される
   * - チャネル閉鎖後の呼び出し → ChannelClosedError
   */
  receive(): Promise<{ value: unknown; done: boolean }>

  /**
   * executorにデータを送信する。
   * バックプレッシャー: バッファ上限到達時はPromiseがブロック。
   */
  send(value: unknown): Promise<void>

  /**
   * タスクをキャンセルする（協調的）。
   * executor側の ctx.signal が abort される。
   */
  cancel(): void

  /**
   * 全中間メッセージを読み飛ばし、最終結果を返す。
   * エラー時はthrow。
   */
  join(): Promise<unknown>

  /**
   * 中間メッセージをyield。done=true でループ終了。
   * ループ終了後、returnValue で最終結果を取得。
   */
  [Symbol.asyncIterator](): AsyncIterableIterator<unknown>
  returnValue: unknown
}
```

#### 使用パターン

```ts
// パターン1: 中間メッセージ不要、結果だけ欲しい
const result = await ch.join()

// パターン2: 中間メッセージを1つずつ処理
const r1 = await ch.receive()   // { value, done: false }
const r2 = await ch.receive()   // { value, done: true }

// パターン3: for-await でストリーミング処理
for await (const msg of ch) {
  console.log(msg)              // 中間メッセージのみ yield
}
console.log(ch.returnValue)     // 最終結果

// パターン4: キャンセル
const ch = node.spawn("longTask", [])
setTimeout(() => ch.cancel(), 5000)
try {
  const result = await ch.join()
} catch (e) {
  // CancelledError or タスク内のエラー
}
```

### 9.3 executor側 API (Context)

```ts
interface Context {
  /**
   * callerに中間データを送信。
   * バックプレッシャー: バッファ上限到達時はPromiseがブロック。
   */
  send(value: unknown): Promise<void>

  /**
   * callerからのデータを受信。
   * キャンセルされた場合は CancelledError を throw。
   */
  receive(): Promise<unknown>

  /**
   * 別プロセスでタスクをspawn（ネストspawn）。
   */
  spawn(func: string, args: unknown[]): Channel

  /**
   * キャンセルシグナル。Web標準のAbortSignal。
   * Deno/Web APIのsignal引数にそのまま渡せる。
   */
  signal: AbortSignal
}
```

#### executor側の使用例

```ts
registry.register("heavyCalc", async (ctx: Context, x: number, y: number) => {
  // キャンセルチェック（AbortSignal連携）
  const data = await fetchSomething(url, { signal: ctx.signal })

  // 中間データ送信
  await ctx.send({ progress: 0.5 })

  // callerからデータ受信
  const extra = await ctx.receive()

  // ネストspawn
  const sub = ctx.spawn("multiply", [x, 2])
  const doubled = await sub.join()

  // return → caller の receive() に { value, done: true } で届く
  return doubled + y
})
```

### 9.4 バックプレッシャー

TCPのフロー制御を活用する。

```
[Sender]                    [TCP]                  [Receiver]
                                                   受信バッファ
ctx.send(v)                                        (highWaterMark=64)
  → write to TCP ──────→ TCP send buffer ──────→ read → enqueue
                                                      │
                                            buffer >= highWaterMark?
                                            YES → read() を停止
                                                      │
                          TCP window shrinks ←────────┘
                                │
                    write() がブロック ←──┘
                        │
  ctx.send() の Promise が pending ←──┘

  callerが receive() を呼ぶ
    → dequeue → buffer < highWaterMark → read() 再開
      → TCP window opens → write() 解放
        → ctx.send() の Promise が resolve
```

- `highWaterMark` デフォルト: 64メッセージ。spawn時にオプションで変更可能
- `ctx.send()` / `ch.send()` は `Promise<void>` を返す
  - バッファに空きあり → 即座にresolve
  - バッファ満杯 → TCP backpressureが波及して自然にブロック

## 10. キャンセル

### 10.1 フロー

```
Caller                   Executor
  │                          │
  │── cancel(taskId) ──────→│
  │                          │  ctx.signal が abort される
  │                          │
  │                          │  executor は協調的に停止:
  │                          │  - signal をチェック
  │                          │  - or CancelledError を catch
  │                          │
  │←── error(CancelledErr) ─│  (executor が throw した場合)
  │  or                      │
  │←── result(partial) ─────│  (executor がクリーンアップして return した場合)
  │                          │
```

### 10.2 設計判断

- **協調的キャンセル**: executorを強制終了しない。`ctx.signal` を通じて通知するのみ
- executorがキャンセルを無視した場合、タスクは最後まで実行される
- callerは `cancel()` 後も `receive()` / `join()` で結果やエラーを待つことができる
- `ctx.receive()` はキャンセル後に `CancelledError` を throw する

## 11. エラーハンドリングとプロセス再利用

### 11.1 エラー伝搬

```ts
// executor内で例外発生
registry.register("risky", async (ctx) => {
  throw new Error("something went wrong")
})

// caller側
const ch = node.spawn("risky", [])
try {
  await ch.join()
} catch (e) {
  // Error: something went wrong
  // e.stack にはexecutorのスタックトレースも含む
}
```

### 11.2 エラー後の再利用判定

タスク実行後（正常/エラー問わず）、ヘルスチェックを行う。

```
タスク完了/エラー
    │
    ├── ヘルスチェック実行
    │     ├── OK → capacity_change (activeTasks--) を Discovery に送信
    │     └── NG → プロセスを終了。Discoveryが検知し、pool < min なら再spawn
    │
    └── ヘルスチェック内容:
          - メモリ使用量が閾値 (512 MiB) 以下か (Deno.memoryUsage())
```

### 11.3 プロセスクラッシュ時

TCP切断として検知される。

```
1. executorがクラッシュ → TCP切断
2. Multiplexer: 全登録タスクに合成 error メッセージを配信
3. caller側: receive() / join() が Error を throw
4. Discovery側: heartbeat応答なし → peersから削除 → pool < min なら再spawn
5. callerのタスクは失敗（自動リトライはしない — 冪等性を保証できないため）
```

## 12. 構造化ログ

### 12.1 Logger

コンテキストフィールド付きの構造化ログを出力する。

```ts
type LogLevel = "debug" | "info" | "warn" | "error"

class Logger {
  constructor(opts?: { level?: LogLevel; fields?: Record<string, string> })
  child(extraFields: Record<string, string>): Logger
  debug(message: string, extra?: Record<string, string>): void
  info(message: string, extra?: Record<string, string>): void
  warn(message: string, extra?: Record<string, string>): void
  error(message: string, extra?: Record<string, string>): void
}
```

### 12.2 出力形式

```
[2024-02-11T10:00:00.000Z] [INFO] [component=node] [node=abc12345] Listening on 127.0.0.1:4001
[2024-02-11T10:00:01.000Z] [INFO] [component=node] [node=abc12345] [task=def67890] [func=calc] Executing task
```

### 12.3 使い方

```ts
const log = new Logger({ level: "info", fields: { component: "node" } });
const taskLog = log.child({ task: "abc12345", func: "calc" });
taskLog.info("Task started");   // 親のフィールド + 子のフィールドが出力される
```

- `warn`, `error` は `console.error` に出力
- `debug`, `info` は `console.log` に出力
- `level` 未満のログは出力されない (デフォルト: `"info"`)

## 13. メトリクス

### 13.1 MetricsCollector

カウンター、ゲージ、パーセンタイルを収集する。

```ts
class MetricsCollector {
  recordTaskSpawned(): void
  recordTaskCompleted(): void
  recordTaskFailed(): void
  recordRejection(reason: "at_capacity" | "unknown_func"): void
  recordTaskLatency(durationMs: number): void
  recordSpawnWait(durationMs: number): void
  recordProcessSpawn(): void
  setActiveNodeCount(count: number): void
  setActiveTaskCount(count: number): void
  setQueueDepth(depth: number): void
  getMetrics(): MetricsSnapshot
  toPrometheus(): string
}
```

### 13.2 MetricsSnapshot

```ts
interface MetricsSnapshot {
  tasksSpawned: number          // spawn された総タスク数
  tasksCompleted: number        // 正常完了したタスク数
  tasksFailed: number           // 失敗したタスク数
  tasksRejectedAtCapacity: number   // at_capacity で拒否された数
  tasksRejectedUnknownFunc: number  // unknown_func で拒否された数
  activeNodeCount: number       // アクティブなノード数
  activeTaskCount: number       // 実行中のタスク数
  queueDepth: number            // Discovery の待機キュー深度
  processSpawnCount: number     // spawn されたプロセス総数
  latencyP50: number            // タスク実行時間 P50 (ms)
  latencyP95: number            // タスク実行時間 P95 (ms)
  latencyP99: number            // タスク実行時間 P99 (ms)
  latencyCount: number          // レイテンシサンプル数
  latencySum: number            // レイテンシ合計 (ms)
  spawnWaitP50: number          // find → accept の待ち時間 P50 (ms)
  spawnWaitP95: number
  spawnWaitP99: number
  spawnWaitCount: number
  spawnWaitSum: number
}
```

### 13.3 Prometheus エンドポイント

DiscoveryConfigの `metricsPort` を指定すると、HTTPサーバーが起動しPrometheus形式でメトリクスを返す。

```
GET http://localhost:9877/

# HELP task_total Total tasks processed
# TYPE task_total counter
task_total{status="spawned"} 42
task_total{status="completed"} 40
task_total{status="failed"} 2

# HELP active_nodes Current number of active nodes
# TYPE active_nodes gauge
active_nodes 4

# HELP task_latency_seconds Task execution latency in seconds
# TYPE task_latency_seconds summary
task_latency_seconds{quantile="0.5"} 0.015
task_latency_seconds{quantile="0.95"} 0.142
task_latency_seconds{quantile="0.99"} 0.523
```

## 14. 動的スケーリング

### 14.1 設定

```ts
interface ScalingConfig {
  min: number              // 最小プロセス数（常時起動）。デフォルト: 2
  max: number              // 最大プロセス数（通常時上限）。デフォルト: CPU論理コア数
  overflowMax: number      // デッドロック回避用の追加上限。デフォルト: max
  idleTimeout: number      // IDLE持続時間(ms)。超過でスケールダウン。デフォルト: 30000
}
```

### 14.2 スケールアップ

```
条件: find要求に対してキャパシティに空きのあるピアが見つからない
動作:
  1. managed プロセス数 < max の場合:
     → 新Denoプロセスをsubprocessとしてspawn
     → find要求をキューに入れ、新プロセスの register 後にドレイン

  2. managed プロセス数 >= max かつ要求元がBUSYプロセス（ネストspawn）の場合:
     → overflowプロセスを生成（デッドロック回避。§15参照）

  3. managed プロセス数 >= max かつ要求元がIDLE/外部の場合:
     → found を空配列で返す（caller側でキューイングまたはタイムアウト）
```

### 14.3 スケールダウン

```
Discovery内のタイマーで定期チェック（10秒ごと）:

条件: managed プロセスが activeTasks=0 で idleTimeout を超過 & managed数 > min
動作:
  1. shutdown メッセージ送信
  2. ノードが graceful に終了（active task のドレイン → bye送信 → TCP切断）
  3. peers から削除
  4. overflowプロセスは即座にスケールダウン対象（idleTimeout待ちなし）
```

## 15. デッドロック防止

### 15.1 問題

全ノードがmaxConcurrency到達で新しいタスクを受け付けられず、各々がサブタスクのspawn完了を待っている状態。

```
例: max=2, maxConcurrency=1
  Node A: activeTasks=1 (spawn("Y") を待っている)
  Node B: activeTasks=1 (spawn("Z") を待っている)
  → 両方フルでキャパシティなし → spawn不可 → 永久に待機
```

### 15.2 対策: オーバーフロースポーン + タイムアウト

```
1. BUSYプロセスからのfind要求にキャパシティのあるピアがない場合、
   Discovery は max を超えてプロセスを生成する（overflow）
   上限: max + overflowMax

2. overflowプロセスの特性:
   - タスク完了後、IDLEに戻らず即座に終了
   - idleTimeout を待たずに回収される
   - 通常のスケーリングカウントには含まれない

3. spawn全体にタイムアウトを設定:
   - デフォルト: 30秒
   - 超過: SpawnTimeoutError を throw
   - node.spawn("func", args, { timeout: 10000 })
```

## 16. ヘルスチェック

### 16.1 Discovery ↔ Process

```
間隔:   15秒ごとに heartbeat 送信
タイムアウト: 3回連続応答なし（45秒）でプロセスを死亡判定

BUSY プロセスの扱い:
  - heartbeat は BUSY 中も送受信する（タスク実行とは別にイベントループで処理）
  - BUSY だからといって死亡判定を免除しない
```

### 16.2 Discovery再接続

ノードはDiscoveryとの接続が切断された場合、指数バックオフで自動再接続する。

```
初回待機: 1秒
最大待機: 30秒
バックオフ: 1s → 2s → 4s → 8s → ... → 30s (上限)

再接続成功後:
  1. 再register (processId, host, port, funcs, maxConcurrency)
  2. capacity_change で現在のキャパシティを報告
  3. discoveryLoop を再開
```

### 16.3 P2P接続のヘルスチェック

TCPのkeep-aliveに依存。追加のアプリケーションレベルheartbeatは不要。
TCP切断を検知したらMultiplexerが全タスクにエラーを配信。

## 17. Registry

### 17.1 定義

```ts
// examples/tasks.ts
import { Registry, type Context } from "../mod.ts"

const registry = new Registry()

registry.register("heavyCalc", async (ctx: Context, x: number, y: number) => {
  await ctx.send({ progress: 0.3 })
  await ctx.send({ progress: 0.7 })

  const sub = ctx.spawn("multiply", [x, 2])
  const doubled = await sub.join()

  return doubled + y
})

registry.register("multiply", async (_ctx: Context, a: number, b: number) => {
  return a * b
})

export default registry
```

### 17.2 Registry 型定義

```ts
type TaskFunction = (ctx: Context, ...args: unknown[]) => Promise<unknown>
type TaskRecord = Record<string, TaskFunction>

class Registry {
  // Record形式から生成する簡易記法
  static from(record: TaskRecord): Registry

  register(name: string, fn: TaskFunction): void
  get(name: string): TaskFunction | undefined
  list(): string[]   // 登録済みタスク関数名一覧

  registerActor(name: string, cls: ActorClass): void
  getActor(name: string): ActorClass | undefined
  listActors(): string[]  // 登録済みActor名一覧
}
```

### 17.3 Record形式の簡易記法

```ts
// Registry.from() を使った簡易定義
export default Registry.from({
  multiply: async (_ctx, a: number, b: number) => a * b,
  add: async (_ctx, a: number, b: number) => a + b,
});

// connect() / node_main.ts のレジストリ読み込みは
// Registry インスタンスでも TaskRecord でも受け付ける
```

## 18. セキュリティ

### v1 (ローカル実行)

- Discovery は `127.0.0.1` でのみ listen
- 各プロセスも `127.0.0.1` でのみ listen
- 認証なし（同一マシン内で信頼）

### v2 (リモート対応時に追加)

```
1. TLS: 全TCP接続をTLS化
2. トークン認証: register / exec 時にトークンを送信
   { type: "register", ..., token: "shared-secret" }
3. 関数ACL: Discoveryで「誰がどの関数を呼べるか」を制御
4. ネットワーク分離: Discoveryのみ外部公開、ワーカーはVPN内
```

## 19. ファイル構成

```
js-concurrency-system/
├── deno.json                    # Denoプロジェクト設定 (zodインポート含む)
├── mod.ts                       # パブリックAPI
│                                #   connect(), ProcessNode, Registry, Discovery,
│                                #   Logger, MetricsCollector, ObjectRef, ObjectStore,
│                                #   createActorHandle, Channel, Context, etc.
├── DESIGN.md
│
├── src/
│   ├── types.ts                 # 全メッセージ型、設定型、エラー型、Actor型、Metrics型
│   ├── protocol.ts              # フレーミング + 拡張JSON (encode/decode/ObjectRef対応)
│   ├── schemas.ts               # Zodスキーマ定義 + バリデーション付きパーサー
│   │
│   ├── channel.ts               # Channel実装（caller側）
│   ├── context.ts               # Context実装（executor側）
│   ├── registry.ts              # 関数 & Actorレジストリ (from(record) 対応)
│   ├── multiplexer.ts           # TCP接続多重化 (taskIdベースのルーティング)
│   │
│   ├── actor.ts                 # ActorInstance (メールボックス) + ActorHandle
│   ├── object_store.ts          # ObjectRef + ObjectStore (参照カウント・キャッシュ)
│   ├── logger.ts                # 構造化ログ (LogLevel, child logger)
│   ├── metrics.ts               # MetricsCollector + Prometheus exposition
│   │
│   ├── discovery.ts             # Discovery Service 本体
│   ├── discovery_main.ts        # CLI: Discovery起動エントリポイント
│   │
│   ├── node.ts                  # ProcessNode (listen + Discovery接続 + タスク/Actor実行)
│   └── node_main.ts             # CLI: managed node起動エントリポイント
│
└── examples/
    ├── tasks.ts                 # サンプルレジストリ
    └── main.ts                  # サンプル: Discovery起動 → spawn → 結果取得
```

## 20. 起動フロー

### 20.1 Discovery Service 起動

```bash
deno run --allow-net --allow-run --allow-read src/discovery_main.ts \
  --port=9876 \
  --registry=./examples/tasks.ts \
  --min=2 \
  --max=8 \
  --idle-timeout=30000 \
  --metrics-port=9877
```

1. TCPサーバーを `127.0.0.1:9876` で起動
2. `--metrics-port` 指定時: Prometheus HTTPサーバーを起動
3. `min` 個のDenoサブプロセスをspawn (各々 `node_main.ts` を実行)
4. 各プロセスが起動 → registry を import → 自ポートで listen → Discovery に register
5. Discovery: registered 返却、キャパシティ追跡開始

### 20.2 managed node 起動 (node_main.ts)

Discoveryがsubprocessとして起動する。手動起動も可能。

```bash
deno run --allow-net --allow-read src/node_main.ts \
  --discovery-host=localhost \
  --discovery-port=9876 \
  --listen-port=0 \
  --max-concurrency=4 \
  --registry=./examples/tasks.ts
```

```
1. registryファイルを dynamic import → 関数名 + Actor名 一覧取得
2. TCP listener を起動（ポートはOS自動割り当て or 指定）
3. ObjectStore のアドレスを設定
4. Discovery に TCP接続 → register 送信 (processId, host, port, funcs, maxConcurrency)
5. registered 受信 → 待機開始
6. 並行ループ開始:
   a. Discovery接続: heartbeat応答、shutdown処理、capacity_change送信
   b. Listener: P2P接続受入 → Multiplexer作成 → exec/actor/object_fetchメッセージ処理
```

### 20.3 外部プロセスからの接続

```ts
import { ProcessNode, Registry } from "./mod.ts"

// Discoveryに接続し、ピアとして参加
const node = new ProcessNode({
  discoveryHost: "127.0.0.1",
  discoveryPort: 9876,
}, new Registry())

await node.start()

const ch = node.spawn("heavyCalc", [10, 20])
const result = await ch.join()   // 35

await node.close()
```

## 21. 将来の拡張

### リモートワーカー

TCP通信のため、リモートサーバー上のプロセスも同じプロトコルで参加可能。

```bash
# リモートサーバーで手動起動
deno run --allow-net src/node_main.ts \
  --discovery-host=192.168.1.100 \
  --discovery-port=9876 \
  --listen-host=0.0.0.0 \
  --listen-port=5000 \
  --registry=./tasks.ts
```

Discovery側の変更:
- `managed: false` として扱う（スケールアップ/ダウン対象外）
- ヘルスチェック間隔をネットワーク遅延に合わせて調整

### Discovery冗長化

- 複数のDiscoveryインスタンスをActive/Standbyで運用
- プロセスは複数のDiscoveryに同時登録
- Primary障害時にSecondaryに自動フェイルオーバー

### シリアライゼーション拡張

protocol.ts の encode/decode を差し替えるだけで対応可能。
- MessagePack: バイナリ効率
- CBOR: IETF標準
- Protocol Buffers: スキーマ定義型
