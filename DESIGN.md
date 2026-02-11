# js-concurrency-system 設計書

## 1. 概要

Denoプロセスをプールし、タスクをP2Pで分散実行する並行処理システム。

- **Discovery Service**: 軽量なピア追跡サービス。メタデータのみ管理し、タスクデータは中継しない
- **全プロセスは対等**: spawn()もexec受付も全プロセスが可能。直接通信する
- **1プロセス = 1タスク**: 実行中はBUSY。ただしBUSY中もspawn()発行は可能
- **エラー後もプロセス再利用**: ヘルスチェック付き
- **TCP通信**: ローカルもリモートも同じプロトコル

## 2. アーキテクチャ

```
┌──────────────────────────────┐
│      Discovery Service       │  軽量・置換可能
│                              │
│  ・ピア登録/一覧             │  ← 死んでも実行中タスクに影響なし
│  ・状態ヒント (idle/busy)     │
│  ・managedプロセスのspawn/kill │
│  ・タスクデータは中継しない    │
└──────┬───────────────────────┘
       │ TCP (登録・問い合わせのみ)
       │
  ┌────┼──────────┬────────────┐
  │    │          │            │
  ▼    ▼          ▼            ▼
┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐
│  A  │  │  B  │  │  C  │  │  D  │
│:4001│  │:4002│  │:4003│  │:4004│  各プロセスが自分のTCPポートでlisten
└──┬──┘  └──┬──┘  └──┬──┘  └─────┘
   │        │       │
   └── P2P ─┘       │    タスク実行時、callerがexecutorに直接接続
            └─ P2P ─┘    Discoveryを経由しない
```

### Discoveryが落ちた場合の影響

| 状況 | 影響 |
|------|------|
| 実行中タスク (A↔B直接接続) | **影響なし** — 直接接続は維持される |
| 新規spawn | **不可** — ピア一覧を取得できない |
| スケールアップ/ダウン | **停止** — Discoveryのみがmanagedプロセスを管理 |
| Discovery復旧後 | 全プロセスが再登録 → 正常復帰 |

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
```

- `$type` キーを持つプレーンオブジェクトとの衝突を避けるため、ユーザーデータの `$type` キーは `$$type` にエスケープされる
- protocol.ts の `encode()` / `decode()` に集約。将来MessagePack等に差し替え可能

### 3.3 Process ↔ Discovery メッセージ

```ts
// --- 登録 ---

// Process → Discovery: 起動時に自身を登録
{
  type: "register"
  processId: string         // UUID
  host: string              // 自プロセスのlistenアドレス
  port: number              // 自プロセスのlistenポート
  funcs: string[]           // 実行可能な関数名一覧
}

// Discovery → Process: 登録受理
{
  type: "registered"
  processId: string
}

// --- 状態通知 ---

// Process → Discovery: 状態変更を報告
{
  type: "state_change"
  state: "idle" | "busy"
}

// --- ピア検索 ---

// Process → Discovery: 指定関数を実行可能なIDLEピアを要求
{
  type: "find"
  func: string
  exclude?: string[]        // 除外するprocessId（リトライ時）
}

// Discovery → Process: 候補ピアを返却
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

### 3.4 Process ↔ Process (P2P) メッセージ

callerがexecutorに直接TCP接続して送受信する。

```ts
// --- タスク開始 ---

// Caller → Executor: タスク実行要求
{
  type: "exec"
  taskId: string            // caller側で生成 (UUID)
  func: string
  args: unknown[]           // Extended JSONでシリアライズ
}

// Executor → Caller: 受諾
{ type: "accept", taskId: string }

// Executor → Caller: 拒否（BUSYまたはfunc不明）
{
  type: "reject"
  taskId: string
  reason: "busy" | "unknown_func"
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

## 4. spawnフロー

### 4.1 正常系

```
Process A (caller)         Discovery         Process B (executor)
    │                          │                     │
    │── find(func="calc") ───→│                     │
    │                          │                     │
    │←── found([B:4002]) ─────│                     │
    │                          │                     │
    │══ TCP接続 ═══════════════════════════════════→│ ← 直接接続
    │                          │                     │
    │── exec(T1,"calc",args) ─────────────────────→│
    │                          │                     │
    │                          │←── state:busy ─────│ ← Discoveryに通知
    │                          │                     │
    │←── accept(T1) ──────────────────────────────│
    │                          │                     │
    │←── send(T1, progress) ──────────────────────│ ← 直接P2P通信
    │                          │                     │
    │── send(T1, data) ──────────────────────────→│ ← 直接P2P通信
    │                          │                     │
    │←── result(T1, value) ───────────────────────│ ← 関数return
    │                          │                     │
    │══ TCP切断 ═══════════════════════════════════│
    │                          │                     │
    │                          │←── state:idle ─────│ ← Discoveryに通知
    │                          │                     │
```

### 4.2 リジェクト & リトライ

Discoveryの状態情報はベストエフォート（staleの可能性あり）。
**最終的な受け入れ判定はexecutor自身が行う。**

```
Process A               Discovery               Process B         Process C
    │                       │                        │                 │
    │── find("calc") ──────→│                        │                 │
    │←── found([B,C]) ─────│                        │                 │
    │                       │                        │                 │
    │── exec(T1) ──────────────────────────────────→│                 │
    │←── reject(busy) ─────────────────────────────│  ← 既にBUSYだった
    │── TCP切断 ────────────────────────────────────│                 │
    │                       │                        │                 │
    │── exec(T1) ───────────────────────────────────────────────────→│
    │←── accept(T1) ────────────────────────────────────────────────│  ← 成功
    │                       │                        │                 │
```

リトライ上限: 3回。超過時はDiscoveryに `request_spawn` を送り新プロセスを要求。

### 4.3 BUSY中のネストspawn

BUSYプロセスも他プロセスへのspawn()は可能。

```
Process A              Discovery           Process B           Process C
    │                      │                    │                   │
    │── exec(T1) ────────────────────────────→│                   │
    │                      │                    │                   │
    │                      │←── find("mul") ───│  ctx.spawn()      │
    │                      │── found([C]) ────→│                   │
    │                      │                    │                   │
    │                      │                    │── exec(T2) ─────→│ ← B→C直接接続
    │                      │                    │←── accept(T2) ──│
    │                      │                    │←── result(T2) ──│
    │                      │                    │── TCP切断 ───────│
    │                      │                    │                   │
    │←── result(T1) ────────────────────────│                   │
    │                      │                    │                   │
```

## 5. プロセスライフサイクル

### 5.1 状態遷移

```
起動 → INITIALIZING → register → IDLE ──→ exec受信 ──→ BUSY ──→ result/error送信 ──→ IDLE
                                   │                      │                            │
                                   │  spawn()発行可能     │  spawn()発行可能           │
                                   │                      │                            │
                                   └──── shutdown ────→ 終了                          │
                                                                                      │
                                              ヘルスチェック失敗 → 終了 (再起動) ←──────┘
```

### 5.2 exec受信時の判定 (executor側)

JavaScriptのシングルスレッド性を利用し、状態チェックと変更を同期的に行う。
これによりロックなしで競合を回避できる。

```ts
// 同期コードブロック内（awaitなし）で判定・変更するため安全
function handleExecRequest(msg: ExecMessage, conn: TcpConn): void {
  if (state !== "idle") {
    sendSync(conn, { type: "reject", taskId: msg.taskId, reason: "busy" })
    conn.close()
    return
  }
  // ↓ ここまで同期的に到達 — 他のexecが割り込む余地なし
  state = "busy"
  sendSync(conn, { type: "accept", taskId: msg.taskId })
  executeTask(conn, msg)  // async — ここからは非同期
}
```

## 6. Channel API

### 6.1 receive() のセマンティクス

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

### 6.2 caller側 API

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

### 6.3 executor側 API (Context)

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
  return doubled + y + extra.value
})
```

### 6.4 AsyncIterator の動作

```ts
// 内部実装イメージ
class Channel {
  async *[Symbol.asyncIterator]() {
    while (true) {
      const { value, done } = await this.receive()
      if (done) {
        this.returnValue = value
        return  // ループ終了。yieldしない
      }
      yield value
    }
  }
}
```

| 呼び出し | 返却 |
|----------|------|
| `for await (const v of ch)` | 中間メッセージのみ yield。`done: true` でループ終了 |
| ループ後 `ch.returnValue` | 関数の return 値 |
| executor が throw した場合 | `for await` がその例外を throw |

### 6.5 バックプレッシャー

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

## 7. キャンセル

### 7.1 フロー

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

### 7.2 設計判断

- **協調的キャンセル**: executorを強制終了しない。`ctx.signal` を通じて通知するのみ
- executorがキャンセルを無視した場合、タスクは最後まで実行される
- callerは `cancel()` 後も `receive()` / `join()` で結果やエラーを待つことができる
- `ctx.receive()` はキャンセル後に `CancelledError` を throw する

## 8. エラーハンドリングとプロセス再利用

### 8.1 エラー伝搬

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

### 8.2 エラー後の再利用判定

タスク実行後（正常/エラー問わず）、プロセスをIDLEに戻す前にヘルスチェックを行う。

```
タスク完了/エラー
    │
    ├── ヘルスチェック実行
    │     ├── OK → IDLE に戻す。Discoveryに state:idle 送信
    │     └── NG → プロセスを終了。Discoveryが検知し、pool < min なら再spawn
    │
    └── ヘルスチェック内容:
          - メモリ使用量が閾値以下か (Deno.memoryUsage())
          - 未処理のPromise rejectionがないか
          - イベントループが応答するか（setTimeoutが期待通り発火するか）
```

### 8.3 プロセスクラッシュ時

TCP切断として検知される。

```
1. executorがクラッシュ → TCP切断
2. caller側: receive() / join() が ConnectionResetError を throw
3. Discovery側: heartbeat応答なし → processesから削除 → pool < min なら再spawn
4. callerのタスクは失敗（自動リトライはしない — 冪等性を保証できないため）
```

## 9. 動的スケーリング

### 9.1 設定

```ts
interface ScalingConfig {
  min: number              // 最小プロセス数（常時起動）。デフォルト: 2
  max: number              // 最大プロセス数（通常時上限）。デフォルト: CPU論理コア数
  overflowMax: number      // デッドロック回避用の追加上限。デフォルト: max
  idleTimeout: number      // IDLE持続時間(ms)。超過でスケールダウン。デフォルト: 30000
}
```

### 9.2 スケールアップ

```
条件: find要求に対してIDLEピアが見つからない
動作:
  1. managed プロセス数 < max の場合:
     → 新Denoプロセスをsubprocessとしてspawn
     → 引数: --discovery-host --discovery-port --listen-port --registry
     → プロセスが register → IDLEとして登録

  2. managed プロセス数 >= max かつ要求元がBUSYプロセス（ネストspawn）の場合:
     → overflowプロセスを生成（デッドロック回避。§10参照）

  3. managed プロセス数 >= max かつ要求元がIDLE/外部の場合:
     → found を空配列で返す（caller側でキューイングまたはタイムアウト）
```

### 9.3 スケールダウン

```
Discovery内のタイマーで定期チェック（10秒ごと）:

条件: managed プロセスが IDLE 状態で idleTimeout を超過 & managed数 > min
動作:
  1. shutdown メッセージ送信
  2. プロセスがgracefulに終了（bye送信 → TCP切断）
  3. processesから削除
  4. overflowプロセスは即座にスケールダウン対象（idleTimeout待ちなし）
```

## 10. デッドロック防止

### 10.1 問題

全プロセスがBUSYかつ、各々が別タスクのspawn完了を待っている状態。

```
例: max=2
  Process A: BUSY (spawn("Y") を待っている)
  Process B: BUSY (spawn("Z") を待っている)
  → 両方BUSYでIDLEプロセスなし → spawn不可 → 永久に待機
```

### 10.2 対策: オーバーフロースポーン + タイムアウト

```
1. BUSYプロセスからのfind要求にIDLEピアがない場合、
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

### 10.3 フロー

```
Process A (BUSY)            Discovery
    │                           │
    │── find("Z") ────────────→│
    │                           │  IDLEピアなし
    │                           │  要求元はBUSY → overflow生成
    │                           │── subprocess spawn ──→ Process E (overflow)
    │                           │←── register ─────────│
    │←── found([E]) ───────────│                       │
    │                           │                       │
    │── exec(T3) to E ─────────────────────────────→│
    │←── result(T3) ────────────────────────────────│
    │                           │                       │
    │                           │  E: タスク完了 → 即終了
    │                           │                       │
```

## 11. ヘルスチェック

### 11.1 Discovery ↔ Process

```
間隔:   15秒ごとに heartbeat 送信
タイムアウト: 3回連続応答なし（45秒）でプロセスを死亡判定

BUSY プロセスの扱い:
  - heartbeat は BUSY 中も送受信する（タスク実行とは別にイベントループで処理）
  - BUSY だからといって死亡判定を免除しない
  - ただしタイムアウトを延長する設定は可能
```

### 11.2 P2P接続のヘルスチェック

TCPのkeep-aliveに依存。追加のアプリケーションレベルheartbeatは不要。
TCP切断を検知したら即座にエラーとして処理。

## 12. セキュリティ

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

## 13. ファイル構成

```
js-concurrency-system/
├── deno.json
├── mod.ts                      # パブリックAPI
│                               #   connect(), Registry, Channel, Context
├── DESIGN.md
│
├── src/
│   ├── types.ts                # 全メッセージ型、設定型、エラー型
│   ├── protocol.ts             # フレーミング + 拡張JSON (encode/decode)
│   │
│   ├── channel.ts              # Channel実装（caller側）
│   ├── context.ts              # Context実装（executor側）
│   ├── registry.ts             # 関数レジストリ
│   │
│   ├── discovery.ts            # Discovery Service 本体
│   ├── discovery_main.ts       # CLI: Discovery起動エントリポイント
│   │
│   ├── node.ts                 # プロセスノード（listen + Discovery接続 + タスク実行）
│   └── node_main.ts            # CLI: managed node起動エントリポイント
│
└── examples/
    ├── tasks.ts                # サンプルレジストリ
    └── main.ts                 # サンプル: Discovery起動 → spawn → 結果取得
```

## 14. 起動フロー

### 14.1 Discovery Service 起動

```bash
deno run --allow-net --allow-run src/discovery_main.ts \
  --port=9876 \
  --registry=./examples/tasks.ts \
  --min=2 \
  --max=8 \
  --idle-timeout=30000
```

1. TCPサーバーを `0.0.0.0:9876` で起動
2. `min` 個のDenoサブプロセスをspawn (各々 `node_main.ts` を実行)
3. 各プロセスが起動 → registry を import → 自ポートで listen → Discovery に register
4. Discovery: registered 返却、IDLEとして登録

### 14.2 managed node 起動 (node_main.ts)

Discoveryがsubprocessとして起動する。手動起動も可能。

```bash
deno run --allow-net src/node_main.ts \
  --discovery-host=localhost \
  --discovery-port=9876 \
  --listen-port=0 \          # 0 = OS自動割り当て
  --registry=./examples/tasks.ts
```

```
1. registryファイルを dynamic import → 関数名一覧取得
2. TCP listener を起動（ポートはOS自動割り当て or 指定）
3. Discovery に TCP接続 → register 送信 (processId, host, port, funcs)
4. registered 受信 → IDLEとして待機
5. 並行ループ開始:
   a. Discovery接続: heartbeat応答、shutdown処理
   b. Listener: exec受信 → accept/reject → タスク実行
```

### 14.3 外部プロセスからの接続

```ts
import { connect } from "./mod.ts"

// Discoveryに接続し、ピアとして参加
const node = await connect({
  discoveryHost: "localhost",
  discoveryPort: 9876,
  registry: "./tasks.ts",   // 省略可: execを受けない場合
  listenPort: 0,             // 省略可: execを受けない場合
})

const ch = node.spawn("heavyCalc", [10, 20])
const result = await ch.join()   // 35

await node.close()
```

## 15. Registry

### 15.1 定義

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

### 15.2 Registry 型定義

```ts
type TaskFunction = (ctx: Context, ...args: unknown[]) => Promise<unknown>

class Registry {
  register(name: string, fn: TaskFunction): void
  get(name: string): TaskFunction | undefined
  list(): string[]   // 登録済み関数名一覧（hello時に使用）
}
```

## 16. 将来の拡張

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
