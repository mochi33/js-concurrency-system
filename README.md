# js-concurrency-system

Deno 向けの分散タスク実行システム。プロセスプーリング、P2P 通信、双方向ストリーミング、動的スケーリングを備える。

## 特徴

- **プロセスプール管理** — min/max 指定でワーカープロセスを自動スケーリング
- **P2P 通信** — Discovery はメタデータのみ管理、タスクデータはノード間で直接やり取り
- **双方向ストリーミング** — `send()` / `receive()` による進捗報告やデータ交換
- **ネストされたタスク生成** — タスク内から別ノードにサブタスクを spawn 可能
- **コネクション多重化** — Multiplexer により同一ピアへの複数タスクが 1 TCP 接続を共有
- **マルチタスク並行実行** — ノードあたり最大 `maxConcurrency` タスクを同時処理
- **Actor モデル** — ステートフルな Actor をリモートノードに配置しメソッド呼び出し
- **共有オブジェクトストア** — 大規模データを `ObjectRef` で参照渡し、必要時にリモートフェッチ
- **構造化ログ** — コンテキストフィールド付き Logger、child logger 対応
- **メトリクス収集** — カウンター/ゲージ/パーセンタイル収集、Prometheus 互換エンドポイント
- **ランタイムバリデーション** — Zod スキーマによる全メッセージの検証
- **協調的キャンセル** — `AbortSignal` ベースのキャンセル機構
- **実行タイムアウト** — タスク単位のタイムアウト設定
- **拡張 JSON** — BigInt, Date, Map, Set, Uint8Array, ObjectRef 等をそのままシリアライズ
- **Discovery 自動再接続** — 指数バックオフによる自動復旧

## アーキテクチャ

```
┌──────────────────────────────────────────────┐
│                  Discovery                    │
│  - ノード登録・キャパシティ追跡               │
│  - タスクルーティング (find → found)          │
│  - プロセスの起動・停止・ヘルスチェック       │
│  - Prometheus メトリクスエンドポイント        │
└──────┬──────────────┬──────────────┬─────────┘
       │              │              │
   register       register       register
       │              │              │
┌──────▼──┐    ┌──────▼──┐    ┌──────▼──┐
│  Node A  │◄══╪════════╪══►│  Node B  │
│ (Caller) │   │         │   │(Executor)│
└──────────┘   │         │   └──────────┘
               │         │
         ┌─────▼───┐  ┌──▼───────┐
         │  Node C  │  │  Node D  │
         │(Executor)│  │(Executor)│
         └──────────┘  └──────────┘

  ═══ P2P 直接通信 (Multiplexer: 1 TCP で複数タスク)
  ─── Discovery 経由 (register, find, heartbeat)
```

Discovery はメタデータのみを管理する軽量サービス。タスクの実行データはノード間で直接 P2P 通信される。

## クイックスタート

### 前提条件

- [Deno](https://deno.land/) v1.40+

### 1. タスクを定義する

```typescript
// tasks.ts
import { Registry } from "./mod.ts";
import type { Context } from "./mod.ts";

const registry = new Registry();

registry.register("multiply", async (_ctx: Context, a: number, b: number) => {
  return a * b;
});

registry.register("fibonacci", async (ctx: Context, n: number) => {
  if (n <= 1) return n;
  const a = ctx.spawn("fibonacci", [n - 1]);
  const b = ctx.spawn("fibonacci", [n - 2]);
  return (await a.join() as number) + (await b.join() as number);
});

export default registry;
```

Record 形式の簡易記法も使える:

```typescript
// tasks.ts
import { Registry } from "./mod.ts";

export default Registry.from({
  multiply: async (_ctx, a: number, b: number) => a * b,
  add: async (_ctx, a: number, b: number) => a + b,
});
```

### 2. Discovery を起動する

```bash
deno run --allow-net --allow-run --allow-read src/discovery_main.ts \
  --port=9876 \
  --min=2 \
  --max=4 \
  --registry=./tasks.ts
```

### 3. タスクを実行する

```typescript
import { ProcessNode, Registry } from "./mod.ts";

const caller = new ProcessNode({
  discoveryHost: "127.0.0.1",
  discoveryPort: 9876,
}, new Registry());

await caller.start();

const ch = caller.spawn("multiply", [7, 6]);
const result = await ch.join();
console.log(result); // 42

await caller.close();
```

## API

### Channel（呼び出し側）

`spawn()` が返すオブジェクト。タスクの結果受信・データ送信・キャンセルを行う。

```typescript
const ch = caller.spawn("taskName", [arg1, arg2], options);

// 結果を直接取得
const result = await ch.join();

// 中間メッセージを逐次受信
const { value, done } = await ch.receive();

// AsyncIterator で中間メッセージをストリーミング
for await (const msg of ch) {
  console.log("progress:", msg);
}
console.log("final:", ch.returnValue);

// 呼び出し側 → 実行側へデータ送信
await ch.send({ data: "hello" });

// キャンセル
ch.cancel();
```

### Context（実行側）

タスク関数の第 1 引数。呼び出し側との通信やサブタスク生成に使う。

```typescript
registry.register("myTask", async (ctx: Context, ...args: unknown[]) => {
  // 進捗を送信
  await ctx.send({ progress: 0.5 });

  // 呼び出し側からのデータを受信
  const data = await ctx.receive();

  // サブタスクを生成
  const sub = ctx.spawn("otherTask", [data]);
  const subResult = await sub.join();

  // AbortSignal でキャンセルを検知
  if (ctx.signal.aborted) throw new Error("cancelled");

  return subResult;
});
```

### Actor（ステートフルオブジェクト）

リモートノード上にステートフルな Actor を生成し、メソッド呼び出しで操作する。メールボックスパターンにより並行呼び出しでも状態の一貫性を保証。

```typescript
// Actor クラスを定義
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
registry.registerActor("Counter", Counter);

// リモートで Actor を生成・呼び出し
const handle = await node.createActor("Counter");
await handle.call("increment", 5);   // → 5
await handle.call("increment", 3);   // → 8
await handle.call("getCount");        // → 8
await handle.destroy();               // Actor を破棄
```

### ObjectStore（共有オブジェクトストア）

大規模データをコピーせず参照で渡す。executor 側で必要時にオーナーノードからフェッチされる。

```typescript
// caller 側: 大きなデータを格納
const ref = node.put(largeDataset);

// spawn 時に ObjectRef を引数として渡す（データ本体は転送されない）
const ch = node.spawn("processData", [ref]);
const result = await ch.join();

// ObjectStore に直接アクセス
const store = node.getObjectStore();
store.delete(ref);  // 不要になったら削除
```

### Logger（構造化ログ）

コンテキストフィールド付きの構造化ログ。child logger でフィールドを追加できる。

```typescript
import { Logger } from "./mod.ts";

const log = new Logger({ level: "info", fields: { component: "app" } });
log.info("Server started");
// → [2024-02-11T10:00:00.000Z] [INFO] [component=app] Server started

const taskLog = log.child({ task: "abc123" });
taskLog.info("Processing");
// → [2024-02-11T10:00:01.000Z] [INFO] [component=app] [task=abc123] Processing
```

### MetricsCollector（メトリクス）

Prometheus 互換形式でメトリクスを出力。

```typescript
import { MetricsCollector } from "./mod.ts";

const metrics = new MetricsCollector();
metrics.recordTaskSpawned();
metrics.recordTaskCompleted();
metrics.recordTaskLatency(150);

// Prometheus 形式で取得
console.log(metrics.toPrometheus());

// スナップショットとして取得
const snapshot = metrics.getMetrics();
console.log(snapshot.latencyP95);
```

Discovery で `metricsPort` を指定すると Prometheus エンドポイントが自動起動:

```typescript
const discovery = new Discovery({
  port: 9876,
  registry: "./tasks.ts",
  min: 2,
  max: 8,
  overflowMax: 8,
  idleTimeout: 30000,
  metricsPort: 9877,  // http://localhost:9877/ でメトリクス取得可能
});
```

### SpawnOptions

```typescript
caller.spawn("task", [args], {
  timeout: 30000,       // spawn タイムアウト (ms, デフォルト: 30000)
  execTimeout: 10000,   // 実行タイムアウト (ms)
  highWaterMark: 64,    // メッセージバッファ上限 (デフォルト: 64)
});
```

## 設定

### Discovery

```bash
deno run --allow-net --allow-run --allow-read src/discovery_main.ts \
  --port=9876           # リッスンポート (デフォルト: 9876)
  --host=127.0.0.1      # リッスンアドレス (デフォルト: 127.0.0.1)
  --min=2               # 最小プロセス数 (デフォルト: 2)
  --max=4               # 最大プロセス数 (デフォルト: CPU コア数)
  --overflow-max=2      # オーバーフロープロセス数 (デフォルト: max と同じ)
  --idle-timeout=30000  # アイドルプロセスの停止待ち (ms, デフォルト: 30000)
  --registry=./tasks.ts # タスク定義ファイル
  --metrics-port=9877   # Prometheus メトリクスポート (省略時: 無効)
```

### Worker Node

```bash
deno run --allow-net --allow-read src/node_main.ts \
  --discovery-host=127.0.0.1  # Discovery ホスト
  --discovery-port=9876       # Discovery ポート
  --listen-host=127.0.0.1     # P2P リッスンアドレス
  --listen-port=0             # P2P リッスンポート (0 = 自動)
  --max-concurrency=4         # 最大同時タスク数 (デフォルト: 4)
  --registry=./tasks.ts       # タスク定義ファイル
```

## エクスポート一覧 (mod.ts)

```typescript
// クラス
export { ProcessNode }          // ノード本体
export { Registry }             // タスク関数・Actor レジストリ
export { Discovery }            // Discovery サービス
export { MetricsCollector }     // メトリクス収集
export { Logger }               // 構造化ログ
export { ObjectRef, ObjectStore } // 共有オブジェクトストア
export { connect }              // ProcessNode 作成ヘルパー

// ファクトリ関数
export { createChannel }        // Channel 作成
export { createContext }        // Context 作成
export { createActorHandle, requestActorCreation } // Actor ハンドル

// 型
export type {
  ActorHandle, ActorClass,
  Channel, Context,
  NodeConfig, DiscoveryConfig,
  MetricsSnapshot, SpawnOptions, ReceiveResult,
  TaskFunction, TaskRecord,
  LogLevel, LoggerOptions,
}

// エラー型
export {
  ChannelClosedError,
  CancelledError,
  SpawnTimeoutError,
  ExecTimeoutError,
  SpawnError,
}
```

## エラー型

| エラー | 説明 |
|---|---|
| `SpawnTimeoutError` | spawn がタイムアウト |
| `ExecTimeoutError` | タスク実行がタイムアウト |
| `CancelledError` | タスクがキャンセルされた |
| `ChannelClosedError` | 閉じたチャネルへのアクセス |
| `SpawnError` | spawn の一般的な失敗 |

## 使用例

### HTTP サーバー連携

```typescript
import { Discovery, ProcessNode, Registry } from "./mod.ts";

const discovery = new Discovery({
  port: 9876,
  registry: "./tasks.ts",
  min: 2,
  max: 8,
  overflowMax: 8,
  idleTimeout: 30000,
});
await discovery.start();

const caller = new ProcessNode({
  discoveryHost: "127.0.0.1",
  discoveryPort: 9876,
}, new Registry());
await caller.start();

Deno.serve({ port: 8080 }, async (req: Request) => {
  const { n } = await req.json();
  const ch = caller.spawn("fibonacci", [n]);
  const result = await ch.join();
  return Response.json({ result });
});
```

### SSE ストリーミング

```typescript
Deno.serve({ port: 8080 }, async (req: Request) => {
  const { n } = await req.json();
  const ch = caller.spawn("fibonacci", [n]);
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      for await (const msg of ch) {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(msg)}\n\n`));
      }
      controller.enqueue(
        encoder.encode(`data: ${JSON.stringify({ type: "result", value: ch.returnValue })}\n\n`)
      );
      controller.close();
    },
  });

  return new Response(stream, {
    headers: { "content-type": "text/event-stream" },
  });
});
```

### タイムアウト付き実行

```typescript
try {
  const ch = caller.spawn("slowTask", [], { execTimeout: 5000 });
  const result = await ch.join();
} catch (e) {
  if (e.name === "ExecTimeoutError") {
    console.log("タスクがタイムアウトしました");
  }
}
```

## サンプルの実行

```bash
# 基本サンプル
deno run --allow-net --allow-run --allow-read examples/main.ts

# deno task を使用
deno task discovery   # Discovery 起動
deno task example     # サンプル実行
```

## 設計ドキュメント

アーキテクチャの詳細については [DESIGN.md](./DESIGN.md) を参照。
