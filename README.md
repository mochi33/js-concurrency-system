# js-concurrency-system

Deno 向けの分散タスク実行システム。プロセスプーリング、P2P 通信、双方向ストリーミング、動的スケーリングを備える。

## 特徴

- **プロセスプール管理** — min/max 指定でワーカープロセスを自動スケーリング
- **P2P 通信** — Discovery はメタデータのみ管理、タスクデータはノード間で直接やり取り
- **双方向ストリーミング** — `send()` / `receive()` による進捗報告やデータ交換
- **ネストされたタスク生成** — タスク内から別ノードにサブタスクを spawn 可能
- **コネクションプール** — 同一ピアへの複数タスクが 1 TCP 接続を共有
- **マルチタスク並行実行** — ノードあたり最大 `maxConcurrency` タスクを同時処理
- **協調的キャンセル** — `AbortSignal` ベースのキャンセル機構
- **実行タイムアウト** — タスク単位のタイムアウト設定
- **拡張 JSON** — BigInt, Date, Map, Set, Uint8Array 等をそのままシリアライズ

## アーキテクチャ

```
┌──────────────────────────────────────────────┐
│                  Discovery                    │
│  - ノード登録・キャパシティ追跡               │
│  - タスクルーティング (find → found)          │
│  - プロセスの起動・停止・ヘルスチェック       │
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

  ═══ P2P 直接通信 (exec, result, send)
  ─── Discovery 経由 (register, find, heartbeat)
```

Discovery はメタデータのみを管理する軽量サービス。タスクの実行データはノード間で直接 P2P 通信される。

## クイックスタート

### 前提条件

- [Deno](https://deno.land/) v1.40+

### 1. タスクを定義する

```typescript
// tasks.ts
import { Registry } from "./src/registry.ts";
import type { Context } from "./src/types.ts";

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
  --min=2               # 最小プロセス数 (デフォルト: 2)
  --max=4               # 最大プロセス数 (デフォルト: CPU コア数)
  --overflow-max=2      # オーバーフロープロセス数 (デフォルト: max と同じ)
  --idle-timeout=30000  # アイドルプロセスの停止待ち (ms, デフォルト: 30000)
  --registry=./tasks.ts # タスク定義ファイル
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

# API サーバーサンプル
deno run --allow-net --allow-run --allow-read examples/api_server/server.ts
# 別ターミナルでテスト
deno run --allow-net examples/api_server/test.ts
```

## 設計ドキュメント

アーキテクチャの詳細については [DESIGN.md](./DESIGN.md) を参照。
