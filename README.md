# IntentEffect

A TypeScript full-stack framework prototype built on one idea:

```
intent → authoritative server effect → event → synchronized projection
```

Clients express **intentions** (`task.delete`). The server decides what actually
happens and emits authoritative **events** (`task.deleted`) in the *same database
transaction* as the mutation. Those events simultaneously resolve the initiating
client's `await send(...)` and synchronize every other client's **projections**
over a durable, cursor-based SSE stream.

No event sourcing required — apps keep their ordinary Postgres tables.
IntentEffect owns the mutation/event protocol, not your data model.

## The application experience

```ts
// shared/contracts.ts — runtime values imported by client AND server
export const taskDeleted = event('task.deleted', z.object({ id: z.string() }))
export const deleteTask = intent('task.delete', z.object({ id: z.string() }), {
  emits: [taskDeleted],
})
export const taskList = projection({ name: 'tasks.list', result: z.array(task) })
  .on(taskCreated, (tasks, data) => [...tasks, data])
  .on(taskDeleted, (tasks, data) => tasks.filter((t) => t.id !== data.id))
```

```ts
// server
app.handle(deleteTask, async ({ input, tx, emit }) => {
  await tx.query('delete from tasks where id = $1', [input.id])
  emit(taskDeleted, { id: input.id })
})
app.project(taskList, {
  query: async ({ tx }) => (await tx.query('select id, title from tasks')).rows,
})
```

```tsx
// client
const result = await send(deleteTask, { id })   // Result<SendOk, IntentEffectError>
const tasks = useProjection(taskList)           // stays synchronized across tabs
```

## Results, not exceptions

Every fallible operation in the framework returns `Result<T, E>`
(`{ ok: true, value } | { ok: false, error }`), defined in `@intenteffect/core`:

- `send()` resolves to `Result<SendOk, IntentEffectError>` — validation,
  authorization, handler and transport failures are values.
- Handlers may `return err(...)` for domain failures (or throw; both roll back).
- The wire format itself is a serialized `Result`, so client and server speak
  the same shape end to end.

## Packages

| package | contents |
|---|---|
| `@intenteffect/core` | `Result`, `intent()` / `event()` / `projection()` contracts (Zod-validated, type-inferred), wire protocol. Zero server deps. |
| `@intenteffect/server` | `createIntentEffect()`: `handle()`, `project()`, HTTP + SSE transport, event-visibility hooks, storage SPI (`EventStore`). |
| `@intenteffect/postgres` | `EventStore` implementation: transactional intent execution, durable event log, idempotency, LISTEN/NOTIFY wake-up (multi-consumer). |
| `@intenteffect/memory` | In-memory `EventStore` for tests: same idempotency and cursor-consistency contract, zero infrastructure. |
| `@intenteffect/client` | `send()`, shared SSE bus with reconnect + exactly-once event application, projection stores. |
| `@intenteffect/react` | `IntentEffectProvider`, `useProjection`, `useSend`, `useConnectionStatus`. |

## How the core mechanics work

**Transactional intents + idempotency.** `executeIntent` runs in one Postgres
transaction: claim the `intentId` (`insert … on conflict do nothing` on the
intents table), run the handler's mutations, append the emitted events, mark
the intent completed, `NOTIFY`. A retried `intentId` — including a request that
timed out *after* commit — replays the stored events instead of re-executing;
concurrent duplicates serialize on the primary-key row lock. Failed intents
roll back the mutation, record the error, and replay it as `Err` on retry
(send a fresh `intentId` to actually retry).

**Durable truth, disposable transport.**
```
Postgres event table  = durable truth (monotonic bigint id = the cursor)
NOTIFY                = "new events exist" wake-up only
SSE                   = browser transport
```
The SSE hub never trusts in-memory delivery: on wake-up it re-reads the event
table from its cursor, so multiple server processes need nothing but Postgres.
Connections replay missed events from the client's cursor
(`?cursor=` / `Last-Event-ID`), then switch to live delivery — surviving
drops, sleep, restarts and deploys.

**Exactly-once on the client.** The initiating client applies events from the
HTTP response immediately; the SSE copies (and reconnect replays) are dropped
by an applied-id set. Projection snapshots come with a cursor read in the same
`REPEATABLE READ` transaction as the query, so a client applies exactly the
events newer than its snapshot.

**Event visibility.** Events carry `actorId` / `tenantId`; delivery goes
through `authorizeEvent(event, ctx)` per connection (default: broadcast —
replace it in real apps). Projections and intents get the same per-request
`ctx`.

## Running the demo

Everything runs inside the `intenteffect` OrbStack machine (Node 22, pnpm,
Postgres 18 with `postgres://dev:dev@localhost:5432/intenteffect`):

```sh
orb -m intenteffect pnpm install
orb -m intenteffect pnpm demo:server   # IntentEffect server on :3001
orb -m intenteffect pnpm demo:web      # Vite on :5173 (proxies /_intenteffect)
```

Open http://intenteffect.orb.local:5173 in **two tabs**: adding, renaming or
deleting a task in one resolves its `await send(...)` and updates the other
tab through its projection stream. Kill and restart the server — tabs
reconnect from their cursor without losing events.

Integration tests (two simulated clients against real Postgres):

```sh
orb -m intenteffect pnpm test
```

## Deliberately out of scope (for now)

Kafka/Redis/NATS brokers (the `EventStore` SPI is where they'd plug in),
event-sourced aggregates, sagas, automatic projection inference, offline-first
optimistic mutations, cross-language SDK generation. The contracts already
carry Zod schemas, so JSON Schema / docs / SDK generation is a straight path.

Prototype notes: packages export TypeScript source directly (no build step);
publishing would add one. Events arriving over different transports can apply
slightly out of global order across *different* intents — per-intent order is
always preserved.
