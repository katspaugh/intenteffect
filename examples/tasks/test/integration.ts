/**
 * End-to-end vertical-slice test, run against a real Postgres:
 * two simulated browser clients (A initiates intents, B follows via
 * projection + SSE), idempotency, dedup, cursor catch-up and reconnect.
 */
import assert from 'node:assert/strict'
import { createClient, type IntentEffectClient } from '@intenteffect/client'
import { createTasksApp } from '../server/app.js'
import { createTask, deleteTask, renameTask, taskList } from '../shared/contracts.js'

const connectionString =
  process.env.DATABASE_URL ?? 'postgres://dev:dev@localhost:5432/intenteffect'

const EventSourceImpl: new (url: string) => EventSource =
  (globalThis as { EventSource?: new (url: string) => EventSource }).EventSource ??
  ((await import('eventsource')).EventSource as unknown as new (url: string) => EventSource)

function makeClient(baseUrl: string): IntentEffectClient {
  return createClient({
    baseUrl,
    eventSource: (url) => new EventSourceImpl(url),
  })
}

async function waitFor(
  label: string,
  predicate: () => boolean,
  timeoutMs = 5000,
): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`timed out waiting for: ${label}`)
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

/** Read raw SSE frames from the events endpoint until `count` events arrive. */
async function readSse(
  url: string,
  count: number,
  timeoutMs = 5000,
): Promise<Array<{ id: number; type: string }>> {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  const events: Array<{ id: number; type: string }> = []
  try {
    const response = await fetch(url, {
      headers: { accept: 'text/event-stream' },
      signal: controller.signal,
    })
    assert.equal(response.headers.get('content-type'), 'text/event-stream')
    const reader = response.body!.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    while (events.length < count) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      let sep: number
      while ((sep = buffer.indexOf('\n\n')) !== -1) {
        const frame = buffer.slice(0, sep)
        buffer = buffer.slice(sep + 2)
        const dataLine = frame
          .split('\n')
          .find((line) => line.startsWith('data: '))
        if (dataLine) events.push(JSON.parse(dataLine.slice(6)))
      }
    }
  } finally {
    clearTimeout(timeout)
    controller.abort()
  }
  return events
}

let passed = 0
function check(label: string): void {
  passed++
  console.log(`  ✓ ${label}`)
}

/* ------------------------------------------------------------------ */

console.log('setting up…')
const { app, store } = await createTasksApp(connectionString)
await store.pool.query(
  `truncate intent_effect_events restart identity;
   truncate intent_effect_intents;
   truncate tasks`,
)
const server = await app.listen(0, '127.0.0.1')
const address = server.address()
if (address === null || typeof address === 'string') throw new Error('no port')
const baseUrl = `http://127.0.0.1:${address.port}`
console.log(`server on ${baseUrl}\n`)

const clientA = makeClient(baseUrl)
const clientB = makeClient(baseUrl)

try {
  /* -------- projection initial snapshot (client B) -------- */
  const projectionB = clientB.projection(taskList, {})
  const unsubB = projectionB.subscribe(() => {})
  await waitFor('client B projection ready', () => projectionB.getSnapshot().status === 'ready')
  assert.deepEqual(projectionB.getSnapshot().data, [])
  check('projection fetches an empty initial snapshot')

  /* -------- send() resolves with authoritative events -------- */
  const taskId = crypto.randomUUID()
  const created = await clientA.send(createTask, { id: taskId, title: 'write docs' })
  assert.ok(created.ok, `send failed: ${JSON.stringify(created)}`)
  assert.equal(created.value.deduped, false)
  assert.equal(created.value.events.length, 1)
  assert.equal(created.value.events[0]!.type, 'task.created')
  assert.equal(created.value.events[0]!.intentId, created.value.intentId)
  assert.equal(created.value.events[0]!.correlationId, created.value.intentId)
  assert.ok(created.value.events[0]!.id >= 1)
  check('send() resolves with the persisted events (intent/correlation metadata set)')

  /* -------- the other client converges over SSE -------- */
  await waitFor('client B sees the created task', () => {
    const snapshot = projectionB.getSnapshot()
    return snapshot.status === 'ready' && snapshot.data.some((t) => t.id === taskId)
  })
  check('second client receives the event through its projection stream')

  /* -------- idempotency: same intentId does not re-execute -------- */
  const intentId = crypto.randomUUID()
  const dupTaskId = crypto.randomUUID()
  const first = await clientA.send(
    createTask,
    { id: dupTaskId, title: 'only once' },
    { intentId },
  )
  const second = await clientA.send(
    createTask,
    { id: dupTaskId, title: 'only once' },
    { intentId },
  )
  assert.ok(first.ok && second.ok)
  assert.equal(first.value.deduped, false)
  assert.equal(second.value.deduped, true)
  assert.deepEqual(
    second.value.events.map((e) => e.id),
    first.value.events.map((e) => e.id),
  )
  const eventCount = await store.pool.query(
    `select count(*)::int as n from intent_effect_events where intent_id = $1`,
    [intentId],
  )
  assert.equal(eventCount.rows[0].n, 1)
  check('retrying an intentId replays the stored outcome instead of re-executing')

  /* -------- concurrent duplicate intentIds -------- */
  const raceId = crypto.randomUUID()
  const raceTask = crypto.randomUUID()
  const [r1, r2] = await Promise.all([
    clientA.send(createTask, { id: raceTask, title: 'race' }, { intentId: raceId }),
    clientB.send(createTask, { id: raceTask, title: 'race' }, { intentId: raceId }),
  ])
  assert.ok(r1.ok && r2.ok)
  assert.equal([r1.value, r2.value].filter((v) => v.deduped).length, 1)
  check('concurrent duplicate sends: exactly one executes, the other replays')

  /* -------- failed intents return Err and are replayed as Err -------- */
  const failId = crypto.randomUUID()
  const failed = await clientA.send(
    renameTask,
    { id: 'does-not-exist', title: 'nope' },
    { intentId: failId },
  )
  assert.ok(!failed.ok)
  assert.equal(failed.error.code, 'not_found')
  const failedReplay = await clientA.send(
    renameTask,
    { id: 'does-not-exist', title: 'nope' },
    { intentId: failId },
  )
  assert.ok(!failedReplay.ok)
  assert.equal(failedReplay.error.deduped, true)
  const taskCountAfterFail = await store.pool.query(`select count(*)::int as n from tasks`)
  assert.equal(taskCountAfterFail.rows[0].n, 3)
  check('failed handler → Err result, mutation rolled back, failure replayed on retry')

  /* -------- client-side validation returns Err without a request -------- */
  const invalid = await clientA.send(createTask, { id: 'x', title: '' })
  assert.ok(!invalid.ok)
  assert.equal(invalid.error.code, 'validation_failed')
  check('invalid input → Err(validation_failed) before any network call')

  /* -------- rename + delete propagate; both clients converge -------- */
  const renamed = await clientA.send(renameTask, { id: taskId, title: 'write MORE docs' })
  assert.ok(renamed.ok)
  const deleted = await clientA.send(deleteTask, { id: dupTaskId })
  assert.ok(deleted.ok)
  await waitFor('client B converges after rename+delete', () => {
    const snapshot = projectionB.getSnapshot()
    if (snapshot.status !== 'ready') return false
    const byId = new Map(snapshot.data.map((t) => [t.id, t.title]))
    return byId.get(taskId) === 'write MORE docs' && !byId.has(dupTaskId)
  })
  check('rename/delete events keep the other client synchronized')

  /* -------- dedup: HTTP-returned events + SSE copies applied once -------- */
  const snapshotB = projectionB.getSnapshot()
  assert.ok(snapshotB.status === 'ready')
  const projectionA = clientA.projection(taskList, {})
  const unsubA = projectionA.subscribe(() => {})
  await waitFor('client A projection ready', () => projectionA.getSnapshot().status === 'ready')
  const snapshotA = projectionA.getSnapshot()
  assert.ok(snapshotA.status === 'ready')
  assert.deepEqual(
    [...snapshotA.data].sort((a, b) => a.id.localeCompare(b.id)),
    [...snapshotB.data].sort((a, b) => a.id.localeCompare(b.id)),
  )
  assert.equal(
    snapshotA.data.filter((t) => t.id === raceTask).length,
    1,
    'duplicate application would duplicate rows',
  )
  check('initiating client applies HTTP + SSE copies of an event exactly once')

  /* -------- cursor catch-up: full replay from 0 -------- */
  const head = await store.head()
  assert.ok(head.ok)
  const replayed = await readSse(`${baseUrl}/_intenteffect/events?cursor=0`, head.value)
  assert.equal(replayed.length, head.value)
  assert.deepEqual(
    replayed.map((e) => e.id),
    Array.from({ length: head.value }, (_, i) => i + 1),
    'replay must be complete and ordered',
  )
  check('SSE catch-up from cursor=0 replays the full ordered log')

  /* -------- reconnect with mid-stream cursor -------- */
  const mid = Math.floor(head.value / 2)
  const tail = await readSse(`${baseUrl}/_intenteffect/events?cursor=${mid}`, head.value - mid)
  assert.deepEqual(
    tail.map((e) => e.id),
    Array.from({ length: head.value - mid }, (_, i) => mid + i + 1),
  )
  check('reconnecting with a mid-stream cursor delivers exactly the missed events')

  /* -------- live delivery after catch-up on the same connection -------- */
  const livePromise = readSse(`${baseUrl}/_intenteffect/events?cursor=${head.value}`, 1)
  await new Promise((resolve) => setTimeout(resolve, 100))
  const liveSend = await clientA.send(createTask, {
    id: crypto.randomUUID(),
    title: 'live event',
  })
  assert.ok(liveSend.ok)
  const live = await livePromise
  assert.equal(live.length, 1)
  assert.equal(live[0]!.id, head.value + 1)
  assert.equal(live[0]!.type, 'task.created')
  check('connections switch from catch-up to live delivery without gaps')

  unsubA()
  unsubB()
  console.log(`\nall ${passed} checks passed`)
} finally {
  clientA.close()
  clientB.close()
  server.close()
  await app.close()
}
process.exit(0)
