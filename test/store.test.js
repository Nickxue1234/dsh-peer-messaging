import assert from 'node:assert/strict'
import { mkdtemp, readdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, test } from 'node:test'

import { PeerBus, PROTOCOL_VERSION, makeHostId, sessionKey } from '../lib/store.js'

const dirs = []

async function newBus(options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'peer-bus-'))
  dirs.push(dir)
  const bus = new PeerBus({ stateDir: dir, logger: { warn() {}, info() {}, error() {} }, hostId: 'host-under-test', ...options })
  await bus.ready()
  return bus
}

afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

function envelope(messageId, createdAt, extra = {}) {
  return {
    v: PROTOCOL_VERSION,
    messageId,
    createdAt,
    targetSessionId: 'target-session',
    sender: { sessionId: 'sender-session', workspace: '/ws/a', hostId: 'host-a', pid: 1 },
    wake: true,
    text: `hello ${messageId}`,
    meta: { hops: 0 },
    ...extra,
  }
}

test('sessionKey keeps safe ids and hashes everything else deterministically', () => {
  assert.equal(sessionKey('01J9-ABCD_efg.123'), '01J9-ABCD_efg.123')
  assert.equal(sessionKey('.'), 'h-' + sessionKey('.').slice(2))
  assert.match(sessionKey('a/b:c'), /^h-[0-9a-f]{40}$/)
  assert.equal(sessionKey('a/b:c'), sessionKey('a/b:c'))
  assert.notEqual(sessionKey('a/b:c'), sessionKey('a/b:d'))
  assert.match(sessionKey(''), /^h-[0-9a-f]{40}$/)
})

test('makeHostId is unique per process instance and includes the pid', () => {
  const first = makeHostId()
  assert.ok(first.includes(String(process.pid)))
  assert.ok(first.length <= 100)
  assert.notEqual(first, makeHostId())
})

test('enqueue then listPending returns envelopes oldest first', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('m-late', '2026-01-01T00:00:02.000Z'))
  await bus.enqueue(envelope('m-early', '2026-01-01T00:00:01.000Z'))
  await bus.enqueue(envelope('m-mid', '2026-01-01T00:00:01.500Z'))
  const pending = await bus.listPending('target-session')
  assert.deepEqual(
    pending.map((item) => item.envelope.messageId),
    ['m-early', 'm-mid', 'm-late'],
  )
  assert.equal((await bus.listPending('nobody')).length, 0)
})

test('claim is exclusive: only one poller wins', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('m-1', '2026-01-01T00:00:00.000Z'))
  const [{ fileName }] = await bus.listPending('target-session')
  const first = await bus.claim('target-session', fileName)
  assert.ok(first)
  assert.equal(await bus.claim('target-session', fileName), null)
  assert.equal((await bus.listPending('target-session')).length, 0)
})

test('requeue puts a claimed envelope back into the inbox', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('m-requeue', '2026-01-01T00:00:00.000Z'))
  const [{ fileName }] = await bus.listPending('target-session')
  const claimed = await bus.claim('target-session', fileName)
  assert.ok(claimed)
  await bus.requeue(claimed, 'target-session', fileName)
  const pending = await bus.listPending('target-session')
  assert.equal(pending.length, 1)
  assert.equal(pending[0].envelope.messageId, 'm-requeue')
})

test('complete publishes a delivered marker the sender can observe', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('m-2', '2026-01-01T00:00:00.000Z'))
  const [{ fileName }] = await bus.listPending('target-session')
  assert.deepEqual(await bus.waitForOutcome('m-2', 'target-session', { timeoutMs: 20 }), { state: 'pending' })
  const claimed = await bus.claim('target-session', fileName)
  await bus.complete(claimed, 'target-session', fileName)
  assert.deepEqual(await bus.waitForOutcome('m-2', 'target-session', { timeoutMs: 100 }), { state: 'delivered' })
})

test('fail records the reason and removes the claimed envelope', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('m-3', '2026-01-01T00:00:00.000Z'))
  const [{ fileName, envelope: pending }] = await bus.listPending('target-session')
  const claimed = await bus.claim('target-session', fileName)
  await bus.fail(claimed, 'target-session', fileName, pending, new Error('agent went away'))
  const outcome = await bus.waitForOutcome('m-3', 'target-session', { timeoutMs: 100 })
  assert.equal(outcome.state, 'failed')
  assert.match(outcome.error, /agent went away/)
  assert.equal((await bus.listPending('target-session')).length, 0)
})

test('presence records round-trip and can be removed', async () => {
  const bus = await newBus()
  await bus.writePresence({ startedAt: '2026-01-01T00:00:00.000Z', sessions: [{ sessionId: 's1' }] })
  let hosts = await bus.readHosts()
  assert.equal(hosts.length, 1)
  assert.equal(hosts[0].hostId, 'host-under-test')
  assert.equal(hosts[0].sessions[0].sessionId, 's1')
  await bus.removePresence()
  hosts = await bus.readHosts()
  assert.equal(hosts.length, 0)
})

test('a malformed presence record is ignored, not fatal', async () => {
  const bus = await newBus()
  await bus.writePresence({ startedAt: 'now', sessions: [] })
  const hostsDir = join(bus.stateDir, 'hosts')
  const { writeFile } = await import('node:fs/promises')
  await writeFile(join(hostsDir, 'broken.json'), '{not json', 'utf8')
  const hosts = await bus.readHosts()
  assert.equal(hosts.length, 1)
})

test('cleanup drops expired markers, abandoned claims and dead hosts', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('old', '2026-01-01T00:00:00.000Z'))
  const [{ fileName }] = await bus.listPending('target-session')
  const claimed = await bus.claim('target-session', fileName)
  await bus.complete(claimed, 'target-session', fileName)
  await bus.writePresence({ startedAt: '2026-01-01T00:00:00.000Z', sessions: [{ sessionId: 's1' }] })

  const retentionMs = 60_000
  const future = Date.now() + 10 * retentionMs
  await bus.cleanup({ retentionMs, presenceTtlMs: 1_000, now: future })

  assert.deepEqual(await readdir(join(bus.stateDir, 'processed')), [])
  assert.deepEqual(await readdir(join(bus.stateDir, 'hosts')), ['host-under-test.json'], 'own presence must survive')
})

test('cleanup keeps fresh data', async () => {
  const bus = await newBus()
  await bus.enqueue(envelope('fresh', new Date().toISOString()))
  await bus.cleanup({ retentionMs: 60_000, presenceTtlMs: 15_000 })
  assert.equal((await bus.listPending('target-session')).length, 1)
})
