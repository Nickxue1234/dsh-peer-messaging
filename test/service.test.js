import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, test } from 'node:test'

import { normalizeConfig } from '../lib/config.js'
import { PeerBus, PROTOCOL_VERSION } from '../lib/store.js'
import { PeerMessaging, PeerMessagingError } from '../lib/service.js'

const logger = { warn() {}, info() {}, error() {}, debug() {} }
const cleanups = []

/** @param {string} id */
function makeAgent(id, { workspace = `/ws/${id}`, status = 'idle', kind = 'root' } = {}) {
  /** @type {{inject: any[], steer: any[], followup: any[]}} */
  const calls = { inject: [], steer: [], followup: [] }
  return {
    id,
    status,
    calls,
    inbox: { nextStep: [], nextTurn: [] },
    session: {
      id,
      header: { id, cwd: workspace, origin: kind === 'subagent' ? 'subagent' : undefined },
    },
    inject: (message) => calls.inject.push(message),
    steer: (message) => calls.steer.push(message),
    followup: (message) => calls.followup.push(message),
  }
}

/**
 * @param {any[]} agents
 * @param {{events?: boolean, services?: Record<string, any>, resume?: (id: string) => Promise<any>}} [options]
 */
function makeCtx(agents, { events = false, services: extraServices = {}, resume } = {}) {
  const services = {
    workingDirectory: { get: (session) => session?.header?.cwd },
    workspaceRegistry: { list: () => [] },
    ...extraServices,
  }
  /** @type {Map<string, Array<(payload: any, event?: any) => void>>} */
  const handlers = new Map()
  const ctx = {
    agents: {
      list: () => agents,
      get: (id) => agents.find((agent) => agent.id === id),
      resume: resume ?? (async () => ({ agent: undefined })),
    },
    logger: () => logger,
    get: (name) => services[name],
    emit: () => {},
  }
  if (events) {
    ctx.handlers = handlers
    ctx.on = (type, handler) => {
      const list = handlers.get(type) ?? []
      list.push(handler)
      handlers.set(type, list)
      return () => handlers.set(type, (handlers.get(type) ?? []).filter((candidate) => candidate !== handler))
    }
    ctx.emit = (type, payload) => {
      for (const handler of handlers.get(type) ?? []) handler(payload)
    }
  }
  return ctx
}

async function setup({ agents, config = {}, events = false, services, resume } = {}) {
  const stateDir = await mkdtemp(join(tmpdir(), 'peer-service-'))
  const ctx = makeCtx(agents, { events, services, resume })
  const service = new PeerMessaging(
    ctx,
    normalizeConfig({ stateDir, pollMs: 100, heartbeatMs: 1_000, ackWaitMs: 2_000, ...config }),
  )
  service.start()
  cleanups.push(async () => {
    await service.stop()
    await rm(stateDir, { recursive: true, force: true })
  })
  return { service, ctx, stateDir, agents }
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup()
})

test('describeAgent reports workspace, kind and status', async () => {
  const root = makeAgent('root-1', { workspace: '/ws/alpha' })
  const sub = makeAgent('sub-1', { workspace: '/ws/beta', kind: 'subagent', status: 'running' })
  const { service } = await setup({ agents: [root, sub] })
  assert.deepEqual(service.describeAgent(root), {
    sessionId: 'root-1',
    title: '',
    workspace: '/ws/alpha',
    workspaceTitle: 'alpha',
    status: 'idle',
    kind: 'root',
    live: true,
  })
  assert.equal(service.describeAgent(sub).kind, 'subagent')
  assert.equal(service.describeAgent(sub).status, 'running')
})

test('listAgents merges local sessions with remote presence records', async () => {
  const local = makeAgent('local-1', { workspace: '/ws/alpha' })
  const { service, stateDir } = await setup({ agents: [local] })
  const remoteBus = new PeerBus({ stateDir, logger, hostId: 'remote-host-1' })
  await remoteBus.writePresence({
    startedAt: new Date().toISOString(),
    sessions: [{ sessionId: 'remote-1', workspace: '/ws/beta', workspaceTitle: 'beta', status: 'idle', kind: 'root' }],
  })

  const agents = await service.listAgents()
  const byId = new Map(agents.map((agent) => [agent.sessionId, agent]))
  assert.equal(agents.length, 2)
  assert.equal(byId.get('local-1').local, true)
  assert.equal(byId.get('remote-1').local, false)
  assert.equal(byId.get('remote-1').hostId, 'remote-host-1')
  assert.equal(byId.get('local-1').self, undefined)

  const filtered = await service.listAgents({ workspace: 'beta' })
  assert.deepEqual(filtered.map((agent) => agent.sessionId), ['remote-1'])
})

test('subagents are hidden unless requested', async () => {
  const root = makeAgent('root-1')
  const sub = makeAgent('sub-1', { kind: 'subagent' })
  const { service } = await setup({ agents: [root, sub] })
  assert.deepEqual((await service.listAgents()).map((agent) => agent.sessionId), ['root-1'])
  assert.equal((await service.listAgents({ includeSubagents: true })).length, 2)
})

test('send wakes an idle recipient with a relay message', async () => {
  const sender = makeAgent('sender-1', { workspace: '/ws/alpha' })
  const recipient = makeAgent('recipient-1', { workspace: '/ws/beta' })
  const { service } = await setup({ agents: [sender, recipient] })

  const result = await service.send({ target: 'recipient-1', text: 'please run the tests', agent: sender })
  assert.equal(result.ok, true)
  assert.equal(result.delivery, 'delivered')
  assert.equal(result.target.sessionId, 'recipient-1')
  assert.equal(result.target.local, true)

  // Interjection: the message rides the recipient's next step, not a queue.
  assert.equal(recipient.calls.steer.length, 1)
  assert.equal(recipient.calls.followup.length, 0)
  const message = recipient.calls.steer[0]
  assert.equal(message.id, result.messageId)
  assert.equal(message.role, 'user')
  assert.equal(message.source.kind, 'peer-message')
  assert.equal(message.source.form, 'relay')
  assert.equal(message.source.senderSessionId, 'sender-1')
  assert.equal(message.source.senderWorkspace, '/ws/alpha')
  const text = message.content.map((block) => block.text).join('\n')
  assert.match(text, /^\[peer-message\] from \/ws\/alpha \(session sender-1\) at /)
  assert.match(text, /please run the tests/)
  assert.match(text, /peer_send/)
})

test('send queues into a running recipient and injects when wake is false', async () => {
  const sender = makeAgent('sender-1')
  const running = makeAgent('running-1', { status: 'running' })
  const quiet = makeAgent('quiet-1')
  const { service } = await setup({ agents: [sender, running, quiet] })

  await service.send({ target: 'running-1', text: 'ping', agent: sender })
  // A busy recipient gets the message at its next step boundary (dsh's
  // interjection path), so it reacts inside the current turn; only a config
  // with interject:false queues it for the next turn.
  assert.equal(running.calls.steer.length, 1)
  assert.equal(running.calls.followup.length, 0)

  const result = await service.send({ target: 'quiet-1', text: 'fyi', wake: false, agent: sender })
  assert.equal(result.ok, true)
  assert.equal(result.delivery, 'injected', 'wake:false is a hand-off, not a confirmed read')
  assert.equal(quiet.calls.inject.length, 1)
  assert.equal(quiet.calls.steer.length, 0)
})

test('a same-process unconfirmed hand-off reports its queue state', async () => {
  const sender = makeAgent('sender-1')
  const recipient = makeAgent('recipient-1')
  const { service } = await setup({ agents: [sender, recipient], events: true, config: { ackWaitMs: 100 } })

  // The agent stubs' followup() never populates the fake inbox, so this is the
  // window between "handed to the agent" and "recorded in the session log".
  const handedOff = await service.send({ target: 'recipient-1', text: 'busy now', agent: sender })
  assert.equal(handedOff.delivery, 'queued')
  assert.equal(handedOff.queueState, 'handed-off')

  // A message still parked in the recipient inbox is reported as queued.
  const parkedId = handedOff.messageId
  recipient.inbox.nextTurn.push({ id: parkedId })
  const queued = await service.send({ target: 'recipient-1', text: 'still waiting', agent: sender })
  assert.equal(queued.delivery, 'queued')
  assert.equal(queued.queueState, 'handed-off')
  recipient.inbox.nextTurn.length = 0
})

test('a hand-off counts as delivered only once the recipient session logs it', async () => {
  const sender = makeAgent('sender-1', { workspace: '/ws/alpha' })
  const recipient = makeAgent('recipient-1', { workspace: '/ws/beta' })
  const { service, ctx } = await setup({ agents: [sender, recipient], events: true, config: { ackWaitMs: 100 } })

  const result = await service.send({ target: 'recipient-1', text: 'still queued', agent: sender })
  assert.equal(result.ok, true)
  assert.equal(result.delivery, 'queued')
  assert.equal(recipient.calls.steer.length, 1)
  assert.equal(service.inflight.size, 1)

  // The recipient's turn consumed the message: dsh appends it as a user message.
  for (const handler of ctx.handlers.get('session/event') ?? []) {
    handler({ id: 'recipient-1' }, { type: 'user/message', data: { id: result.messageId } })
  }
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(service.inflight.size, 0)
  assert.deepEqual(await service.bus.waitForOutcome(result.messageId, 'recipient-1', { timeoutMs: 100 }), { state: 'delivered' })
})

test('a message discarded by the recipient is requeued and handed off again', async () => {
  const sender = makeAgent('sender-1')
  const recipient = makeAgent('recipient-1')
  const { service, ctx } = await setup({ agents: [sender, recipient], events: true, config: { ackWaitMs: 100 } })

  const result = await service.send({ target: 'recipient-1', text: 'do not drop me', agent: sender })
  assert.equal(recipient.calls.steer.length, 1)

  // The user stopped the turn (or archiving cleared the inbox): queued input is discarded.
  for (const handler of ctx.handlers.get('agent/inbox/discarded') ?? []) {
    handler({ agent: { id: 'recipient-1' }, message: { id: result.messageId } })
  }
  await new Promise((resolve) => setTimeout(resolve, 25))

  assert.equal(recipient.calls.steer.length, 2)
  const delivered = await service.bus.waitForOutcome(result.messageId, 'recipient-1', { timeoutMs: 100 })
  assert.equal(delivered.state, 'pending')

  for (const handler of ctx.handlers.get('session/event') ?? []) {
    handler({ id: 'recipient-1' }, { type: 'user/message', data: { id: result.messageId } })
  }
  await new Promise((resolve) => setImmediate(resolve))
  assert.deepEqual(await service.bus.waitForOutcome(result.messageId, 'recipient-1', { timeoutMs: 100 }), { state: 'delivered' })
})

test('resolveTarget accepts exact ids, prefixes, workspace paths and titles', async () => {
  const sender = makeAgent('sender-1')
  const alpha = makeAgent('session-alpha-1234', { workspace: '/ws/alpha' })
  const beta = makeAgent('session-beta-5678', { workspace: '/home/me/projects/beta-app' })
  const { service } = await setup({ agents: [sender, alpha, beta] })

  assert.equal((await service.resolveTarget('session-alpha-1234')).sessionId, 'session-alpha-1234')
  assert.equal((await service.resolveTarget('session-b')).sessionId, 'session-beta-5678')
  assert.equal((await service.resolveTarget('session:session-a')).sessionId, 'session-alpha-1234')
  assert.equal((await service.resolveTarget('/ws/alpha')).sessionId, 'session-alpha-1234')
  assert.equal((await service.resolveTarget('alpha')).sessionId, 'session-alpha-1234')
  assert.equal((await service.resolveTarget('beta-app')).sessionId, 'session-beta-5678')
  assert.equal((await service.resolveTarget('workspace:beta-app')).sessionId, 'session-beta-5678')
})

test('resolveTarget refuses unsafe or ambiguous addresses', async () => {
  const sender = makeAgent('sender-1')
  const a = makeAgent('one-1', { workspace: '/ws/shared' })
  const b = makeAgent('two-2', { workspace: '/ws/shared' })
  const { service } = await setup({ agents: [sender, a, b] })

  await assert.rejects(() => service.resolveTarget('sender-1', { senderSessionId: 'sender-1' }), (error) => {
    assert.ok(error instanceof PeerMessagingError)
    assert.equal(error.code, 'self')
    return true
  })
  await assert.rejects(() => service.resolveTarget('does-not-exist'), (error) => {
    assert.equal(error.code, 'not_found')
    assert.ok(error.candidates.length >= 1)
    return true
  })
  await assert.rejects(() => service.resolveTarget('/ws/shared'), (error) => {
    assert.equal(error.code, 'ambiguous')
    assert.equal(error.candidates.length, 2)
    return true
  })
  await assert.rejects(() => service.resolveTarget('   '), (error) => error.code === 'invalid')
})

test('a short prefix does not silently match a session', async () => {
  const sender = makeAgent('sender-1')
  const alpha = makeAgent('session-alpha-1234')
  const { service } = await setup({ agents: [sender, alpha] })
  await assert.rejects(() => service.resolveTarget('sess'), (error) => error.code === 'not_found')
})

test('send to a remote session queues the message for its host', async () => {
  const sender = makeAgent('sender-1', { workspace: '/ws/alpha' })
  const { service, stateDir } = await setup({ agents: [sender], config: { ackWaitMs: 100 } })
  const remoteBus = new PeerBus({ stateDir, logger, hostId: 'remote-host-1' })
  await remoteBus.writePresence({
    startedAt: new Date().toISOString(),
    sessions: [{ sessionId: 'remote-1', workspace: '/ws/beta', workspaceTitle: 'beta', status: 'idle', kind: 'root' }],
  })

  const result = await service.send({ target: 'remote-1', text: 'across processes', agent: sender })
  assert.equal(result.ok, true)
  assert.equal(result.delivery, 'queued')
  assert.equal(result.target.local, false)

  const pending = await remoteBus.listPending('remote-1')
  assert.equal(pending.length, 1)
  assert.equal(pending[0].envelope.text, 'across processes')
  assert.equal(pending[0].envelope.sender.sessionId, 'sender-1')
  assert.equal(pending[0].envelope.v, PROTOCOL_VERSION)
})

test('deliverFor drains pending envelopes and records failures', async () => {
  const healthy = makeAgent('healthy-1')
  const broken = makeAgent('broken-1')
  broken.steer = () => {
    throw new Error('session is gone')
  }
  const { service, stateDir } = await setup({ agents: [healthy, broken] })
  const bus = new PeerBus({ stateDir, logger, hostId: 'other-host' })
  const envelope = (targetSessionId, messageId) => ({
    v: PROTOCOL_VERSION,
    messageId,
    createdAt: new Date().toISOString(),
    targetSessionId,
    sender: { sessionId: 'sender-9', workspace: '/ws/z', hostId: 'other-host', pid: 1 },
    wake: true,
    text: messageId,
    meta: { hops: 0 },
  })
  await bus.enqueue(envelope('healthy-1', 'ok-1'))
  await bus.enqueue(envelope('broken-1', 'bad-1'))

  await service.deliverFor(healthy)
  await service.deliverFor(broken)

  assert.equal(healthy.calls.steer.length, 1)
  assert.deepEqual(await service.bus.waitForOutcome('ok-1', 'healthy-1', { timeoutMs: 100 }), { state: 'delivered' })
  const failure = await service.bus.waitForOutcome('bad-1', 'broken-1', { timeoutMs: 100 })
  assert.equal(failure.state, 'failed')
  assert.match(failure.error, /session is gone/)
})

test('oversized messages are rejected before hitting the bus', async () => {
  const sender = makeAgent('sender-1')
  const recipient = makeAgent('recipient-1')
  const { service } = await setup({ agents: [sender, recipient], config: { maxMessageBytes: 256 } })
  await assert.rejects(
    () => service.send({ target: 'recipient-1', text: 'x'.repeat(512), agent: sender }),
    (error) => error.code === 'too_large',
  )
  assert.equal(recipient.calls.steer.length, 0)
})

test('stop removes the presence record', async () => {
  const agent = makeAgent('solo-1')
  const { service } = await setup({ agents: [agent] })
  await service.ready
  assert.equal((await service.bus.readHosts()).length, 1)
  await service.stop()
  assert.equal((await service.bus.readHosts()).length, 0)
})

// ---------------------------------------------------------------------------
// Closed sessions (dsh attaches no Agent until a session is opened)
// ---------------------------------------------------------------------------

/** A persisted session record, as `ctx.sessionQuery.listSessions()` reports it. */
function coldRecord(id, cwd) {
  return { header: { id, cwd, createdAt: '2026-01-01T00:00:00.000Z' }, live: false, persisted: true }
}

test('listAgents lists closed sessions with live:false so they stay addressable', async () => {
  const self = makeAgent('self-1')
  const { service } = await setup({
    agents: [self],
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/closed')] } },
  })

  const listed = await service.listAgents({})
  const closed = listed.find((entry) => entry.sessionId === 'closed-1')
  assert.ok(closed, 'the closed session should be listed')
  assert.equal(closed.live, false)
  assert.equal(closed.status, 'closed')
  assert.equal(closed.workspace, '/ws/closed')

  // Live agents sort before closed ones.
  assert.equal(listed[0].sessionId, 'self-1')
})

test('includeCold:false hides closed sessions', async () => {
  const { service } = await setup({
    agents: [makeAgent('self-1')],
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/closed')] } },
  })
  assert.equal((await service.listAgents({ includeCold: false })).length, 1)
})

test('the closed-session catalogue is cached for coldListTtlMs', async () => {
  let calls = 0
  const { service } = await setup({
    agents: [makeAgent('self-1')],
    config: { coldListTtlMs: 60_000 },
    services: {
      sessionQuery: {
        listSessions: async () => {
          calls += 1
          return [coldRecord('closed-1', '/ws/closed')]
        },
      },
    },
  })
  await service.listAgents({})
  await service.listAgents({})
  await service.listAgents({})
  assert.equal(calls, 1)
})

test('sending to a closed session resumes it, then delivers', async () => {
  const sender = makeAgent('sender-1')
  const agents = [sender]
  const resumed = makeAgent('closed-1', { workspace: '/ws/closed' })
  const { service } = await setup({
    agents,
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/closed')] } },
    resume: async (sessionId) => {
      agents.push(resumed)
      return { agent: resumed }
    },
  })

  const result = await service.send({ target: 'closed-1', text: 'wake up', agent: sender })
  assert.equal(result.ok, true)
  assert.equal(result.target.sessionId, 'closed-1')
  assert.equal(result.target.live, true)
  assert.equal(resumed.calls.steer.length, 1)
  assert.equal(resumed.calls.steer[0].content[1].text, 'wake up')
})

test('resumeColdTargets:false refuses a closed target instead of silently dropping', async () => {
  const sender = makeAgent('sender-1')
  const { service } = await setup({
    agents: [sender],
    config: { resumeColdTargets: false },
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/closed')] } },
  })

  await assert.rejects(
    () => service.send({ target: 'closed-1', text: 'hello', agent: sender }),
    (error) => error instanceof PeerMessagingError && error.code === 'unavailable',
  )
})

test('a failed resume reports unavailable with the underlying reason', async () => {
  const sender = makeAgent('sender-1')
  const { service } = await setup({
    agents: [sender],
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/closed')] } },
    resume: async () => {
      throw new Error('session is owned by another process')
    },
  })

  await assert.rejects(
    () => service.send({ target: 'closed-1', text: 'hello', agent: sender }),
    (error) =>
      error instanceof PeerMessagingError &&
      error.code === 'unavailable' &&
      /owned by another process/.test(error.message),
  )
})

test('two sends to the same closed session share one resume', async () => {
  const sender = makeAgent('sender-1')
  const agents = [sender]
  const resumed = makeAgent('closed-1', { workspace: '/ws/closed' })
  let resumes = 0
  const { service } = await setup({
    agents,
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/closed')] } },
    resume: async () => {
      resumes += 1
      await new Promise((resolve) => setTimeout(resolve, 10))
      agents.push(resumed)
      return { agent: resumed }
    },
  })

  await Promise.all([
    service.send({ target: 'closed-1', text: 'one', agent: sender }),
    service.send({ target: 'closed-1', text: 'two', agent: sender }),
  ])
  assert.equal(resumes, 1)
  assert.equal(resumed.calls.steer.length, 2)
})

test('a closed session in the same workspace does not make a live target ambiguous', async () => {
  const sender = makeAgent('sender-1', { workspace: '/ws/sender' })
  const live = makeAgent('live-1', { workspace: '/ws/shared' })
  const { service } = await setup({
    agents: [sender, live],
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/shared')] } },
  })

  const resolved = await service.resolveTarget('workspace:/ws/shared', { senderSessionId: 'sender-1' })
  assert.equal(resolved.sessionId, 'live-1')
})

test('a closed session is still resolvable when nothing is running there', async () => {
  const sender = makeAgent('sender-1', { workspace: '/ws/sender' })
  const { service } = await setup({
    agents: [sender],
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-1', '/ws/quiet')] } },
  })

  const resolved = await service.resolveTarget('workspace:/ws/quiet', { senderSessionId: 'sender-1' })
  assert.equal(resolved.sessionId, 'closed-1')
  assert.equal(resolved.live, false)
})

// --- learned from dsh-agent-message: archived sessions, presets, flood guard ---

test('archived sessions are hidden from the catalogue and refused as targets', async () => {
  const self = makeAgent('self-1')
  const { service } = await setup({
    agents: [self],
    services: {
      sessionQuery: {
        listSessions: async () => [coldRecord('archived-1', '/ws/old'), coldRecord('closed-2', '/ws/keep')],
      },
      workspaceRegistry: { list: () => [], archivedSessionIds: ['archived-1'] },
    },
  })

  const listed = await service.listAgents({ includeCold: true })
  assert.equal(listed.some((entry) => entry.sessionId === 'archived-1'), false)
  assert.equal(listed.some((entry) => entry.sessionId === 'closed-2'), true)

  await assert.rejects(
    () => service.send({ target: 'session:archived-1', text: 'hello' }),
    (error) => error.code === 'archived' && /archived/.test(error.message),
  )
})

test('closed subagent sessions are not listed unless subagents are requested', async () => {
  const self = makeAgent('self-1')
  const subagent = { header: { id: 'sub-9', cwd: '/ws/x', origin: 'subagent' }, live: false }
  const { service } = await setup({
    agents: [self],
    services: { sessionQuery: { listSessions: async () => [subagent, coldRecord('closed-3', '/ws/x')] } },
  })

  const listed = await service.listAgents({})
  assert.equal(listed.some((entry) => entry.sessionId === 'sub-9'), false)
  const withSubagents = await service.listAgents({ includeSubagents: true })
  assert.equal(withSubagents.some((entry) => entry.sessionId === 'sub-9'), true)
})

test('closed sessions keep their session title and are matchable by it', async () => {
  const self = makeAgent('self-1')
  const { service } = await setup({
    agents: [self],
    services: {
      sessionQuery: {
        listSessions: async () => [coldRecord('closed-4', '/ws/proj')],
        readTitleSnapshots: async (ids) => ids.map((id) => ({ sessionId: id, status: 'fulfilled', value: { title: { title: 'Refactor landing page' } } })),
      },
    },
  })

  const listed = await service.listAgents({})
  const row = listed.find((entry) => entry.sessionId === 'closed-4')
  assert.equal(row.title, 'Refactor landing page')

  const resolved = await service.resolveTarget('Refactor landing page', { senderSessionId: 'self-1' })
  assert.equal(resolved.sessionId, 'closed-4')
})

test('missing title snapshots leave the catalogue usable', async () => {
  const self = makeAgent('self-1')
  const { service } = await setup({
    agents: [self],
    services: {
      sessionQuery: {
        listSessions: async () => [coldRecord('closed-5', '/ws/proj')],
        readTitleSnapshots: async () => {
          throw new Error('no titles here')
        },
      },
    },
  })
  const listed = await service.listAgents({})
  assert.equal(listed.find((entry) => entry.sessionId === 'closed-5').title, '')
})

test('resuming a closed session restores the preset it was running with', async () => {
  const mounted = []
  const resumedAgent = makeAgent('closed-6', { workspace: '/ws/preset' })
  const handles = []
  let resumeOptions
  const { service } = await setup({
    agents: [],
    services: {
      agentPresets: { mount: async (_ctx, presetId) => mounted.push(presetId) },
      sessionQuery: {
        listSessions: async () => [coldRecord('closed-6', '/ws/preset')],
        readSession: async () => ({
          session: { header: { id: 'closed-6', cwd: '/ws/preset', agentPreset: 'legacy' } },
          events: [{ type: 'agent-preset/selected', data: { agentPreset: 'current-preset' } }],
        }),
      },
    },
    resume: async (options) => {
      resumeOptions = options
      const handle = { agent: resumedAgent, dispose: async () => handles.push('disposed') }
      return handle
    },
  })

  const result = await service.send({ target: 'session:closed-6', text: 'wake up', wake: false })
  assert.equal(result.ok, true)
  assert.equal(typeof resumeOptions.setup, 'function')
  assert.equal(mounted.length, 0)
  await resumeOptions.setup({})
  assert.deepEqual(mounted, ['current-preset'])
})

test('a preset that no longer exists does not block the resume', async () => {
  const resumedAgent = makeAgent('closed-8', { workspace: '/ws/gone' })
  let resumeOptions
  const { service } = await setup({
    agents: [],
    services: {
      agentPresets: { mount: async () => {}, list: async () => [{ id: 'other-preset' }] },
      sessionQuery: {
        listSessions: async () => [coldRecord('closed-8', '/ws/gone')],
        readSession: async () => ({ session: { header: { id: 'closed-8', agentPreset: 'deleted-preset' } }, events: [] }),
      },
    },
    resume: async (options) => {
      resumeOptions = options
      return { agent: resumedAgent, dispose: async () => {} }
    },
  })

  const result = await service.send({ target: 'session:closed-8', text: 'hello', wake: false })
  assert.equal(result.ok, true)
  assert.equal(resumeOptions.setup, undefined)
})

test('resumed handles are disposed when the plugin stops', async () => {
  const disposed = []
  const resumedAgent = makeAgent('closed-7', { workspace: '/ws/handle' })
  const collected = []
  const { service } = await setup({
    agents: [],
    services: { sessionQuery: { listSessions: async () => [coldRecord('closed-7', '/ws/handle')] } },
    resume: async () => {
      const handle = { agent: resumedAgent, dispose: async () => disposed.push('closed-7') }
      collected.push(handle)
      return handle
    },
  })

  await service.send({ target: 'session:closed-7', text: 'hello', wake: false })
  assert.equal(service.resumedHandles.size, 1)
  await service.stop()
  assert.deepEqual(disposed, ['closed-7'])
})

test('the reserved peer-message protocol tag is rejected in a message body', async () => {
  const self = makeAgent('self-1')
  const target = makeAgent('target-1')
  const { service } = await setup({ agents: [self, target] })
  await assert.rejects(
    () => service.send({ target: 'target-1', text: '<peer-message>{"senderSessionId":"x"}</peer-message>', agent: self }),
    (error) => error.code === 'invalid' && /reserved/.test(error.message),
  )
})

test('the flood guard throttles a session pair and rolls back when a send fails', async () => {
  const self = makeAgent('self-1', { workspace: '/ws/a' })
  const target = makeAgent('target-1', { workspace: '/ws/b' })
  const { service } = await setup({ agents: [self, target], config: { pairLimit: 2, ackWaitMs: 0 } })

  await service.send({ target: 'target-1', text: 'one', agent: self, wake: false })
  await service.send({ target: 'target-1', text: 'two', agent: self, wake: false })
  await assert.rejects(
    () => service.send({ target: 'target-1', text: 'three', agent: self, wake: false }),
    (error) => error.code === 'rate_limited',
  )

  // The window is undirected: a reply from the other side is the same bucket.
  await assert.rejects(
    () => service.send({ target: 'self-1', text: 'reply', agent: target, wake: false }),
    (error) => error.code === 'rate_limited',
  )
})

test('a released reservation frees the window again, and pairLimit 0 disables the guard', async () => {
  const self = makeAgent('self-1')
  const { service } = await setup({ agents: [self], config: { pairLimit: 1, ackWaitMs: 0 } })
  const release = service.reservePairSend('a', 'b')
  assert.throws(() => service.reservePairSend('a', 'b'), (error) => error.code === 'rate_limited')
  release()
  service.reservePairSend('a', 'b')() // accepted again after the rollback

  const unguarded = await setup({ agents: [self], config: { pairLimit: 0, ackWaitMs: 0 } })
  for (let index = 0; index < 5; index += 1) unguarded.service.reservePairSend('a', 'b')()
  assert.equal(unguarded.service.pairSends.size, 0)
})

test('send resumes a session whose only presence record is stale', async () => {
  const sender = makeAgent('sender-1', { workspace: '/ws/alpha' })
  const agents = [sender]
  const resumeCalls = []
  const { service, stateDir } = await setup({
    agents,
    resume: async ({ resumeSessionId }) => {
      resumeCalls.push(resumeSessionId)
      const revived = makeAgent(resumeSessionId, { workspace: '/ws/gone' })
      agents.push(revived)
      return { agent: revived }
    },
  })
  // The host that used to own the session is dead: its last heartbeat is older
  // than presenceTtlMs, so the "live" claim it left behind is worthless.
  const deadBus = new PeerBus({ stateDir, logger, hostId: 'dead-host-1', now: () => Date.now() - 60_000 })
  await deadBus.writePresence({
    startedAt: new Date(Date.now() - 120_000).toISOString(),
    sessions: [{ sessionId: 'gone-1', workspace: '/ws/gone', workspaceTitle: 'gone', status: 'idle', kind: 'root' }],
  })

  const result = await service.send({ target: 'session:gone-1', text: 'are you there?', agent: sender })
  assert.deepEqual(resumeCalls, ['gone-1'])
  assert.equal(result.delivery, 'delivered')
  assert.equal(result.target.live, true)
  assert.equal(agents.find((agent) => agent.id === 'gone-1').calls.steer.length, 1)
})

test('rescueOrphans resumes a session that has mail but no live Agent', async () => {
  const agents = []
  const resumeCalls = []
  const { service, stateDir } = await setup({
    agents,
    config: { rescueGraceMs: 0, rescueBackoffMs: 1_000 },
    resume: async ({ resumeSessionId }) => {
      resumeCalls.push(resumeSessionId)
      const revived = makeAgent(resumeSessionId, { workspace: '/ws/orphan' })
      agents.push(revived)
      return { agent: revived }
    },
  })
  const bus = new PeerBus({ stateDir, logger })
  await bus.enqueue({
    v: PROTOCOL_VERSION,
    messageId: 'orphan-msg-1',
    createdAt: new Date().toISOString(),
    targetSessionId: 'orphan-1',
    sender: { sessionId: 'sender-9', workspace: '/ws/x', hostId: 'h9', pid: 9 },
    wake: true,
    text: 'wake up',
    meta: { hops: 0 },
  })

  await service.rescueOrphans()

  assert.deepEqual(resumeCalls, ['orphan-1'], 'the orphan owner must be resumed')
  const revived = agents.find((agent) => agent.id === 'orphan-1')
  assert.equal(revived.calls.steer.length, 1, 'the waiting mail must be handed off')
  assert.deepEqual(await bus.listPending('orphan-1'), [], 'the envelope leaves the inbox')
})

test('rescueOrphans leaves mail alone while a fresh host claims the session', async () => {
  const agents = []
  const resumeCalls = []
  const { service, stateDir } = await setup({
    agents,
    config: { rescueGraceMs: 0 },
    resume: async ({ resumeSessionId }) => {
      resumeCalls.push(resumeSessionId)
      const revived = makeAgent(resumeSessionId)
      agents.push(revived)
      return { agent: revived }
    },
  })
  const liveBus = new PeerBus({ stateDir, logger, hostId: 'live-host-1' })
  await liveBus.writePresence({
    startedAt: new Date().toISOString(),
    sessions: [{ sessionId: 'remote-live-1', workspace: '/ws/r', status: 'idle', kind: 'root' }],
  })
  await new PeerBus({ stateDir, logger }).enqueue({
    v: PROTOCOL_VERSION,
    messageId: 'remote-msg-1',
    createdAt: new Date().toISOString(),
    targetSessionId: 'remote-live-1',
    sender: { sessionId: 'sender-9', workspace: '/ws/x', hostId: 'h9', pid: 9 },
    wake: true,
    text: 'ping',
    meta: { hops: 0 },
  })

  await service.rescueOrphans()

  assert.deepEqual(resumeCalls, [], 'a live owner delivers its own mail')
})

test('a rejected re-splice of a queued message is not a delivery failure', async () => {
  const sender = makeAgent('sender-1')
  const recipient = makeAgent('recipient-1')
  recipient.steer = (message) => {
    const error = new Error(`message "${message.id}" is already pending`)
    throw error
  }
  const { service, stateDir } = await setup({
    agents: [sender, recipient],
    events: true,
    config: { ackWaitMs: 100 },
  })

  const result = await service.send({ target: 'recipient-1', text: 'again', agent: sender })
  assert.equal(result.delivery, 'queued', 'a queued message is not a failure')
  assert.equal(service.inflight.has(result.messageId), true, 'it stays tracked for confirmation')
  const { readdir } = await import('node:fs/promises')
  const failedNames = await readdir(join(stateDir, 'failed', 'recipient-1')).catch(() => [])
  assert.deepEqual(failedNames, [], 'a queued message must not produce a failed marker')
})

test('a resume that someone else owns leaves the message queued, not failed', async () => {
  const sender = makeAgent('sender-1')
  const { service, stateDir } = await setup({
    agents: [sender],
    resume: async () => {
      throw new Error('SessionAlreadyOwnedError: session is open in another dsh process')
    },
  })
  const deadBus = new PeerBus({ stateDir, logger, hostId: 'dead-host-1', now: () => Date.now() - 60_000 })
  await deadBus.writePresence({
    startedAt: new Date(Date.now() - 120_000).toISOString(),
    sessions: [{ sessionId: 'contest-1', workspace: '/ws/c', status: 'idle', kind: 'root' }],
  })

  const result = await service.send({ target: 'session:contest-1', text: 'hello?', agent: sender })
  assert.equal(result.ok, true)
  assert.equal(result.delivery, 'queued')
  const pending = await new PeerBus({ stateDir, logger }).listPending('contest-1')
  assert.equal(pending.length, 1, 'the envelope stays in the inbox for a later hand-off')
})

test('interject can be turned off to queue for the next turn', async () => {
  const sender = makeAgent('sender-1')
  const running = makeAgent('running-1', { status: 'running' })
  const { service } = await setup({ agents: [sender, running], config: { interject: false } })
  await service.send({ target: 'running-1', text: 'ping', agent: sender })
  assert.equal(running.calls.followup.length, 1)
  assert.equal(running.calls.steer.length, 0)
})
