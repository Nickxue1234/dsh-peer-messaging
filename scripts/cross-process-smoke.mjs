/**
 * End-to-end smoke test across two real OS processes.
 *
 *   node scripts/cross-process-smoke.mjs
 *
 * The parent process plays "sender" (its own dsh-like context with one agent),
 * the child process plays "host" and owns a different agent in a different
 * workspace. They share only a state directory, exactly like two dsh processes
 * that share one DSH_HOME.
 */
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { normalizeConfig } from '../lib/config.js'
import { PeerMessaging } from '../lib/service.js'
import { PeerBus } from '../lib/store.js'

const silent = { warn() {}, info() {}, error() {}, debug() {} }
const HOST_SESSION = 'remote-agent-1'
const SENDER_SESSION = 'sender-agent-1'

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function fakeAgent(id, workspace) {
  /** @type {any[]} */
  const received = []
  return {
    id,
    status: 'idle',
    received,
    session: { id, header: { id, cwd: workspace } },
    inject: (message) => received.push(message),
    steer: (message) => received.push(message),
    followup: (message) => received.push(message),
  }
}

function fakeCtx(agents) {
  return {
    agents: { list: () => agents, get: (id) => agents.find((agent) => agent.id === id) },
    logger: () => silent,
    get: (name) => (name === 'workingDirectory' ? { get: (session) => session?.header?.cwd } : undefined),
    emit: () => {},
  }
}

async function waitFor(check, { timeoutMs = 15_000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const value = await check()
    if (value) return value
    if (Date.now() >= deadline) return undefined
    await sleep(intervalMs)
  }
}

async function runHost(stateDir) {
  const agent = fakeAgent(HOST_SESSION, '/ws/remote-workspace')
  const service = new PeerMessaging(
    fakeCtx([agent]),
    normalizeConfig({ stateDir, pollMs: 50, heartbeatMs: 200, ackWaitMs: 5_000 }),
  )
  service.start()
  try {
    const message = await waitFor(() => agent.received[0])
    if (!message) {
      console.error('host: no message arrived')
      process.exitCode = 2
      return
    }
    console.log(
      JSON.stringify({
        ok: true,
        to: HOST_SESSION,
        from: message.source.senderSessionId,
        sourceKind: message.source.kind,
        text: message.content[1]?.text,
      }),
    )
  } finally {
    await service.stop()
  }
}

async function runParent() {
  const stateDir = await mkdtemp(join(tmpdir(), 'peer-smoke-'))
  const childPath = fileURLToPath(import.meta.url)
  const child = spawn(process.execPath, [childPath, 'host'], {
    env: { ...process.env, PEER_SMOKE_STATE: stateDir },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let childStdout = ''
  let childStderr = ''
  child.stdout.on('data', (chunk) => (childStdout += chunk))
  child.stderr.on('data', (chunk) => (childStderr += chunk))

  const sender = fakeAgent(SENDER_SESSION, '/ws/sender-workspace')
  const service = new PeerMessaging(
    fakeCtx([sender]),
    normalizeConfig({ stateDir, pollMs: 50, heartbeatMs: 200, ackWaitMs: 500 }),
  )
  service.start()

  try {
    const probe = new PeerBus({ stateDir, logger: silent, hostId: 'smoke-probe' })
    const discovered = await waitFor(async () => {
      const hosts = await probe.readHosts()
      return hosts.some((host) => host.sessions?.some((session) => session.sessionId === HOST_SESSION))
    })
    assert.ok(discovered, 'the host process never published its presence record')

    const agents = await service.listAgents()
    const hostAgent = agents.find((agent) => agent.sessionId === HOST_SESSION)
    assert.ok(hostAgent, 'peer_agents must see the agent of the other process')
    assert.equal(hostAgent.local, false)
    assert.equal(hostAgent.workspace, '/ws/remote-workspace')

    const result = await service.send({
      target: HOST_SESSION,
      text: 'hello from another process',
      agent: sender,
    })
    assert.equal(result.ok, true)
    assert.equal(result.target.sessionId, HOST_SESSION)

    const outcome = await service.bus.waitForOutcome(result.messageId, HOST_SESSION, { timeoutMs: 10_000 })
    assert.equal(outcome.state, 'delivered', 'the other process must report delivery')

    const exitCode = await new Promise((resolve) => child.once('exit', resolve))
    assert.equal(exitCode, 0, `host process failed:\n${childStderr}`)
    const report = JSON.parse(childStdout.trim().split('\n').pop())
    assert.equal(report.ok, true)
    assert.equal(report.from, SENDER_SESSION)
    assert.equal(report.sourceKind, 'peer-message')
    assert.equal(report.text, 'hello from another process')

    console.log(`smoke: ok — ${report.from} -> ${report.to}: ${JSON.stringify(report.text)}`)
  } finally {
    child.kill('SIGTERM')
    await service.stop()
    await rm(stateDir, { recursive: true, force: true })
  }
}

const stateDir = process.env.PEER_SMOKE_STATE
if (process.argv[2] === 'host') {
  assert.ok(stateDir, 'PEER_SMOKE_STATE is required for host mode')
  await runHost(stateDir)
} else {
  await runParent()
}
