import assert from 'node:assert/strict'
import { test } from 'node:test'

import { PeerMessagingError } from '../lib/service.js'
import { registerPeerTools } from '../lib/tools.js'

const ALLOWED_SCHEMA_KEYS = new Set([
  'type',
  'properties',
  'required',
  'additionalProperties',
  'items',
  'enum',
  'const',
  'oneOf',
  'description',
  'title',
  'default',
  'examples',
])
const SCALAR_TYPES = new Set(['object', 'array', 'string', 'number', 'integer', 'boolean', 'null'])

/** Assert a schema stays inside the subset dsh enforces. */
function assertSupportedSchema(schema, path = 'schema') {
  assert.equal(typeof schema, 'object', `${path} must be an object`)
  assert.ok(!Array.isArray(schema), `${path} must not be an array`)
  for (const key of Object.keys(schema)) {
    assert.ok(ALLOWED_SCHEMA_KEYS.has(key), `${path} uses unsupported keyword ${key}`)
  }
  if ('type' in schema) {
    assert.equal(typeof schema.type, 'string', `${path}.type must be a single string`)
    assert.ok(SCALAR_TYPES.has(schema.type), `${path}.type ${schema.type} is unknown`)
  }
  if (schema.properties) {
    assert.equal(schema.type, 'object', `${path}.properties requires type object`)
    for (const [name, child] of Object.entries(schema.properties)) assertSupportedSchema(child, `${path}.properties.${name}`)
  }
  if (schema.required) {
    assert.ok(Array.isArray(schema.required) && schema.required.length > 0, `${path}.required must be a non-empty array`)
    for (const name of schema.required) assert.ok(schema.properties?.[name], `${path}.required names unknown property ${name}`)
  }
  if (schema.items) assertSupportedSchema(schema.items, `${path}.items`)
  if (schema.oneOf) {
    assert.ok(Array.isArray(schema.oneOf) && schema.oneOf.length >= 2, `${path}.oneOf needs two branches`)
    schema.oneOf.forEach((branch, index) => assertSupportedSchema(branch, `${path}.oneOf[${index}]`))
  }
}

function makeLogger() {
  return { warn() {}, info() {}, error() {}, debug() {} }
}

function makeService(overrides = {}) {
  const service = {
    config: { stateDir: '/tmp/peer-state', includeSubagentsInList: false },
    hostId: 'host-under-test',
    describeAgent: (agent) => ({
      sessionId: String(agent.id),
      workspace: '/ws/alpha',
      workspaceTitle: 'alpha',
      kind: 'root',
      status: 'idle',
    }),
    listAgents: async () => [
      {
        sessionId: 'agent-a',
        workspace: '/ws/alpha',
        workspaceTitle: 'alpha',
        status: 'idle',
        kind: 'root',
        hostId: 'host-under-test',
        hostname: 'box',
        local: true,
        stale: false,
        updatedAt: '2026-01-01T00:00:00.000Z',
      },
      {
        sessionId: 'agent-b',
        workspace: '/ws/beta',
        workspaceTitle: 'beta',
        status: 'running',
        kind: 'root',
        hostId: 'host-remote',
        hostname: 'other',
        local: false,
        stale: false,
        updatedAt: '2026-01-01T00:00:01.000Z',
      },
    ],
    bus: { listPending: async () => [{}, {}] },
    send: async (input) => ({
      ok: true,
      messageId: 'msg-1',
      delivery: 'delivered',
      waitedMs: 12,
      target: { sessionId: 'agent-b', workspace: '/ws/beta', workspaceTitle: 'beta', status: 'running', local: false },
      echo: input.text,
    }),
    ...overrides,
  }
  return service
}

/** @returns {{ctx: any, tools: Map<string, any>}} */
function makeCtx() {
  /** @type {Map<string, any>} */
  const tools = new Map()
  const ctx = {
    tools: {
      register: (definition) => {
        tools.set(definition.name, definition)
        return () => tools.delete(definition.name)
      },
    },
    logger: () => makeLogger(),
    agents: { list: () => [], get: () => undefined },
    get: () => undefined,
    emit: () => {},
  }
  return { ctx, tools }
}

test('registers the three peer tools with subset-compatible schemas', () => {
  const { ctx, tools } = makeCtx()
  const disposers = registerPeerTools(ctx, makeService())
  assert.deepEqual([...tools.keys()], ['peer_agents', 'peer_send', 'peer_whoami'])
  assert.equal(disposers.length, 3)

  for (const [name, definition] of tools) {
    assert.equal(typeof definition.description, 'string')
    assert.ok(definition.description.length > 20, `${name} needs a useful description`)
    assert.equal(definition.parameters.type, 'object', `${name} parameters must have an object root`)
    assertSupportedSchema(definition.parameters, `${name}.parameters`)
    assert.equal(typeof definition.output.render, 'function')
    assertSupportedSchema(definition.output.schema, `${name}.output.schema`)
  }

  assert.deepEqual(tools.get('peer_send').parameters.required, ['target', 'message'])
  assert.deepEqual(tools.get('peer_agents').parameters.required ?? [], [])

  for (const dispose of disposers) dispose()
  assert.equal(tools.size, 0)
})

test('peer_agents renders the merged list and marks the caller', async () => {
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, makeService())
  const definition = tools.get('peer_agents')
  const caller = { id: 'agent-a', session: { id: 'agent-a', header: { cwd: '/ws/alpha' } } }

  const value = await definition.execute({}, { agent: caller })
  assert.equal(value.ok, true)
  assert.equal(value.count, 2)
  assert.equal(value.agents[0].self, true)
  assert.equal(value.agents[1].self, false)
  assert.equal(value.agents[1].local, false)

  const [rendered] = definition.output.render({}, value)
  assert.equal(rendered.type, 'text')
  assert.match(rendered.text, /"sessionId": "agent-a"/)
})

test('peer_agents forwards filters to the service', async () => {
  const seen = []
  const service = makeService({
    listAgents: async (options) => {
      seen.push(options)
      return []
    },
  })
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, service)
  const value = await tools.get('peer_agents').execute(
    { workspace: 'beta', includeSubagents: true, includeStale: true, includeCold: false },
    {},
  )
  assert.deepEqual(seen, [{ workspace: 'beta', includeSubagents: true, includeStale: true, includeCold: false }])
  assert.equal(value.count, 0)
})

test('peer_send passes the message through and reports delivery', async () => {
  let received
  const service = makeService({
    send: async (input) => {
      received = input
      return { ok: true, messageId: 'msg-9', delivery: 'delivered', waitedMs: 3, target: { sessionId: 'agent-b' } }
    },
  })
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, service)
  const caller = { id: 'agent-a', session: { id: 'agent-a', header: { cwd: '/ws/alpha' } } }

  const value = await tools.get('peer_send').execute({ target: 'agent-b', message: 'hi', wake: false }, { agent: caller })
  assert.equal(value.ok, true)
  assert.equal(value.delivery, 'delivered')
  assert.equal(received.target, 'agent-b')
  assert.equal(received.text, 'hi')
  assert.equal(received.wake, false)
  assert.equal(received.agent, caller)
})

test('peer_send results survive the lossless JSON round-trip dsh enforces', async () => {
  const service = makeService({
    send: async () => ({
      ok: true,
      messageId: 'msg-10',
      delivery: 'delivered',
      waitedMs: 0,
      error: undefined,
      candidates: undefined,
      target: { sessionId: 'agent-b', workspace: undefined, local: true },
    }),
  })
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, service)
  const caller = { id: 'agent-a', session: { id: 'agent-a', header: { cwd: '/ws/alpha' } } }

  const value = await tools.get('peer_send').execute({ target: 'agent-b', message: 'hi' }, { agent: caller })
  assert.deepEqual(JSON.parse(JSON.stringify(value)), value)
  assert.ok(!('error' in value), 'undefined error field must be pruned')
  assert.ok(!('workspace' in value.target), 'undefined nested field must be pruned')
})

test('tool results prune non-finite numbers and dense arrays', async () => {
  const service = makeService({
    listAgents: async () => [{ sessionId: 'agent-a', workspace: '/ws/alpha', stale: false }],
  })
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, service)
  const value = await tools.get('peer_whoami').execute({}, { agent: { id: 'agent-a', session: { id: 'agent-a', header: {} } } })
  assert.deepEqual(JSON.parse(JSON.stringify(value)), value)
})

test('peer_send without an agent context is rejected cleanly', async () => {
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, makeService())
  const value = await tools.get('peer_send').execute({ target: 'x', message: 'y' }, {})
  assert.equal(value.ok, false)
  assert.equal(value.code, 'no_agent')
})

test('peer_send surfaces addressing errors and candidates', async () => {
  const service = makeService({
    send: async () => {
      throw new PeerMessagingError('ambiguous', 'matches 2 agents', { candidates: [{ sessionId: 'a' }, { sessionId: 'b' }] })
    },
  })
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, service)
  const caller = { id: 'agent-a', session: { id: 'agent-a', header: { cwd: '/ws/alpha' } } }
  const value = await tools.get('peer_send').execute({ target: '/ws/shared', message: 'hey' }, { agent: caller })
  assert.equal(value.ok, false)
  assert.equal(value.code, 'ambiguous')
  assert.match(value.error, /matches 2 agents/)
  assert.equal(value.candidates.length, 2)
})

test('peer_whoami reports identity, inbox depth and reachability', async () => {
  const { ctx, tools } = makeCtx()
  registerPeerTools(ctx, makeService())
  const caller = { id: 'agent-a', session: { id: 'agent-a', header: { cwd: '/ws/alpha' } } }
  const value = await tools.get('peer_whoami').execute({}, { agent: caller })
  assert.equal(value.ok, true)
  assert.equal(value.sessionId, 'agent-a')
  assert.equal(value.workspace, '/ws/alpha')
  assert.equal(value.inboxPending, 2)
  assert.equal(value.reachableAgents, 2)
  assert.equal(value.hostId, 'host-under-test')
  assert.equal(value.stateDir, '/tmp/peer-state')

  const missing = await tools.get('peer_whoami').execute({}, {})
  assert.equal(missing.ok, false)
  assert.equal(missing.code, 'no_agent')
})
