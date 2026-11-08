/**
 * Tool definitions exposed to agents: discovery, sending and self-inspection.
 *
 * The tool schemas are written as raw JSON Schema on purpose — that is exactly
 * what `ctx.tools.register()` accepts — and they stay inside the schema subset
 * dsh enforces (single scalar `type`, no `type` arrays, no patterns/ranges).
 */
import { hostname } from 'node:os'

import { PeerMessagingError } from './service.js'

/** @param {unknown} value */
function renderJson(_args, value) {
  return [{ type: 'text', text: JSON.stringify(value, null, 2) }]
}

/**
 * Strip values that cannot survive `JSON.stringify` losslessly. dsh's tool
 * runtime rejects a result that does not round-trip ("value is not lossless
 * JSON"), so `undefined` fields and non-finite numbers are pruned here rather
 * than failing the whole call after the side effect already happened.
 *
 * @param {unknown} value
 * @returns {unknown}
 */
function lossless(value) {
  if (value === null) return null
  const type = typeof value
  if (type === 'string' || type === 'boolean') return value
  if (type === 'number') return Number.isFinite(value) ? value : undefined
  if (type === 'bigint') return String(value)
  if (Array.isArray(value)) {
    return Array.from(value, (item) => {
      const clean = lossless(item)
      return clean === undefined ? null : clean
    })
  }
  if (type === 'object') {
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? undefined : value.toISOString()
    const out = {}
    for (const [key, item] of Object.entries(value)) {
      const clean = lossless(item)
      if (clean !== undefined) out[key] = clean
    }
    return out
  }
  return undefined
}

const AGENT_SCHEMA = {
  type: 'object',
  properties: {
    sessionId: { type: 'string', description: 'Session id of the peer agent.' },
    title: { type: 'string', description: 'Display title of that session, when it has one.' },
    workspace: { type: 'string', description: 'Absolute working directory of the peer agent.' },
    workspaceTitle: { type: 'string', description: 'Display title of that workspace.' },
    status: { type: 'string', description: 'idle, running, closed (persisted session nobody has open) or unknown.' },
    kind: { type: 'string', description: 'root or subagent.' },
    hostId: { type: 'string', description: 'Identity of the dsh process hosting the agent.' },
    hostname: { type: 'string', description: 'Machine name of that process.' },
    live: { type: 'boolean', description: 'False for a closed session: it has a log but no running agent yet.' },
    local: { type: 'boolean', description: 'True when the agent lives in this dsh process.' },
    stale: { type: 'boolean', description: 'True when the presence heartbeat is older than the TTL.' },
    updatedAt: { type: 'string', description: 'Timestamp of the presence record that reported the agent.' },
    self: { type: 'boolean', description: 'True for the calling agent itself.' },
  },
  required: ['sessionId', 'workspace', 'workspaceTitle', 'status', 'kind', 'local', 'self'],
}

const SEND_RESULT_SCHEMA = {
  type: 'object',
  properties: {
    ok: { type: 'boolean', description: 'False when the message could not be enqueued.' },
    code: { type: 'string', description: 'Machine-readable error code when ok is false.' },
    error: { type: 'string', description: 'Human-readable error when ok is false.' },
    messageId: { type: 'string', description: 'Id of the enqueued message.' },
    delivery: {
      type: 'string',
      description:
        'delivered (the recipient session logged it), injected (wake:false — parked in the context, ' +
        'not confirmed and may wait for the recipient to run again), queued or failed.',
    },
    queueState: {
      type: 'string',
      description:
        'For same-process targets still unconfirmed: queued (waiting in the recipient inbox), ' +
        'handed-off (delivered to the agent, waiting for its next turn to record it) or unknown.',
    },
    waitedMs: { type: 'number', description: 'Milliseconds spent waiting for the delivery ack.' },
    candidates: {
      type: 'array',
      description: 'Candidate targets when the address was ambiguous or unknown.',
      items: { type: 'object', properties: AGENT_SCHEMA.properties },
    },
    target: {
      type: 'object',
      description: 'The resolved recipient.',
      properties: {
        sessionId: { type: 'string' },
        workspace: { type: 'string' },
        workspaceTitle: { type: 'string' },
        status: { type: 'string' },
        live: { type: 'boolean', description: 'True once the recipient agent is running.' },
        hostId: { type: 'string' },
        local: { type: 'boolean' },
      },
      required: ['sessionId'],
    },
  },
  required: ['ok'],
}

const AGENTS_RESULT_SCHEMA = {
  type: 'object',
  properties: {
    ok: { type: 'boolean' },
    count: { type: 'number', description: 'Number of agents returned.' },
    stateDir: { type: 'string', description: 'Shared peer-messaging state directory.' },
    agents: { type: 'array', items: AGENT_SCHEMA },
  },
  required: ['ok', 'count', 'agents'],
}

const WHOAMI_RESULT_SCHEMA = {
  type: 'object',
  properties: {
    ok: { type: 'boolean' },
    sessionId: { type: 'string' },
    workspace: { type: 'string' },
    workspaceTitle: { type: 'string' },
    kind: { type: 'string' },
    status: { type: 'string' },
    hostId: { type: 'string' },
    hostname: { type: 'string' },
    pid: { type: 'number' },
    stateDir: { type: 'string' },
    inboxPending: { type: 'number', description: 'Messages still waiting for this agent.' },
    reachableAgents: { type: 'number', description: 'Agents reachable from this process, including itself.' },
  },
  required: ['ok', 'sessionId', 'workspace', 'hostId'],
}

const TARGET_DESCRIPTION =
  'Recipient address. Accepts a full session id, a session id prefix of at least 8 characters, ' +
  'a workspace path or its last directory name, or a workspace title. Prefix with "session:" or ' +
  '"workspace:" to restrict the match. Call peer_agents first when unsure.'

/**
 * @param {any} ctx
 * @param {import('./service.js').PeerMessaging} service
 * @returns {Array<() => void>}
 */
export function registerPeerTools(ctx, service) {
  /** @type {Array<() => void>} */
  const disposers = []

  /**
   * Register one tool definition, guaranteeing a lossless-JSON result.
   * @param {any} definition
   */
  const register = (definition) => {
    const { execute } = definition
    disposers.push(
      ctx.tools.register({
        ...definition,
        execute: async (args, exec) => lossless(await execute(args, exec)),
      }),
    )
  }

  register({
      name: 'peer_agents',
      description:
        'List agents that can receive peer messages: every live agent in this dsh process, grouped by ' +
        'workspace, plus agents reported by other dsh processes that share the same peer-messaging state ' +
        'directory, plus closed sessions (status "closed") that peer_send can wake on demand. Use the ' +
        'returned sessionId or workspace as the target of peer_send.',
      parameters: {
        type: 'object',
        properties: {
          workspace: {
            type: 'string',
            description: 'Only return agents whose workspace path or title contains this text.',
          },
          includeSubagents: {
            type: 'boolean',
            description: 'Include subagent sessions. Defaults to the plugin configuration.',
          },
          includeStale: {
            type: 'boolean',
            description: 'Include agents whose presence heartbeat is older than the TTL (probably dead processes).',
          },
          includeCold: {
            type: 'boolean',
            description:
              'Include closed sessions that no dsh process has open. Defaults to the plugin configuration; ' +
              'pass false to list only agents that are already running.',
          },
        },
        additionalProperties: false,
      },
      output: { schema: AGENTS_RESULT_SCHEMA, render: renderJson },
      execute: async (args, exec) => {
        const agents = await service.listAgents({
          workspace: typeof args?.workspace === 'string' ? args.workspace : undefined,
          includeSubagents: typeof args?.includeSubagents === 'boolean' ? args.includeSubagents : undefined,
          includeStale: args?.includeStale === true,
          includeCold: typeof args?.includeCold === 'boolean' ? args.includeCold : undefined,
        })
        const self = exec?.agent ? String(exec.agent.id) : ''
        return {
          ok: true,
          count: agents.length,
          stateDir: service.config.stateDir,
          agents: agents.map((entry) => ({
            sessionId: entry.sessionId,
            title: entry.title ?? '',
            workspace: entry.workspace ?? '',
            workspaceTitle: entry.workspaceTitle ?? '',
            status: entry.status ?? 'unknown',
            kind: entry.kind ?? 'root',
            live: entry.live !== false,
            hostId: entry.hostId ?? '',
            hostname: entry.hostname ?? '',
            local: Boolean(entry.local),
            stale: Boolean(entry.stale),
            updatedAt: entry.updatedAt ?? '',
            self: self !== '' && entry.sessionId === self,
          })),
        }
      },
    })

  register({
      name: 'peer_send',
      description:
        'Send a message to another agent, including an agent working in a different workspace. ' +
        'The recipient receives the text as a user message attributed to the sending session. ' +
        'By default the recipient reacts right away: an idle agent starts a new turn, and a running agent ' +
        'takes the message at its next step inside the current turn (dsh interjection/steer semantics) ' +
        'rather than waiting for the turn to end. A hand-off is only reported as delivered once the ' +
        'recipient session actually logs it; if ' +
        'the recipient discards the queued message (stopped turn or archived session) it is requeued and ' +
        'handed off again. A closed session (no window open anywhere) is resumed first, so the recipient ' +
        'does not have to be open for it to receive mail. Pass wake:false to only inject it into the ' +
        'conversation without waking anyone.',
      parameters: {
        type: 'object',
        properties: {
          target: {
            type: 'string',
            description: TARGET_DESCRIPTION,
          },
          message: {
            type: 'string',
            description: 'Message body. Keep it self-contained: the recipient cannot see your conversation.',
          },
          wake: {
            type: 'boolean',
            description: 'Wake an idle recipient and start a new turn. Defaults to true.',
          },
        },
        required: ['target', 'message'],
        additionalProperties: false,
      },
      output: { schema: SEND_RESULT_SCHEMA, render: renderJson },
      execute: async (args, exec) => {
        const agent = exec?.agent
        if (!agent) {
          return { ok: false, code: 'no_agent', error: 'peer_send must be called from an agent session' }
        }
        try {
          const result = await service.send({
            target: String(args?.target ?? ''),
            text: String(args?.message ?? ''),
            wake: args?.wake !== false,
            agent,
          })
          return result
        } catch (error) {
          if (error instanceof PeerMessagingError) {
            return {
              ok: false,
              code: error.code,
              error: error.message,
              candidates: Array.isArray(error.candidates) ? error.candidates : undefined,
            }
          }
          return { ok: false, code: 'internal', error: error instanceof Error ? error.message : String(error) }
        }
      },
    })

  register({
      name: 'peer_whoami',
      description:
        'Describe the calling agent as a peer-messaging participant: its session id, workspace, host id, ' +
        'shared state directory and how many messages are still waiting for it. Use this id when asking ' +
        'another agent to reply.',
      parameters: { type: 'object', properties: {}, additionalProperties: false },
      output: { schema: WHOAMI_RESULT_SCHEMA, render: renderJson },
      execute: async (_args, exec) => {
        const agent = exec?.agent
        if (!agent) {
          return { ok: false, code: 'no_agent', error: 'peer_whoami must be called from an agent session' }
        }
        const descriptor = service.describeAgent(agent) ?? {
          sessionId: String(agent.id),
          workspace: '',
          workspaceTitle: '',
          kind: 'root',
          status: 'unknown',
        }
        const pending = await service.bus.listPending(String(agent.id)).catch(() => [])
        const peers = await service.listAgents({ includeStale: true, includeSubagents: true }).catch(() => [])
        return {
          ok: true,
          sessionId: descriptor.sessionId,
          workspace: descriptor.workspace,
          workspaceTitle: descriptor.workspaceTitle,
          kind: descriptor.kind,
          status: descriptor.status,
          hostId: service.hostId,
          hostname: hostname(),
          pid: process.pid,
          stateDir: service.config.stateDir,
          inboxPending: pending.length,
          reachableAgents: peers.length,
        }
      },
    })

  return disposers
}
