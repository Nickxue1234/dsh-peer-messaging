/**
 * Delivers peer messages to live agents and keeps this process's presence
 * record up to date.
 *
 * One instance exists per dsh process. Every live agent in the process shares
 * it, so agents in different workspaces (each workspace is a separate session
 * tree in the same process) can address each other. Agents in *other* dsh
 * processes are reached through the filesystem bus (see ./store.js) as long as
 * those processes point at the same `stateDir`.
 */
import { randomUUID } from 'node:crypto'
import { hostname } from 'node:os'
import { basename } from 'node:path'

import { PeerBus, PROTOCOL_VERSION, makeHostId } from './store.js'

/** Minimum prefix length accepted when addressing a session id by prefix. */
const MIN_SESSION_PREFIX = 8

/** Interval between housekeeping sweeps. */
const CLEANUP_EVERY_MS = 10 * 60 * 1_000

/**
 * How long a handed-off message may wait for the recipient's session log to
 * prove it was consumed before the hand-off is checked and, if the message
 * vanished from the recipient inbox, redelivered.
 */
export const CONFIRM_TIMEOUT_MS = 10_000

/** How many times one lost message is handed off again before giving up. */
export const MAX_REDELIVERIES = 3

/** Upper bound on how long a message may sit queued in the recipient inbox. */
export const MAX_QUEUE_WAIT_MS = 30 * 60 * 1_000

/** One orphan-inbox rescue scan every this many polls (500 ms default → ~2 s). */
const RESCUE_EVERY_TICKS = 4

export class PeerMessagingError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {{candidates?: unknown[]}} [details]
   */
  constructor(code, message, details = {}) {
    super(message)
    this.name = 'PeerMessagingError'
    this.code = code
    if (details.candidates) this.candidates = details.candidates
  }
}

/** @param {string} text */
function textBlock(text) {
  return { type: 'text', text }
}

/**
 * @param {unknown} error
 */
function errorMessage(error) {
  if (error instanceof Error) return error.message
  return String(error)
}

/**
 * dsh rejects a second splice of a message id that is still queued. That is the
 * queue telling us the message is alive, not a delivery failure.
 * @param {unknown} error
 */
function isAlreadyPending(error) {
  return /is already pending/i.test(errorMessage(error))
}

/** @param {unknown} value */
function isRecord(value) {
  return typeof value === 'object' && value !== null
}

export class PeerMessaging {
  /**
   * @param {import('./tools.js').PeerToolHost} ctx
   * @param {import('./config.js').PeerMessagingConfig} config
   */
  constructor(ctx, config) {
    this.ctx = ctx
    this.config = config
    this.logger = ctx.logger('peer-messaging')
    this.hostId = makeHostId()
    this.bus = new PeerBus({
      stateDir: config.stateDir,
      logger: this.logger,
      hostId: this.hostId,
    })
    this.startedAt = new Date().toISOString()
    this.ready = Promise.resolve()
    this.pollTimer = null
    this.heartbeatTimer = null
    this.cleanupTimer = null
    this.stopped = true
    /** @type {Set<string>} */
    this.delivering = new Set()
    /**
     * Hand-offs whose consumption is not yet proven by the recipient session
     * log, keyed by message id. See {@link PeerMessaging.checkHandoff}.
     * @type {Map<string, {messageId: string, sessionId: string, fileName: string, envelope: any, attempts: number, firstHandoffAt: number, claimedPath: string|null, timer: ReturnType<typeof setTimeout>|null}>}
     */
    this.inflight = new Map()
    /** @type {Array<() => void>} */
    this.offEvents = []
    this.canConfirm = false
    /** @type {{at: number, rows: any[]}|null} Catalogue of closed sessions, rebuilt at most every `coldListTtlMs`. */
    this.coldCache = null
    /** @type {Map<string, Promise<any>>} In-flight resumes, so two sends wake one agent. */
    this.resuming = new Map()
    /** @type {Map<string, any>} Handles we own from resuming, so a woken session stays loaded. */
    this.resumedHandles = new Map()
    /** @type {Map<string, Array<{at: number}>>} Recent sends per session pair: the flood guard. */
    this.pairSends = new Map()
    this.lastPairPruneAt = 0
    /** @type {Map<string, number>} Last rescue attempt per session, so a wedged session cannot spin resumes. */
    this.rescueAttempts = new Map()
    this.pollTicks = 0
  }

  /** Begin polling for mail and publishing presence. */
  start() {
    if (!this.stopped) return
    this.stopped = false
    this.listen()
    this.ready = this.bus
      .ready()
      .then(() => this.beat())
      .catch((error) => {
        this.logger.error('peer-messaging: failed to initialize state dir %C: %s', this.config.stateDir, errorMessage(error))
      })
    this.pollTimer = setInterval(() => void this.poll(), this.config.pollMs)
    this.pollTimer.unref?.()
    this.heartbeatTimer = setInterval(() => void this.beat(), this.config.heartbeatMs)
    this.heartbeatTimer.unref?.()
    this.cleanupTimer = setInterval(() => void this.sweep(), CLEANUP_EVERY_MS)
    this.cleanupTimer.unref?.()
    void this.poll()
  }

  /** Stop polling and remove this process's presence record. */
  async stop() {
    if (this.stopped) return
    this.stopped = true
    for (const timer of [this.pollTimer, this.heartbeatTimer, this.cleanupTimer]) {
      if (timer) clearInterval(timer)
    }
    this.pollTimer = this.heartbeatTimer = this.cleanupTimer = null
    for (const off of this.offEvents.splice(0)) {
      try {
        off?.()
      } catch {
        // Listener teardown is best-effort.
      }
    }
    for (const record of this.inflight.values()) {
      if (record.timer) clearTimeout(record.timer)
      record.timer = null
    }
    this.inflight.clear()
    // A handle we own is a capability: leaving it behind keeps the resumed
    // agent alive for the rest of the process, so release it with the plugin.
    const handles = [...this.resumedHandles.values()]
    this.resumedHandles.clear()
    for (const handle of handles) {
      try {
        await handle?.dispose?.()
      } catch (error) {
        this.logger.warn('peer-messaging: cannot dispose a resumed agent: %s', errorMessage(error))
      }
    }
    await this.ready.catch(() => {})
    await this.bus.removePresence().catch(() => {})
  }

  /**
   * Watch the session log and the inbox for proof that a hand-off was
   * consumed, or that the recipient dropped it before consuming it.
   */
  listen() {
    const on = this.ctx.on?.bind(this.ctx)
    if (typeof on !== 'function') return
    this.canConfirm = true
    this.offEvents.push(on('session/event', (session, event) => this.onSessionEvent(session, event)))
    this.offEvents.push(on('agent/inbox/discarded', (payload) => this.onInboxDiscarded(payload)))
    this.offEvents.push(on('agent/disposed', (payload) => this.onAgentDisposed(payload)))
  }

  /**
   * Someone else can tear a resumed agent down; drop the handle we were holding
   * so the plugin unload never disposes it twice.
   * @param {{agent?: {id?: unknown}}} payload
   */
  onAgentDisposed(payload) {
    const sessionId = payload?.agent?.id
    if (sessionId === undefined || sessionId === null) return
    this.resumedHandles.delete(String(sessionId))
  }

  /**
   * A message handed to an agent only counts as delivered once the recipient
   * session logs it as an ordinary user message; that append happens when the
   * turn actually consumes it.
   * @param {{id?: unknown}} session
   * @param {{type?: string, data?: {id?: unknown}}} event
   */
  onSessionEvent(session, event) {
    if (event?.type !== 'user/message') return
    const messageId = event?.data?.id
    if (typeof messageId !== 'string') return
    const record = this.inflight.get(messageId)
    if (!record || record.sessionId !== String(session?.id)) return
    void this.settle(record)
  }

  /**
   * A stopped or archived turn clears its inbox, dropping queued input. Peer
   * messages are put back on the bus and handed off again instead of being
   * lost with the turn.
   * @param {{agent?: {id?: unknown}, message?: {id?: unknown}}} payload
   */
  onInboxDiscarded(payload) {
    const messageId = payload?.message?.id
    if (typeof messageId !== 'string') return
    const record = this.inflight.get(messageId)
    if (!record || record.sessionId !== String(payload?.agent?.id)) return
    const reason = new Error('the recipient discarded the message before its turn consumed it')
    void this.redeliver(record, reason)
  }

  /**
   * Record one delivered message once its recipient session logged it.
   * @param {{messageId: string, sessionId: string, fileName: string, claimedPath: string|null, envelope: any, timer: ReturnType<typeof setTimeout>|null}} record
   */
  async settle(record) {
    if (this.inflight.get(record.messageId) !== record) return
    if (record.timer) {
      clearTimeout(record.timer)
      record.timer = null
    }
    this.inflight.delete(record.messageId)
    if (record.claimedPath) {
      await this.bus.complete(record.claimedPath, record.sessionId, record.fileName).catch((error) => {
        this.logger.warn('peer-messaging: cannot record delivery of %C: %s', record.messageId, errorMessage(error))
      })
    }
    this.emit('peer-messaging/delivered', {
      messageId: record.messageId,
      to: record.sessionId,
      from: record.envelope?.sender?.sessionId,
    })
  }

  /**
   * Put a lost message back on the bus and hand it off again, giving up after
   * {@link MAX_REDELIVERIES}.
   * @param {{messageId: string, sessionId: string, fileName: string, claimedPath: string|null, envelope: any, attempts: number, timer: ReturnType<typeof setTimeout>|null}} record
   * @param {unknown} reason
   */
  async redeliver(record, reason) {
    if (this.inflight.get(record.messageId) !== record) return
    if (record.timer) {
      clearTimeout(record.timer)
      record.timer = null
    }
    if (record.attempts > MAX_REDELIVERIES) {
      this.inflight.delete(record.messageId)
      if (record.claimedPath) {
        await this.bus.fail(record.claimedPath, record.sessionId, record.fileName, record.envelope, reason).catch(() => {})
      }
      this.logger.warn('peer-messaging: giving up on %C after %d hand-offs: %s', record.messageId, record.attempts, errorMessage(reason))
      return
    }
    const claimedPath = record.claimedPath
    record.claimedPath = null
    if (claimedPath) {
      await this.bus.requeue(claimedPath, record.sessionId, record.fileName).catch((error) => {
        this.logger.warn('peer-messaging: cannot requeue %C: %s', record.messageId, errorMessage(error))
      })
    }
    this.logger.warn('peer-messaging: %C was not consumed (%s); hand-off %d of %d', record.messageId, errorMessage(reason), record.attempts, MAX_REDELIVERIES)
    const agent = this.ctx.agents.get?.(record.sessionId)
    if (agent) await this.deliverFor(agent).catch(() => {})
  }

  /**
   * @param {{messageId: string, timer: ReturnType<typeof setTimeout>|null}} record
   */
  scheduleConfirm(record) {
    if (record.timer) clearTimeout(record.timer)
    record.timer = setTimeout(() => void this.checkHandoff(record), CONFIRM_TIMEOUT_MS)
    record.timer.unref?.()
  }

  /**
   * Decide whether an unconfirmed hand-off is still queued inside the
   * recipient's inbox (keep waiting) or was dropped without being consumed
   * (redeliver).
   * @param {any} record
   */
  async checkHandoff(record) {
    if (this.inflight.get(record.messageId) !== record) return
    record.timer = null
    const agent = this.ctx.agents.get?.(record.sessionId)
    if (!agent) {
      // The recipient went away while the message was claimed: release it so a
      // later attach (or another process) can deliver it.
      this.inflight.delete(record.messageId)
      if (record.claimedPath) {
        await this.bus.requeue(record.claimedPath, record.sessionId, record.fileName).catch(() => {})
      }
      return
    }
    const inbox = agent.inbox
    const queued = [...(inbox?.nextStep ?? []), ...(inbox?.nextTurn ?? [])].some((message) => message?.id === record.messageId)
    if (queued && Date.now() - record.firstHandoffAt < MAX_QUEUE_WAIT_MS) {
      this.scheduleConfirm(record)
      return
    }
    const reason = queued
      ? new Error('the recipient never consumed the queued message')
      : new Error('the message disappeared from the recipient inbox without being consumed')
    await this.redeliver(record, reason)
  }

  /** Publish this process's presence record. */
  async beat() {
    if (this.stopped) return
    try {
      const sessions = []
      for (const agent of this.ctx.agents.list()) {
        const descriptor = this.describeAgent(agent)
        if (descriptor) sessions.push(descriptor)
      }
      await this.bus.writePresence({ startedAt: this.startedAt, sessions })
    } catch (error) {
      this.logger.warn('peer-messaging: presence heartbeat failed: %s', errorMessage(error))
    }
  }

  /** Scan every local agent for deliverable mail. */
  async poll() {
    if (this.stopped) return
    let agents
    try {
      agents = this.ctx.agents.list()
    } catch (error) {
      this.logger.warn('peer-messaging: cannot list agents: %s', errorMessage(error))
      return
    }
    await Promise.all(agents.map((agent) => this.deliverFor(agent).catch(() => {})))
    this.pollTicks += 1
    if (this.config.rescueOrphans && this.pollTicks % RESCUE_EVERY_TICKS === 0) {
      await this.rescueOrphans().catch(() => {})
    }
  }

  /**
   * Mail can be addressed to a session that no process has open: the sender
   * only enqueues it, or the host that was supposed to hold the Agent died
   * between heartbeat and delivery. Nothing polls such a session, so without
   * this pass the envelope sits in the inbox until a human opens that
   * conversation in the app — the "it only starts working when I click it"
   * symptom. Here every poller looks at the inbox side and resumes the owner
   * on demand, so mail self-heals instead of waiting for a window.
   */
  async rescueOrphans() {
    if (this.stopped || !this.config.resumeColdTargets) return
    let waiting
    try {
      waiting = await this.bus.pendingSessions()
    } catch (error) {
      this.logger.warn('peer-messaging: cannot scan inboxes for orphans: %s', errorMessage(error))
      return
    }
    if (waiting.length === 0) return
    const now = Date.now()
    const archived = this.archivedIds()
    // A live process beats every `heartbeatMs`, so a record that missed three
    // beats is dead even if `presenceTtlMs` has not expired yet. Using the
    // tighter window here only shortens how long mail waits; a wrong call is
    // harmless because the session write lease refuses a second owner.
    const freshMs = Math.min(this.config.presenceTtlMs, this.config.heartbeatMs * 3)
    // Sessions some *live* process has published in the last TTL: that process
    // owns the Agent and will deliver on its own.
    const liveElsewhere = new Set()
    try {
      for (const host of await this.bus.readHosts()) {
        if (host.hostId === this.hostId) continue
        const at = Date.parse(host.updatedAt)
        if (!Number.isFinite(at) || now - at > freshMs) continue
        for (const session of host.sessions ?? []) {
          if (isRecord(session) && typeof session.sessionId === 'string') liveElsewhere.add(session.sessionId)
        }
      }
    } catch {
      // Presence is advisory here; a read failure just means we rescue more.
    }
    for (const entry of waiting) {
      const sessionId = String(entry.sessionId)
      if (!sessionId || archived.has(sessionId)) continue
      if (entry.oldestAgeMs < this.config.rescueGraceMs) continue
      const local = this.ctx.agents.get?.(sessionId)
      if (local) {
        await this.deliverFor(local).catch(() => {})
        continue
      }
      if (liveElsewhere.has(sessionId)) continue
      const last = this.rescueAttempts.get(sessionId) ?? 0
      if (now - last < this.config.rescueBackoffMs) continue
      this.rescueAttempts.set(sessionId, now)
      try {
        const agent = await this.ensureLive({ sessionId, live: false })
        this.logger.info?.('peer-messaging: resumed %C to deliver waiting mail', sessionId)
        await this.deliverFor(agent).catch(() => {})
      } catch (error) {
        this.logger.debug?.('peer-messaging: cannot rescue %C: %s', sessionId, errorMessage(error))
      }
    }
  }

  /** Drop expired markers and stale presence files. */
  async sweep() {
    if (this.stopped) return
    try {
      await this.bus.cleanup({ retentionMs: this.config.retentionMs, presenceTtlMs: this.config.presenceTtlMs })
    } catch (error) {
      this.logger.warn('peer-messaging: housekeeping failed: %s', errorMessage(error))
    }
  }

  // ---------------------------------------------------------------------------
  // Presence / agent discovery
  // ---------------------------------------------------------------------------

  /**
   * @param {import('@deepseek-ai/dsh-agent').Agent} agent
   * @returns {{sessionId: string, workspace: string, workspaceTitle: string, status: string, kind: 'root'|'subagent'}|null}
   */
  describeAgent(agent) {
    const session = agent?.session
    if (!session) return null
    const header = session.header
    const workspace = this.workspaceOf(agent)
    const isSubagent = header?.origin === 'subagent' || (header?.delegationDepth ?? 0) > 0
    return {
      sessionId: String(agent.id),
      title: this.sessionTitleOf(session),
      workspace,
      workspaceTitle: this.workspaceTitle(workspace, agent),
      status: typeof agent.status === 'string' ? agent.status : 'unknown',
      kind: isSubagent ? 'subagent' : 'root',
      live: true,
    }
  }

  /**
   * A persisted session that no dsh process currently has open: it has a log
   * and a header, but no live Agent to hand messages to. dsh only attaches an
   * Agent when a session is opened (or explicitly resumed), which is why these
   * sessions are invisible to `ctx.agents` and therefore to a live-only listing.
   *
   * @param {any} record  SessionRecord from `ctx.sessionQuery.listSessions()`.
   */
  describeColdSession(record) {
    const header = record?.header
    if (!header || typeof header.id !== 'string' || record.live) return null
    const workspace = typeof header.cwd === 'string' && header.cwd ? header.cwd : ''
    const isSubagent = header.origin === 'subagent' || (header.delegationDepth ?? 0) > 0
    return {
      sessionId: header.id,
      title: '',
      workspace,
      workspaceTitle: this.workspaceTitle(workspace, header.id),
      status: 'closed',
      kind: isSubagent ? 'subagent' : 'root',
      live: false,
      createdAt: header.createdAt,
      updatedAt: header.createdAt,
    }
  }

  /**
   * Every persisted session of this state directory, including the ones that
   * are closed right now. Cached for `coldListTtlMs` because listing reads the
   * headers of every session log.
   *
   * @returns {Promise<any[]>}
   */
  async coldSessions() {
    const ttl = this.config.coldListTtlMs
    const cached = this.coldCache
    if (cached && Date.now() - cached.at < ttl) return cached.rows
    /** @type {any[]} */
    let rows = []
    const query = this.ctx.get?.('sessionQuery')
    const archived = this.archivedIds()
    try {
      const records = typeof query?.listSessions === 'function' ? await query.listSessions() : []
      if (Array.isArray(records)) {
        for (const record of records) {
          const descriptor = this.describeColdSession(record)
          if (descriptor && !archived.has(descriptor.sessionId)) rows.push(descriptor)
        }
      }
      await this.attachTitles(query, rows)
    } catch (error) {
      this.logger.warn('peer-messaging: cannot list persisted sessions: %s', errorMessage(error))
    }
    this.coldCache = { at: Date.now(), rows }
    return rows
  }

  /** Display title of a live session, when this composition exposes one. */
  sessionTitleOf(session) {
    try {
      const title = this.ctx.get?.('sessionTitle')?.get?.(session)?.title
      if (typeof title === 'string') return title
    } catch {
      // Optional service: a missing title only costs a label.
    }
    return ''
  }

  /** Sessions the user archived: out of the catalogue, and never a target. */
  archivedIds() {
    try {
      const ids = this.ctx.get?.('workspaceRegistry')?.archivedSessionIds
      return new Set(Array.isArray(ids) ? ids.map((id) => String(id)) : [])
    } catch {
      return new Set()
    }
  }

  /**
   * Attach human session titles to catalogue rows in one batched read; a
   * composition without title snapshots keeps the directory label alone.
   * @param {any} query
   * @param {any[]} rows
   */
  async attachTitles(query, rows) {
    if (rows.length === 0 || typeof query?.readTitleSnapshots !== 'function') return
    try {
      const snapshots = await query.readTitleSnapshots(rows.map((row) => row.sessionId))
      const byId = new Map()
      for (const snapshot of Array.isArray(snapshots) ? snapshots : []) {
        if (snapshot?.status !== 'fulfilled') continue
        const title = snapshot.value?.title?.title
        if (typeof title === 'string' && title) byId.set(String(snapshot.sessionId), title)
      }
      for (const row of rows) {
        const title = byId.get(row.sessionId)
        if (title) row.title = title
      }
    } catch (error) {
      this.logger.debug?.('peer-messaging: cannot read session titles: %s', errorMessage(error))
    }
  }

  /**
   * Working directory of an agent, preferring the committed working-directory
   * projection over the raw session header.
   * @param {import('@deepseek-ai/dsh-agent').Agent} agent
   */
  workspaceOf(agent) {
    try {
      const service = this.ctx.get?.('workingDirectory')
      const value = service?.get?.(agent.session)
      if (typeof value === 'string' && value) return value
    } catch {
      // Service absent or not ready: fall through to the header.
    }
    const cwd = agent?.session?.header?.cwd
    return typeof cwd === 'string' && cwd ? cwd : process.cwd()
  }

  /**
   * @param {string} workspace
   * @param {import('@deepseek-ai/dsh-agent').Agent|string} agent  Agent or session id.
   */
  workspaceTitle(workspace, agent) {
    const sessionId = typeof agent === 'string' ? agent : String(agent?.id ?? '')
    try {
      const registry = this.ctx.get?.('workspaceRegistry')
      const list = registry?.list?.()
      if (Array.isArray(list)) {
        const match =
          list.find((entry) => Array.isArray(entry?.sessionIds) && entry.sessionIds.includes(sessionId)) ??
          list.find((entry) => entry?.path === workspace)
        if (match && typeof match.title === 'string' && match.title) return match.title
      }
    } catch {
      // Optional service: a missing registry only costs a nicer label.
    }
    const trimmed = workspace.replace(/[\\/]+$/, '')
    return basename(trimmed) || workspace
  }

  /**
   * List reachable agents, merging live local sessions with the presence
   * records of every other dsh process sharing this state directory.
   *
   * @param {{includeStale?: boolean, includeSubagents?: boolean, workspace?: string}} [options]
   */
  async listAgents({
    includeStale = false,
    includeSubagents = this.config.includeSubagentsInList,
    includeCold = this.config.includeColdInList,
    workspace,
  } = {}) {
    const now = Date.now()
    const archived = this.archivedIds()
    /** @type {Map<string, any>} */
    const agents = new Map()

    for (const agent of this.ctx.agents.list()) {
      const descriptor = this.describeAgent(agent)
      if (!descriptor) continue
      agents.set(descriptor.sessionId, {
        ...descriptor,
        hostId: this.hostId,
        hostname: hostname(),
        local: true,
        stale: false,
        updatedAt: new Date(now).toISOString(),
      })
    }

    let hosts = []
    try {
      hosts = await this.bus.readHosts()
    } catch (error) {
      this.logger.warn('peer-messaging: cannot read presence records: %s', errorMessage(error))
    }
    for (const host of hosts) {
      if (host.hostId === this.hostId) continue
      const updatedAtMs = Date.parse(host.updatedAt)
      const stale = !Number.isFinite(updatedAtMs) || now - updatedAtMs > this.config.presenceTtlMs
      if (stale && !includeStale) continue
      for (const session of host.sessions) {
        if (!isRecord(session) || typeof session.sessionId !== 'string' || agents.has(session.sessionId)) continue
        agents.set(session.sessionId, {
          sessionId: session.sessionId,
          title: typeof session.title === 'string' ? session.title : '',
          workspace: typeof session.workspace === 'string' ? session.workspace : '',
          workspaceTitle: typeof session.workspaceTitle === 'string' ? session.workspaceTitle : '',
          status: typeof session.status === 'string' ? session.status : 'unknown',
          kind: session.kind === 'subagent' ? 'subagent' : 'root',
          hostId: host.hostId,
          hostname: host.hostname,
          local: false,
          stale,
          live: true,
          updatedAt: host.updatedAt,
        })
      }
    }

    // Closed sessions: no Agent exists for them, so they can only be reached by
    // resuming them first (see ensureLive). Skipped when some process already
    // published a live entry for the same session id.
    if (includeCold) {
      for (const entry of await this.coldSessions()) {
        if (agents.has(entry.sessionId)) continue
        agents.set(entry.sessionId, {
          ...entry,
          hostId: null,
          hostname: null,
          local: true,
          stale: false,
          updatedAt: new Date(now).toISOString(),
        })
      }
    }

    let result = [...agents.values()].filter((entry) => !archived.has(entry.sessionId))
    if (!includeSubagents) result = result.filter((entry) => entry.kind !== 'subagent')
    if (workspace) {
      const needle = workspace.trim().toLowerCase()
      if (needle) {
        result = result.filter(
          (entry) =>
            String(entry.workspace ?? '').toLowerCase().includes(needle) ||
            String(entry.workspaceTitle ?? '').toLowerCase().includes(needle) ||
            String(entry.title ?? '').toLowerCase().includes(needle),
        )
      }
    }
    result.sort((left, right) => {
      if (left.local !== right.local) return left.local ? -1 : 1
      if (left.live !== right.live) return left.live ? -1 : 1
      const workspaceOrder = String(left.workspace ?? '').localeCompare(String(right.workspace ?? ''))
      if (workspaceOrder !== 0) return workspaceOrder
      return String(left.sessionId).localeCompare(String(right.sessionId))
    })
    return result
  }

  /**
   * Resolve a user-supplied target (session id, session prefix, workspace path,
   * workspace title, optionally prefixed with `session:` or `workspace:`) to
   * exactly one reachable agent.
   *
   * @param {string} rawTarget
   * @param {{senderSessionId?: string}} [options]
   */
  async resolveTarget(rawTarget, { senderSessionId } = {}) {
    if (typeof rawTarget !== 'string' || rawTarget.trim() === '') {
      throw new PeerMessagingError('invalid', 'target must be a non-empty string')
    }
    const target = rawTarget.trim()
    let mode = 'auto'
    let needle = target
    const colon = target.indexOf(':')
    if (colon > 0) {
      const prefix = target.slice(0, colon).toLowerCase()
      if (prefix === 'session') {
        mode = 'session'
        needle = target.slice(colon + 1).trim()
      } else if (prefix === 'workspace') {
        mode = 'workspace'
        needle = target.slice(colon + 1).trim()
      }
    }
    if (!needle) throw new PeerMessagingError('invalid', `target ${JSON.stringify(target)} is missing its value`)

    const entries = await this.listAgents({ includeStale: true, includeSubagents: true })
    /** @type {Array<{entry: any, score: number}>} */
    const matches = []
    const lowerNeedle = needle.toLowerCase()
    const lowerNeedleBase = basename(lowerNeedle.replace(/[\\/]+$/, ''))
    for (const entry of entries) {
      const sessionId = String(entry.sessionId)
      let score = 0
      if (mode !== 'workspace') {
        if (sessionId === needle) score = 100
        else if (needle.length >= MIN_SESSION_PREFIX && sessionId.startsWith(needle)) score = 80
      }
      if (score === 0 && mode !== 'session') {
        const sessionTitle = String(entry.title ?? '').toLowerCase()
        const workspace = String(entry.workspace ?? '').toLowerCase()
        const title = String(entry.workspaceTitle ?? '').toLowerCase()
        if (sessionTitle && sessionTitle === lowerNeedle) score = 58
        const workspaceBase = basename(workspace.replace(/[\\/]+$/, ''))
        if (workspace === lowerNeedle) score = 60
        else if (title === lowerNeedle || title === lowerNeedleBase) score = 55
        else if (workspaceBase === lowerNeedle) score = 50
        else if (workspace.endsWith(`/${lowerNeedle}`)) score = 40
      }
      if (score > 0) matches.push({ entry, score })
    }

    const summarize = (entry) => ({
      sessionId: entry.sessionId,
      title: entry.title,
      workspace: entry.workspace,
      workspaceTitle: entry.workspaceTitle,
      status: entry.status,
      hostId: entry.hostId,
      local: entry.local,
      stale: entry.stale,
    })

    if (matches.length === 0) {
      if (this.archivedIds().has(needle)) {
        throw new PeerMessagingError(
          'archived',
          `session ${needle} is archived; restore it in the app before messaging it`,
        )
      }
      throw new PeerMessagingError(
        'not_found',
        `no reachable agent matches target ${JSON.stringify(target)}; call peer_agents to list reachable agents`,
        { candidates: entries.slice(0, 12).map(summarize) },
      )
    }
    // A closed session in the same workspace must not make a running target
    // ambiguous: prefer agents that are already live and only fall back to
    // closed sessions when nothing is running.
    const liveMatches = matches.filter((match) => match.entry.live !== false)
    const pool = liveMatches.length > 0 ? liveMatches : matches
    pool.sort((left, right) => right.score - left.score)
    const bestScore = pool[0].score
    const best = pool.filter((match) => match.score === bestScore).map((match) => match.entry)
    if (best.length > 1) {
      throw new PeerMessagingError(
        'ambiguous',
        `target ${JSON.stringify(target)} matches ${best.length} agents; disambiguate with session:<id> or workspace:<path>`,
        { candidates: best.map(summarize) },
      )
    }
    const chosen = best[0]
    if (senderSessionId && chosen.sessionId === senderSessionId) {
      throw new PeerMessagingError('self', 'refusing to deliver a message to the calling agent itself')
    }
    return chosen
  }

  /**
   * Flood guard: one pair of sessions cannot ping-pong without limit. The
   * window is undirected, so a reply storm is throttled either way.
   *
   * @param {string} fromSessionId
   * @param {string} toSessionId
   * @returns {() => void} Rollback, used when the send itself fails.
   */
  reservePairSend(fromSessionId, toSessionId) {
    const limit = this.config.pairLimit
    if (!Number.isFinite(limit) || limit <= 0) return () => {}
    const windowMs = this.config.pairWindowMs
    const now = Date.now()
    if (now - this.lastPairPruneAt >= windowMs) {
      for (const [key, entries] of this.pairSends) {
        const active = entries.filter((entry) => now - entry.at < windowMs)
        if (active.length === 0) this.pairSends.delete(key)
        else this.pairSends.set(key, active)
      }
      this.lastPairPruneAt = now
    }
    const key = JSON.stringify([String(fromSessionId), String(toSessionId)].sort())
    const recent = (this.pairSends.get(key) ?? []).filter((entry) => now - entry.at < windowMs)
    if (recent.length >= limit) {
      this.pairSends.set(key, recent)
      const retryInSeconds = Math.max(1, Math.ceil((windowMs - (now - recent[0].at)) / 1000))
      throw new PeerMessagingError(
        'rate_limited',
        `at most ${limit} messages per ${Math.round(windowMs / 1000)}s between these two sessions; retry in about ${retryInSeconds}s`,
      )
    }
    const reservation = { at: now }
    recent.push(reservation)
    this.pairSends.set(key, recent)
    return () => {
      const current = this.pairSends.get(key)
      if (!current) return
      const index = current.indexOf(reservation)
      if (index !== -1) current.splice(index, 1)
      if (current.length === 0) this.pairSends.delete(key)
    }
  }

  // ---------------------------------------------------------------------------
  // Waking a closed session
  // ---------------------------------------------------------------------------

  /**
   * Guarantee that the target session has a live Agent in this process.
   *
   * dsh attaches an Agent to a session only when the app opens it (or when a
   * plugin resumes it). A session that is not open has a persisted log and a
   * header but no Agent, so nothing can hand a message to it — that is why a
   * live-only view can only reach sessions whose window is open. Resuming
   * creates the missing Agent on demand.
   *
   * @param {{sessionId: string, live?: boolean}} recipient
   * @returns {Promise<import('@deepseek-ai/dsh-agent').Agent>}
   */
  async ensureLive(recipient) {
    const existing = this.ctx.agents.get?.(recipient.sessionId)
    if (existing) return existing
    if (!this.config.resumeColdTargets) {
      throw new PeerMessagingError(
        'unavailable',
        `session ${recipient.sessionId} is not open in any dsh process; open it in the app, or set resumeColdTargets to resume it on demand`,
      )
    }
    const pending = this.resuming.get(recipient.sessionId)
    if (pending) return pending
    const task = this.resumeSession(recipient.sessionId).finally(() => this.resuming.delete(recipient.sessionId))
    this.resuming.set(recipient.sessionId, task)
    return task
  }

  /**
   * Resume one persisted session. The web composition owns the full resume
   * path (preset composition plus the user's model selection), so prefer its
   * controller and fall back to the bare agent registry elsewhere.
   *
   * @param {string} sessionId
   */
  async resumeSession(sessionId) {
    let controllerError = null
    const controller = this.ctx.get?.('sessionController')
    if (typeof controller?.resolveAgent === 'function') {
      try {
        const result = await controller.resolveAgent(sessionId)
        const agent = result?.agent ?? this.ctx.agents.get?.(sessionId)
        if (agent) return this.announceResume(agent)
        controllerError = result?.error ?? null
      } catch (error) {
        controllerError = error
      }
    }
    try {
      const agentOptions = this.defaultAgentOptions()
      const setup = await this.presetSetup(sessionId)
      const resumed = await this.ctx.agents.resume({
        resumeSessionId: sessionId,
        ...(agentOptions ? { agentOptions } : {}),
        ...(setup ? { setup } : {}),
      })
      const agent = resumed?.agent ?? this.ctx.agents.get?.(sessionId)
      if (agent) {
        // A resumed agent is ours to release: hold the handle until unload so
        // an idle stop does not tear the session out of the store mid-use.
        if (typeof resumed?.dispose === 'function') this.resumedHandles.set(String(sessionId), resumed)
        return this.announceResume(agent)
      }
    } catch (error) {
      const detail = controllerError ? `${errorMessage(error)}; ${errorMessage(controllerError)}` : errorMessage(error)
      // Losing the race for a session another process just reopened is not a
      // broken target: that process will deliver the envelope itself.
      const code = /already owned|SessionAlreadyOwned|is locked/i.test(detail) ? 'busy' : 'unavailable'
      throw new PeerMessagingError(code, `cannot resume session ${sessionId}: ${detail}`)
    }
    throw new PeerMessagingError(
      'unavailable',
      `cannot resume session ${sessionId}: ${controllerError ? errorMessage(controllerError) : 'the agent registry returned no agent'}`,
    )
  }

  /**
   * The composition a resumed session ran with. Without this, a woken session
   * would come back with the default preset — missing the tools, prompts and
   * settings it had when the user last worked in it.
   *
   * @param {string} sessionId
   * @returns {Promise<((agentCtx: any) => Promise<void>)|null>}
   */
  async presetSetup(sessionId) {
    const presets = this.ctx.get?.('agentPresets')
    if (!presets || typeof presets.mount !== 'function') return null
    const presetId = this.presetIdOf(await this.inspectSession(sessionId))
    if (!presetId) return null
    if (!(await this.presetExists(presets, presetId))) {
      this.logger.warn('peer-messaging: preset %C no longer exists; resuming without it', presetId)
      return null
    }
    return async (agentCtx) => {
      await presets.mount(agentCtx, presetId)
    }
  }

  /**
   * Whether a preset id is still declared. A deleted preset must not abort the
   * resume: the message still has to reach the session.
   * @param {any} presets
   * @param {string} presetId
   */
  async presetExists(presets, presetId) {
    if (typeof presets.list !== 'function') return true
    try {
      const list = await presets.list()
      if (!Array.isArray(list)) return true
      return list.some((preset) => String(preset?.id ?? '') === presetId)
    } catch {
      return true
    }
  }

  /** Read a persisted session, log included, for resume-time composition. */
  async inspectSession(sessionId) {
    try {
      const query = this.ctx.get?.('sessionQuery')
      if (typeof query?.readSession !== 'function') return null
      return await query.readSession(sessionId)
    } catch {
      return null
    }
  }

  /**
   * The preset a session was running with: its header, or the last preset the
   * user selected inside it.
   * @param {any} inspected
   */
  presetIdOf(inspected) {
    const header = inspected?.session?.header ?? inspected?.header
    let presetId = header?.agentPreset
    const events = inspected?.events ?? []
    for (let index = events.length - 1; index >= 0; index -= 1) {
      const event = events[index]
      if (event?.type === 'agent-preset/selected') {
        presetId = event.data?.agentPreset
        break
      }
    }
    return typeof presetId === 'string' && presetId ? presetId : null
  }

  /** Publish presence immediately after a resume so other processes see it too. */
  announceResume(agent) {
    this.coldCache = null
    this.logger.info('peer-messaging: resumed %C to deliver a message', String(agent.id))
    void this.beat()
    return agent
  }

  /** The profile's current default model, used when resuming without a controller. */
  defaultAgentOptions() {
    try {
      const defaults = this.ctx.get?.('agentDefaultModel')
      const selection = defaults?.currentSelection?.()
      if (selection && typeof selection.provider === 'string' && typeof selection.model === 'string') {
        return {
          provider: selection.provider,
          model: selection.model,
          ...(selection.reasoningEffort ? { reasoningEffort: selection.reasoningEffort } : {}),
        }
      }
    } catch {
      // Optional service: a composition without model defaults resumes with its own.
    }
    return null
  }

  // ---------------------------------------------------------------------------
  // Sending
  // ---------------------------------------------------------------------------

  /**
   * Enqueue one message and (when the recipient is local) deliver it right
   * away. The returned promise resolves once the outcome is known or the
   * acknowledgement window elapses.
   *
   * @param {{target: string, text: string, wake?: boolean, agent?: import('@deepseek-ai/dsh-agent').Agent}} input
   */
  async send({ target, text, wake = true, agent }) {
    if (typeof text !== 'string' || text.trim() === '') {
      throw new PeerMessagingError('invalid', 'message must be a non-empty string')
    }
    if (/<\/?peer-message\b/i.test(text)) {
      throw new PeerMessagingError(
        'invalid',
        'message must not contain the reserved <peer-message> protocol tag; quote it as plain text instead',
      )
    }
    const bytes = Buffer.byteLength(text, 'utf8')
    if (bytes > this.config.maxMessageBytes) {
      throw new PeerMessagingError('too_large', `message is ${bytes} bytes, limit is ${this.config.maxMessageBytes}`)
    }
    const recipient = await this.resolveTarget(target, { senderSessionId: agent?.id ? String(agent.id) : undefined })
    // A heartbeat is only a claim about another process. When it is stale the
    // claim is worthless: resume here instead of trusting a host that may be
    // gone, otherwise the envelope lands in an inbox nobody polls.
    if (recipient.live === false || recipient.stale === true) {
      try {
        const resumed = await this.ensureLive(recipient)
        const descriptor = this.describeAgent(resumed)
        if (descriptor) {
          recipient.workspace = descriptor.workspace
          recipient.workspaceTitle = descriptor.workspaceTitle
          recipient.status = descriptor.status
          recipient.live = true
          recipient.local = true
          recipient.stale = false
        }
      } catch (error) {
        // A refusal we can explain (archived, resume disabled, not found) is the
        // caller's problem to fix. Anything else means we could not take over
        // the session right now — most likely another process still owns it —
        // so leave the message queued for the rescue pass instead of failing it.
        if (error instanceof PeerMessagingError && error.code !== 'busy') throw error
        this.logger.warn(
          'peer-messaging: could not resume %C (%s); the message stays queued',
          recipient.sessionId,
          errorMessage(error),
        )
      }
    }

    const sender = agent ? this.describeAgent(agent) : null
    const envelope = {
      v: PROTOCOL_VERSION,
      messageId: randomUUID(),
      createdAt: new Date().toISOString(),
      targetSessionId: recipient.sessionId,
      sender: {
        sessionId: sender ? sender.sessionId : 'unknown',
        workspace: sender ? sender.workspace : '',
        hostId: this.hostId,
        pid: process.pid,
      },
      wake: wake !== false,
      text,
      meta: { hops: 0 },
    }

    const releasePairSend = this.reservePairSend(envelope.sender.sessionId, recipient.sessionId)
    const startedAt = Date.now()
    try {
      await this.bus.enqueue(envelope)
    } catch (error) {
      releasePairSend()
      throw error
    }
    this.emit('peer-messaging/sent', { messageId: envelope.messageId, to: recipient.sessionId, from: envelope.sender.sessionId })

    const localAgent = this.ctx.agents.get?.(recipient.sessionId)
    if (localAgent) await this.deliverFor(localAgent).catch(() => {})

    let outcome = { state: 'pending' }
    const waitMs = this.config.ackWaitMs
    if (waitMs > 0) {
      outcome = await this.bus.waitForOutcome(envelope.messageId, recipient.sessionId, {
        timeoutMs: waitMs,
        intervalMs: Math.max(25, Math.min(250, Math.floor(this.config.pollMs / 2))),
      })
    }
    if (outcome.state === 'delivered') {
      this.emit('peer-messaging/delivered', { messageId: envelope.messageId, to: recipient.sessionId })
    }

    let delivery = outcome.state === 'failed' ? 'failed' : outcome.state === 'delivered' ? 'delivered' : 'queued'
    // `wake: false` only parks the message in the recipient's context: dsh's
    // inject() never starts a turn, so an idle recipient may not touch it until
    // something else wakes it. Reporting that as "delivered" overclaims, so the
    // hand-off is reported as "injected" (accepted, not yet read).
    if (delivery === 'delivered' && envelope.wake === false) delivery = 'injected'
    // Same-process targets can be inspected directly: an unconfirmed hand-off
    // that is still sitting in the recipient's queue is "queued", while one that
    // had already vanished at send time (turn finished before we could confirm)
    // was handed off and is waiting for its own confirmation timer.
    const inFlight = this.inflight.get(envelope.messageId)
    const handedOff = inFlight && inFlight.claimedPath !== null
    let queueState = 'unknown'
    if (delivery === 'queued' && recipient.local && this.canConfirm) {
      const targetAgent = this.ctx.agents.get?.(recipient.sessionId)
      const stillQueued = Boolean(
        inFlight &&
          [...(targetAgent?.inbox?.nextStep ?? []), ...(targetAgent?.inbox?.nextTurn ?? [])].some(
            (message) => message?.id === envelope.messageId,
          ),
      )
      queueState = stillQueued ? 'queued' : handedOff ? 'handed-off' : 'unknown'
    }
    return {
      ok: true,
      messageId: envelope.messageId,
      delivery,
      queueState,
      error: outcome.error,
      waitedMs: Date.now() - startedAt,
      target: {
        sessionId: recipient.sessionId,
        workspace: recipient.workspace,
        workspaceTitle: recipient.workspaceTitle,
        status: recipient.status,
        live: recipient.live !== false,
        hostId: recipient.hostId ?? '',
        local: Boolean(recipient.local),
      },
    }
  }

  // ---------------------------------------------------------------------------
  // Delivery
  // ---------------------------------------------------------------------------

  /**
   * Claim and deliver every pending envelope for one local agent. A hand-off is
   * only marked delivered once the recipient session logs it; see
   * {@link PeerMessaging.onSessionEvent} and {@link PeerMessaging.redeliver}.
   * @param {import('@deepseek-ai/dsh-agent').Agent} agent
   */
  async deliverFor(agent) {
    const sessionId = String(agent.id)
    if (this.delivering.has(sessionId)) return
    this.delivering.add(sessionId)
    try {
      const pending = await this.bus.listPending(sessionId)
      for (const item of pending) {
        const claimed = await this.bus.claim(sessionId, item.fileName)
        if (!claimed) continue // Another process won the race.
        const messageId = String(item.envelope?.messageId ?? '')
        /** @type {any} */
        const record = messageId ? (this.inflight.get(messageId) ?? {
          messageId,
          sessionId,
          fileName: item.fileName,
          envelope: item.envelope,
          attempts: 0,
          firstHandoffAt: Date.now(),
          claimedPath: null,
          timer: null,
        }) : null
        if (record) {
          record.claimedPath = claimed
          record.attempts += 1
          this.inflight.set(messageId, record)
        }
        try {
          await this.deliverMessage(agent, item.envelope)
        } catch (error) {
          if (isAlreadyPending(error)) {
            // The recipient inbox still holds this exact message id (a
            // redelivery raced its own first hand-off). It is queued, not lost:
            // keep waiting for the session log to prove consumption.
            this.logger.debug?.('peer-messaging: %C is already queued for %C', messageId, sessionId)
            if (record && this.canConfirm) this.scheduleConfirm(record)
            else if (record) {
              this.inflight.delete(messageId)
              await this.bus.complete(claimed, sessionId, item.fileName).catch(() => {})
            }
            continue
          }
          if (record) {
            this.inflight.delete(messageId)
            if (record.timer) clearTimeout(record.timer)
            record.timer = null
          }
          this.logger.warn('peer-messaging: delivery to %C failed: %s', sessionId, errorMessage(error))
          await this.bus.fail(claimed, sessionId, item.fileName, item.envelope, error)
          continue
        }
        // A no-wake injection is best-effort by definition: it waits for the
        // recipient's next turn, so it cannot be confirmed here.
        if (record && this.canConfirm && item.envelope?.wake !== false) {
          this.scheduleConfirm(record)
          continue
        }
        if (record) {
          this.inflight.delete(messageId)
          if (record.timer) clearTimeout(record.timer)
          record.timer = null
        }
        await this.bus.complete(claimed, sessionId, item.fileName)
        this.emit('peer-messaging/delivered', {
          messageId: item.envelope.messageId,
          to: sessionId,
          from: item.envelope?.sender?.sessionId,
        })
      }
    } finally {
      this.delivering.delete(sessionId)
    }
  }

  /**
   * Hand one envelope to its live agent.
   *
   * `agent.steer()` is dsh's interjection path: the message joins the *next
   * step* of the turn in flight, so a busy recipient reacts inside its current
   * turn instead of waiting for the turn to end (`followup()` would queue it
   * for the next turn). The loop re-reads the inbox at every step boundary and
   * keeps the turn alive while steering is parked, and an idle driver starts a
   * turn on the same call. `wake: false` uses `inject()`, which parks context
   * without starting a turn. Set `interject: false` to fall back to queueing.
   *
   * @param {import('@deepseek-ai/dsh-agent').Agent} agent
   * @param {any} envelope
   */
  async deliverMessage(agent, envelope) {
    const senderWorkspace = envelope?.sender?.workspace || 'unknown workspace'
    const senderSessionId = envelope?.sender?.sessionId || 'unknown'
    const createdAt = envelope?.createdAt || new Date().toISOString()
    const text = String(envelope?.text ?? '')
    const hint = `Reply with the peer_send tool (target: session:${senderSessionId}) if a response is needed.`
    const message = {
      id: envelope?.messageId,
      role: 'user',
      content: [
        textBlock(`[peer-message] from ${senderWorkspace} (session ${senderSessionId}) at ${createdAt}`),
        textBlock(text),
        textBlock(hint),
      ],
      source: {
        kind: 'peer-message',
        form: 'relay',
        messageId: envelope?.messageId,
        senderSessionId,
        senderWorkspace,
        senderHostId: envelope?.sender?.hostId,
      },
    }
    if (envelope?.wake === false) agent.inject(message)
    else if (this.config.interject === false) agent.followup(message)
    else agent.steer(message)
  }

  /**
   * Best-effort event publication; the bus is filesystem state, not an event
   * emitter, so a missing listener must never break delivery.
   * @param {string} event
   * @param {unknown} payload
   */
  emit(event, payload) {
    try {
      this.ctx.emit?.(event, payload)
    } catch {
      // Event listeners are advisory.
    }
  }
}
