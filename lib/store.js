/**
 * Filesystem peer bus shared by every dsh process that points at the same
 * `stateDir` (by default `$DSH_HOME/peer-messaging`).
 *
 * Layout:
 *   hosts/<hostId>.json                       presence heartbeat of one dsh process
 *   inbox/<sessionKey>/<messageId>.json       envelopes awaiting delivery
 *   processing/<sessionKey>/<messageId>.json  envelopes claimed by one poller
 *   processed/<sessionKey>/<messageId>.json   delivered marker (sender ack)
 *   failed/<sessionKey>/<messageId>.json      delivery-failure record (sender ack)
 *   tmp/                                      atomic-write staging
 *
 * Every cross-process mutation is a single `rename()` on the same filesystem,
 * so a claim is atomic: exactly one poller can move an envelope out of
 * `inbox/`. Delivery is therefore at-most-once; the sender learns the outcome
 * through the `processed/` / `failed/` marker.
 */
import { createHash, randomUUID } from 'node:crypto'
import { hostname } from 'node:os'
import { mkdir, readdir, readFile, rename, rmdir, stat, unlink, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

/** Envelope wire version. */
export const PROTOCOL_VERSION = 1

const SAFE_DIR_COMPONENT = /^[A-Za-z0-9._-]{1,120}$/
const TMP_MAX_AGE_MS = 60 * 60 * 1_000

/**
 * Map a session id to a safe, stable directory name. Session ids are normally
 * already filesystem-safe; anything else is hashed so both sides agree.
 *
 * @param {string} sessionId
 * @returns {string}
 */
export function sessionKey(sessionId) {
  const id = String(sessionId ?? '')
  if (SAFE_DIR_COMPONENT.test(id) && id !== '.' && id !== '..') return id
  return `h-${createHash('sha1').update(id).digest('hex')}`
}

/** Stable-ish identity of one dsh process on the bus. */
export function makeHostId() {
  const host = hostname().replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 48) || 'host'
  return `${host}-${process.pid}-${randomUUID().slice(0, 8)}`
}

/** @param {string} path */
async function exists(path) {
  try {
    await stat(path)
    return true
  } catch (error) {
    if (error?.code === 'ENOENT') return false
    throw error
  }
}

/** @param {unknown} error */
function isMissing(error) {
  return /** @type {{code?: string}} */ (error)?.code === 'ENOENT'
}

/** @param {unknown} error */
function errorMessage(error) {
  if (error instanceof Error) return error.message
  return String(error)
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms))
}

/**
 * @typedef {object} PeerSessionDescriptor
 * @property {string} sessionId
 * @property {string} workspace
 * @property {string} workspaceTitle
 * @property {'idle'|'running'|'unknown'} status
 * @property {'root'|'subagent'} kind
 */

/**
 * @typedef {object} PresetHostRecord
 * @property {number} v
 * @property {string} hostId
 * @property {number} pid
 * @property {string} hostname
 * @property {string} startedAt
 * @property {string} updatedAt
 * @property {PeerSessionDescriptor[]} sessions
 */

export class PeerBus {
  /**
   * @param {object} options
   * @param {string} options.stateDir
   * @param {{warn: Function, info?: Function, error?: Function, debug?: Function}} [options.logger]
   * @param {string} [options.hostId]
   * @param {() => number} [options.now]
   */
  constructor({ stateDir, logger = console, hostId = makeHostId(), now = () => Date.now() }) {
    this.stateDir = stateDir
    this.logger = logger
    this.hostId = hostId
    this.now = now
    this.paths = {
      root: stateDir,
      hosts: join(stateDir, 'hosts'),
      inbox: join(stateDir, 'inbox'),
      processing: join(stateDir, 'processing'),
      processed: join(stateDir, 'processed'),
      failed: join(stateDir, 'failed'),
      tmp: join(stateDir, 'tmp'),
    }
    this._ready = null
  }

  /** Ensure the directory tree exists (idempotent, cached). */
  ready() {
    this._ready ??= this._init()
    return this._ready
  }

  async _init() {
    for (const path of Object.values(this.paths)) await mkdir(path, { recursive: true })
    return this
  }

  /** @param {string} [hostId] */
  hostPath(hostId = this.hostId) {
    return join(this.paths.hosts, `${sessionKey(hostId)}.json`)
  }

  /**
   * @param {'inbox'|'processing'|'processed'|'failed'} area
   * @param {string} sessionId
   */
  sessionDir(area, sessionId) {
    return join(this.paths[area], sessionKey(sessionId))
  }

  /**
   * Write one JSON document into `dir` atomically (tmp file + rename).
   * @param {string} dir
   * @param {string} fileName
   * @param {unknown} value
   */
  async writeJsonAtomic(dir, fileName, value) {
    await mkdir(dir, { recursive: true })
    const tmpPath = join(this.paths.tmp, `${process.pid}-${randomUUID()}.json`)
    await writeFile(tmpPath, `${JSON.stringify(value)}\n`, 'utf8')
    try {
      await rename(tmpPath, join(dir, fileName))
    } catch (error) {
      await unlink(tmpPath).catch(() => {})
      throw error
    }
  }

  /**
   * Publish this process's presence record.
   * @param {{startedAt: string, sessions: PeerSessionDescriptor[]}} input
   * @returns {Promise<PresetHostRecord>}
   */
  async writePresence({ startedAt, sessions }) {
    await this.ready()
    /** @type {PresetHostRecord} */
    const record = {
      v: PROTOCOL_VERSION,
      hostId: this.hostId,
      pid: process.pid,
      hostname: hostname(),
      startedAt,
      updatedAt: new Date(this.now()).toISOString(),
      sessions,
    }
    await this.writeJsonAtomic(this.paths.hosts, `${sessionKey(this.hostId)}.json`, record)
    return record
  }

  /** Remove this process's presence file (clean shutdown). */
  async removePresence(hostId = this.hostId) {
    await unlink(this.hostPath(hostId)).catch(() => {})
  }

  /**
   * Read every presence record currently on disk. Unparsable or partially
   * written records are skipped: presence is advisory, never authoritative.
   * @returns {Promise<PresetHostRecord[]>}
   */
  async readHosts() {
    await this.ready()
    const names = await readdir(this.paths.hosts).catch(() => [])
    const records = []
    for (const name of names) {
      if (name.startsWith('.') || !name.endsWith('.json')) continue
      try {
        const parsed = JSON.parse(await readFile(join(this.paths.hosts, name), 'utf8'))
        if (parsed && typeof parsed === 'object' && typeof parsed.hostId === 'string' && Array.isArray(parsed.sessions)) {
          records.push(parsed)
        }
      } catch {
        // A heartbeat renamed over a partially read file is the only expected
        // failure here; presence self-heals on the next heartbeat.
      }
    }
    return records
  }

  /**
   * Persist one envelope into the target session's inbox.
   * @param {object} envelope
   * @param {string} envelope.targetSessionId
   * @param {string} envelope.messageId
   */
  async enqueue(envelope) {
    await this.ready()
    const dir = this.sessionDir('inbox', envelope.targetSessionId)
    await this.writeJsonAtomic(dir, `${sessionKey(envelope.messageId)}.json`, envelope)
  }

  /**
   * List envelopes waiting for one session, oldest first.
   * @param {string} sessionId
   * @returns {Promise<Array<{fileName: string, envelope: any}>>}
   */
  async listPending(sessionId) {
    await this.ready()
    const dir = this.sessionDir('inbox', sessionId)
    const names = await readdir(dir).catch((error) => {
      if (isMissing(error)) return []
      throw error
    })
    const items = []
    for (const name of names) {
      if (name.startsWith('.') || !name.endsWith('.json')) continue
      try {
        const envelope = JSON.parse(await readFile(join(dir, name), 'utf8'))
        if (envelope && typeof envelope === 'object' && typeof envelope.messageId === 'string') {
          items.push({ fileName: name, envelope })
        }
      } catch {
        this.logger.warn?.('peer-messaging: dropping unreadable envelope %C', JSON.stringify(join(dir, name)))
        await unlink(join(dir, name)).catch(() => {})
      }
    }
    items.sort((a, b) => {
      const left = String(a.envelope.createdAt ?? '')
      const right = String(b.envelope.createdAt ?? '')
      if (left !== right) return left < right ? -1 : 1
      return a.envelope.messageId < b.envelope.messageId ? -1 : 1
    })
    return items
  }

  /**
   * Every session that currently has mail waiting, with the age of its oldest
   * envelope. This is the inbox side of the bus, so a process can discover mail
   * addressed to a session no process has open (and therefore no Agent polls).
   *
   * @returns {Promise<Array<{sessionId: string, count: number, oldestAgeMs: number}>>}
   */
  async pendingSessions() {
    await this.ready()
    const root = join(this.stateDir, 'inbox')
    const sessions = await readdir(root).catch((error) => {
      if (isMissing(error)) return []
      throw error
    })
    const nowMs = this.now()
    const rows = []
    for (const sessionId of sessions) {
      if (sessionId.startsWith('.')) continue
      const dir = join(root, sessionId)
      const names = await readdir(dir).catch((error) => {
        if (isMissing(error)) return []
        throw error
      })
      let count = 0
      let oldest = Infinity
      let targetId = ''
      for (const name of names) {
        if (name.startsWith('.') || !name.endsWith('.json')) continue
        count += 1
        const info = await stat(join(dir, name)).catch(() => null)
        if (info && Number.isFinite(info.mtimeMs)) oldest = Math.min(oldest, info.mtimeMs)
        if (!targetId) {
          const envelope = await readFile(join(dir, name), 'utf8').then(
            (text) => JSON.parse(text),
            () => null,
          )
          if (envelope && typeof envelope.targetSessionId === 'string') targetId = envelope.targetSessionId
        }
      }
      if (count === 0) continue
      rows.push({
        // The directory name is the hashed key only for exotic ids; the envelope
        // carries the real session id, which is what a resume needs.
        sessionId: targetId || sessionId,
        count,
        oldestAgeMs: Number.isFinite(oldest) ? Math.max(0, nowMs - oldest) : 0,
      })
    }
    return rows
  }

  /**
   * Atomically claim one envelope. Returns the claimed path, or null when
   * another process (or a previous tick) already claimed it.
   * @param {string} sessionId
   * @param {string} fileName
   * @returns {Promise<string|null>}
   */
  async claim(sessionId, fileName) {
    await this.ready()
    const from = join(this.sessionDir('inbox', sessionId), fileName)
    const to = join(this.sessionDir('processing', sessionId), fileName)
    try {
      await mkdir(this.sessionDir('processing', sessionId), { recursive: true })
      await rename(from, to)
      return to
    } catch (error) {
      if (isMissing(error)) return null
      throw error
    }
  }

  /**
   * Turn a claimed envelope into a delivered marker.
   * @param {string} claimedPath
   * @param {string} sessionId
   * @param {string} fileName
   */
  async complete(claimedPath, sessionId, fileName) {
    const to = join(this.sessionDir('processed', sessionId), fileName)
    await mkdir(this.sessionDir('processed', sessionId), { recursive: true })
    await rename(claimedPath, to)
  }

  /**
   * Return a claimed envelope to its recipient inbox so it can be handed off
   * again. Used when the recipient discarded the message before consuming it
   * (a stopped or archived turn clears its inbox).
   * @param {string} claimedPath
   * @param {string} sessionId
   * @param {string} fileName
   */
  async requeue(claimedPath, sessionId, fileName) {
    await this.ready()
    const to = join(this.sessionDir('inbox', sessionId), fileName)
    await mkdir(this.sessionDir('inbox', sessionId), { recursive: true })
    await rename(claimedPath, to)
  }

  /**
   * Record a delivery failure and drop the claimed envelope.
   * @param {string} claimedPath
   * @param {string} sessionId
   * @param {string} fileName
   * @param {unknown} envelope
   * @param {unknown} error
   */
  async fail(claimedPath, sessionId, fileName, envelope, error) {
    const record = {
      v: PROTOCOL_VERSION,
      hostId: this.hostId,
      failedAt: new Date(this.now()).toISOString(),
      error: errorMessage(error),
      envelope,
    }
    await this.writeJsonAtomic(this.sessionDir('failed', sessionId), fileName, record).catch(() => {})
    await unlink(claimedPath).catch(() => {})
  }

  /**
   * Wait for the sender-side delivery marker of one message.
   * @param {string} messageId
   * @param {string} sessionId
   * @param {{timeoutMs?: number, intervalMs?: number}} [options]
   * @returns {Promise<{state: 'delivered'|'failed'|'pending', error?: string}>}
   */
  async waitForOutcome(messageId, sessionId, { timeoutMs = 0, intervalMs = 50 } = {}) {
    const fileName = `${sessionKey(messageId)}.json`
    const deliveredPath = join(this.sessionDir('processed', sessionId), fileName)
    const failedPath = join(this.sessionDir('failed', sessionId), fileName)
    const deadline = this.now() + Math.max(0, timeoutMs)
    for (;;) {
      if (await exists(deliveredPath)) return { state: 'delivered' }
      if (await exists(failedPath)) {
        let detail
        try {
          detail = JSON.parse(await readFile(failedPath, 'utf8'))?.error
        } catch {
          // Marker exists; the reason is best-effort.
        }
        return { state: 'failed', error: detail || 'delivery failed' }
      }
      if (this.now() >= deadline) return { state: 'pending' }
      await sleep(Math.min(intervalMs, Math.max(1, deadline - this.now())))
    }
  }

  /**
   * Drop old markers, abandoned claims, stale presence files and envelopes
   * addressed to sessions that never came back.
   * @param {{retentionMs: number, presenceTtlMs: number, now?: number}} options
   */
  async cleanup({ retentionMs, presenceTtlMs, now = this.now() }) {
    await this.ready()
    const cutoff = now - retentionMs
    const hostCutoff = now - Math.max(presenceTtlMs * 4, retentionMs)

    const sweep = async (dir, { olderThan, skip = () => false } = {}) => {
      const entries = await readdir(dir, { withFileTypes: true }).catch(() => [])
      for (const entry of entries) {
        const path = join(dir, entry.name)
        if (entry.isDirectory()) {
          await sweep(path, { olderThan, skip }).catch(() => {})
          const rest = await readdir(path).catch(() => null)
          if (rest && rest.length === 0) await rmdir(path).catch(() => {})
          continue
        }
        if (skip(entry.name)) continue
        try {
          const info = await stat(path)
          if (info.mtimeMs < olderThan) await unlink(path).catch(() => {})
        } catch {
          // Concurrent removal by another host: fine.
        }
      }
    }

    for (const area of ['processed', 'failed', 'inbox', 'processing']) {
      await sweep(this.paths[area], { olderThan: cutoff })
    }
    await sweep(this.paths.tmp, { olderThan: now - TMP_MAX_AGE_MS })
    await sweep(this.paths.hosts, { olderThan: hostCutoff, skip: (name) => name === `${sessionKey(this.hostId)}.json` })
  }
}
