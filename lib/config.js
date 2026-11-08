/**
 * Runtime configuration for the peer-messaging plugin.
 *
 * The plugin deliberately exports no `Config` schema: cordis then passes the
 * raw profile config through untouched (see `resolveConfig` in cordis) and we
 * normalise it here. That keeps this package dependency-free — it can be
 * installed into any dsh profile without pulling extra packages into the
 * profile's pnpm tree.
 */
import { homedir } from 'node:os'
import { join, resolve, sep } from 'node:path'

/**
 * @typedef {object} PeerMessagingConfig
 * @property {string} stateDir           Absolute peer-bus directory shared by every participating dsh process.
 * @property {number} pollMs             Inbox poll interval for pending envelopes (ms).
 * @property {number} heartbeatMs        Presence heartbeat interval (ms).
 * @property {number} presenceTtlMs      A host whose heartbeat is older than this is considered dead (ms).
 * @property {number} retentionMs        How long processed/failed markers are kept (ms).
 * @property {number} maxMessageBytes    Upper bound on one message body (UTF-8 bytes).
 * @property {number} ackWaitMs          How long `peer_send` waits for the delivery marker before returning `queued`.
 * @property {boolean} includeSubagentsInList  Whether `peer_agents` lists subagent sessions by default.
 * @property {boolean} includeColdInList  Whether `peer_agents` also lists persisted sessions that no dsh process has open.
 * @property {number} coldListTtlMs      How long the persisted-session catalogue is cached between rebuilds (ms).
 * @property {boolean} resumeColdTargets  Whether a message to a closed session may resume it in this process.
 * @property {boolean} rescueOrphans    Whether a poller may resume a session that has mail waiting but no live Agent anywhere.
 * @property {number} rescueGraceMs     How long an envelope may sit before an orphan rescue is attempted (ms).
 * @property {number} rescueBackoffMs   Minimum gap between rescue attempts for the same session (ms).
 * @property {boolean} interject       Interject into a busy recipient's current turn (`steer`) instead of queueing for its next turn.
 * @property {number} pairLimit          How many messages two sessions may exchange inside `pairWindowMs` (0 disables the guard).
 * @property {number} pairWindowMs       Sliding window for `pairLimit`, counted in both directions (ms).
 * @property {boolean} enabled           Set false to mount no tools and run no loops.
 */

/** Defaults; every field can be overridden from the profile patch layer. */
export const DEFAULT_CONFIG = Object.freeze({
  stateDir: '',
  pollMs: 500,
  heartbeatMs: 2_000,
  presenceTtlMs: 15_000,
  retentionMs: 7 * 24 * 60 * 60 * 1_000,
  maxMessageBytes: 64 * 1024,
  ackWaitMs: 900,
  includeSubagentsInList: false,
  includeColdInList: true,
  coldListTtlMs: 5_000,
  resumeColdTargets: true,
  rescueOrphans: true,
  rescueGraceMs: 2_000,
  rescueBackoffMs: 15_000,
  interject: true,
  pairLimit: 10,
  pairWindowMs: 60_000,
  enabled: true,
})

/** Numeric bounds: below these values a config would spin or misbehave. */
const BOUNDS = Object.freeze({
  pollMs: [50, 60_000],
  heartbeatMs: [250, 600_000],
  presenceTtlMs: [1_000, 24 * 60 * 60 * 1_000],
  retentionMs: [60_000, 365 * 24 * 60 * 60 * 1_000],
  maxMessageBytes: [256, 4 * 1024 * 1024],
  ackWaitMs: [0, 30_000],
  coldListTtlMs: [250, 10 * 60 * 1_000],
  rescueGraceMs: [0, 300_000],
  rescueBackoffMs: [1_000, 30 * 60 * 1_000],
  pairLimit: [0, 1_000],
  pairWindowMs: [1_000, 60 * 60 * 1_000],
})

/** `$DSH_HOME`, or the platform default used by the dsh CLI. */
export function defaultDshHome() {
  const fromEnv = process.env.DSH_HOME?.trim()
  if (fromEnv) return resolve(fromEnv)
  return join(homedir(), '.dsh')
}

/** Default peer bus root: inside DSH_HOME so every profile of one installation shares it. */
export function defaultStateDir() {
  return join(defaultDshHome(), 'peer-messaging')
}

/** Expand a leading `~` in a config path. */
function expandTilde(value) {
  if (value === '~') return homedir()
  if (value.startsWith('~/') || value.startsWith(`~${sep}`)) return join(homedir(), value.slice(2))
  return value
}

/**
 * Normalise user config. Invalid or out-of-range numbers fall back to the
 * default with a warning instead of failing dsh boot: a broken knob in a UI
 * preference must not take the whole harness down.
 *
 * @param {unknown} input  Raw profile config (may be undefined/null).
 * @param {{ logger?: { warn: Function } }} [options]
 * @returns {PeerMessagingConfig}
 */
export function normalizeConfig(input, options = {}) {
  const logger = options.logger
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {}
  const known = new Set(Object.keys(DEFAULT_CONFIG))
  for (const key of Object.keys(raw)) {
    if (!known.has(key)) logger?.warn?.('unknown peer-messaging config key %C ignored', JSON.stringify(key))
  }

  const config = { ...DEFAULT_CONFIG }

  const rawStateDir = raw.stateDir
  if (typeof rawStateDir === 'string' && rawStateDir.trim() !== '') {
    config.stateDir = resolve(expandTilde(rawStateDir.trim()))
  } else {
    config.stateDir = defaultStateDir()
  }

  for (const [key, [min, max]] of Object.entries(BOUNDS)) {
    if (raw[key] === undefined || raw[key] === null) continue
    const value = Number(raw[key])
    if (!Number.isFinite(value)) {
      logger?.warn?.('peer-messaging config %C is not a number; using default %C', JSON.stringify(key), JSON.stringify(config[key]))
      continue
    }
    if (value < min || value > max) {
      logger?.warn?.('peer-messaging config %C=%C is outside [%C, %C]; clamping', JSON.stringify(key), value, min, max)
      config[key] = Math.min(max, Math.max(min, Math.round(value)))
      continue
    }
    config[key] = Math.round(value)
  }

  if (raw.includeSubagentsInList !== undefined) config.includeSubagentsInList = Boolean(raw.includeSubagentsInList)
  if (raw.includeColdInList !== undefined) config.includeColdInList = Boolean(raw.includeColdInList)
  if (raw.resumeColdTargets !== undefined) config.resumeColdTargets = Boolean(raw.resumeColdTargets)
  if (raw.rescueOrphans !== undefined) config.rescueOrphans = Boolean(raw.rescueOrphans)
  if (raw.interject !== undefined) config.interject = Boolean(raw.interject)
  if (raw.enabled !== undefined) config.enabled = Boolean(raw.enabled)

  return Object.freeze(config)
}
