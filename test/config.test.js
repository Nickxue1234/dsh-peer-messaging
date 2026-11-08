import assert from 'node:assert/strict'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'

import { DEFAULT_CONFIG, defaultDshHome, defaultStateDir, normalizeConfig } from '../lib/config.js'

const warnings = []
const logger = { warn: (...args) => warnings.push(args), info() {} }

test('defaults are stable and produce an absolute state dir', () => {
  const config = normalizeConfig(undefined, { logger })
  assert.equal(config.pollMs, DEFAULT_CONFIG.pollMs)
  assert.equal(config.enabled, true)
  assert.equal(config.includeSubagentsInList, false)
  assert.ok(config.stateDir.startsWith('/'), 'stateDir must be absolute')
  assert.equal(config.stateDir, join(defaultDshHome(), 'peer-messaging'))
  assert.ok(Object.isFrozen(config))
})

test('DSH_HOME is respected at call time', () => {
  const previous = process.env.DSH_HOME
  try {
    process.env.DSH_HOME = '/tmp/dsh-home-test'
    assert.equal(defaultDshHome(), resolve('/tmp/dsh-home-test'))
    assert.equal(defaultStateDir(), resolve('/tmp/dsh-home-test', 'peer-messaging'))
  } finally {
    if (previous === undefined) delete process.env.DSH_HOME
    else process.env.DSH_HOME = previous
  }
})

test('explicit stateDir wins and expands ~ and relative paths', () => {
  assert.equal(normalizeConfig({ stateDir: '/var/lib/peers' }).stateDir, resolve('/var/lib/peers'))
  assert.equal(normalizeConfig({ stateDir: '~/peers' }).stateDir, join(homedir(), 'peers'))
  assert.equal(normalizeConfig({ stateDir: 'relative/peers' }).stateDir, resolve('relative/peers'))
  assert.equal(normalizeConfig({ stateDir: '   ' }).stateDir, defaultStateDir())
})

test('numeric values are rounded and clamped', () => {
  assert.equal(normalizeConfig({ pollMs: 1 }).pollMs, 50)
  assert.equal(normalizeConfig({ pollMs: 10_000_000 }).pollMs, 60_000)
  assert.equal(normalizeConfig({ pollMs: 750.6 }).pollMs, 751)
  assert.equal(normalizeConfig({ ackWaitMs: -1 }).ackWaitMs, 0)
  assert.equal(normalizeConfig({ ackWaitMs: 99_999 }).ackWaitMs, 30_000)
  assert.equal(normalizeConfig({ maxMessageBytes: 1 }).maxMessageBytes, 256)
})

test('invalid values fall back with a warning instead of throwing', () => {
  warnings.length = 0
  const config = normalizeConfig({ pollMs: 'soon', heartbeatMs: Number.NaN, mystery: true }, { logger })
  assert.equal(config.pollMs, DEFAULT_CONFIG.pollMs)
  assert.equal(config.heartbeatMs, DEFAULT_CONFIG.heartbeatMs)
  assert.ok(warnings.length >= 3, `expected warnings, got ${warnings.length}`)
  assert.ok(warnings.some(([message]) => String(message).includes('unknown')))
})

test('orphan rescue knobs have safe defaults and clamp', () => {
  const config = normalizeConfig(undefined, { logger })
  assert.equal(config.rescueOrphans, true)
  assert.equal(config.rescueGraceMs, 2_000)
  assert.equal(config.rescueBackoffMs, 15_000)
  assert.equal(normalizeConfig({ rescueOrphans: false }).rescueOrphans, false)
  assert.equal(normalizeConfig({ rescueGraceMs: -5 }).rescueGraceMs, 0)
  assert.equal(normalizeConfig({ rescueBackoffMs: 10 }).rescueBackoffMs, 1_000)
})

test('booleans are coerced, not string-compared', () => {
  assert.equal(normalizeConfig({ enabled: false }).enabled, false)
  assert.equal(normalizeConfig({ enabled: 'no' }).enabled, true)
  assert.equal(normalizeConfig({ includeSubagentsInList: 1 }).includeSubagentsInList, true)
})

test('normalizeConfig never mutates its input', () => {
  const input = Object.freeze({ pollMs: 100, stateDir: '/tmp/x' })
  assert.doesNotThrow(() => normalizeConfig(input))
})
