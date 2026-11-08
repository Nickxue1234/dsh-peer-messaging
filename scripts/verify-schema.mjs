/**
 * Dev-only guard: hand-written tool schemas must be accepted by the *real*
 * dsh-tools validator of the installed dsh, not just by our unit tests.
 *
 *   node scripts/verify-schema.mjs
 *
 * Skips (exit 0) when dsh cannot be located, so it stays optional in CI.
 */
import { execFileSync } from 'node:child_process'
import { existsSync, realpathSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { pathToFileURL } from 'node:url'

function resolveDshRoot() {
  if (process.env.DSH_CHECKOUT && existsSync(join(process.env.DSH_CHECKOUT, 'package.json'))) {
    return process.env.DSH_CHECKOUT
  }
  try {
    const found = execFileSync('/bin/bash', ['-lc', 'command -v dsh'], { encoding: 'utf8' }).trim()
    // <root>/lib/bin.js -> <root> (resolve the global bin symlink first)
    return dirname(dirname(realpathSync(found)))
  } catch {
    return undefined
  }
}

const dshRoot = resolveDshRoot()
const toolsEntry = dshRoot ? join(dshRoot, 'node_modules/@deepseek-ai/dsh-tools/lib/index.js') : undefined
if (!toolsEntry || !existsSync(toolsEntry)) {
  console.log('verify-schema: skipped (cannot locate @deepseek-ai/dsh-tools; set DSH_CHECKOUT to the dsh package root)')
  process.exit(0)
}

const { assertObjectJsonSchema, assertSupportedJsonSchema } = await import(pathToFileURL(toolsEntry).href)
const { registerPeerTools } = await import(new URL('../lib/tools.js', import.meta.url).href)

/** @type {any[]} */
const registered = []
registerPeerTools(
  {
    tools: { register: (definition) => (registered.push(definition), () => {}) },
    logger: () => ({ warn() {}, info() {}, error() {}, debug() {} }),
    agents: { list: () => [], get: () => undefined },
    get: () => undefined,
    emit: () => {},
  },
  {
    config: { stateDir: '/tmp/peer-messaging-verify' },
    hostId: 'verify',
    describeAgent: () => null,
    listAgents: async () => [],
    bus: { listPending: async () => [] },
    send: async () => ({ ok: true }),
  },
)

for (const definition of registered) {
  assertObjectJsonSchema(definition.parameters)
  assertSupportedJsonSchema(definition.parameters)
  assertSupportedJsonSchema(definition.output.schema)
  console.log(`ok ${definition.name}: parameters + output accepted by ${dshRoot}`)
}
