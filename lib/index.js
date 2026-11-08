/**
 * dsh-peer-messaging — let one agent message agents that live in other
 * workspaces (other sessions of the same dsh process, and other dsh processes
 * sharing the same state directory).
 *
 * Plugin contract: an object plugin exporting `name`, `inject`, `apply`.
 * No `Config` export on purpose — cordis then passes the raw profile config
 * straight through and ./config.js performs its own validation, which keeps
 * this package free of runtime dependencies.
 */
import { normalizeConfig } from './config.js'
import { PeerMessaging } from './service.js'
import { registerPeerTools } from './tools.js'

export const name = 'peer-messaging'

/** `tools` for registration, `agents` to enumerate and wake live sessions. */
export const inject = ['tools', 'agents']

/**
 * @param {import('cordis').Context} ctx
 * @param {unknown} rawConfig
 */
export function apply(ctx, rawConfig) {
  const logger = ctx.logger('peer-messaging')
  const config = normalizeConfig(rawConfig && typeof rawConfig === 'object' ? rawConfig : {}, { logger })
  if (!config.enabled) {
    logger.warn('peer-messaging: disabled by configuration, no tools registered')
    return
  }

  const service = new PeerMessaging(ctx, config)
  const disposers = registerPeerTools(ctx, service)
  service.start()

  ctx.effect(() => async () => {
    for (const dispose of disposers) {
      try {
        dispose()
      } catch (error) {
        logger.warn('peer-messaging: failed to unregister a tool: %s', error instanceof Error ? error.message : String(error))
      }
    }
    await service.stop()
  }, 'peer-messaging: shutdown')

  logger.info(
    'peer-messaging: state %s, poll %dms, presence ttl %dms, ack window %dms',
    config.stateDir,
    config.pollMs,
    config.presenceTtlMs,
    config.ackWaitMs,
  )
}
