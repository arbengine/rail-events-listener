// workers/rail-events-listener/src/main.ts – entrypoint wrapper
// ---------------------------------------------------------------------------
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'; // ⚠️ dev-only: trust self-signed certs

import pino from 'pino';
import pRetry from 'p-retry';
import { bootListener } from './index.js'; // compiled JS path
import { getTemporalClient, closeTemporalClient } from './temporalClient.js';
import { closePool } from './pg.js';

const log = (pino as any)({ name: 'main', level: process.env.LOG_LEVEL || 'info' });

async function main(): Promise<void> {
  log.info('🚀 Starting rail-events-listener…');

  try {
    log.info('Booting PostgreSQL listener…');
    await pRetry(bootListener, {
      retries: Number(process.env.INITIAL_BOOT_RETRIES ?? 5),
      minTimeout: Number(process.env.INITIAL_BOOT_MIN_TIMEOUT_MS ?? 1_000),
      maxTimeout: Number(process.env.INITIAL_BOOT_MAX_TIMEOUT_MS ?? 30_000),
      factor: 2.5,
      onFailedAttempt: (err) =>
        log.warn({ attempt: err.attemptNumber, left: err.retriesLeft, msg: err.message }, 'Listener boot failed'),
    });
    log.info('✅ PostgreSQL listener ready');

    log.info('Pre-warming Temporal client…');
    await getTemporalClient();
    log.info('✅ Temporal client ready');

    log.info('🎉 Application fully started and listening for events');
  } catch (err: any) {
    log.fatal({ err }, '💥 Startup failed');
    await shutdown(err);
    process.exit(1);
  }
}

async function shutdown(cause?: Error) {
  if (cause) log.warn({ cause }, '🚦 Shutting down due to error');
  try { await closeTemporalClient(); } catch {}
  try { await closePool(); } catch {}
  log.info('Shutdown complete');
}

process.on('SIGINT', () => shutdown().then(() => process.exit(0)));
process.on('SIGTERM', () => shutdown().then(() => process.exit(0)));

main().catch(async (err) => {
  log.fatal({ err }, 'Unhandled rejection in main');
  await shutdown(err);
  process.exit(1);
});