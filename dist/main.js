// src/main.ts
import pino from 'pino';
import { bootListener } from './index.js'; // Assuming .js for compiled output
import { getTemporalClient, closeTemporalClient } from './temporalClient.js'; // Assuming .js
import { closePool } from './pg.js'; // pg.js is already JavaScript
import retry from 'p-retry'; // Import p-retry
const mainLogger = pino({ name: 'main-ts', level: process.env.LOG_LEVEL || 'info' });
async function main() {
    mainLogger.info('🚀 Starting application...');
    try {
        // Initialize the PostgreSQL listener with retry logic for initial boot
        mainLogger.info('Attempting initial boot of PostgreSQL listener...');
        await retry(bootListener, {
            retries: process.env.INITIAL_BOOT_RETRIES ? parseInt(process.env.INITIAL_BOOT_RETRIES, 10) : 5,
            minTimeout: process.env.INITIAL_BOOT_MIN_TIMEOUT_MS ? parseInt(process.env.INITIAL_BOOT_MIN_TIMEOUT_MS, 10) : 1000,
            maxTimeout: process.env.INITIAL_BOOT_MAX_TIMEOUT_MS ? parseInt(process.env.INITIAL_BOOT_MAX_TIMEOUT_MS, 10) : 30000,
            factor: 2.5,
            onFailedAttempt: error => {
                mainLogger.warn({
                    attemptNumber: error.attemptNumber,
                    retriesLeft: error.retriesLeft,
                    message: error.message,
                    code: error?.code,
                }, 'Initial listener boot attempt failed. Retrying...');
            },
        });
        mainLogger.info('✅ PostgreSQL listener booted successfully.');
        // Optionally, pre-warm the Temporal client connection
        // getTemporalClient also has internal error handling and will throw if connection fails
        mainLogger.info('Initializing Temporal client...');
        await getTemporalClient();
        mainLogger.info('✅ Temporal client initialized (or confirmed ready).');
        mainLogger.info('🎉 Application started successfully and is listening for events.');
    }
    catch (err) {
        mainLogger.fatal({
            err,
            message: err.message,
            stack: err.stack,
            originalError: err.originalError // p-retry wraps the original error
        }, '💥 Failed to start application after multiple retries');
        // Attempt graceful shutdown of any partially initialized components before exiting
        await shutdown(err.originalError || err);
        process.exit(1);
    }
}
async function shutdown(error) {
    if (error) {
        mainLogger.warn({ err: error }, `🚦 Initiating shutdown due to error: ${error.message}`);
    }
    else {
        mainLogger.info('🚦 Initiating graceful shutdown...');
    }
    try {
        await closeTemporalClient();
    }
    catch (err) {
        mainLogger.error({ err }, 'Error during Temporal client shutdown');
    }
    try {
        await closePool(); // Close the PostgreSQL connection pool
    }
    catch (err) {
        mainLogger.error({ err }, 'Error during PostgreSQL pool shutdown');
    }
    if (error) {
        mainLogger.info('🚪 Application shutdown complete (with error).');
    }
    else {
        mainLogger.info('🚪 Application shutdown complete.');
    }
    process.exit(error ? 1 : 0);
}
// Handle graceful shutdown signals
process.on('SIGINT', () => {
    mainLogger.info('Received SIGINT. Starting graceful shutdown...');
    shutdown();
});
process.on('SIGTERM', () => {
    mainLogger.info('Received SIGTERM. Starting graceful shutdown...');
    shutdown();
});
// Start the application
main().catch(async (err) => {
    // This catch is a fallback for unhandled promise rejections from main() itself,
    // though the try/catch inside main() should handle startup errors.
    mainLogger.fatal({ err, message: err.message }, '💥 Unhandled error during main execution sequence.');
    await shutdown(err); // Ensure shutdown is called
    process.exit(1);
});
