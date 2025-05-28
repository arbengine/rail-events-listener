import { WebSocketServer, WebSocket } from 'ws';

let wss: WebSocketServer;
let localLogger: any; // To store the passed logger

const WS_PORT = parseInt(process.env.WS_PORT || '8080', 10);

export function initializeWebSocketServer(loggerInstance: any) { // Accept logger
  localLogger = loggerInstance; // Store logger
  wss = new WebSocketServer({ port: WS_PORT });

  wss.on('listening', () => {
    localLogger.info({ port: WS_PORT }, '🌿 WebSocket server started and listening');
  });

  wss.on('connection', (ws) => {
    localLogger.info('🔗 Client connected to WebSocket');
    ws.on('message', (message) => {
      localLogger.info({ receivedMessage: message.toString() }, 'Received message from client');
    });
    ws.on('close', () => {
      localLogger.info('🔌 Client disconnected from WebSocket');
    });
    ws.on('error', (error) => {
      localLogger.error({ error }, '💥 WebSocket client error');
    });
  });

  wss.on('error', (error) => {
    localLogger.error({ error }, '💥 WebSocket server error');
  });

  localLogger.info(`Attempting to start WebSocket server on port ${WS_PORT}...`);
}

export function broadcast(data: object) {
  if (!wss) {
    if (localLogger) localLogger.warn('WebSocket server not initialized. Cannot broadcast.');
    else console.warn('WebSocket server not initialized. Cannot broadcast. Logger not available.');
    return;
  }

  const jsonData = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(jsonData, (err) => {
        if (err) {
          if (localLogger) localLogger.error({ err, targetClient: client.url }, 'Error sending message to WebSocket client');
          else console.error('Error sending message to WebSocket client. Logger not available.', err);
        }
      });
    }
  });
}

export function closeWebSocketServer(callback?: () => void) {
  if (wss) {
    if (localLogger) localLogger.info('Attempting to close WebSocket server...');
    else console.log('Attempting to close WebSocket server... Logger not available.');
    wss.close(() => {
      if (localLogger) localLogger.info('🚪 WebSocket server closed.');
      else console.log('🚪 WebSocket server closed. Logger not available.');
      if (callback) callback();
    });
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.terminate();
      }
    });
  } else {
    if (callback) callback();
  }
}
