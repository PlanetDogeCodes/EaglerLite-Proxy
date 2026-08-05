#!/usr/bin/env node
/**
 * EaglerLite Hybrid Proxy Server
 *
 * Serves:
 * 1. Epoxy-TLS standalone JS + WASM (as static files for <script> tag loading)
 * 2. Wisp protocol server (for epoxy-tls TCP/TLS tunneling)
 * 3. Simple WebSocket proxy (fallback for servers that accept server-side WS)
 *
 * DEPLOYMENT (Render.com):
 *   1. Create a new Web Service on render.com
 *   2. Upload: proxy-server.js, package.json, epoxy-standalone.js
 *   3. Build: npm install
 *   4. Start: node proxy-server.js
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

let WebSocket;
try { WebSocket = require('ws'); } catch (e) { console.error('ERROR: npm install ws'); process.exit(1); }

let WispServer;
try { WispServer = require('wisp-server-node'); } catch (e) { console.log('[INFO] wisp-server-node not installed, wisp proxy disabled'); }

const PORT = process.env.PORT || 8080;
const MAX_CONN_PER_IP = parseInt(process.env.MAX_CONN_PER_IP || '10', 10);
const PROXY_API_KEY = process.env.PROXY_API_KEY || '';

// Cache static files in memory
let EPOXY_JS_CACHE = null;
try {
    EPOXY_JS_CACHE = fs.readFileSync(path.join(__dirname, 'epoxy-standalone.js'));
    console.log(`[INFO] Cached epoxy-standalone.js (${EPOXY_JS_CACHE.length} bytes)`);
} catch (e) {
    console.log('[WARN] epoxy-standalone.js not found, epoxy fallback disabled');
}

// Rate limiting
const connCounts = new Map();
function checkRateLimit(ip) {
    if (MAX_CONN_PER_IP <= 0) return true;
    const count = connCounts.get(ip) || 0;
    if (count >= MAX_CONN_PER_IP) return false;
    connCounts.set(ip, count + 1);
    return true;
}
function releaseConn(ip) {
    if (MAX_CONN_PER_IP <= 0) return;
    const count = connCounts.get(ip) || 0;
    if (count <= 1) connCounts.delete(ip);
    else connCounts.set(ip, count - 1);
}

// HTTP server
const server = http.createServer((req, res) => {
    const parsedUrl = new URL(req.url, `http://localhost:${PORT}`);
    const pathname = parsedUrl.pathname;

    if (pathname === '/' || pathname === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('EaglerLite Hybrid Proxy — OK');
        return;
    }

    if (pathname === '/epoxy.js' && EPOXY_JS_CACHE) {
        res.writeHead(200, {
            'Content-Type': 'application/javascript',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=3600'
        });
        res.end(EPOXY_JS_CACHE);
        return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found. Use /health, /epoxy.js, or WebSocket.');
});

// Simple WebSocket proxy (for servers that accept server-side WS)
const wss = new WebSocket.Server({ server, maxPayload: 16 * 1024 * 1024 });

wss.on('connection', (clientWs, req) => {
    const clientIp = req.socket.remoteAddress;
    let parsedUrl;
    try { parsedUrl = new URL(req.url, 'http://localhost'); } catch (e) {
        clientWs.close(1008, 'Invalid URL');
        return;
    }

    // Check if this is a wisp connection (no ?target= param)
    const targetUrl = parsedUrl.searchParams.get('target');
    if (!targetUrl) {
        // If wisp server is available, let it handle this connection
        if (WispServer) {
            // Wisp connections are handled by the wisp server instance (see below)
            // This code path shouldn't be reached if wisp is set up correctly
            clientWs.close(1008, 'Wisp connections should go through the wisp handler');
        } else {
            clientWs.close(1008, 'Missing target URL');
        }
        return;
    }

    const key = parsedUrl.searchParams.get('key');
    if (PROXY_API_KEY && key !== PROXY_API_KEY) {
        clientWs.close(1008, 'Unauthorized');
        return;
    }

    let parsedTarget;
    try { parsedTarget = new URL(targetUrl); } catch (e) {
        clientWs.close(1008, 'Invalid target URL');
        return;
    }
    if (parsedTarget.protocol !== 'ws:' && parsedTarget.protocol !== 'wss:') {
        clientWs.close(1008, 'Target must be ws:// or wss://');
        return;
    }
    if (!checkRateLimit(clientIp)) {
        clientWs.close(1008, 'Too many connections');
        return;
    }

    console.log(`[WS-PROXY] ${clientIp} → ${targetUrl}`);

    const protosHeader = req.headers['sec-websocket-protocol'];
    const protos = protosHeader ? protosHeader.split(',').map(s => s.trim()) : undefined;

    let targetWs;
    try {
        targetWs = new WebSocket(targetUrl, protos, {
            headers: {
                'Origin': 'https://eaglerlite-proxy.local',
                'User-Agent': 'EaglerLite-WS-Proxy/1.0'
            },
            rejectUnauthorized: process.env.DISABLE_TLS_CHECK === 'true'
        });
    } catch (e) {
        clientWs.close(1008, 'Failed to connect to target');
        releaseConn(clientIp);
        return;
    }

    let targetOpened = false;
    let closed = false;

    function cleanup() {
        if (closed) return;
        closed = true;
        releaseConn(clientIp);
        try { if (targetWs.readyState === WebSocket.OPEN || targetWs.readyState === WebSocket.CONNECTING) targetWs.close(); } catch(_) {}
        try { if (clientWs.readyState === WebSocket.OPEN || clientWs.readyState === WebSocket.CONNECTING) clientWs.close(); } catch(_) {}
    }

    targetWs.on('open', () => { targetOpened = true; console.log(`[WS-PROXY OPEN] ${clientIp} → ${targetUrl}`); });
    targetWs.on('message', (data, isBinary) => {
        if (clientWs.readyState === WebSocket.OPEN) { try { clientWs.send(data, { binary: isBinary }); } catch(_) { cleanup(); } }
    });
    clientWs.on('message', (data, isBinary) => {
        if (targetWs.readyState === WebSocket.OPEN) { try { targetWs.send(data, { binary: isBinary }); } catch(_) { cleanup(); } }
    });
    targetWs.on('close', (code, reason) => {
        console.log(`[WS-PROXY CLOSE] ${clientIp} ← ${targetUrl} (code: ${code})`);
        if (clientWs.readyState === WebSocket.OPEN) { try { clientWs.close(code, reason); } catch(_) {} }
        cleanup();
    });
    clientWs.on('close', () => { console.log(`[WS-PROXY CLOSE] ${clientIp} (disconnected)`); cleanup(); });
    targetWs.on('error', (err) => {
        console.log(`[WS-PROXY ERROR] ${clientIp} — ${err.message}`);
        if (!targetOpened) { try { clientWs.close(1011, 'Target connection failed'); } catch(_) {} }
        cleanup();
    });
    clientWs.on('error', () => { cleanup(); });
});

// Wisp server (for epoxy-TLS tunneling)
// The wisp server needs to intercept WebSocket connections WITHOUT ?target= param.
// We use a separate WebSocket server on a different path for wisp.
if (WispServer) {
    const wispWss = new WebSocket.Server({ noServer: true });
    
    server.on('upgrade', (req, socket, head) => {
        const parsedUrl = new URL(req.url, `http://localhost:${PORT}`);
        const targetUrl = parsedUrl.searchParams.get('target');
        
        // If no ?target= param, route to wisp server
        if (!targetUrl) {
            wispWss.handleUpgrade(req, socket, head, (ws) => {
                console.log(`[WISP] New wisp connection from ${req.socket.remoteAddress}`);
                // Wisp server handles the connection
            });
        }
        // Otherwise, let the default WebSocket.Server handle it (simple WS proxy)
    });
    
    try {
        // Start wisp server using the wispWss
        WispServer.startWispProxy({
            port: 0, // Don't listen on a separate port
            websocketServer: wispWss,
            logLevel: 'info'
        });
        console.log('[INFO] Wisp server started');
    } catch (e) {
        console.log('[WARN] Failed to start wisp server:', e.message);
    }
}

server.listen(PORT, () => {
    console.log(`[EaglerLite Hybrid Proxy] Listening on port ${PORT}`);
    console.log(`  /health     — health check`);
    console.log(`  /epoxy.js   — epoxy standalone script (${EPOXY_JS_CACHE ? EPOXY_JS_CACHE.length + ' bytes' : 'not loaded'})`);
    console.log(`  WS ?target= — simple WS proxy`);
    console.log(`  WS (no target) — wisp proxy (${WispServer ? 'enabled' : 'disabled'})`);
    console.log(`  Max conns/IP: ${MAX_CONN_PER_IP}`);
    console.log(`  API key: ${PROXY_API_KEY ? 'enabled' : 'disabled'}`);
});
