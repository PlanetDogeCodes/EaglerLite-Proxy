#!/usr/bin/env node
/*
 EaglerLite WebSocket Proxy Server
 License: Apache 2.0
 */

const http = require('http');
const https = require('https');
const { URL } = require('url');

// Try to load ws (WebSocket library)
let WebSocket;
try {
    WebSocket = require('ws');
} catch (e) {
    console.error('ERROR: "ws" package not found. Run: npm install ws');
    process.exit(1);
}

const PORT = process.env.PORT || 8080;
const API_KEY = process.env.PROXY_API_KEY || '';
const MAX_CONN_PER_IP = parseInt(process.env.MAX_CONN_PER_IP || '0', 10);

// --- Rate limiting ---
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

// --- HTTP server (health check + WebSocket upgrade) ---
const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('EaglerLite WebSocket Proxy — OK');
        return;
    }
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found. Connect via WebSocket.');
});

const wss = new WebSocket.Server({ server, maxPayload: 16 * 1024 * 1024 });

wss.on('connection', (clientWs, req) => {
    const clientIp = req.socket.remoteAddress;

    // Parse the target URL from query string
    let parsedUrl;
    try {
        parsedUrl = new URL(req.url, 'http://localhost');
    } catch (e) {
        clientWs.close(1008, 'Invalid URL');
        return;
    }

    const targetUrl = parsedUrl.searchParams.get('target');
    const key = parsedUrl.searchParams.get('key');

    // Check API key if configured
    if (API_KEY && key !== API_KEY) {
        console.log(`[REJECT] ${clientIp} — invalid or missing API key`);
        clientWs.close(1008, 'Unauthorized');
        return;
    }

    // Validate target URL
    if (!targetUrl) {
        console.log(`[REJECT] ${clientIp} — no target parameter`);
        clientWs.close(1008, 'Missing target URL');
        return;
    }

    let parsedTarget;
    try {
        parsedTarget = new URL(targetUrl);
    } catch (e) {
        console.log(`[REJECT] ${clientIp} — invalid target URL: ${targetUrl}`);
        clientWs.close(1008, 'Invalid target URL');
        return;
    }

    if (parsedTarget.protocol !== 'ws:' && parsedTarget.protocol !== 'wss:') {
        console.log(`[REJECT] ${clientIp} — invalid target protocol: ${parsedTarget.protocol}`);
        clientWs.close(1008, 'Target must be ws:// or wss://');
        return;
    }

    // Check rate limit
    if (!checkRateLimit(clientIp)) {
        console.log(`[REJECT] ${clientIp} — rate limited`);
        clientWs.close(1008, 'Too many connections');
        return;
    }

    console.log(`[CONNECT] ${clientIp} → ${targetUrl}`);

    // Connect to the target server
    // Node.js handles TLS natively for wss:// — no WASM needed!
    let targetWs;
    try {
        targetWs = new WebSocket(targetUrl, {
            // Set a real Origin so the target doesn't see "null"
            headers: {
                'Origin': 'https://eaglerlite-proxy.local',
                'User-Agent': 'EaglerLite-WS-Proxy/1.0'
            },
            // Don't reject self-signed certs by default (some game servers use them)
            rejectUnauthorized: false
        });
    } catch (e) {
        console.log(`[ERROR] ${clientIp} — failed to create target WebSocket: ${e.message}`);
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

    // Target → Client
    targetWs.on('open', () => {
        targetOpened = true;
        console.log(`[OPEN] ${clientIp} → ${targetUrl}`);
    });

    targetWs.on('message', (data, isBinary) => {
        if (clientWs.readyState === WebSocket.OPEN) {
            try { clientWs.send(data, { binary: isBinary }); } catch(_) { cleanup(); }
        }
    });

    // Client → Target
    clientWs.on('message', (data, isBinary) => {
        if (targetWs.readyState === WebSocket.OPEN) {
            try { targetWs.send(data, { binary: isBinary }); } catch(_) { cleanup(); }
        }
    });

    // Close handlers
    targetWs.on('close', (code, reason) => {
        console.log(`[CLOSE] ${clientIp} ← ${targetUrl} (code: ${code})`);
        if (clientWs.readyState === WebSocket.OPEN) {
            try { clientWs.close(code, reason); } catch(_) {}
        }
        cleanup();
    });

    clientWs.on('close', () => {
        console.log(`[CLOSE] ${clientIp} (client disconnected)`);
        cleanup();
    });

    // Error handlers
    targetWs.on('error', (err) => {
        console.log(`[ERROR] ${clientIp} — target error: ${err.message}`);
        if (!targetOpened) {
            try { clientWs.close(1008, 'Target connection failed'); } catch(_) {}
        }
        cleanup();
    });

    clientWs.on('error', () => {
        cleanup();
    });
});

server.listen(PORT, () => {
    console.log(`[EaglerLite WebSocket Proxy] Listening on port ${PORT}`);
    console.log(`[EaglerLite WebSocket Proxy] API key: ${API_KEY ? 'enabled' : 'disabled'}`);
    console.log(`[EaglerLite WebSocket Proxy] Max conns/IP: ${MAX_CONN_PER_IP || 'unlimited'}`);
});
