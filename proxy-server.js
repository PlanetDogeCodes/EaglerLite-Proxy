#!/usr/bin/env node
/**
 * EaglerLite Hybrid Proxy Server — v2
 *
 * Routes WebSocket connections:
 * - URL with "?target=" → simple WS-to-WS proxy (with heartbeat)
 * - URL with "?warmup=1" → immediate close (TLS pre-warming probe)
 * - URL ending with "/" or no params → wisp protocol (for epoxy-TLS tunneling)
 *
 * Also serves:
 *   /health    — liveness probe (200 OK)
 *   /stats     — JSON stats (active conns, total served, uptime)
 *   /epoxy.js  — epoxy standalone JS (cached in memory)
 *   /epoxy.wasm — epoxy wasm (cached in memory)
 *
 * Reliability features:
 *   - Per-connection heartbeat: server pings every 25s, terminates on no-pong for 10s
 *   - Upstream-dead detection: if target WS dies, client WS is closed promptly
 *   - Idle timeout: 5min idle connections are terminated
 *   - Structured logging with timestamps
 *   - Graceful shutdown on SIGTERM/SIGINT (for Render redeployments)
 *
 * DEPLOYMENT (Render.com):
 *   Upload: proxy-server.js, package.json, epoxy-standalone.js, epoxy.wasm
 *   Build:  npm install
 *   Start:  node proxy-server.js
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

let WebSocket;
try { WebSocket = require('ws'); } catch (e) { console.error('FATAL: npm install ws'); process.exit(1); }

let wisp;
try { wisp = require('wisp-server-node'); } catch (e) { console.log('[WARN] wisp-server-node not installed — epoxy-TLS layer disabled'); }

const PORT = process.env.PORT || 8080;
const MAX_CONN_PER_IP = parseInt(process.env.MAX_CONN_PER_IP || '10', 10);
const HEARTBEAT_INTERVAL_MS = 25000;  // ping every 25s
const HEARTBEAT_TIMEOUT_MS = 10000;   // wait 10s for pong before terminating
const IDLE_TIMEOUT_MS = 5 * 60 * 1000; // 5min idle = terminate
const UPSTREAM_CONNECT_TIMEOUT_MS = 8000; // 8s to establish upstream connection

// ---------- Stats ----------
const stats = {
    startedAt: Date.now(),
    totalConnections: 0,
    activeConnections: 0,
    totalProxied: 0,
    totalWisp: 0,
    totalWarmup: 0,
    failedConnections: 0,
    lastHealthCheck: 0
};

// ---------- Static files cache ----------
let EPOXY_JS = null;
let EPOXY_WASM = null;
try {
    EPOXY_JS = fs.readFileSync(path.join(__dirname, 'epoxy-standalone.js'));
    console.log(`[INFO] Cached epoxy-standalone.js (${EPOXY_JS.length} bytes)`);
} catch (e) { console.log('[WARN] epoxy-standalone.js not found'); }
try {
    EPOXY_WASM = fs.readFileSync(path.join(__dirname, 'epoxy.wasm'));
    console.log(`[INFO] Cached epoxy.wasm (${EPOXY_WASM.length} bytes)`);
} catch (e) { console.log('[WARN] epoxy.wasm not found'); }

// ---------- Rate limiting ----------
const connCounts = new Map();
function checkRate(ip) {
    if (MAX_CONN_PER_IP <= 0) return true;
    const c = connCounts.get(ip) || 0;
    if (c >= MAX_CONN_PER_IP) return false;
    connCounts.set(ip, c + 1);
    return true;
}
function release(ip) {
    if (MAX_CONN_PER_IP <= 0) return;
    const c = connCounts.get(ip) || 0;
    if (c <= 1) connCounts.delete(ip);
    else connCounts.set(ip, c - 1);
}

// ---------- Logging helpers ----------
function ts() { return new Date().toISOString(); }
function log(tag, msg) { console.log(`[${ts()}] [${tag}] ${msg}`); }
function logErr(tag, msg) { console.error(`[${ts()}] [${tag}] ${msg}`); }

// ---------- HTTP server ----------
const server = http.createServer((req, res) => {
    // Always set keep-alive friendly headers
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Keep-Alive', 'timeout=5, max=1000');

    let parsedUrl;
    try { parsedUrl = new URL(req.url, `http://localhost:${PORT}`); } catch (e) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Bad URL');
        return;
    }

    // Health check (also hit by the Cloudflare Worker keep-alive pinger)
    if (parsedUrl.pathname === '/' || parsedUrl.pathname === '/health') {
        stats.lastHealthCheck = Date.now();
        res.writeHead(200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-store'
        });
        res.end(JSON.stringify({
            ok: true,
            service: 'EaglerLite Hybrid Proxy',
            version: 2,
            uptime: Math.floor((Date.now() - stats.startedAt) / 1000),
            active: stats.activeConnections,
            total: stats.totalConnections,
            wisp: wisp ? 'enabled' : 'disabled'
        }));
        return;
    }

    // Stats endpoint (for monitoring)
    if (parsedUrl.pathname === '/stats') {
        res.writeHead(200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-store'
        });
        res.end(JSON.stringify({
            ...stats,
            uptimeSeconds: Math.floor((Date.now() - stats.startedAt) / 1000),
            memMB: Math.round(process.memoryUsage().rss / 1024 / 1024)
        }));
        return;
    }

    if (parsedUrl.pathname === '/epoxy.js' && EPOXY_JS) {
        res.writeHead(200, {
            'Content-Type': 'application/javascript',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=3600'
        });
        res.end(EPOXY_JS);
        return;
    }

    if (parsedUrl.pathname === '/epoxy.wasm' && EPOXY_WASM) {
        res.writeHead(200, {
            'Content-Type': 'application/wasm',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=3600'
        });
        res.end(EPOXY_WASM);
        return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
});

// ---------- WebSocket upgrade handler ----------
server.on('upgrade', (req, socket, head) => {
    const clientIp = req.socket.remoteAddress;
    let parsedUrl;
    try { parsedUrl = new URL(req.url, `http://localhost:${PORT}`); } catch (e) {
        socket.destroy();
        return;
    }

    const targetUrl = parsedUrl.searchParams.get('target');
    const warmup = parsedUrl.searchParams.get('warmup');

    stats.totalConnections++;
    stats.activeConnections++;

    if (warmup === '1') {
        // TLS/WS pre-warming probe — accept then immediately close
        stats.totalWarmup++;
        handleWarmupProbe(req, socket, head, clientIp);
    } else if (targetUrl) {
        // Simple WS proxy: has ?target= parameter
        stats.totalProxied++;
        handleSimpleProxy(req, socket, head, targetUrl, clientIp);
    } else if (wisp) {
        // Wisp protocol: no ?target= parameter
        stats.totalWisp++;
        try {
            wisp.routeRequest(req, socket, head, {
                logLevel: 1,
                pingInterval: 30
            });
            // Note: wisp handles its own connection lifecycle; we can't easily
            // track when it closes. The activeConnections counter is approximate
            // for wisp connections — we decrement on socket close.
            socket.on('close', () => {
                stats.activeConnections = Math.max(0, stats.activeConnections - 1);
            });
        } catch (e) {
            logErr('WISP', `Error: ${e.message}`);
            socket.destroy();
            stats.activeConnections = Math.max(0, stats.activeConnections - 1);
            stats.failedConnections++;
        }
    } else {
        socket.destroy();
        stats.activeConnections = Math.max(0, stats.activeConnections - 1);
        stats.failedConnections++;
    }
});

// ---------- Warmup probe handler ----------
// Accepts the WS upgrade then immediately closes with code 1000.
// This warms the TCP+TLS+WS handshake for the next connection from the same client.
function handleWarmupProbe(req, socket, head, clientIp) {
    const wss = new WebSocket.Server({ noServer: true });
    wss.handleUpgrade(req, socket, head, (ws) => {
        log('WARMUP', `Probe from ${clientIp}`);
        try { ws.close(1000, 'warmup'); } catch (_) {}
    });
    // Don't count against rate limit — warmup is harmless
    // Don't decrement activeConnections here; the close event will do it
}

// ---------- Simple WS-to-WS proxy with heartbeat ----------
function handleSimpleProxy(req, socket, head, targetUrl, clientIp) {
    if (!checkRate(clientIp)) {
        log('WS-PROXY', `Rate-limited ${clientIp}`);
        socket.destroy();
        stats.activeConnections = Math.max(0, stats.activeConnections - 1);
        stats.failedConnections++;
        return;
    }

    let parsedTarget;
    try { parsedTarget = new URL(targetUrl); } catch (e) {
        logErr('WS-PROXY', `Bad target URL from ${clientIp}: ${targetUrl}`);
        socket.destroy();
        release(clientIp);
        stats.activeConnections = Math.max(0, stats.activeConnections - 1);
        stats.failedConnections++;
        return;
    }
    if (parsedTarget.protocol !== 'ws:' && parsedTarget.protocol !== 'wss:') {
        logErr('WS-PROXY', `Non-WS protocol from ${clientIp}: ${parsedTarget.protocol}`);
        socket.destroy();
        release(clientIp);
        stats.activeConnections = Math.max(0, stats.activeConnections - 1);
        stats.failedConnections++;
        return;
    }

    log('WS-PROXY', `${clientIp} → ${targetUrl}`);

    const protosHeader = req.headers['sec-websocket-protocol'];
    const protos = protosHeader ? protosHeader.split(',').map(s => s.trim()) : undefined;

    const wss = new WebSocket.Server({ noServer: true });
    wss.handleUpgrade(req, socket, head, (clientWs) => {
        let targetWs;
        try {
            targetWs = new WebSocket(targetUrl, protos, {
                headers: {
                    'Origin': 'https://eaglerlite-proxy.local',
                    'User-Agent': 'EaglerLite-WS-Proxy/2.0'
                },
                rejectUnauthorized: false
                // Note: ws library auto-negotiates perMessageDeflate by default;
                // explicitly enabling it here can cause message flow issues with
                // some target servers. Let ws use its defaults instead.
            });
        } catch (e) {
            try { clientWs.close(1011, 'Failed to connect to target'); } catch (_) {}
            release(clientIp);
            stats.activeConnections = Math.max(0, stats.activeConnections - 1);
            stats.failedConnections++;
            return;
        }

        let opened = false;
        let closed = false;
        let lastActivity = Date.now();
        let lastPong = Date.now();
        let heartbeatTimer = null;
        let idleTimer = null;
        let connectTimeout = null;
        let pendingClientMessages = []; // queued until target opens

        function clearTimers() {
            if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
            if (idleTimer) { clearInterval(idleTimer); idleTimer = null; }
            if (connectTimeout) { clearTimeout(connectTimeout); connectTimeout = null; }
        }

        function flushPending() {
            if (pendingClientMessages.length === 0) return;
            if (targetWs.readyState !== WebSocket.OPEN) return;
            for (const msg of pendingClientMessages) {
                try { targetWs.send(msg.data, { binary: msg.isBinary }); } catch (_) {}
            }
            pendingClientMessages = [];
        }

        function cleanup() {
            if (closed) return;
            closed = true;
            clearTimers();
            pendingClientMessages = [];
            release(clientIp);
            stats.activeConnections = Math.max(0, stats.activeConnections - 1);
            try { if (targetWs.readyState === WebSocket.OPEN || targetWs.readyState === WebSocket.CONNECTING) targetWs.close(); } catch (_) {}
            try { if (clientWs.readyState === WebSocket.OPEN || clientWs.readyState === WebSocket.CONNECTING) clientWs.close(); } catch (_) {}
        }

        // Upstream connect timeout — if target doesn't open within 8s, kill it
        connectTimeout = setTimeout(() => {
            if (!opened) {
                logErr('WS-PROXY', `Connect timeout ${clientIp} → ${targetUrl}`);
                try { clientWs.close(1011, 'Upstream connect timeout'); } catch (_) {}
                cleanup();
            }
        }, UPSTREAM_CONNECT_TIMEOUT_MS);

        // Heartbeat: ping every 25s, kill if no pong for 10s
        heartbeatTimer = setInterval(() => {
            if (closed) return;
            try {
                if (targetWs.readyState === WebSocket.OPEN) targetWs.ping();
                if (clientWs.readyState === WebSocket.OPEN) clientWs.ping();
            } catch (_) {}
            if (Date.now() - lastPong > HEARTBEAT_INTERVAL_MS + HEARTBEAT_TIMEOUT_MS) {
                logErr('WS-PROXY', `Heartbeat timeout ${clientIp} → ${targetUrl}`);
                try { clientWs.close(1001, 'Heartbeat timeout'); } catch (_) {}
                cleanup();
            }
        }, HEARTBEAT_INTERVAL_MS);

        // Idle timeout — kill connections that haven't seen activity in 5min
        idleTimer = setInterval(() => {
            if (closed) return;
            if (Date.now() - lastActivity > IDLE_TIMEOUT_MS) {
                log('WS-PROXY', `Idle timeout ${clientIp} → ${targetUrl}`);
                try { clientWs.close(1001, 'Idle timeout'); } catch (_) {}
                cleanup();
            }
        }, 30000);

        targetWs.on('open', () => {
            opened = true;
            if (connectTimeout) { clearTimeout(connectTimeout); connectTimeout = null; }
            log('WS-PROXY', `OPEN ${clientIp} → ${targetUrl}`);
            flushPending();
        });
        targetWs.on('pong', () => { lastPong = Date.now(); lastActivity = Date.now(); });
        targetWs.on('message', (data, isBinary) => {
            lastActivity = Date.now();
            if (clientWs.readyState === WebSocket.OPEN) {
                try { clientWs.send(data, { binary: isBinary }); } catch (_) { cleanup(); }
            }
        });
        clientWs.on('message', (data, isBinary) => {
            lastActivity = Date.now();
            if (targetWs.readyState === WebSocket.OPEN) {
                try { targetWs.send(data, { binary: isBinary }); } catch (_) { cleanup(); }
            } else if (targetWs.readyState === WebSocket.CONNECTING) {
                // Queue message until target opens
                pendingClientMessages.push({ data, isBinary });
            }
        });
        clientWs.on('pong', () => { lastPong = Date.now(); lastActivity = Date.now(); });

        targetWs.on('close', (code, reason) => {
            log('WS-PROXY', `CLOSE ${clientIp} ← ${targetUrl} (code=${code})`);
            if (clientWs.readyState === WebSocket.OPEN) {
                try { clientWs.close(code, reason); } catch (_) {}
            }
            cleanup();
        });
        clientWs.on('close', () => {
            log('WS-PROXY', `CLOSE ${clientIp} disconnected`);
            cleanup();
        });
        targetWs.on('error', (err) => {
            logErr('WS-PROXY', `ERROR ${clientIp} — ${err.message}`);
            if (!opened) {
                try { clientWs.close(1011, 'Target failed'); } catch (_) {}
            }
            cleanup();
        });
        clientWs.on('error', () => { cleanup(); });
    });
}

// ---------- Periodic stats log ----------
setInterval(() => {
    log('STATS', `active=${stats.activeConnections} total=${stats.totalConnections} proxied=${stats.totalProxied} wisp=${stats.totalWisp} warmup=${stats.totalWarmup} failed=${stats.failedConnections} mem=${Math.round(process.memoryUsage().rss/1024/1024)}MB`);
}, 60000);

// ---------- Graceful shutdown ----------
function shutdown(sig) {
    log('SHUTDOWN', `${sig} received, closing server...`);
    server.close(() => {
        log('SHUTDOWN', 'Server closed');
        process.exit(0);
    });
    // Force exit after 5s if connections don't drain
    setTimeout(() => { process.exit(0); }, 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ---------- Start ----------
server.listen(PORT, () => {
    log('STARTUP', `EaglerLite Proxy v2 listening on port ${PORT}`);
    log('STARTUP', `  /health        — liveness probe (JSON)`);
    log('STARTUP', `  /stats         — stats endpoint (JSON)`);
    log('STARTUP', `  /epoxy.js      — epoxy standalone (${EPOXY_JS ? EPOXY_JS.length + ' bytes' : 'not loaded'})`);
    log('STARTUP', `  /epoxy.wasm    — epoxy wasm (${EPOXY_WASM ? EPOXY_WASM.length + ' bytes' : 'not loaded'})`);
    log('STARTUP', `  WS ?target=    — simple WS proxy (heartbeat: ${HEARTBEAT_INTERVAL_MS}ms)`);
    log('STARTUP', `  WS ?warmup=1   — TLS pre-warm probe`);
    log('STARTUP', `  WS /           — wisp proxy (${wisp ? 'enabled' : 'DISABLED'})`);
    log('STARTUP', `  Max conns/IP:  ${MAX_CONN_PER_IP}`);
    log('STARTUP', `  Idle timeout:  ${IDLE_TIMEOUT_MS / 1000}s`);
    log('STARTUP', `  Heartbeat:     every ${HEARTBEAT_INTERVAL_MS / 1000}s, timeout ${HEARTBEAT_TIMEOUT_MS / 1000}s`);
});
