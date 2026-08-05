# EaglerLite-Proxy

A simple WebSocket-to-WebSocket proxy that gives EaglerLite connections a real origin instead of "Origin: null" (from about:blank).
 HOW IT WORKS:
 *   Client: new WebSocket("wss://your-proxy.onrender.com/?target=wss%3A%2F%2Fmc.voidsent.net")
 *   Proxy:  parses ?target=, opens server-side WebSocket to target, tunnels frames

DEPLOYMENT (Render.com):
1. Create a new Web Service on render.com
2. Connect your GitHub repo (fork this repo)
3. Go back to render
  - Build Command:  npm install
  - Start Command:  node proxy-server.js
6. Your proxy URL: wss://your-app-name.onrender.com

SECURITY:
- Set PROXY_API_KEY env var to require ?key=XXX on all connections
- Set MAX_CONN_PER_IP to limit concurrent connections per IP (0 = unlimited)
- The proxy sets Origin: https://eaglerlite-proxy.local on outbound connections so the target server sees a real origin instead of "null"
