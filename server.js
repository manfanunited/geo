// Сервер игры «Песня на карте»: раздаёт index.html и пересылает сообщения между игроками по WebSocket.
// Без внешних зависимостей: нужен только Node.js. Туннель Cloudflare (cloudflared) запускается автоматически.
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const zlib = require("zlib"), os = require("os"), { spawn } = require("child_process");
const PORT = +process.env.PORT || 8080;
const rooms = new Map();
let nextIdx = 0;

/* ---------- HTTP ---------- */
const server = http.createServer((req, res) => {
  const u = req.url.split("?")[0];
  if (u === "/ping") { res.writeHead(200, { "Content-Type": "text/plain", "Cache-Control": "no-store" }); res.end("sgeo-relay"); return; }
  if (u === "/" || u === "/index.html") {
    fs.readFile(path.join(__dirname, "index.html"), (e, d) => {
      if (e) { res.writeHead(500); res.end("index.html not found next to server.js"); return; }
      const h = { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" };
      if (/\bgzip\b/.test(req.headers["accept-encoding"] || "")) { h["Content-Encoding"] = "gzip"; res.writeHead(200, h); res.end(zlib.gzipSync(d)); }
      else { res.writeHead(200, h); res.end(d); }
    });
    return;
  }
  res.writeHead(404); res.end("not found");
});

/* ---------- WebSocket (минимальная реализация RFC 6455) ---------- */
function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([128 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}
server.on("upgrade", (req, sock) => {
  const key = req.headers["sec-websocket-key"];
  if (req.url.split("?")[0] !== "/ws" || !key) { sock.destroy(); return; }
  const acc = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + acc + "\r\n\r\n");
  sock.setNoDelay(true);
  const c = { sock, room: null, role: null, id: null, alive: true, closed: false,
    send(o) { if (!this.closed && sock.writable) sock.write(frame(1, Buffer.from(JSON.stringify(o)))); } };
  let buf = Buffer.alloc(0), frag = [];
  sock.on("data", d => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      if (buf.length < 2) return;
      const b0 = buf[0], b1 = buf[1], op = b0 & 15, fin = !!(b0 & 128);
      let len = b1 & 127, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const masked = !!(b1 & 128); if (masked) off += 4;
      if (len > 1e6) { sock.destroy(); return; }
      if (buf.length < off + len) return;
      let p = Buffer.from(buf.slice(off, off + len));
      if (masked) { const mk = buf.slice(off - 4, off); for (let i = 0; i < p.length; i++) p[i] ^= mk[i & 3]; }
      buf = buf.slice(off + len);
      if (op === 8) { sock.end(frame(8, Buffer.alloc(0))); return; }
      if (op === 9) { sock.write(frame(10, p)); continue; }
      if (op === 10) { c.alive = true; continue; }
      if (op === 0 || op === 1) {
        if (op === 1) frag = [];
        frag.push(p);
        if (fin) { const t = Buffer.concat(frag).toString("utf8"); frag = []; try { onMsg(c, JSON.parse(t)); } catch (e) {} }
      }
    }
  });
  const end = () => { if (c.closed) return; c.closed = true; onClose(c); };
  sock.on("close", end); sock.on("error", end); sock.on("end", end);
  c.ping = () => { if (!c.closed) { if (!c.alive) { sock.destroy(); return; } c.alive = false; sock.write(frame(9, Buffer.alloc(0))); } };
  clients.add(c);
});
const clients = new Set();
setInterval(() => clients.forEach(c => c.closed ? clients.delete(c) : c.ping()), 20000);

/* ---------- комнаты ---------- */
function genCode() {
  const a = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  for (;;) { let s = ""; for (let i = 0; i < 5; i++) s += a[crypto.randomInt(a.length)]; if (!rooms.has(s)) return s; }
}
function onMsg(c, d) {
  if (d.op === "create" && !c.room) {
    if (rooms.size >= 200) return;
    const code = genCode(); rooms.set(code, { host: c, clients: new Map() });
    c.room = code; c.role = "host"; c.send({ op: "created", code });
    log("room " + code + " created (rooms: " + rooms.size + ")");
  } else if (d.op === "join" && !c.room) {
    const r = rooms.get(String(d.code || "").toUpperCase());
    if (!r || typeof d.id !== "string" || d.id.length > 40 || r.clients.has(d.id)) { c.send({ op: "nojoin" }); return; }
    c.room = String(d.code).toUpperCase(); c.role = "client"; c.id = d.id; r.clients.set(d.id, c); c.send({ op: "joined" });
  } else if (d.op === "up" && c.role === "client") {
    const r = rooms.get(c.room); if (r) r.host.send({ op: "up", from: c.id, m: d.m });
  } else if (d.op === "dn" && c.role === "host") {
    const r = rooms.get(c.room); if (!r) return;
    if (d.to) { const t = r.clients.get(d.to); if (t) t.send({ op: "dn", m: d.m }); }
    else r.clients.forEach(t => t.send({ op: "dn", m: d.m }));
  }
}
function onClose(c) {
  clients.delete(c);
  const r = rooms.get(c.room); if (!r) return;
  if (c.role === "host") { r.clients.forEach(t => { t.send({ op: "dn", m: { t: "hostgone" } }); t.sock.end(); }); rooms.delete(c.room); log("room " + c.room + " closed"); }
  else if (c.role === "client") { r.clients.delete(c.id); r.host.send({ op: "up", from: c.id, m: { t: "bye" } }); }
}

/* ---------- start and public access ---------- */
const dgram = require("dgram");
// On hosting platforms the PORT variable is set: no tunnel is needed there.
const KIND = (process.env.TUNNEL || (process.env.PORT ? "none" : "cloudflared")).toLowerCase();
function log(s) { console.log("[" + new Date().toLocaleTimeString() + "] " + s); }
function banner(lines) { const w = Math.max(...lines.map(l => l.length)) + 4; console.log("\n" + "=".repeat(w)); lines.forEach(l => console.log("  " + l)); console.log("=".repeat(w) + "\n"); }
function lanAddrs() { const r = []; Object.values(os.networkInterfaces()).forEach(a => a.forEach(i => { if (i.family === "IPv4" && !i.internal) r.push(i.address); })); return r; }
function showLink(url, extra) {
  banner(["LINK FOR FRIENDS (open the same link yourself too):", url, "", ...(extra || []), "Keep this window open while you play."]);
  try { fs.writeFileSync(path.join(__dirname, "link.txt"), url + "\n"); } catch (e) {}
  if (process.platform === "win32") { try { const p = spawn("clip", { shell: true }); p.stdin.end(url); log("Link copied to clipboard."); } catch (e) {} }
}
const TEST = "Test it first: open the link on a phone using MOBILE DATA (Wi-Fi off, no VPN).";
server.listen(PORT, () => {
  const lan = lanAddrs();
  banner(["Game server is running.", "For yourself:        http://localhost:" + PORT, ...(lan.length ? ["Same Wi-Fi / LAN:     " + lan.map(a => "http://" + a + ":" + PORT).join("   ")] : []), "Public access mode: " + KIND + "  (the link appears below)"]);
  if (KIND === "cloudflared") {
    const cands = [process.env.CLOUDFLARED, path.join(__dirname, "cloudflared.exe"), path.join(__dirname, "cloudflared"), "cloudflared"].filter(Boolean);
    const exe = cands.find(p => p === "cloudflared" || fs.existsSync(p));
    runTunnel(exe, ["tunnel", "--no-autoupdate", "--protocol", process.env.TUNNEL_PROTOCOL || "http2", "--url", "http://127.0.0.1:" + PORT], /https:\/\/[a-z0-9-]+\.trycloudflare\.com/,
      ["cloudflared not found. Put cloudflared.exe next to server.js (see README), or use start-lhr.bat / start-direct.bat."]);
  } else if (KIND === "lhr" || KIND === "serveo") {
    const host = KIND === "lhr" ? "nokey@localhost.run" : "serveo.net";
    runTunnel("ssh", ["-T", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=" + (process.platform === "win32" ? "NUL" : "/dev/null"), "-o", "ServerAliveInterval=30", "-o", "ExitOnForwardFailure=yes", "-R", "80:127.0.0.1:" + PORT, host],
      KIND === "lhr" ? /https:\/\/[a-z0-9-]+\.lhr\.life/ : /https:\/\/[a-z0-9-]+\.(serveousercontent\.com|serveo\.net)/,
      ["ssh not found. Windows 10/11: Settings > Apps > Optional features > add 'OpenSSH Client'."]);
  } else if (KIND === "direct") directMode();
});
function runTunnel(exe, args, re, missing) {
  let shown = false, errs = 0;
  const child = spawn(exe, args, { stdio: ["ignore", "pipe", "pipe"] });
  const onData = d => String(d).split(/\r?\n/).forEach(line => {
    const m = line.match(re);
    if (m && !shown) { shown = true; showLink(m[0], [TEST]); }
    else if (/\b(ERR|WRN|error|denied|refused|failed)\b/i.test(line) && errs++ < 8) log(exe + ": " + line.slice(0, 200));
  });
  child.stdout.on("data", onData); child.stderr.on("data", onData);
  child.on("error", () => banner(missing));
  child.on("exit", c => log(exe + " stopped (code " + c + "). Restart the .bat file."));
  process.on("exit", () => { try { child.kill(); } catch (e) {} });
  process.on("SIGINT", () => process.exit(0));
}

/* direct mode: no third-party tunnel; tries UPnP port forwarding on the router */
function ssdp() {
  return new Promise(res => {
    const s = dgram.createSocket("udp4"), found = [];
    const msg = Buffer.from('M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: "ssdp:discover"\r\nMX: 2\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n');
    s.on("message", m => { const l = String(m).match(/location:\s*(\S+)/i); if (l) found.push(l[1]); });
    s.on("error", () => {});
    s.bind(() => { try { s.send(msg, 1900, "239.255.255.250"); } catch (e) {} });
    setTimeout(() => { try { s.close(); } catch (e) {} res([...new Set(found)]); }, 3000);
  });
}
async function upnpMap(lan) {
  const locs = await ssdp();
  if (!locs.length) throw new Error("the router did not answer (UPnP is probably disabled)");
  let last = null;
  for (const loc of locs) {
    try {
      const xml = await fetch(loc, { signal: AbortSignal.timeout(4000) }).then(r => r.text());
      const m = xml.match(/<serviceType>(urn:schemas-upnp-org:service:WAN(?:IP|PPP)Connection:\d)<\/serviceType>[\s\S]*?<controlURL>([^<]+)<\/controlURL>/);
      if (!m) continue;
      const type = m[1], ctl = new URL(m[2], loc).href;
      const call = async (action, args) => {
        const body = '<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:' + action + ' xmlns:u="' + type + '">' + args + '</u:' + action + '></s:Body></s:Envelope>';
        const r = await fetch(ctl, { method: "POST", headers: { "Content-Type": 'text/xml; charset="utf-8"', SOAPAction: '"' + type + "#" + action + '"' }, body, signal: AbortSignal.timeout(5000) });
        const t = await r.text();
        if (!r.ok) throw new Error(action + " failed: " + ((t.match(/<errorDescription>([^<]*)/) || [0, r.status])[1]));
        return t;
      };
      const add = lease => call("AddPortMapping", "<NewRemoteHost></NewRemoteHost><NewExternalPort>" + PORT + "</NewExternalPort><NewProtocol>TCP</NewProtocol><NewInternalPort>" + PORT + "</NewInternalPort><NewInternalClient>" + lan + "</NewInternalClient><NewEnabled>1</NewEnabled><NewPortMappingDescription>songgeo</NewPortMappingDescription><NewLeaseDuration>" + lease + "</NewLeaseDuration>");
      try { await add(7200); setInterval(() => add(7200).catch(() => {}), 3600000); } catch (e) { await add(0); }
      const ip = ((await call("GetExternalIPAddress", "")).match(/<NewExternalIPAddress>([^<]*)/) || [])[1];
      return ip || "unknown";
    } catch (e) { last = e; }
  }
  throw last || new Error("no suitable UPnP service found on the router");
}
async function directMode() {
  const lan = lanAddrs()[0], notes = [];
  let routerIp = null;
  try { routerIp = await upnpMap(lan); notes.push("Port " + PORT + " was opened on the router automatically (UPnP)."); }
  catch (e) { notes.push("UPnP: " + e.message, "Forward TCP port " + PORT + " to " + lan + " manually in the router settings."); }
  let pub = null;
  for (const u of ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]) {
    try { const t = (await fetch(u, { signal: AbortSignal.timeout(5000) }).then(r => r.text())).trim(); if (/^\d+\.\d+\.\d+\.\d+$/.test(t)) { pub = t; break; } } catch (e) {}
  }
  const priv = ip => /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.)/.test(ip || "");
  if (priv(routerIp) || (routerIp && pub && routerIp !== pub)) notes.push("WARNING: your provider seems to use shared (CGNAT) addressing - direct access from the internet will not work. Ask the provider for a public IP or use another mode.");
  const ip = pub || (routerIp && !priv(routerIp) ? routerIp : null);
  if (!ip) { banner(["Could not determine the public IP address.", ...notes]); return; }
  showLink("http://" + ip + ":" + PORT, [...notes, "If Windows asks, allow Node.js in the firewall (private network).", TEST]);
}
