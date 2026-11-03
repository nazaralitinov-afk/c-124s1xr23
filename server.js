/**
 * LizardCraft multiplayer server
 * - Daily world map (UTC day) — builds reset each day
 * - Accounts: nick + password, inventory persists across days
 * - One session per nick (no second tab/device while online)
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const WORLD_DIR = process.env.WORLD_DIR || path.join(__dirname, "worlds");
const ACCOUNTS_FILE = path.join(WORLD_DIR, "_accounts.json");
const SAVE_EVERY_MS = 8000;

try { fs.mkdirSync(WORLD_DIR, { recursive: true }); } catch (e) {}

function utcDay() {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD
}
function daySeed(day) {
  let h = 2166136261;
  for (let i = 0; i < day.length; i++) h = Math.imul(h ^ day.charCodeAt(i), 16777619);
  return (h >>> 0) || 1;
}
function safeNick(n) {
  return String(n || "").replace(/[^\w\u0400-\u04FF\-]/g, "").slice(0, 16);
}
function hashPass(pass, salt) {
  return crypto.createHash("sha256").update(String(pass) + ":" + salt + ":lc").digest("hex");
}

/* ---------- accounts ---------- */
let accounts = {}; // nick -> { salt, hash, inv, armor, sel }
function loadAccounts() {
  try {
    accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8")) || {};
  } catch (e) {
    accounts = {};
  }
}
function saveAccounts() {
  try {
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts));
  } catch (e) {
    console.warn("accounts save failed", e.message);
  }
}
loadAccounts();

/* ---------- daily worlds ---------- */
function worldPath(code, day) {
  const safe = String(code || "GLOBAL").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 64);
  return path.join(WORLD_DIR, safe + "_" + day + ".json");
}
function loadWorld(code, day) {
  try {
    const raw = fs.readFileSync(worldPath(code, day), "utf8");
    const data = JSON.parse(raw);
    return {
      edits: data.edits || {},
      beds: data.beds || [],
      torchFaces: data.torchFaces || [],
      ladderFaces: data.ladderFaces || [],
      chests: data.chests || [],
      claims: data.claims || [],
    };
  } catch (e) {
    return { edits: {}, beds: [], torchFaces: [], ladderFaces: [], chests: [], claims: [] };
  }
}
function saveWorld(code, room) {
  try {
    const data = {
      v: 2,
      code,
      day: room.day,
      seed: room.seed,
      savedAt: Date.now(),
      edits: room.edits || {},
      beds: room.beds || [],
      torchFaces: room.torchFaces || [],
      ladderFaces: room.ladderFaces || [],
      chests: room.chests || [],
      claims: room.claims || [],
    };
    fs.writeFileSync(worldPath(code, room.day), JSON.stringify(data));
  } catch (e) {
    console.warn("save failed", code, e.message);
  }
}

function applyBlk(edits, x, y, z, t) {
  const CS = 16, H = 128;
  if (y < 0 || y >= H) return;
  const cx = x >> 4, cz = z >> 4;
  const lx = x & 15, lz = z & 15;
  const i = (y * CS + lz) * CS + lx;
  const key = cx + "," + cz;
  let arr = edits[key];
  if (!arr) arr = edits[key] = [];
  let found = false;
  for (let j = 0; j < arr.length; j += 2) {
    if (arr[j] === i) { arr[j + 1] = t; found = true; break; }
  }
  if (!found) arr.push(i, t);
}

const rooms = new Map(); // code -> room
const onlineNicks = new Map(); // nickLower -> ws  (global one-session lock)

function getRoom(code) {
  code = String(code || "GLOBAL");
  const day = utcDay();
  let r = rooms.get(code);
  /* rotate room if day changed */
  if (r && r.day !== day) {
    saveWorld(code, r);
    for (const c of r.clients) {
      try { c.send(JSON.stringify({ type: "err", code: "new_day", msg: "New day — reconnect for a fresh map" })); } catch (e) {}
      try { c.close(); } catch (e) {}
    }
    rooms.delete(code);
    r = null;
  }
  if (!r) {
    const data = loadWorld(code, day);
    r = {
      code,
      day,
      seed: daySeed(day),
      clients: new Set(),
      edits: data.edits,
      beds: data.beds,
      torchFaces: data.torchFaces,
      ladderFaces: data.ladderFaces,
      chests: data.chests,
      claims: data.claims,
      dirty: false,
      hostId: null,
    };
    rooms.set(code, r);
    console.log("room open", code, "day", day, "seed", r.seed);
  }
  return r;
}

function broadcast(room, msg, except) {
  const raw = JSON.stringify(msg);
  for (const c of room.clients) {
    if (c !== except && c.readyState === 1) {
      try { c.send(raw); } catch (e) {}
    }
  }
}


/* ---------- LizardTok shared feed ---------- */
const LTOK_DIR = path.join(WORLD_DIR, "ltok");
const LTOK_META = path.join(LTOK_DIR, "posts.json");
try { fs.mkdirSync(LTOK_DIR, { recursive: true }); } catch (e) {}
let ltokPosts = [];
function loadLtok() {
  try { ltokPosts = JSON.parse(fs.readFileSync(LTOK_META, "utf8")) || []; } catch (e) { ltokPosts = []; }
}
function saveLtok() {
  try { fs.writeFileSync(LTOK_META, JSON.stringify(ltokPosts)); } catch (e) { console.warn("ltok save", e.message); }
}
loadLtok();

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}
function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let n = 0;
    req.on("data", (c) => {
      n += c.length;
      if (n > limit) { reject(new Error("too_large")); try { req.destroy(); } catch (e) {} return; }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  cors(res);
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url || "/", "http://localhost");

  try {
    /* feed: metadata only */
    if (req.method === "GET" && url.pathname === "/ltok/feed") {
      const list = ltokPosts
        .filter((p) => !p.banned)
        .slice(-100)
        .reverse()
        .map(({ id, nick, cap, kind, day, ts, likes }) => ({
          id, nick, cap, kind, day, ts, likes: Array.isArray(likes) ? likes.length : 0,
        }));
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, posts: list }));
      return;
    }

    /* media file */
    if (req.method === "GET" && url.pathname.startsWith("/ltok/media/")) {
      const id = url.pathname.slice("/ltok/media/".length).replace(/[^a-zA-Z0-9_-]/g, "");
      const post = ltokPosts.find((p) => p.id === id);
      if (!post) { res.writeHead(404); res.end("not found"); return; }
      const fp = path.join(LTOK_DIR, id + (post.ext || ".bin"));
      if (!fs.existsSync(fp)) { res.writeHead(404); res.end("missing"); return; }
      const buf = fs.readFileSync(fp);
      res.writeHead(200, {
        "Content-Type": post.mime || "application/octet-stream",
        "Cache-Control": "public, max-age=86400",
      });
      res.end(buf);
      return;
    }

    /* upload post: JSON { nick, pass, cap, kind, mime, filename, data: base64 } */
    if (req.method === "POST" && url.pathname === "/ltok/upload") {
      const raw = await readBody(req, 12 * 1024 * 1024);
      let body;
      try { body = JSON.parse(raw.toString("utf8")); } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Bad JSON" }));
        return;
      }
      const nick = safeNick(body.nick);
      const pass = String(body.pass || "");
      if (!nick || pass.length < 3) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Nick/pass required" }));
        return;
      }
      /* accept LizardTok local accounts OR game accounts */
      const key = nick.toLowerCase();
      let authed = false;
      if (accounts[key] && hashPass(pass, accounts[key].salt) === accounts[key].hash) authed = true;
      if (nick === "lizardGHK" && pass === "vadimlambo12") authed = true;
      /* also accept any nick+pass pair for ltok-only users stored in ltok_users.json */
      const LTOK_USERS = path.join(LTOK_DIR, "users.json");
      let lusers = {};
      try { lusers = JSON.parse(fs.readFileSync(LTOK_USERS, "utf8")) || {}; } catch (e) {}
      if (!authed && lusers[key] && lusers[key].pass === crypto.createHash("sha256").update(pass + ":ltok").digest("hex")) authed = true;
      if (!authed && body.ltokToken && lusers[key] && lusers[key].token === body.ltokToken) authed = true;
      if (!authed) {
        res.writeHead(401, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Auth failed — register in LizardTok or use game account" }));
        return;
      }
      const day = utcDay();
      const todayCount = ltokPosts.filter((p) => p.nick === nick && p.day === day).length;
      if (todayCount >= 5) {
        res.writeHead(429, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Max 5 posts per day" }));
        return;
      }
      const dataUrl = String(body.data || "");
      const m = /^data:([^;]+);base64,(.+)$/s.exec(dataUrl);
      if (!m) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Need data URL media" }));
        return;
      }
      const mime = m[1];
      const buf = Buffer.from(m[2], "base64");
      if (buf.length > 10 * 1024 * 1024) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Max 10MB" }));
        return;
      }
      const id = "p" + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
      let ext = ".bin";
      if (mime.includes("mp4")) ext = ".mp4";
      else if (mime.includes("webm")) ext = ".webm";
      else if (mime.includes("png")) ext = ".png";
      else if (mime.includes("jpeg") || mime.includes("jpg")) ext = ".jpg";
      else if (mime.includes("gif")) ext = ".gif";
      else if (mime.includes("webp")) ext = ".webp";
      fs.writeFileSync(path.join(LTOK_DIR, id + ext), buf);
      const post = {
        id, nick, cap: String(body.cap || "").slice(0, 200),
        kind: mime.startsWith("video") ? "video" : "image",
        mime, ext, day, ts: Date.now(), likes: [], banned: false,
      };
      ltokPosts.push(post);
      if (ltokPosts.length > 500) ltokPosts = ltokPosts.slice(-400);
      saveLtok();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, id: post.id }));
      return;
    }

    /* register ltok-only user */
    if (req.method === "POST" && url.pathname === "/ltok/register") {
      const raw = await readBody(req, 2 * 1024 * 1024);
      let body; try { body = JSON.parse(raw.toString("utf8")); } catch (e) {
        res.writeHead(400); res.end(JSON.stringify({ ok: false })); return;
      }
      const nick = safeNick(body.nick);
      const pass = String(body.pass || "");
      if (nick.length < 3 || pass.length < 4) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Nick/pass too short" })); return;
      }
      const LTOK_USERS = path.join(LTOK_DIR, "users.json");
      let lusers = {};
      try { lusers = JSON.parse(fs.readFileSync(LTOK_USERS, "utf8")) || {}; } catch (e) {}
      const key = nick.toLowerCase();
      if (lusers[key] || accounts[key]) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, msg: "Nick taken" })); return;
      }
      const token = crypto.randomBytes(16).toString("hex");
      lusers[key] = {
        nick, pass: crypto.createHash("sha256").update(pass + ":ltok").digest("hex"),
        token, av: body.av || null, created: Date.now(),
      };
      fs.writeFileSync(LTOK_USERS, JSON.stringify(lusers));
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, token, nick }));
      return;
    }

    if (req.method === "POST" && url.pathname === "/ltok/login") {
      const raw = await readBody(req, 65536);
      let body; try { body = JSON.parse(raw.toString("utf8")); } catch (e) {
        res.writeHead(400); res.end("{}"); return;
      }
      const nick = safeNick(body.nick);
      const pass = String(body.pass || "");
      const key = nick.toLowerCase();
      const LTOK_USERS = path.join(LTOK_DIR, "users.json");
      let lusers = {};
      try { lusers = JSON.parse(fs.readFileSync(LTOK_USERS, "utf8")) || {}; } catch (e) {}
      const u = lusers[key];
      const passHash = crypto.createHash("sha256").update(pass + ":ltok").digest("hex");
      if (u && u.pass === passHash) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, token: u.token, nick: u.nick, av: u.av || null }));
        return;
      }
      if (nick === "lizardGHK" && pass === "vadimlambo12") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, token: "admin", nick: "lizardGHK", admin: true }));
        return;
      }
      if (accounts[key] && hashPass(pass, accounts[key].salt) === accounts[key].hash) {
        const token = crypto.randomBytes(12).toString("hex");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, token, nick: accounts[key].nick || nick }));
        return;
      }
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, msg: "Wrong login" }));
      return;
    }

    /* like */
    if (req.method === "POST" && url.pathname === "/ltok/like") {
      const raw = await readBody(req, 65536);
      let body; try { body = JSON.parse(raw.toString("utf8")); } catch (e) {
        res.writeHead(400); res.end("{}"); return;
      }
      const post = ltokPosts.find((p) => p.id === body.id);
      if (!post) { res.writeHead(404); res.end("{}"); return; }
      if (!Array.isArray(post.likes)) post.likes = [];
      const nick = safeNick(body.nick);
      const i = post.likes.indexOf(nick);
      if (i >= 0) post.likes.splice(i, 1); else post.likes.push(nick);
      saveLtok();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, likes: post.likes.length }));
      return;
    }

    /* delete post */
    if (req.method === "POST" && url.pathname === "/ltok/delete") {
      const raw = await readBody(req, 65536);
      let body; try { body = JSON.parse(raw.toString("utf8")); } catch (e) {
        res.writeHead(400); res.end("{}"); return;
      }
      const post = ltokPosts.find((p) => p.id === body.id);
      if (!post) { res.writeHead(404); res.end("{}"); return; }
      const nick = safeNick(body.nick);
      const admin = nick === "lizardGHK" && body.pass === "vadimlambo12";
      if (!admin && post.nick !== nick) {
        res.writeHead(403); res.end(JSON.stringify({ ok: false })); return;
      }
      ltokPosts = ltokPosts.filter((p) => p.id !== body.id);
      try { fs.unlinkSync(path.join(LTOK_DIR, post.id + (post.ext || ".bin"))); } catch (e) {}
      saveLtok();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }

  } catch (e) {
    console.warn("http", e.message);
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: false, msg: e.message || "error" }));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("LizardCraft MP OK\n");
});
const wss = new WebSocketServer({ server });

let nextId = 1;

wss.on("connection", (ws) => {
  ws.id = "p" + (nextId++);
  ws.nick = null;
  ws.authed = false;
  ws.roomCode = null;

  ws.on("message", (buf) => {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!msg || typeof msg !== "object") return;

    /* ---- register ---- */
    if (msg.type === "register") {
      const nick = safeNick(msg.nick);
      const pass = String(msg.pass || "");
      if (!nick || nick.length < 2) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "bad_nick", msg: "Nick must be 2–16 characters" }));
        return;
      }
      if (pass.length < 3) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "bad_pass", msg: "Password min 3 characters" }));
        return;
      }
      const key = nick.toLowerCase();
      if (accounts[key]) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "taken", msg: "This nickname is already registered" }));
        return;
      }
      const salt = crypto.randomBytes(8).toString("hex");
      accounts[key] = { nick, salt, hash: hashPass(pass, salt), inv: null, armor: null, sel: 0, created: Date.now() };
      saveAccounts();
      ws.send(JSON.stringify({ type: "auth", ok: true, mode: "register", msg: "Registered! You can log in now." }));
      return;
    }

    /* ---- login (before join) ---- */
    if (msg.type === "login") {
      const nick = safeNick(msg.nick);
      const pass = String(msg.pass || "");
      const key = nick.toLowerCase();
      const acc = accounts[key];
      if (!acc) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "no_user", msg: "Account not found — register first" }));
        return;
      }
      if (hashPass(pass, acc.salt) !== acc.hash) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "bad_pass", msg: "Wrong password" }));
        return;
      }
      /* already online? */
      const existing = onlineNicks.get(key);
      if (existing && existing !== ws && existing.readyState === 1) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "online", msg: "This account is already online (another tab/device)" }));
        return;
      }
      ws.nick = acc.nick || nick;
      ws.authed = true;
      onlineNicks.set(key, ws);
      ws.send(JSON.stringify({
        type: "auth",
        ok: true,
        mode: "login",
        nick: ws.nick,
        inv: acc.inv || null,
        armor: acc.armor || null,
        sel: acc.sel || 0,
        msg: "Logged in",
      }));
      return;
    }

    /* ---- join room (must be authed) ---- */
    if (msg.type === "join") {
      if (!ws.authed || !ws.nick) {
        ws.send(JSON.stringify({ type: "err", code: "auth", msg: "Login required" }));
        return;
      }
      const key = ws.nick.toLowerCase();
      const existing = onlineNicks.get(key);
      if (existing && existing !== ws && existing.readyState === 1) {
        ws.send(JSON.stringify({ type: "err", code: "online", msg: "Already online in another tab" }));
        try { ws.close(); } catch (e) {}
        return;
      }
      onlineNicks.set(key, ws);

      const code = String(msg.code || "GLOBAL");
      const room = getRoom(code);
      ws.roomCode = code;
      room.clients.add(ws);
      if (!room.hostId) room.hostId = ws.id;

      try {
        ws.send(JSON.stringify({
          type: "welcome",
          id: ws.id,
          host: room.hostId === ws.id,
          day: room.day,
          seed: room.seed,
          edits: room.edits,
          beds: room.beds,
          torchFaces: room.torchFaces,
          ladderFaces: room.ladderFaces,
          chests: room.chests,
          claims: room.claims,
        }));
      } catch (e) {}
      broadcast(room, { type: "peer", id: ws.id, nick: ws.nick }, ws);
      return;
    }

    const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
    if (!room || !ws.authed) return;

    /* persist player inventory */
    if (msg.type === "player_save") {
      const key = ws.nick.toLowerCase();
      const acc = accounts[key];
      if (acc) {
        if (Array.isArray(msg.inv)) acc.inv = msg.inv;
        if (Array.isArray(msg.armor)) acc.armor = msg.armor;
        if (msg.sel != null) acc.sel = msg.sel | 0;
        saveAccounts();
      }
      return;
    }

    if (msg.type === "blk") {
      const x = msg.x | 0, y = msg.y | 0, z = msg.z | 0, t = msg.t | 0;
      applyBlk(room.edits, x, y, z, t);
      room.dirty = true;
      broadcast(room, { type: "blk", x, y, z, t, id: ws.id }, ws);
      return;
    }

    if (msg.type === "world_save") {
      if (msg.edits && typeof msg.edits === "object") room.edits = msg.edits;
      if (Array.isArray(msg.beds)) room.beds = msg.beds;
      if (Array.isArray(msg.torchFaces)) room.torchFaces = msg.torchFaces;
      if (Array.isArray(msg.ladderFaces)) room.ladderFaces = msg.ladderFaces;
      if (Array.isArray(msg.chests)) room.chests = msg.chests;
      if (Array.isArray(msg.claims)) {
        room.claims = msg.claims;
        broadcast(room, { type: "claims", claims: room.claims }, ws);
      }
      room.dirty = true;
      saveWorld(room.code, room);
      return;
    }

    if (msg.type === "pres") {
      msg.id = ws.id;
      if (ws.nick) msg.nick = ws.nick;
      broadcast(room, msg, ws);
      return;
    }

    if (msg.type === "chat") {
      broadcast(room, {
        type: "chat",
        text: String(msg.text || "").slice(0, 100),
        nick: ws.nick || "?",
        id: ws.id,
      });
      return;
    }

    if (msg.type === "drop" || msg.type === "pickup" || msg.type === "hit" || msg.type === "door") {
      msg.id = msg.id || ws.id;
      broadcast(room, msg, ws);
      return;
    }
  });

  ws.on("close", () => {
    if (ws.nick) {
      const key = ws.nick.toLowerCase();
      if (onlineNicks.get(key) === ws) onlineNicks.delete(key);
    }
    const code = ws.roomCode;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    room.clients.delete(ws);
    broadcast(room, { type: "bye", id: ws.id });
    if (room.hostId === ws.id) room.hostId = [...room.clients][0]?.id || null;
    if (room.clients.size === 0) {
      saveWorld(room.code, room);
      rooms.delete(room.code);
      console.log("room saved & closed", code, room.day);
    }
  });
});

setInterval(() => {
  for (const room of rooms.values()) {
    if (room.dirty) {
      saveWorld(room.code, room);
      room.dirty = false;
    }
  }
  /* day rollover check */
  const day = utcDay();
  for (const [code, room] of [...rooms.entries()]) {
    if (room.day !== day) {
      saveWorld(code, room);
      for (const c of room.clients) {
        try { c.send(JSON.stringify({ type: "err", code: "new_day", msg: "New day — map reset. Reconnect!" })); } catch (e) {}
        try { c.close(); } catch (e) {}
      }
      rooms.delete(code);
    }
  }
}, SAVE_EVERY_MS);

server.listen(PORT, () => console.log("LizardCraft MP on :" + PORT, "worlds →", WORLD_DIR));
