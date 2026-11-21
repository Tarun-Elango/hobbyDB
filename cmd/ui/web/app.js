// kvStore cluster monitor: plain JS, no build step.
//
// Contract with the Go server (cmd/ui):
//
//   GET  /events                      Server-Sent Events
//        event: state  data: {"connected":"n1","nodes":[{"id":"n1","addr":"127.0.0.1:9000",
//                             "up":true,"role":"leader|follower|candidate|down",
//                             "term":3,"leader":"n1"}]}
//        event: log    data: {"ts":1732190000000,"kind":"election|redirect|node|kv|error","msg":"..."}
//
//   POST /api/kv                      body {"op":"get|set|del|len|ping","key":"a","value":"b"}
//        -> {"status":"ok|not_found|error|not_leader","value":"...","node":"n2","error":"..."}
//
//   POST /api/nodes/{id}/{start|kill|restart}
//        -> {"ok":true} or {"ok":false,"error":"..."}
//
// Open the page with ?mock (or straight from disk) to run a simulated cluster instead.

(() => {
  "use strict";

  const $ = (sel, root = document) => root.querySelector(sel);
  const nodesEl = $("#nodes");
  const tpl = $("#node-tpl");
  const logEl = $("#log");
  const outEl = $("#console-out");
  const cmdEl = $("#cmd");

  const cards = new Map(); // id -> card element
  const pending = new Set(); // node ids with an action in flight
  let lastState = null;
  let logFilter = "all";

  const params = new URLSearchParams(location.search);
  const useMock = params.has("mock") || location.protocol === "file:";
  const api = useMock ? makeMock() : makeRemote();

  // ---------- rendering ----------

  function renderState(state) {
    lastState = state;
    const nodes = state.nodes || [];
    const up = nodes.filter((n) => n.up);
    const leader = nodes.find((n) => n.up && n.role === "leader");
    const maxTerm = nodes.reduce((m, n) => Math.max(m, n.term || 0), 0);
    const need = Math.floor(nodes.length / 2) + 1;

    $("#sum-term").textContent = maxTerm || "—";
    $("#sum-leader").textContent = leader ? leader.id : "NONE";
    $("#sum-leader").style.color = leader ? "var(--red)" : "var(--orange)";
    $("#sum-quorum").textContent = `${up.length}/${nodes.length}`;
    $("#sum-quorum").style.color = up.length >= need ? "var(--green)" : "var(--red)";
    $("#sum-conn").textContent = state.connected || "—";

    const seen = new Set();
    for (const n of nodes) {
      seen.add(n.id);
      let card = cards.get(n.id);
      if (!card) {
        card = tpl.content.firstElementChild.cloneNode(true);
        card.dataset.id = n.id;
        card.querySelectorAll("[data-act]").forEach((b) =>
          b.addEventListener("click", () => nodeAction(n.id, b.dataset.act))
        );
        cards.set(n.id, card);
        nodesEl.appendChild(card);
      }
      const role = n.up ? n.role || "follower" : "down";
      card.className = `node ${role}` +
        (n.id === state.connected ? " connected" : "") +
        (pending.has(n.id) ? " pending" : "");
      $(".node-id", card).textContent = n.id;
      $(".node-addr", card).textContent = n.addr || "";
      $(".node-role", card).textContent = role.toUpperCase();
      $(".v-term", card).textContent = n.up ? n.term ?? "—" : "—";
      const upEl = $(".v-up", card);
      upEl.textContent = n.up ? "ONLINE" : "OFFLINE";
      upEl.className = "v-up " + (n.up ? "ok" : "bad");
      $(".v-leader", card).textContent = n.up ? n.leader || "?" : "—";

      const busy = pending.has(n.id);
      $('[data-act="start"]', card).disabled = busy || n.up;
      $('[data-act="kill"]', card).disabled = busy || !n.up;
      $('[data-act="restart"]', card).disabled = busy;
    }
    for (const [id, card] of cards) {
      if (!seen.has(id)) { card.remove(); cards.delete(id); }
    }
  }

  function addLog(ev) {
    $(".empty", logEl)?.remove();
    const li = document.createElement("li");
    li.className = `k-${ev.kind}`;
    li.dataset.kind = ev.kind;
    if (logFilter !== "all" && logFilter !== ev.kind) li.classList.add("hidden");
    const t = document.createElement("time");
    t.textContent = new Date(ev.ts || Date.now()).toLocaleTimeString([], { hour12: false });
    const k = document.createElement("span");
    k.className = "kind";
    k.textContent = ev.kind.toUpperCase();
    const m = document.createElement("span");
    m.className = "msg";
    m.textContent = ev.msg;
    li.append(t, k, m);
    logEl.prepend(li);
    while (logEl.children.length > 300) logEl.lastElementChild.remove();
  }

  function setLink(kind, text) {
    const el = $("#link-state");
    el.className = `pill ${kind}`;
    el.textContent = text;
  }

  function print(text, cls) {
    const line = document.createElement("div");
    if (cls) line.className = cls;
    line.textContent = text;
    outEl.appendChild(line);
    outEl.scrollTop = outEl.scrollHeight;
  }

  // ---------- actions ----------

  async function nodeAction(id, act) {
    pending.add(id);
    if (lastState) renderState(lastState);
    try {
      const res = await api.node(id, act);
      if (!res.ok) addLog({ ts: Date.now(), kind: "error", msg: `${act} ${id}: ${res.error || "failed"}` });
    } catch (e) {
      addLog({ ts: Date.now(), kind: "error", msg: `${act} ${id}: ${e.message}` });
    } finally {
      pending.delete(id);
      if (lastState) renderState(lastState);
    }
  }

  function parseCommand(line) {
    const parts = line.trim().split(/\s+/);
    const op = (parts[0] || "").toLowerCase();
    const usage = {
      set: [3, "usage: set <key> <value>"],
      get: [2, "usage: get <key>"],
      del: [2, "usage: del <key>"],
      len: [1, "usage: len"],
      ping: [1, "usage: ping"],
    }[op];
    if (!usage) return { error: "unknown command (set, get, del, len, ping)" };
    // value may contain spaces: everything after the key
    if (op === "set") {
      const m = line.trim().match(/^\S+\s+(\S+)\s+(.+)$/);
      if (!m) return { error: usage[1] };
      return { op, key: m[1], value: m[2] };
    }
    if (parts.length !== usage[0]) return { error: usage[1] };
    return { op, key: parts[1] || "" };
  }

  async function runCommand(line) {
    if (!line.trim()) return;
    print(line, "in");
    const cmd = parseCommand(line);
    if (cmd.error) { print(cmd.error, "err"); return; }
    const t0 = performance.now();
    try {
      const r = await api.kv(cmd);
      const ms = Math.round(performance.now() - t0);
      const via = r.node ? ` · via ${r.node}` : "";
      switch (r.status) {
        case "ok":
          print(r.value != null && r.value !== "" ? r.value : "OK", "ok");
          break;
        case "not_found":
          print("(not found)", "nf");
          break;
        default:
          print(`ERR ${r.error || r.status}`, "err");
      }
      print(`  ${ms}ms${via}`, "meta");
    } catch (e) {
      print(`request error: ${e.message}`, "err");
    }
  }

  // ---------- wiring ----------

  const history = [];
  let hIdx = 0;
  $("#console-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const line = cmdEl.value;
    if (line.trim()) { history.push(line); hIdx = history.length; }
    cmdEl.value = "";
    runCommand(line);
  });
  cmdEl.addEventListener("keydown", (e) => {
    if (e.key === "ArrowUp" && hIdx > 0) { cmdEl.value = history[--hIdx]; e.preventDefault(); }
    if (e.key === "ArrowDown") {
      hIdx = Math.min(history.length, hIdx + 1);
      cmdEl.value = history[hIdx] || "";
      e.preventDefault();
    }
  });
  document.querySelectorAll(".quick button").forEach((b) =>
    b.addEventListener("click", () => {
      if (b.hasAttribute("data-clear")) { outEl.textContent = ""; return; }
      if (b.dataset.run) { runCommand(b.dataset.run); return; }
      cmdEl.value = b.dataset.fill;
      cmdEl.focus();
    })
  );
  document.querySelectorAll(".chip").forEach((c) =>
    c.addEventListener("click", () => {
      logFilter = c.dataset.kind;
      document.querySelectorAll(".chip").forEach((x) => x.classList.toggle("on", x === c));
      logEl.querySelectorAll("li[data-kind]").forEach((li) =>
        li.classList.toggle("hidden", logFilter !== "all" && li.dataset.kind !== logFilter)
      );
    })
  );

  logEl.innerHTML = '<li class="empty">waiting for events…</li>';
  print("kvStore console — commands: set <key> <value>, get <key>, del <key>, len, ping", "hint");
  $("#foot-mode").textContent = useMock ? "MOCK CLUSTER · SIMULATED" : `LIVE · ${location.host}`;
  api.subscribe({ state: renderState, log: addLog, link: setLink });

  // ---------- live backend ----------

  function makeRemote() {
    async function post(url, body) {
      const r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok && !("status" in data) && !("ok" in data)) throw new Error(`HTTP ${r.status}`);
      return data;
    }
    return {
      kv: (cmd) => post("/api/kv", cmd),
      node: (id, act) => post(`/api/nodes/${encodeURIComponent(id)}/${act}`),
      subscribe({ state, log, link }) {
        link("", "CONNECTING");
        const es = new EventSource("/events");
        es.onopen = () => link("live", "LIVE");
        es.onerror = () => link("lost", "LOST");
        es.addEventListener("state", (e) => state(JSON.parse(e.data)));
        es.addEventListener("log", (e) => log(JSON.parse(e.data)));
      },
    };
  }

  // ---------- mock backend ----------

  function makeMock() {
    const ids = ["n1", "n2", "n3"];
    const nodes = ids.map((id, i) => ({
      id, addr: `127.0.0.1:${9000 + i * 10}`, up: true, role: "follower", term: 0, leader: "",
      timer: 1 + Math.floor(Math.random() * 3),
    }));
    const kv = new Map();
    let connected = "n1";
    let subs = null;

    const emit = (kind, msg) => subs?.log({ ts: Date.now(), kind, msg });
    const byId = (id) => nodes.find((n) => n.id === id);
    const quorum = () => nodes.filter((n) => n.up).length > nodes.length / 2;
    const leader = () => nodes.find((n) => n.up && n.role === "leader");
    const snapshot = () => ({
      connected,
      nodes: nodes.map(({ timer, ...n }) => ({ ...n, role: n.up ? n.role : "down" })),
    });

    function tick() {
      const L = leader();
      if (L) {
        for (const n of nodes) if (n.up) { n.term = L.term; n.leader = L.id; if (n !== L) n.role = "follower"; }
      } else {
        const cand = nodes.find((n) => n.up && n.role === "candidate");
        for (const n of cand ? [cand] : nodes) {
          if (!n.up) continue;
          if (n.role === "candidate") {
            if (quorum()) {
              n.role = "leader";
              for (const m of nodes) if (m.up) { m.term = n.term; m.leader = n.id; if (m !== n) m.role = "follower"; }
              emit("election", `${n.id} won election for term ${n.term}`);
            } else {
              n.term++;
              emit("election", `${n.id} lost election (no quorum), retrying term ${n.term}`);
            }
            break;
          }
          if (--n.timer <= 0) {
            n.role = "candidate"; n.term++; n.leader = "";
            n.timer = 2 + Math.floor(Math.random() * 3);
            emit("election", `${n.id} election timeout → candidate, term ${n.term}`);
            break;
          }
        }
      }
      if (!byId(connected).up) {
        const next = nodes.find((n) => n.up);
        if (next) { emit("redirect", `client lost ${connected}, reconnected to ${next.id}`); connected = next.id; }
      }
      subs?.state(snapshot());
    }

    const delay = (ms) => new Promise((r) => setTimeout(r, ms));

    return {
      subscribe(s) {
        subs = s;
        s.link("mock", "MOCK");
        s.state(snapshot());
        setInterval(tick, 1000);
      },
      async node(id, act) {
        await delay(250 + Math.random() * 300);
        const n = byId(id);
        if (act === "kill" || act === "restart") {
          if (n.up) {
            n.up = false;
            emit("node", `${id} killed` + (n.role === "leader" ? " (was leader)" : ""));
            n.role = "follower"; n.leader = "";
            if (!leader()) for (const m of nodes) if (m.up) m.leader = "";
          }
          if (act === "restart") await delay(600);
        }
        if (act === "start" || act === "restart") {
          if (!n.up) {
            n.up = true; n.role = "follower"; n.leader = ""; n.timer = 3;
            emit("node", `${id} started`);
          }
        }
        tick();
        return { ok: true };
      },
      async kv(cmd) {
        await delay(10 + Math.random() * 30);
        const write = cmd.op === "set" || cmd.op === "del";
        const L = leader();
        if (write && !quorum()) return { status: "error", error: "no quorum", node: connected };
        if (write) {
          if (!L) return { status: "error", error: "no leader elected yet", node: connected };
          if (connected !== L.id) {
            emit("redirect", `${connected} → NotLeader, following leader ${L.id}`);
            connected = L.id;
            await delay(100);
          }
        }
        let res;
        switch (cmd.op) {
          case "set": kv.set(cmd.key, cmd.value); res = { status: "ok" }; break;
          case "del": res = kv.delete(cmd.key) ? { status: "ok" } : { status: "not_found" }; break;
          case "get": res = kv.has(cmd.key) ? { status: "ok", value: kv.get(cmd.key) } : { status: "not_found" }; break;
          case "len": res = { status: "ok", value: String(kv.size) }; break;
          case "ping": res = { status: "ok", value: "PONG" }; break;
        }
        subs?.state(snapshot());
        return { ...res, node: connected };
      },
    };
  }
})();
