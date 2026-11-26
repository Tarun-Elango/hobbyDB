// kestrelKV cluster monitor: plain JS, no build step.
//
// Contract with the Go server (cmd/ui, run with `go run ./cmd/ui`):
//
//   GET  /events                      Server-Sent Events
//        event: state  data: {"connected":"n1","nodes":[{"id":"n1","addr":"127.0.0.1:9000",
//                             "up":true,"role":"leader|follower|candidate|down",
//                             "term":3,"leader":"n1","proc":true,"cfg":{...node settings}}]}
//        event: log    data: {"ts":1732190000000,"kind":"election|redirect|node|kv|error","msg":"..."}
//
//   POST /api/kv                      body {"op":"get|set|del|len|ping","key":"a","value":"b"}
//        -> {"status":"ok|not_found|error|not_leader","value":"...","node":"n2","error":"..."}
//
//   POST /api/nodes/{id}/{start|kill|restart}
//        -> {"ok":true} or {"ok":false,"error":"..."}
//
//   POST   /api/nodes                 body {"id","clientAddr","replAddr","wal","pprofAddr","verbose"}  (add node)
//   PUT    /api/nodes/{id}/config     same body, applied on restart                                   (edit node)
//   DELETE /api/nodes/{id}                                                                           (remove node)
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
  const nodeErrs = new Map(); // id -> last failed action message, shown on the card
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
        $("[data-cfg]", card).addEventListener("click", () => openNodeDialog(n.id));
        cards.set(n.id, card);
        nodesEl.appendChild(card);
      }
      const proc = n.proc ?? n.up; // process running (it may not answer yet)
      const role = n.up ? n.role || "follower" : "down";
      card.className = `node ${role}` +
        (n.id === state.connected ? " connected" : "") +
        (pending.has(n.id) ? " pending" : "");
      // hexagon shows a short label: "node-a" -> "A"; ids without a dash stay as-is
      $(".node-id", card).textContent = n.id.includes("-") ? n.id.split("-").pop().toUpperCase() : n.id;
      $(".hex", card).title = n.id;
      $(".node-addr", card).textContent = n.addr || "";
      $(".node-role", card).textContent = !n.up && proc ? "NO REPLY" : role.toUpperCase();
      $(".v-term", card).textContent = n.up ? n.term ?? "—" : "—";
      const upEl = $(".v-up", card);
      upEl.textContent = n.up ? "ONLINE" : "OFFLINE";
      upEl.className = "v-up " + (n.up ? "ok" : "bad");
      $(".v-leader", card).textContent = n.up ? n.leader || "?" : "—";

      const busy = pending.has(n.id);
      $('[data-act="start"]', card).disabled = busy || proc;
      $('[data-act="kill"]', card).disabled = busy || !proc;
      $(".node-err", card).textContent = nodeErrs.get(n.id) || "";
      $('[data-act="restart"]', card).disabled = busy;
      $("[data-cfg]", card).disabled = busy;
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
    nodeErrs.delete(id);
    if (lastState) renderState(lastState);
    const fail = (msg) => {
      nodeErrs.set(id, `${act.toUpperCase()} FAILED · ${msg}`);
      addLog({ ts: Date.now(), kind: "error", msg: `${act} ${id}: ${msg}` });
    };
    try {
      const res = await api.node(id, act);
      if (!res.ok) fail(res.error || "failed");
    } catch (e) {
      fail(e.message);
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

  // ---------- tool modals (UI only — backend not wired yet) ----------

  document.querySelectorAll("[data-open]").forEach((b) =>
    b.addEventListener("click", () => {
      const dlg = document.getElementById(b.dataset.open);
      if (dlg.id === "compare-dlg") renderCompareRows();
      dlg.showModal();
    })
  );
  document.querySelectorAll("dialog.modal").forEach((dlg) => {
    dlg.querySelectorAll("[data-close]").forEach((b) => b.addEventListener("click", () => dlg.close()));
    dlg.addEventListener("click", (e) => { if (e.target === dlg) dlg.close(); }); // backdrop click
  });

  function renderCompareRows() {
    const rows = $("#cmp-rows");
    const nodes = (lastState && lastState.nodes) || [];
    if (!nodes.length) { rows.innerHTML = '<tr><td colspan="6" class="cmp-empty">no nodes</td></tr>'; return; }
    rows.innerHTML = "";
    for (const n of nodes) {
      const tr = document.createElement("tr");
      const cells = [
        [n.id, ""],
        [n.up ? "UP" : "DOWN", n.up ? "ok" : "bad"],
        [n.term ?? "—", ""],
        ["—", "muted"],
        ["—", "muted"],
        [n.up ? "—" : "SKIP", "muted"],
      ];
      for (const [text, cls] of cells) {
        const td = document.createElement("td");
        td.textContent = text;
        if (cls) td.className = cls;
        tr.appendChild(td);
      }
      rows.appendChild(tr);
    }
  }

  $("#cmp-run").addEventListener("click", () => {
    renderCompareRows();
    const v = $("#cmp-verdict");
    v.className = "verdict pending";
    v.textContent = "NOT CONNECTED — BACKEND TODO";
  });

  $("#test-run").addEventListener("click", () => {
    const out = $("#test-out");
    const picked = [...document.querySelectorAll("#suites input:checked")];
    if (!picked.length) { out.textContent = "no suites selected"; return; }
    out.textContent = picked.map((i) => `$ go test ./test/${i.value}/...\n  (not connected — backend TODO)`).join("\n");
  });

  // ---------- node settings (add / edit / remove) ----------

  const nodeDlg = $("#node-dlg");
  const nodeForm = $("#node-form");
  const nodeCfg = new Map(); // id -> last saved settings (the server doesn't report these yet)
  let editingId = null; // null = adding a new node

  const ADDR_RE = /^[A-Za-z0-9.\-]*:\d{1,5}$/; // host:port, host may be empty (":9000")

  function splitAddr(addr) {
    const i = (addr || "").lastIndexOf(":");
    return i < 0 ? ["127.0.0.1", 0] : [addr.slice(0, i) || "127.0.0.1", Number(addr.slice(i + 1))];
  }

  function defaultsFor(id, addr) {
    const [host, port] = splitAddr(addr);
    return { id, clientAddr: addr, replAddr: `${host}:${port + 1}`, wal: `data/${id}.wal`, pprofAddr: "", verbose: false };
  }

  function suggestNew() {
    const nodes = (lastState && lastState.nodes) || [];
    const ids = new Set(nodes.map((n) => n.id));
    let k = nodes.length + 1;
    while (ids.has(`n${k}`)) k++;
    const ports = nodes.map((n) => splitAddr(n.addr)[1]).filter(Boolean);
    const port = ports.length ? Math.max(...ports) + 10 : 9000;
    return defaultsFor(`n${k}`, `127.0.0.1:${port}`);
  }

  function openNodeDialog(id) {
    editingId = id;
    const node = id && lastState?.nodes.find((n) => n.id === id);
    const cfg = id ? node?.cfg || nodeCfg.get(id) || defaultsFor(id, node?.addr) : suggestNew();
    for (const el of nodeForm.elements) {
      if (!el.name) continue;
      if (el.type === "checkbox") el.checked = !!cfg[el.name];
      else el.value = cfg[el.name] ?? "";
      el.classList.remove("invalid");
    }
    nodeForm.elements.id.readOnly = !!id;
    $("#node-dlg-badge").textContent = id || "+";
    $("#node-dlg-heading").textContent = id ? `EDIT ${id.toUpperCase()}` : "ADD NODE";
    $("#node-dlg-sub").textContent = id ? "接続設定" : "ノード追加";
    $("#node-submit").textContent = id ? "SAVE & RESTART" : "ADD NODE";
    $("#node-remove").hidden = !id;
    $("#node-dlg-note").textContent = id
      ? (node?.up ? `${id} restarts to apply new settings. Its Raft state is kept in the WAL.` : "")
      : useMock ? "The node starts as a follower and catches up from the leader."
        : "Added stopped. Every node must be stopped to change membership; press START when ready.";
    $("#node-dlg-error").textContent = "";
    nodeDlg.showModal();
    (id ? nodeForm.elements.clientAddr : nodeForm.elements.id).focus();
  }

  function readNodeForm() {
    const f = nodeForm.elements;
    const cfg = {
      id: f.id.value.trim(), clientAddr: f.clientAddr.value.trim(), replAddr: f.replAddr.value.trim(),
      wal: f.wal.value.trim(), pprofAddr: f.pprofAddr.value.trim(), verbose: f.verbose.checked,
    };
    const errs = [];
    const bad = (name, msg) => { f[name].classList.add("invalid"); errs.push(msg); };
    for (const el of f) el.classList?.remove("invalid");

    const others = ((lastState && lastState.nodes) || []).filter((n) => n.id !== editingId);
    if (!/^[A-Za-z0-9_-]{1,16}$/.test(cfg.id)) bad("id", "Node ID: use 1–16 letters, digits, - or _.");
    else if (!editingId && others.some((n) => n.id === cfg.id)) bad("id", `Node ID: ${cfg.id} already exists.`);
    if (!ADDR_RE.test(cfg.clientAddr)) bad("clientAddr", "Client address: use host:port, e.g. 127.0.0.1:9030.");
    if (!ADDR_RE.test(cfg.replAddr)) bad("replAddr", "Replication address: use host:port.");
    if (cfg.pprofAddr && !ADDR_RE.test(cfg.pprofAddr)) bad("pprofAddr", "Pprof address: use host:port or leave it empty.");
    if (cfg.clientAddr && cfg.clientAddr === cfg.replAddr) bad("replAddr", "Client and replication addresses must differ.");
    if (others.some((n) => n.addr === cfg.clientAddr)) bad("clientAddr", `Client address: ${cfg.clientAddr} is used by another node.`);
    return { cfg, errs };
  }

  async function submitNode(call, busyLabel) {
    const btns = nodeDlg.querySelectorAll(".modal-foot .btn");
    const submit = $("#node-submit");
    const label = submit.textContent;
    btns.forEach((b) => (b.disabled = true));
    submit.textContent = busyLabel;
    try {
      const res = await call();
      if (!res.ok) throw new Error(res.error || "request failed");
      nodeDlg.close();
      return true;
    } catch (e) {
      $("#node-dlg-error").textContent = `Not saved: ${e.message}`;
      return false;
    } finally {
      btns.forEach((b) => (b.disabled = false));
      submit.textContent = label;
    }
  }

  $("#add-slot").addEventListener("click", () => openNodeDialog(null));

  nodeForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const { cfg, errs } = readNodeForm();
    $("#node-dlg-error").textContent = errs.join(" ");
    if (errs.length) { nodeForm.querySelector(".invalid")?.focus(); return; }
    const isNew = !editingId;
    const ok = await submitNode(
      () => (isNew ? api.addNode(cfg) : api.updateNode(cfg.id, cfg)),
      isNew ? "ADDING…" : "SAVING…"
    );
    if (ok) nodeCfg.set(cfg.id, cfg);
  });

  $("#node-remove").addEventListener("click", async () => {
    const id = editingId;
    const btn = $("#node-remove");
    if (btn.dataset.confirm !== id) { // two-step: first click arms, second click removes
      btn.dataset.confirm = id;
      btn.textContent = `CONFIRM REMOVE ${id.toUpperCase()}`;
      return;
    }
    const ok = await submitNode(() => api.removeNode(id), "REMOVING…");
    if (ok) nodeCfg.delete(id);
  });
  nodeDlg.addEventListener("close", () => {
    const btn = $("#node-remove");
    delete btn.dataset.confirm;
    btn.textContent = "REMOVE NODE";
  });

  logEl.innerHTML = '<li class="empty">waiting for events…</li>';
  print("kestrelKV console — commands: set <key> <value>, get <key>, del <key>, len, ping", "hint");
  $("#foot-mode").textContent = useMock ? "MOCK CLUSTER · SIMULATED" : `LIVE · ${location.host}`;
  api.subscribe({ state: renderState, log: addLog, link: setLink });

  // ---------- live backend ----------

  function makeRemote() {
    async function post(url, body, method = "POST") {
      const r = await fetch(url, {
        method,
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
      addNode: (cfg) => post("/api/nodes", cfg),
      updateNode: (id, cfg) => post(`/api/nodes/${encodeURIComponent(id)}/config`, cfg, "PUT"),
      removeNode: (id) => post(`/api/nodes/${encodeURIComponent(id)}`, null, "DELETE"),
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
      async addNode(cfg) {
        await delay(400);
        nodes.push({ id: cfg.id, addr: cfg.clientAddr, up: true, role: "follower", term: 0, leader: "", timer: 3 });
        emit("node", `${cfg.id} joined the cluster at ${cfg.clientAddr}`);
        tick();
        return { ok: true };
      },
      async updateNode(id, cfg) {
        await delay(300);
        const n = byId(id);
        if (!n) return { ok: false, error: `${id} no longer exists` };
        const wasUp = n.up;
        if (wasUp) { n.up = false; if (n.role === "leader") for (const m of nodes) m.leader = ""; n.role = "follower"; }
        n.addr = cfg.clientAddr;
        emit("node", `${id} settings saved (client ${cfg.clientAddr}, replication ${cfg.replAddr})`);
        tick();
        if (wasUp) {
          await delay(600);
          n.up = true; n.timer = 3;
          emit("node", `${id} restarted with new settings`);
          tick();
        }
        return { ok: true };
      },
      async removeNode(id) {
        await delay(300);
        if (nodes.length <= 1) return { ok: false, error: "the cluster needs at least one node" };
        const i = nodes.findIndex((n) => n.id === id);
        if (i < 0) return { ok: false, error: `${id} no longer exists` };
        const [gone] = nodes.splice(i, 1);
        if (gone.role === "leader") for (const m of nodes) { m.leader = ""; }
        if (connected === id) connected = (nodes.find((n) => n.up) || nodes[0]).id;
        emit("node", `${id} removed from the cluster`);
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
