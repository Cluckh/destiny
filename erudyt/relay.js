/* =========================================================
   Relay — зв'язок між телефонами через публічні MQTT-брокери.
   Працює на будь-якій мережі (мобільний інтернет, 5G, Wi-Fi), бо телефони
   не з'єднуються напряму, а обидва ходять до брокера по звичайному WebSocket.

   • Кожне повідомлення йде одразу через два брокери — якщо один ляже, працює інший.
   • Вміст шифрується AES-GCM ключем, виведеним із коду кімнати (код є лише в посиланні),
     а назва каналу — це хеш, тож брокер не бачить ні коду, ні змісту гри.
   • Повідомлення від кожного відправника кожному адресату доставляються строго по порядку.
   ========================================================= */
(function () {
  'use strict';
  const DEFAULT_BROKERS = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'];
  const enc = new TextEncoder(), dec = new TextDecoder();
  const hex = (buf) => Array.from(new Uint8Array(buf), (b) => b.toString(16).padStart(2, '0')).join('');
  const rid = (n) => { const a = new Uint8Array(n); crypto.getRandomValues(a); return hex(a); };

  class Relay {
    constructor({ app, code, me, onMessage, onStatus }) {
      this.app = app; this.code = code; this.me = me;
      this.onMessage = onMessage; this.onStatus = onStatus || (() => {});
      this.session = rid(6);
      this.seqs = {};          // окрема нумерація для кожного адресата: інакше в чужих повідомленнях з’являлися б «дірки»
      this.clients = [];
      this.up = 0;
      this.seen = new Map();      // id → час, щоб відкинути дубль із другого брокера
      this.streams = new Map();   // відправник:сесія → { next, buf, timer }
      this.stopped = false;
    }
    brokers() {
      const q = new URLSearchParams(location.search).get('mq');   // для локального тесту: ?mq=ws://127.0.0.1:8888
      return q ? q.split(',') : DEFAULT_BROKERS;
    }
    async start() {
      const base = this.app + '|' + this.code;
      this.topic = 'cluckh-games/' + hex(await crypto.subtle.digest('SHA-256', enc.encode('topic|' + base))).slice(0, 40);
      const raw = await crypto.subtle.digest('SHA-256', enc.encode('key|' + base));
      this.key = await crypto.subtle.importKey('raw', raw, 'AES-GCM', false, ['encrypt', 'decrypt']);
      if (this.stopped) return;
      this.brokers().forEach((url) => this.connect(url));
    }
    connect(url) {
      const c = mqtt.connect(url, {
        clientId: 'cg_' + rid(8), clean: true, keepalive: 20,
        reconnectPeriod: 2500, connectTimeout: 9000, protocolVersion: 4,
      });
      c._up = false;
      c.on('connect', () => {
        c.subscribe(this.topic, { qos: 0 }, (err) => { if (!err && !c._up) { c._up = true; this.recount(); } });
      });
      const down = () => { if (c._up) { c._up = false; this.recount(); } };
      c.on('close', down); c.on('offline', down); c.on('error', () => {});
      c.on('message', (t, payload) => { if (t === this.topic) this.receive(payload); });
      this.clients.push(c);
    }
    recount() {
      const up = this.clients.filter((c) => c._up).length;
      if (up !== this.up) { this.up = up; this.onStatus(up); }
    }
    async send(to, payload, except) {
      if (!this.key) return;
      this.seqs[to] = (this.seqs[to] || 0) + 1;
      const msg = { f: this.me, s: this.session, n: this.seqs[to], t: to, i: rid(8), p: payload };
      if (except) msg.x = except;
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, this.key, enc.encode(JSON.stringify(msg))));
      const out = new Uint8Array(12 + ct.length);
      out.set(iv, 0); out.set(ct, 12);
      this.clients.forEach((c) => { if (c.connected) c.publish(this.topic, out, { qos: 0 }); });
    }
    async receive(payload) {
      let m;
      try {
        const data = new Uint8Array(payload);
        const pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: data.slice(0, 12) }, this.key, data.slice(12));
        m = JSON.parse(dec.decode(pt));
      } catch (e) { return; }   // чуже або пошкоджене — ігноруємо
      if (!m || m.f === this.me || this.seen.has(m.i)) return;
      if (m.t !== '*' && m.t !== this.me) return;
      const now = Date.now();
      this.seen.set(m.i, now);
      if (this.seen.size > 600) { for (const [k, t] of this.seen) { if (now - t > 60000 || this.seen.size > 400) this.seen.delete(k); else break; } }
      this.order(m);
    }
    order(m) {
      const key = m.f + ':' + m.s + ':' + m.t;
      let st = this.streams.get(key);
      if (!st) { st = { next: m.n, buf: new Map(), timer: null }; this.streams.set(key, st); }
      if (m.n < st.next) return;
      st.buf.set(m.n, m);
      this.flush(st);
    }
    flush(st) {
      while (st.buf.has(st.next)) {
        const m = st.buf.get(st.next);
        st.buf.delete(st.next); st.next++;
        if (m.x && m.x === this.me) continue;   // розсилка «усім, крім мене»
        try { this.onMessage(m.f, m.p, m.s); } catch (e) { console.error(e); }
      }
      if (!st.buf.size) { if (st.timer) { clearTimeout(st.timer); st.timer = null; } return; }
      if (!st.timer) {
        // повідомлення справді загубилось — трохи чекаємо, потім ідемо далі
        st.timer = setTimeout(() => {
          st.timer = null;
          if (st.buf.size) { st.next = Math.min(...st.buf.keys()); this.flush(st); }
        }, 1200);
      }
    }
    wake() {
      this.clients.forEach((c) => { if (!c.connected && !c.reconnecting) { try { c.reconnect(); } catch (e) {} } });
    }
    stop() {
      this.stopped = true;
      this.clients.forEach((c) => { try { c.end(true); } catch (e) {} });
      this.clients = []; this.up = 0;
      this.streams.forEach((st) => st.timer && clearTimeout(st.timer));
    }
  }
  window.Relay = Relay;

  /* ---------- Лобі: список відкритих кімнат ----------
     Господар кожні кілька секунд оголошує кімнату «закріпленим» (retained) повідомленням —
     брокер віддає його кожному, хто відкриє головний екран, тож список з’являється одразу.
     Шифрується спільним ключем гри: це не секрет, а лише щоб брокер не бачив відкритого тексту. */
  class Lobby {
    constructor({ apps, onChange }) {
      this.apps = apps; this.onChange = onChange || (() => {});
      this.rooms = new Map();   // app:id → { app, info, seen }
      this.clients = []; this.up = 0; this.ready = false; this.stopped = false;
      this.topics = {}; this.keys = {};
    }
    brokers() { const q = new URLSearchParams(location.search).get('mq'); return q ? q.split(',') : DEFAULT_BROKERS; }
    async start(watch) {
      for (const app of this.apps) {
        this.topics[app] = 'cluckh-games/lobby/' + hex(await crypto.subtle.digest('SHA-256', enc.encode('lobby|' + app))).slice(0, 24);
        const raw = await crypto.subtle.digest('SHA-256', enc.encode('lobbykey|' + app));
        this.keys[app] = await crypto.subtle.importKey('raw', raw, 'AES-GCM', false, ['encrypt', 'decrypt']);
      }
      if (this.stopped) return;
      this.watch = watch !== false;
      this.brokers().forEach((url) => {
        const c = mqtt.connect(url, { clientId: 'cl_' + rid(8), clean: true, keepalive: 30, reconnectPeriod: 3000, connectTimeout: 9000, protocolVersion: 4 });
        c.on('connect', () => {
          c._up = true; this.up = this.clients.filter((x) => x._up).length;
          if (this.watch) this.apps.forEach((app) => c.subscribe(this.topics[app] + '/+', { qos: 0 }));
          if (this.pending) this.pending.forEach((fn) => fn());
          this.onChange();
        });
        const down = () => { c._up = false; this.up = this.clients.filter((x) => x._up).length; };
        c.on('close', down); c.on('offline', down); c.on('error', () => {});
        c.on('message', (t, payload, packet) => this.receive(t, payload, packet));
        this.clients.push(c);
      });
      setTimeout(() => { this.ready = true; this.onChange(); }, 2500);   // через 2,5 с вважаємо, що «закріплені» вже прийшли
    }
    async receive(topic, payload, packet) {
      const app = this.apps.find((a) => topic.indexOf(this.topics[a] + '/') === 0);
      if (!app) return;
      const key = app + ':' + topic.slice(this.topics[app].length + 1);
      if (!payload || payload.length === 0) { if (this.rooms.delete(key)) this.onChange(); return; }
      try {
        const data = new Uint8Array(payload);
        const pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: data.slice(0, 12) }, this.keys[app], data.slice(12));
        const info = JSON.parse(dec.decode(pt));
        if (!info || !info.c) return;
        // закріплене повідомлення могло пролежати довго — рахуємо його вік за часом відправника
        const seen = packet && packet.retain ? Math.min(Date.now(), +info.t || 0) : Date.now();
        this.rooms.set(key, { app, info, seen });
        this.onChange();
      } catch (e) {}
    }
    list(app) {
      const now = Date.now();
      return Array.from(this.rooms.values()).filter((r) => (!app || r.app === app) && now - r.seen < 30000).sort((a, b) => (b.info.o - a.info.o) || (b.seen - a.seen));
    }
    async roomTopic(app, code) {
      const k = app + ':' + code;
      this.tcache = this.tcache || {};
      if (!this.tcache[k]) this.tcache[k] = this.topics[app] + '/' + hex(await crypto.subtle.digest('SHA-256', enc.encode('room|' + code))).slice(0, 16);
      return this.tcache[k];
    }
    clearSync(app, code) {   // для pagehide: без await, лише якщо тему вже пораховано
      const t = this.tcache && this.tcache[app + ':' + code];
      if (t) this.clients.forEach((c) => { if (c.connected) c.publish(t, new Uint8Array(0), { qos: 0, retain: true }); });
    }
    whenUp(fn) { if (this.clients.some((c) => c._up)) fn(); else (this.pending = this.pending || []).push(fn); }
    async announce(app, info) {
      if (!this.keys[app]) return;
      const topic = await this.roomTopic(app, info.c);
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, this.keys[app], enc.encode(JSON.stringify(Object.assign({ t: Date.now() }, info)))));
      const out = new Uint8Array(12 + ct.length); out.set(iv, 0); out.set(ct, 12);
      this.clients.forEach((c) => { if (c.connected) c.publish(topic, out, { qos: 0, retain: true }); });
    }
    async clear(app, code) {
      if (!this.keys[app]) return;
      const topic = await this.roomTopic(app, code);
      this.clients.forEach((c) => { if (c.connected) c.publish(topic, new Uint8Array(0), { qos: 0, retain: true }); });
    }
    stop() {
      this.stopped = true;
      this.clients.forEach((c) => { try { c.end(false); } catch (e) {} });
      this.clients = [];
    }
  }
  window.RelayLobby = Lobby;
})();
