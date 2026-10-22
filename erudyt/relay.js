/* =========================================================
   Relay — зв'язок між телефонами через публічні MQTT-брокери.
   Працює на будь-якій мережі (мобільний інтернет, 5G, Wi-Fi), бо телефони
   не з'єднуються напряму, а обидва ходять до брокера по звичайному WebSocket.

   • Кожне повідомлення йде одразу через два брокери — якщо один ляже, працює інший.
   • Вміст шифрується AES-GCM ключем, виведеним із коду кімнати (код є лише в посиланні),
     а назва каналу — це хеш, тож брокер не бачить ні коду, ні змісту гри.
   • Повідомлення від кожного відправника доставляються строго по порядку.
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
      this.seq = 0;
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
      const msg = { f: this.me, s: this.session, n: ++this.seq, t: to, i: rid(8), p: payload };
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
      if (m.x && m.x === this.me) return;
      const now = Date.now();
      this.seen.set(m.i, now);
      if (this.seen.size > 600) { for (const [k, t] of this.seen) { if (now - t > 60000 || this.seen.size > 400) this.seen.delete(k); else break; } }
      this.order(m);
    }
    order(m) {
      const key = m.f + ':' + m.s;
      let st = this.streams.get(key);
      if (!st) { st = { next: m.n, buf: new Map(), timer: null }; this.streams.set(key, st); }
      if (m.n < st.next) return;
      st.buf.set(m.n, m);
      this.flush(st);
      if (st.buf.size && !st.timer) {
        // чекаємо пропущене повідомлення трохи, потім ідемо далі
        st.timer = setTimeout(() => { st.timer = null; if (st.buf.size) { st.next = Math.min(...st.buf.keys()); this.flush(st); } }, 1200);
      }
    }
    flush(st) {
      while (st.buf.has(st.next)) {
        const m = st.buf.get(st.next);
        st.buf.delete(st.next); st.next++;
        try { this.onMessage(m.f, m.p, m.s); } catch (e) { console.error(e); }
      }
      if (!st.buf.size && st.timer) { clearTimeout(st.timer); st.timer = null; }
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
})();
