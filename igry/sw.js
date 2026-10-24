// Окремий service worker для гри: його scope (/destiny/igry/) вужчий за scope
// рандомайзера, тож сторінки гри обслуговує саме він, а не cache-first воркер із кореня.
// Стратегія network-first: оновлення видно одразу, кеш — лише на випадок офлайну.
const CACHE = 'igry-v1';
const ASSETS = ['./', './index.html'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(ASSETS)).catch(() => {}).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k.startsWith('igry-') && k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  const own = url.origin === location.origin;
  const fonts = url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com';
  if (!own && !fonts) return; // брокери та інше — напряму, без кешу

  e.respondWith(
    fetch(req).then((res) => {
      if (res && (res.ok || res.type === 'opaque')) {
        const copy = res.clone();
        caches.open(CACHE).then((c) => c.put(req, copy));
      }
      return res;
    }).catch(() => caches.match(req, { ignoreSearch: own }).then((r) => r || (req.mode === 'navigate' ? caches.match('./index.html') : undefined)))
  );
});
