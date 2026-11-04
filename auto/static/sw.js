// Service worker for the installable dashboard. Live data comes over the
// websocket, so all this does is keep the page shell available: the installed
// app opens (and then reconnects) even while the station is unreachable.
const CACHE = 'gs-shell-v1';
const SHELL = ['/', '/static/satellite-6.min.js', '/static/coastlines.geojson',
               '/static/icon-192.png', '/manifest.webmanifest'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});

// Network first, cached shell as fallback. Artifacts and APIs are not cached.
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET' || url.origin !== location.origin) return;
  const path = e.request.mode === 'navigate' ? '/' : url.pathname;
  if (!SHELL.includes(path)) return;
  e.respondWith(fetch(e.request).then(resp => {
    if (resp.ok) { const copy = resp.clone(); caches.open(CACHE).then(c => c.put(path, copy)); }
    return resp;
  }).catch(() => caches.match(path)));
});
