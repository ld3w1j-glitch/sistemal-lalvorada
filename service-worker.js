const CACHE_NAME = 'alvorada-pwa-v1';
const STATIC_ASSETS = [
  '/static/manifest.webmanifest',
  '/static/css/app.css',
  '/static/js/pwa-loading.js',
  '/static/img/favicon-32.png',
  '/static/img/favicon-16.png',
  '/static/img/favicon.ico',
  '/static/img/apple-touch-icon.png',
  '/static/img/pwa-192.png',
  '/static/img/pwa-512.png'
];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(STATIC_ASSETS)).catch(() => null));
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))))
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') return;
  event.respondWith(
    caches.match(request).then(cached => cached || fetch(request).catch(() => cached))
  );
});
