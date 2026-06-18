# Service Worker Patterns with RxJS

Push notifications, background sync, cache-first strategies, and communicating between the Service Worker and the main thread — all wrapped in Observables.

---

## SW ↔ Main Thread Communication

Service Workers run in a separate thread. The bridge is `postMessage` / `navigator.serviceWorker`. Wrapping in RxJS gives you composition and lifetime management.

```typescript
import { fromEvent, Observable } from 'rxjs';
import { filter, map, share } from 'rxjs/operators';

// Listen to all messages from the Service Worker:
const swMessages$ = fromEvent<MessageEvent>(navigator.serviceWorker, 'message').pipe(
  map(e => e.data),
  share()
);

// Send a typed message to the active SW:
function postToSW(message: unknown): void {
  navigator.serviceWorker.controller?.postMessage(message);
}
```

---

## Pattern 1: Push Notification Permission + Subscription

```typescript
import { from, of, EMPTY } from 'rxjs';
import { switchMap, catchError, map } from 'rxjs/operators';

const VAPID_PUBLIC_KEY = 'your-vapid-public-key';

function urlB64ToUint8Array(base64: string): Uint8Array {
  const padded  = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
  const raw     = atob(padded.replace(/-/g, '+').replace(/_/g, '/'));
  return Uint8Array.from(Array.from(raw), c => c.charCodeAt(0));
}

function subscribeToPush(): Observable<PushSubscription> {
  return from(navigator.serviceWorker.ready).pipe(
    switchMap(reg =>
      from(Notification.requestPermission()).pipe(
        switchMap(permission => {
          if (permission !== 'granted') return EMPTY;
          return from(reg.pushManager.subscribe({
            userVisibleOnly:      true,
            applicationServerKey: urlB64ToUint8Array(VAPID_PUBLIC_KEY)
          }));
        })
      )
    ),
    catchError(err => {
      console.error('Push subscription failed:', err);
      return EMPTY;
    })
  );
}

// Subscribe and send token to server:
subscribeToPush().pipe(
  switchMap(sub =>
    this.api.savePushSubscription(sub.toJSON())
  ),
  takeUntilDestroyed()
).subscribe(() => showNotificationOptIn(true));
```

---

## Pattern 2: Receive Push Notifications as Observable

```typescript
// In the Service Worker (sw.ts):
self.addEventListener('push', (event: PushEvent) => {
  const data = event.data?.json() ?? { title: 'New notification' };
  event.waitUntil(
    (self as ServiceWorkerGlobalScope).registration.showNotification(
      data.title,
      { body: data.body, icon: data.icon, data: data.payload }
    )
  );
});

self.addEventListener('notificationclick', (event: NotificationEvent) => {
  event.notification.close();
  // Post to main thread:
  event.waitUntil(
    (self as ServiceWorkerGlobalScope).clients.matchAll().then(clients =>
      clients.forEach(client =>
        client.postMessage({ type: 'NOTIFICATION_CLICK', payload: event.notification.data })
      )
    )
  );
});

// In the main thread:
interface SWMessage {
  type:    'NOTIFICATION_CLICK' | 'SYNC_COMPLETE' | 'CACHE_UPDATED';
  payload: unknown;
}

const swMessages$ = fromEvent<MessageEvent<SWMessage>>(navigator.serviceWorker, 'message').pipe(
  map(e => e.data),
  share()
);

const notificationClicks$ = swMessages$.pipe(
  filter(m => m.type === 'NOTIFICATION_CLICK'),
  map(m => m.payload as NotificationPayload)
);

notificationClicks$.pipe(takeUntilDestroyed()).subscribe(payload => {
  router.navigateByUrl(payload.url);
  notificationStore.markRead(payload.id);
});
```

---

## Pattern 3: Background Sync — Queue Offline Mutations

```typescript
// In the Service Worker (sw.ts):
self.addEventListener('sync', (event: SyncEvent) => {
  if (event.tag === 'sync-mutations') {
    event.waitUntil(flushOfflineQueue());
  }
});

async function flushOfflineQueue(): Promise<void> {
  const db      = await openQueueDB();
  const queued  = await db.getAll('mutations');
  for (const mutation of queued) {
    try {
      await fetch(mutation.url, mutation.options);
      await db.delete('mutations', mutation.id);
    } catch {
      break; // still offline — leave remaining for next sync
    }
  }
  // Notify main thread:
  const clients = await (self as ServiceWorkerGlobalScope).clients.matchAll();
  clients.forEach(c => c.postMessage({ type: 'SYNC_COMPLETE' }));
}

// Main thread — register sync and react to completion:
function queueMutation(url: string, options: RequestInit): Observable<void> {
  return from(navigator.serviceWorker.ready).pipe(
    switchMap(reg => from(reg.sync.register('sync-mutations'))),
    tap(() => saveToLocalQueue(url, options)),
    map(() => void 0),
    catchError(() => {
      // Background sync not supported — try direct request:
      return from(fetch(url, options)).pipe(map(() => void 0));
    })
  );
}

// Listen for sync completion:
swMessages$.pipe(
  filter(m => m.type === 'SYNC_COMPLETE'),
  takeUntilDestroyed()
).subscribe(() => {
  showToast('Changes synced successfully');
  refreshData();
});
```

---

## Pattern 4: Cache Update Notifications

React when the Service Worker updates cached resources:

```typescript
// In the Service Worker (sw.ts):
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames
          .filter(name => name !== CACHE_VERSION)
          .map(name => caches.delete(name))
      );
    }).then(() => {
      // Notify all clients of cache refresh:
      return (self as ServiceWorkerGlobalScope).clients.matchAll().then(clients =>
        clients.forEach(c => c.postMessage({ type: 'CACHE_UPDATED', version: CACHE_VERSION }))
      );
    })
  );
});

// Main thread — prompt user to reload for new version:
swMessages$.pipe(
  filter(m => m.type === 'CACHE_UPDATED'),
  takeUntilDestroyed()
).subscribe(({ version }) => {
  showUpdateBanner(`New version ${version} available`, {
    action: 'Reload',
    onClick: () => window.location.reload()
  });
});
```

---

## Pattern 5: SW Registration Lifecycle as Observable

```typescript
import { from, fromEvent, merge, EMPTY } from 'rxjs';
import { switchMap, map, filter, shareReplay } from 'rxjs/operators';

type SWStatus = 'unsupported' | 'registering' | 'active' | 'waiting' | 'error';

function serviceWorkerStatus$(): Observable<SWStatus> {
  if (!('serviceWorker' in navigator)) return of('unsupported' as SWStatus);

  return from(navigator.serviceWorker.register('/sw.js')).pipe(
    switchMap(reg => {
      // Map registration state changes to typed statuses:
      const stateChange$ = fromEvent(reg, 'updatefound').pipe(
        switchMap(() => {
          const installing = reg.installing;
          if (!installing) return EMPTY;
          return fromEvent(installing, 'statechange').pipe(
            map(() => {
              switch (installing.state) {
                case 'installed': return reg.active ? 'waiting' : 'active';
                case 'activated': return 'active';
                default:          return 'registering';
              }
            })
          );
        })
      );

      const initial: SWStatus = reg.active ? 'active' : 'registering';
      return merge(of(initial), stateChange$);
    }),
    catchError(() => of('error' as SWStatus)),
    shareReplay(1)
  );
}

serviceWorkerStatus$().pipe(takeUntilDestroyed()).subscribe(status => {
  if (status === 'waiting') showUpdateAvailableBanner();
  if (status === 'active')  hideUpdateBanner();
});
```

---

## Pattern 6: Skip Waiting (Force SW Activation)

```typescript
// Main thread — tell waiting SW to activate immediately:
function skipWaiting(): Observable<void> {
  return from(navigator.serviceWorker.ready).pipe(
    switchMap(reg => {
      if (!reg.waiting) return EMPTY;
      reg.waiting.postMessage({ type: 'SKIP_WAITING' });
      return fromEvent(navigator.serviceWorker, 'controllerchange').pipe(
        take(1),
        map(() => void 0)
      );
    })
  );
}

// Update banner "Click to reload" button:
updateBannerClick$.pipe(
  switchMap(() => skipWaiting()),
  tap(() => window.location.reload()),
  takeUntilDestroyed()
).subscribe();

// In the Service Worker:
self.addEventListener('message', (event: MessageEvent) => {
  if (event.data?.type === 'SKIP_WAITING') {
    (self as ServiceWorkerGlobalScope).skipWaiting();
  }
});
```

---

## Common Pitfalls

### Subscribing Before SW is Ready

```typescript
// ❌ Posting to SW before it's active — message lost:
navigator.serviceWorker.controller?.postMessage(payload);
// controller is null until SW is activated

// ✅ Always wait for ready:
from(navigator.serviceWorker.ready).pipe(
  tap(reg => reg.active?.postMessage(payload))
).subscribe();
```

### Not Handling Push Permission Denial

```typescript
// ❌ Assuming permission is granted:
reg.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: key });
// Throws if user denied permission

// ✅ Check permission before subscribing:
from(Notification.requestPermission()).pipe(
  filter(p => p === 'granted'),
  switchMap(() => from(reg.pushManager.subscribe(...)))
).subscribe(sub => sendToServer(sub));
```
