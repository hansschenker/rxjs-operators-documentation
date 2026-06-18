# RxJS with Server-Side Rendering

Patterns for Angular Universal, Next.js (with RxJS), and generic SSR environments — async data resolution before render, hydration streams, and preventing subscription leaks in server contexts.

---

## The SSR RxJS Challenge

```
Browser (hydrated)                    Server (SSR render)
─────────────────────                 ───────────────────────
Components mount once                 Components render once, then destroyed
Subscriptions persist                 ALL subscriptions must complete
BehaviorSubject replays               Same, but must be seeded with server data
WebSocket: connect on load            Never connect on server
setInterval / polling                 Never poll on server — render hangs
```

The core rule: **every Observable used during SSR must complete before the render finishes**, or the server render hangs waiting for a stream that never ends.

---

## Pattern 1: Angular Universal — Resolvers and HTTP Completion

```typescript
// ✅ HTTP calls complete naturally — no special handling needed:
@Component({ standalone: true })
class ProductPageComponent {
  readonly product = toSignal(
    inject(ActivatedRoute).data.pipe(
      map(data => data['product'] as Product)
    ),
    { requireSync: true }  // resolver ensures sync data
  );
}

// Angular Universal (SSR) with TransferState — hydration bridge:
import { TransferState, makeStateKey, isPlatformBrowser } from '@angular/core';
import { PLATFORM_ID } from '@angular/core';

const PRODUCT_KEY = makeStateKey<Product>('product');

@Injectable()
class ProductService {
  private readonly transferState = inject(TransferState);
  private readonly platformId    = inject(PLATFORM_ID);

  getProduct$(id: string): Observable<Product> {
    // On server: fetch and store in TransferState
    if (!isPlatformBrowser(this.platformId)) {
      return this.http.get<Product>(`/api/products/${id}`).pipe(
        tap(product => this.transferState.set(PRODUCT_KEY, product))
      );
    }

    // On browser: read from TransferState (no extra HTTP call):
    const cached = this.transferState.get(PRODUCT_KEY, null);
    if (cached) {
      this.transferState.remove(PRODUCT_KEY);
      return of(cached);  // immediate, no HTTP
    }

    return this.http.get<Product>(`/api/products/${id}`);
  }
}
```

---

## Pattern 2: Conditional Stream Activation by Platform

Use platform detection to disable streams that can't run on the server:

```typescript
import { inject, PLATFORM_ID } from '@angular/core';
import { isPlatformBrowser, isPlatformServer } from '@angular/common';
import { EMPTY } from 'rxjs';

@Injectable({ providedIn: 'root' })
class RealtimeService {
  private readonly platformId = inject(PLATFORM_ID);

  // WebSocket must NOT connect on server:
  readonly messages$: Observable<Message> = isPlatformBrowser(this.platformId)
    ? webSocket<Message>('wss://api.example.com/ws').pipe(
        retry({ count: 3, delay: 2000 })
      )
    : EMPTY; // server gets EMPTY — no connection, completes immediately

  // Polling must NOT run on server (would hang render):
  readonly pollData$: Observable<Data> = isPlatformBrowser(this.platformId)
    ? timer(0, 10_000).pipe(
        switchMap(() => this.http.get<Data>('/api/data'))
      )
    : this.http.get<Data>('/api/data').pipe(take(1)); // server: fetch once

  // animationFrames must NOT run on server:
  readonly animation$: Observable<number> = isPlatformBrowser(this.platformId)
    ? animationFrames()
    : EMPTY;
}
```

---

## Pattern 3: Ensuring Streams Complete for SSR

Angular Universal waits until the zone is stable (no pending async tasks). Streams that don't complete can prevent this:

```typescript
import { ApplicationRef } from '@angular/core';
import { firstValueFrom } from 'rxjs';
import { filter } from 'rxjs/operators';

// Angular Universal waits for zone to be stable:
async function renderApp(url: string): Promise<string> {
  const appRef = inject(ApplicationRef);

  // Wait for zone to stabilize (all async work done):
  await firstValueFrom(
    appRef.isStable.pipe(filter(stable => stable))
  );

  return renderHtml(); // safe to capture HTML now
}

// Problematic: BehaviorSubject subscription in constructor (stays open):
@Component({ standalone: true })
class BadComponent {
  constructor() {
    // ❌ This subscription stays open — zone never stabilizes:
    interval(1000).pipe(
      switchMap(() => this.http.get('/api/tick'))
    ).subscribe(data => this.tick = data);
  }
}

// ✅ Gate polling behind platform check:
@Component({ standalone: true })
class GoodComponent {
  private readonly platformId = inject(PLATFORM_ID);

  constructor() {
    if (!isPlatformBrowser(this.platformId)) return; // skip on server

    interval(1000).pipe(
      switchMap(() => this.http.get('/api/tick')),
      takeUntilDestroyed()
    ).subscribe(data => this.tick = data);
  }
}
```

---

## Pattern 4: Next.js + RxJS (React Server Components)

In Next.js, RxJS is used in server utilities and data fetching layers:

```typescript
import { firstValueFrom } from 'rxjs';
import { retry, timeout } from 'rxjs/operators';

// Server Action with Observable:
'use server';

async function fetchUserData(userId: string): Promise<UserData> {
  return firstValueFrom(
    userApiService.getUser$(userId).pipe(
      retry({ count: 2, delay: 500 }),
      timeout(5000)
    )
  );
}

// Page component (Server Component):
export default async function UserPage({ params }: { params: { id: string } }) {
  const user = await fetchUserData(params.id);
  return <UserProfile user={user} />;
}

// Client Component with RxJS (browser only):
'use client';

import { useEffect, useState } from 'react';
import { webSocket } from 'rxjs/webSocket';
import { retry } from 'rxjs/operators';

function LiveUserStatus({ userId }: { userId: string }) {
  const [status, setStatus] = useState<string>('offline');

  useEffect(() => {
    const sub = webSocket<UserStatus>(`wss://api.example.com/users/${userId}`)
      .pipe(retry({ count: 3, delay: 2000 }))
      .subscribe(s => setStatus(s.online ? 'online' : 'offline'));

    return () => sub.unsubscribe(); // cleanup on unmount
  }, [userId]);

  return <span>{status}</span>;
}
```

---

## Pattern 5: Hydration-Safe State Seeding

Bridge server-fetched state to client hydration without double-fetching:

```typescript
// Angular 17+ with SSR hydration:
import { provideClientHydration } from '@angular/platform-browser';

// Bootstrap:
bootstrapApplication(AppComponent, {
  providers: [
    provideClientHydration()  // enables hydration with state transfer
  ]
});

// Service that seeds from server state on hydration:
@Injectable({ providedIn: 'root' })
class HydratedDataService {
  private readonly transferState = inject(TransferState);
  private readonly platformId    = inject(PLATFORM_ID);
  private readonly http          = inject(HttpClient);

  private readonly USERS_KEY = makeStateKey<User[]>('users');

  readonly users$: Observable<User[]> = defer(() => {
    // Server path: fetch and cache
    if (isPlatformServer(this.platformId)) {
      return this.http.get<User[]>('/api/users').pipe(
        tap(users => this.transferState.set(this.USERS_KEY, users))
      );
    }

    // Browser first render: read cached, schedule removal
    const cached = this.transferState.get<User[]>(this.USERS_KEY, []);
    if (cached.length > 0) {
      this.transferState.remove(this.USERS_KEY);
      return of(cached).pipe(
        // Re-fetch in background to get fresh data after hydration:
        concat(this.http.get<User[]>('/api/users'))
      );
    }

    return this.http.get<User[]>('/api/users');
  }).pipe(shareReplay(1));
}
```

---

## Pattern 6: SSR-Safe Custom Operators

Build platform-aware operators for reuse across components:

```typescript
import { inject, PLATFORM_ID } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import { OperatorFunction, EMPTY } from 'rxjs';

// Operator that no-ops on server:
function browserOnly<T>(): OperatorFunction<T, T> {
  return (source$: Observable<T>) => {
    try {
      const platformId = inject(PLATFORM_ID);
      return isPlatformBrowser(platformId) ? source$ : EMPTY;
    } catch {
      // Outside injection context — assume browser:
      return source$;
    }
  };
}

// Usage — any stream that should only run in browser:
animationFrames().pipe(
  browserOnly(),
  map(frame => frame.elapsed)
).subscribe(renderFrame);

webSocketMessages$.pipe(
  browserOnly()  // WebSocket silently disabled on server
).subscribe(handleMessage);

// Timeout operator that's more lenient on server (slower):
function ssrAwareTimeout<T>(browserMs: number, serverMs: number): OperatorFunction<T, T> {
  const platformId = inject(PLATFORM_ID);
  const ms = isPlatformBrowser(platformId) ? browserMs : serverMs;
  return timeout(ms);
}

this.http.get('/api/data').pipe(
  ssrAwareTimeout(3000, 10000) // 3s browser, 10s server (cold start)
).subscribe(handleData);
```

---

## Common Pitfalls

### Infinite Streams Hanging the Server Render

```typescript
// ❌ interval() never completes — render hangs forever:
@Component({ standalone: true })
class ClockComponent {
  readonly time$ = interval(1000).pipe(map(() => new Date().toISOString()));
  // SSR render waits for zone to stabilize — never does because interval is running
}

// ✅ Platform-guard the infinite stream:
@Component({ standalone: true })
class ClockComponent {
  private readonly platformId = inject(PLATFORM_ID);

  readonly time$ = isPlatformBrowser(this.platformId)
    ? interval(1000).pipe(map(() => new Date().toISOString()))
    : of(new Date().toISOString()); // server: static timestamp
}
```

### BehaviorSubject Not Seeded on Server

```typescript
// ❌ BehaviorSubject initialized with null — server renders null state:
@Injectable({ providedIn: 'root' })
class UserStore {
  private readonly user$ = new BehaviorSubject<User | null>(null);
  // Server renders the null/loading state instead of the actual user
}

// ✅ Seed from server-side data source during request handling:
@Injectable({ providedIn: 'root' })
class UserStore {
  private readonly user$ = new BehaviorSubject<User | null>(null);

  // Call from app initializer or route resolver:
  async initFromServer(req: Request) {
    const user = await authenticate(req.headers.authorization);
    this.user$.next(user);
  }
}
```

---

**Key insight**: The two SSR rules are: (1) every Observable that runs during a server render must eventually complete — infinite streams (interval, WebSocket, polling) must be gated behind `isPlatformBrowser()` or replaced with `EMPTY`/`take(1)` equivalents; (2) use Angular's `TransferState` (or Next.js `cache`) to bridge server-fetched data to the client, avoiding duplicate HTTP calls on hydration. The `browserOnly()` custom operator pattern centralizes the platform check into a reusable operator so it doesn't clutter every component.
