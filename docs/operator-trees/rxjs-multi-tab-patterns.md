# Multi-Tab Patterns with RxJS

Cross-tab communication, shared state, tab coordination, and leader election using RxJS and browser APIs.

---

## The Cross-Tab Toolkit

| API | Range | RxJS wrapper | Use for |
|---|---|---|---|
| `BroadcastChannel` | Same origin | `fromEvent` | State broadcast, notifications |
| `localStorage` | Same origin | `fromEvent(window, 'storage')` | Shared state, config sync |
| `SharedWorker` | Same origin | `fromEvent` on port | Coordinated background work |
| `ServiceWorker` | Same origin | `fromEvent` | Push notifications, background sync |

---

## Pattern 1: BroadcastChannel State Sync

Broadcast application state across all open tabs:

```typescript
import { fromEvent, Subject } from 'rxjs';
import { filter, map, tap, share } from 'rxjs/operators';

type TabMessage =
  | { type: 'STATE_UPDATE'; payload: AppState }
  | { type: 'USER_ACTION';  action: string; userId: string }
  | { type: 'LOGOUT' }
  | { type: 'TAB_CLOSED';   tabId: string };

class CrossTabBus {
  private channel  = new BroadcastChannel('app-sync');
  private outbound = new Subject<TabMessage>();
  readonly tabId   = crypto.randomUUID();

  readonly messages$ = fromEvent<MessageEvent<TabMessage>>(this.channel, 'message').pipe(
    map(e => e.data),
    filter(msg => (msg as any).senderId !== this.tabId), // ignore own echoes
    share()
  );

  constructor() {
    this.outbound.subscribe(msg => {
      this.channel.postMessage({ ...msg, senderId: this.tabId });
    });
    window.addEventListener('beforeunload', () => {
      this.channel.postMessage({ type: 'TAB_CLOSED', tabId: this.tabId, senderId: this.tabId });
      this.channel.close();
    });
  }

  send(msg: TabMessage): void { this.outbound.next(msg); }

  listen<T extends TabMessage['type']>(type: T): Observable<Extract<TabMessage, { type: T }>> {
    return this.messages$.pipe(
      filter((msg): msg is Extract<TabMessage, { type: T }> => msg.type === type)
    );
  }
}

// Usage — sync auth state across tabs:
const bus = new CrossTabBus();

// When user logs out in any tab, clear auth in all tabs:
bus.listen('LOGOUT').pipe(
  takeUntilDestroyed()
).subscribe(() => {
  authStore.clearSession();
  router.navigate(['/login']);
});

// Broadcast state updates:
appStore.state$.pipe(
  debounceTime(100),
  distinctUntilChanged()
).subscribe(state => bus.send({ type: 'STATE_UPDATE', payload: state }));
```

---

## Pattern 2: localStorage-Based State Sync (Older Browser Support)

```typescript
import { fromEvent } from 'rxjs';
import { filter, map, share } from 'rxjs/operators';

function localStorageChannel<T>(key: string) {
  const messages$ = fromEvent<StorageEvent>(window, 'storage').pipe(
    filter(e => e.key === key && e.newValue !== null),
    map(e => JSON.parse(e.newValue!) as T),
    share()
  );

  function broadcast(value: T): void {
    // Write triggers storage event in OTHER tabs (not current one):
    localStorage.setItem(key, JSON.stringify({ ...value, _ts: Date.now() }));
  }

  function read(): T | null {
    const stored = localStorage.getItem(key);
    return stored ? JSON.parse(stored) : null;
  }

  return { messages$, broadcast, read };
}

// Sync dark mode preference across tabs:
const themeSync = localStorageChannel<{ dark: boolean }>('theme-pref');

themeSync.messages$.pipe(
  takeUntilDestroyed()
).subscribe(({ dark }) => applyTheme(dark));

// When user changes theme:
toggleDark$.subscribe(dark => {
  applyTheme(dark);              // apply locally immediately
  themeSync.broadcast({ dark }); // broadcast to other tabs
});
```

---

## Pattern 3: Tab Leader Election

Elect one "leader" tab to own expensive background work (polling, WebSocket, timers):

```typescript
import { BehaviorSubject, interval, fromEvent } from 'rxjs';
import { switchMap, filter, startWith, debounceTime } from 'rxjs/operators';

class TabLeaderElection {
  private static LEADER_KEY   = 'tab-leader';
  private static HEARTBEAT_MS = 2_000;
  private static TIMEOUT_MS   = 6_000;

  private tabId     = crypto.randomUUID();
  private isLeader$ = new BehaviorSubject(false);

  readonly leader$ = this.isLeader$.asObservable();

  start(): void {
    this.tryClaimLeadership();

    // Heartbeat: if leader, keep renewing claim:
    this.isLeader$.pipe(
      switchMap(isLeader =>
        isLeader
          ? interval(TabLeaderElection.HEARTBEAT_MS).pipe(
              tap(() => this.renewLeadership())
            )
          : interval(TabLeaderElection.TIMEOUT_MS + 500).pipe(
              tap(() => this.tryClaimLeadership())
            )
      )
    ).subscribe();

    window.addEventListener('beforeunload', () => {
      if (this.isLeader$.getValue()) {
        localStorage.removeItem(TabLeaderElection.LEADER_KEY);
      }
    });
  }

  private tryClaimLeadership(): void {
    const current = localStorage.getItem(TabLeaderElection.LEADER_KEY);
    if (current) {
      const { tabId, ts } = JSON.parse(current);
      if (Date.now() - ts < TabLeaderElection.TIMEOUT_MS) {
        this.isLeader$.next(tabId === this.tabId);
        return;
      }
    }
    // Claim leadership:
    localStorage.setItem(TabLeaderElection.LEADER_KEY, JSON.stringify({
      tabId: this.tabId,
      ts:    Date.now()
    }));
    this.isLeader$.next(true);
  }

  private renewLeadership(): void {
    localStorage.setItem(TabLeaderElection.LEADER_KEY, JSON.stringify({
      tabId: this.tabId,
      ts:    Date.now()
    }));
  }
}

// Usage — only leader tab polls:
const election = new TabLeaderElection();
election.start();

election.leader$.pipe(
  switchMap(isLeader =>
    isLeader
      ? interval(30_000).pipe(
          switchMap(() => api.pollForUpdates()),
          tap(updates => bus.send({ type: 'STATE_UPDATE', payload: updates }))
        )
      : EMPTY
  ),
  takeUntilDestroyed()
).subscribe();
```

---

## Pattern 4: Shared Auth State Across Tabs

Keep authentication state in sync — essential for token refresh:

```typescript
import { BehaviorSubject } from 'rxjs';

class SharedAuthState {
  private bus = new CrossTabBus();
  private auth$ = new BehaviorSubject<AuthState | null>(
    this.loadPersistedAuth()
  );

  readonly isAuthenticated$ = this.auth$.pipe(
    map(a => a !== null && Date.now() < a.expiresAt),
    distinctUntilChanged()
  );

  constructor() {
    // Listen for auth changes from other tabs:
    this.bus.listen('LOGOUT').subscribe(() => {
      this.auth$.next(null);
      this.clearPersistedAuth();
    });

    // When any tab refreshes token, update all tabs:
    fromEvent<StorageEvent>(window, 'storage').pipe(
      filter(e => e.key === 'auth-token' && e.newValue !== null),
      map(e => JSON.parse(e.newValue!) as AuthState)
    ).subscribe(authState => this.auth$.next(authState));
  }

  login(credentials: Credentials): Observable<void> {
    return this.api.login(credentials).pipe(
      tap(authState => {
        this.auth$.next(authState);
        this.persistAuth(authState);
        // Other tabs pick up auth via storage event automatically
      }),
      map(() => void 0)
    );
  }

  logout(): void {
    this.auth$.next(null);
    this.clearPersistedAuth();
    this.bus.send({ type: 'LOGOUT' }); // tell other tabs
  }

  private loadPersistedAuth(): AuthState | null {
    const stored = localStorage.getItem('auth-token');
    return stored ? JSON.parse(stored) : null;
  }

  private persistAuth(a: AuthState): void {
    localStorage.setItem('auth-token', JSON.stringify(a));
  }

  private clearPersistedAuth(): void {
    localStorage.removeItem('auth-token');
  }
}
```

---

## Pattern 5: Cart / Shopping State Across Tabs

```typescript
import { BehaviorSubject } from 'rxjs';

class SharedCartStore {
  private STORAGE_KEY = 'cart-state';
  private bus         = new CrossTabBus();
  private cart$       = new BehaviorSubject<CartItem[]>(this.loadCart());

  readonly items$    = this.cart$.asObservable();
  readonly count$    = this.cart$.pipe(map(items => items.reduce((n, i) => n + i.qty, 0)));
  readonly total$    = this.cart$.pipe(map(items => items.reduce((n, i) => n + i.price * i.qty, 0)));

  constructor() {
    // Receive cart changes from other tabs:
    this.bus.listen('STATE_UPDATE').pipe(
      filter(msg => 'cart' in (msg.payload as any))
    ).subscribe(({ payload }) => {
      this.cart$.next((payload as any).cart);
    });
  }

  addItem(item: CartItem): void {
    const items  = this.cart$.getValue();
    const exists = items.find(i => i.id === item.id);
    const next   = exists
      ? items.map(i => i.id === item.id ? { ...i, qty: i.qty + 1 } : i)
      : [...items, { ...item, qty: 1 }];

    this.cart$.next(next);
    this.persistCart(next);
    this.bus.send({ type: 'STATE_UPDATE', payload: { cart: next } });
  }

  private loadCart(): CartItem[] {
    const stored = localStorage.getItem(this.STORAGE_KEY);
    return stored ? JSON.parse(stored) : [];
  }

  private persistCart(items: CartItem[]): void {
    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(items));
  }
}
```

---

## Common Pitfalls

### Echo Loops (Receiving Your Own Broadcasts)

```typescript
// ❌ localStorage storage event fires in ALL tabs INCLUDING the one that set it:
// (Actually: storage event does NOT fire in the setting tab — but BroadcastChannel does)

// BroadcastChannel: ❌ messages DO arrive in the sender tab:
channel.onmessage = e => applyUpdate(e.data); // including own updates!

// ✅ Tag each message with sender tab ID and filter:
channel.postMessage({ ...msg, senderId: myTabId });
fromEvent(channel, 'message').pipe(
  filter(e => e.data.senderId !== myTabId) // ignore echoes
)
```

### Memory Leaks from Unclosed BroadcastChannels

```typescript
// ❌ Channel never closed — keeps page from being GC'd:
const channel = new BroadcastChannel('app');
// ... use it ...
// never closed

// ✅ Close on destroy (Angular) or unmount (React):
ngOnDestroy() { this.channel.close(); }
useEffect(() => () => channel.close(), []);
```

### Stale State After Tab Comes Back to Focus

```typescript
// ❌ Long-backgrounded tab has stale state when re-focused:
combineLatest([localState$, serverState$]).subscribe(render);
// Tab backgrounded for 10 minutes: server diverged

// ✅ Refresh when tab regains visibility:
fromEvent(document, 'visibilitychange').pipe(
  filter(() => document.visibilityState === 'visible'),
  switchMap(() => api.getLatest()), // force refresh
  startWith(null)
).subscribe(serverState => {
  if (serverState) store.setServerState(serverState);
});
```
