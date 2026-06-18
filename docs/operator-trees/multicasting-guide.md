# Multicasting Guide

By default every RxJS Observable is **cold** — each subscriber gets its own independent execution. Multicasting makes an Observable **hot** — a single upstream execution shared among all subscribers.

---

## The Core Problem

```typescript
// Cold Observable — runs TWICE (two HTTP requests):
const user$ = ajax.getJSON('/api/user');
user$.subscribe(renderHeader);
user$.subscribe(renderSidebar);

// Multicast — runs ONCE (one HTTP request, shared):
const user$ = ajax.getJSON('/api/user').pipe(shareReplay(1));
user$.subscribe(renderHeader);   // same response
user$.subscribe(renderSidebar);  // same response
```

---

## The Four Multicasting Operators

### `share()` — Ref-counted, no replay

```typescript
import { share } from 'rxjs/operators';

const events$ = source$.pipe(share());
```

**Behavior**:
- Connects on first subscriber; disconnects when last subscriber leaves
- No replay — late subscribers get only future emissions
- `resetOnRefCountZero: true` by default in RxJS 7 — source resets when all unsubscribe

**Use when**: Multiple live subscribers need the same stream; you don't need late subscribers to get past values (UI events, WebSocket messages, live feeds).

---

### `shareReplay(n)` — Ref-counted, replays last N

```typescript
import { shareReplay } from 'rxjs/operators';

const config$ = ajax.getJSON('/api/config').pipe(shareReplay(1));
```

**Behavior**:
- Replays last `n` values to new subscribers immediately on subscription
- `refCount: true` (default in RxJS 7) — disconnects from source when all unsubscribe
- `refCount: false` — stays connected even with zero subscribers (cache that never resets)

**Use when**: HTTP responses, computed values, or any data where late subscribers need the current/recent value. The most common multicasting operator.

```typescript
// Cache with auto-reset on zero subscribers (default):
const data$ = fetch$().pipe(shareReplay({ bufferSize: 1, refCount: true }));

// Permanent cache (never re-fetches even after all unsub):
const config$ = fetch$().pipe(shareReplay({ bufferSize: 1, refCount: false }));
```

---

### `connectable(source)` — Manual connect/disconnect

```typescript
import { connectable, Subject } from 'rxjs';

const multi$ = connectable(source$, { connector: () => new Subject() });
const conn   = multi$.connect(); // explicit start
// ...
conn.unsubscribe();              // explicit stop
```

**Use when**: You need to start the source before any subscribers exist, or control connect/disconnect timing precisely. Everything else use `share`/`shareReplay`.

---

### `connect(selector)` — Pipeable multicast graph

```typescript
import { connect } from 'rxjs/operators';

source$.pipe(
  connect(shared$ => merge(
    shared$.pipe(filter(isError), map(toErrorAlert)),
    shared$.pipe(filter(isSuccess), map(toSuccessItem))
  ))
).subscribe(render);
```

**Use when**: You need to fork a single source into multiple branches within one pipe, all sharing one subscription.

---

## Decision Guide

```
Do you need to multicast?
├── No — only one subscriber         → plain Observable (no operator needed)
│
└── Yes — multiple subscribers
           │
           ├── Do late subscribers need past values?
           │   ├── Yes → shareReplay(n)
           │   └── No  → share()
           │
           ├── Need to start source before first subscriber?
           │   └── connectable(source)
           │
           └── Need to fork stream into branches in one pipe?
               └── connect(selector)
```

---

## `share` vs `shareReplay` — The Key Distinction

```typescript
// share: late subscriber gets NOTHING from before they subscribed
const tick$ = interval(1000).pipe(share());
tick$.subscribe(v => console.log('A:', v)); // A: 0, 1, 2, 3...
setTimeout(() => {
  tick$.subscribe(v => console.log('B:', v)); // B: 3, 4, 5... (misses 0-2)
}, 3500);

// shareReplay(1): late subscriber gets LAST value immediately
const state$ = userActions$.pipe(
  scan(reducer, initialState),
  shareReplay(1)
);
state$.subscribe(renderHeader);   // gets current state immediately
state$.subscribe(renderSidebar);  // also gets current state immediately
```

---

## Common Pitfalls

### Forgetting `shareReplay` on HTTP calls used in multiple places

```typescript
// ❌ TWO HTTP REQUESTS
const user$ = ajax.getJSON('/api/me');
combineLatest([
  user$.pipe(map(u => u.name)),
  user$.pipe(map(u => u.role))
]).subscribe(([name, role]) => render(name, role));

// ✅ ONE HTTP REQUEST — shareReplay(1) makes it a shared cache
const user$ = ajax.getJSON('/api/me').pipe(shareReplay(1));
combineLatest([
  user$.pipe(map(u => u.name)),
  user$.pipe(map(u => u.role))
]).subscribe(([name, role]) => render(name, role));
```

### `shareReplay` Without `refCount` Leaking the Source

```typescript
// ❌ SOURCE NEVER UNSUBSCRIBED — WebSocket stays open forever
const messages$ = webSocket('wss://api').pipe(
  shareReplay({ bufferSize: 1, refCount: false }) // no ref counting!
);

// Connection opens on first subscribe and NEVER closes, even with 0 subscribers.

// ✅ CORRECT — refCount: true (the RxJS 7 default via shareReplay(1))
const messages$ = webSocket('wss://api').pipe(shareReplay(1));
// Disconnects when all subscribers leave; reconnects on next subscribe
```

### Using `share` Where `shareReplay` Is Needed

```typescript
// ❌ RACE CONDITION — second subscriber may miss the HTTP response
const data$ = ajax.getJSON('/api/data').pipe(share());

data$.subscribe(renderChart);  // subscribes, triggers request
// If response arrives before second subscribe:
data$.subscribe(renderTable);  // misses the already-arrived value!

// ✅ shareReplay(1) guarantees the second subscriber gets the value
const data$ = ajax.getJSON('/api/data').pipe(shareReplay(1));
```

---

## Quick Reference

```typescript
// Share a live stream (events, WebSocket)
source$.pipe(share())

// Cache HTTP response for all subscribers
ajax.getJSON('/api/data').pipe(shareReplay(1))

// Permanent cache (survives zero-subscriber periods)
ajax.getJSON('/api/config').pipe(shareReplay({ bufferSize: 1, refCount: false }))

// Manual control
const multi$ = connectable(source$);
multi$.connect();

// In-pipe branching
source$.pipe(connect(s$ => merge(s$.pipe(mapA), s$.pipe(mapB))))
```
