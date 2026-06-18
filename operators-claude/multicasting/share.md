# share

## Identity

- **Name**: share
- **Category**: Multicasting Operators
- **Type**: Reference-counted multicast — converts a cold Observable to hot, sharing one execution among all subscribers; tears down when all subscribers unsubscribe
- **Import**:
  ```typescript
  import { share } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function share<T>(options?: ShareConfig<T>): MonoTypeOperatorFunction<T>

  interface ShareConfig<T> {
    connector?: () => SubjectLike<T>;     // default: new Subject<T>()
    resetOnError?: boolean;               // default: true
    resetOnComplete?: boolean;            // default: true
    resetOnRefCountZero?: boolean | (() => ObservableInput<void>); // default: true
  }
  ```

## Functional Specification

**Concept**: `share()` is shorthand for `pipe(multicast(() => new Subject<T>()), refCount())`. It:
1. Multicasts the source through a `Subject` (all subscribers share one execution)
2. Subscribes to the source when the first subscriber arrives (refCount goes 1)
3. Unsubscribes from the source when the last subscriber leaves (refCount goes 0)

**`share` vs `shareReplay`**:

| | `share` | `shareReplay(1)` |
|---|---|---|
| Replay buffer | None — late subscribers miss past values | 1 value replayed to late subscribers |
| On last unsubscribe | Unsubscribes from source (resets) | Keeps source alive (by default with `refCount: false`) |
| On source complete | Subject completes, resets on next subscribe | Replays last value to all future subscribers |
| Use when | Live events, no history needed | State/cache that late subscribers must receive |

**`resetOnRefCountZero`**: When `false`, the source stays subscribed even with zero subscribers (keeps the multicast alive for reconnects). When `true` (default), disconnects and resets on zero subscribers.

**`resetOnComplete`**: When `false`, late subscribers after source completion receive the completion notification immediately (like a completed Subject). When `true` (default), resets so the next subscriber triggers a fresh source subscription.

## Marble Diagram

```
Cold source:  ---a---b---c---|  (new execution per subscriber without share)

Without share — TWO source executions:
Sub A subscribes at t=0:  ---a---b---c---|
Sub B subscribes at t=0:  ---a---b---c---|  (separate HTTP request)

With share() — ONE source execution, shared:
Sub A subscribes at t=0:  ---a---b---c---|
Sub B subscribes at t=0:  ---a---b---c---|  (same HTTP request)

Late subscriber with share() — misses past values:
Sub A subscribes at t=0:  ---a---b---c---|
Sub B subscribes at t=2:  ---------c---|   (missed a, b — no replay)

Late subscriber with shareReplay(1) — gets last value:
Sub A subscribes at t=0:  ---a---b---c---|
Sub B subscribes at t=2:  ------b--c---|   (replays b, then gets c live)
                           ↑ replayed

share() reset behavior — after all unsubscribe, next subscribe triggers fresh source:
Sub A subscribes, then unsubscribes → refCount = 0 → source unsubscribed
Sub B subscribes → NEW source execution starts (fresh cold Observable)
```

## Type System Integration

```typescript
import { interval } from 'rxjs';
import { share, take } from 'rxjs/operators';

// Type preserved — same T in/out
const shared$: Observable<number> = interval(100).pipe(share());

// Both subscribers share ONE interval
shared$.pipe(take(3)).subscribe(v => console.log('A:', v));
shared$.pipe(take(3)).subscribe(v => console.log('B:', v));
// A: 0, B: 0, A: 1, B: 1, A: 2, B: 2  (interleaved from one source)
```

## Examples

### Basic — Prevent Duplicate HTTP Requests
```typescript
import { ajax } from 'rxjs/ajax';
import { share } from 'rxjs/operators';

// Without share: two components → two HTTP requests
const users$ = ajax.getJSON<User[]>('/api/users'); // cold
usersTable$.subscribe(renderTable);   // HTTP request 1
usersCount$.subscribe(renderCount);   // HTTP request 2

// With share: two components → one HTTP request
const users$ = ajax.getJSON<User[]>('/api/users').pipe(share());
usersTable$.subscribe(renderTable);   // HTTP request fires
usersCount$.subscribe(renderCount);   // reuses same request
```

### Common Pattern — Live Event Stream (No Replay Needed)
```typescript
import { webSocket } from 'rxjs/webSocket';
import { share, filter, map } from 'rxjs/operators';

// One WebSocket connection, multiple consumers
const ws$ = webSocket<Message>('wss://api.example.com/events').pipe(share());

// Different parts of the app subscribe to the same connection
ws$.pipe(filter(m => m.type === 'price')).subscribe(updatePriceDisplay);
ws$.pipe(filter(m => m.type === 'news')).subscribe(updateNewsFeed);
ws$.pipe(filter(m => m.type === 'alert')).subscribe(showAlert);
// One WebSocket, three subscribers — share() ensures a single connection
```

### Common Pattern — `share` With `resetOnRefCountZero: false` for Persistent Streams
```typescript
import { webSocket } from 'rxjs/webSocket';
import { share } from 'rxjs/operators';
import { timer } from 'rxjs';

// Keep the WebSocket alive even when all subscribers temporarily unsubscribe
// (e.g., during component navigation)
const ws$ = webSocket<Message>('wss://api.example.com/stream').pipe(
  share({
    resetOnRefCountZero: () => timer(2000) // wait 2s before disconnecting
    // If a new subscriber arrives within 2s, reuses existing connection
  })
);
```

### Common Pattern — Distinguishing `share` from `shareReplay`
```typescript
import { interval } from 'rxjs';
import { share, shareReplay, tap } from 'rxjs/operators';

const source$ = interval(1000).pipe(
  tap(n => console.log('source emitted:', n))
);

// share() — late subscriber misses past values
const shared$ = source$.pipe(share());
shared$.subscribe(v => console.log('A:', v)); // subscribes at t=0
// (2 seconds later)
shared$.subscribe(v => console.log('B:', v)); // subscribes at t=2, misses 0 and 1
// B starts receiving from 2 onward

// shareReplay(1) — late subscriber gets last value replayed
const replayed$ = source$.pipe(shareReplay(1));
replayed$.subscribe(v => console.log('A:', v)); // subscribes at t=0
// (2 seconds later)
replayed$.subscribe(v => console.log('B:', v)); // gets 1 replayed immediately, then 2, 3...
```

## Common Pitfalls

### Anti-pattern: `share` for State That Late Subscribers Must See
```typescript
import { ajax } from 'rxjs/ajax';
import { share, shareReplay } from 'rxjs/operators';

// ❌ RACE CONDITION — if component B subscribes after the HTTP response arrives,
// it misses the data (share has no replay buffer)
const config$ = ajax.getJSON('/api/config').pipe(share());

configHeaderComponent.init(config$);   // subscribes immediately
// ... time passes, HTTP response arrives and is emitted, share completes ...
configFooterComponent.init(config$);   // subscribes after response — gets nothing!
// config$ has already completed; share resets; next subscribe triggers a NEW request

// ✅ CORRECT — shareReplay(1) for data that late subscribers need
const config$ = ajax.getJSON('/api/config').pipe(shareReplay(1));
configHeaderComponent.init(config$);   // subscribes, gets response
configFooterComponent.init(config$);   // gets response replayed immediately ✓

// WHY: share() has no buffer. Once a value is emitted, it's gone for any
// subscriber that wasn't connected at that exact moment. For HTTP requests
// or any one-shot data that multiple components might need at different times,
// shareReplay(1) is the correct choice.
```

### Anti-pattern: Using `share` Without Understanding Reset Behavior
```typescript
import { interval } from 'rxjs';
import { share, take } from 'rxjs/operators';

const counter$ = interval(100).pipe(share());

const sub1 = counter$.pipe(take(3)).subscribe(v => console.log('sub1:', v));
// sub1: 0, sub1: 1, sub1: 2 — then sub1 completes (take(3))
// refCount drops to 0 → share resets → source interval restarted!

const sub2 = counter$.subscribe(v => console.log('sub2:', v));
// sub2: 0, sub2: 1, ...  ← counter RESTARTED from 0, not continued

// ✅ CORRECT — use resetOnRefCountZero: false to preserve the source across resubscriptions
const counter$ = interval(100).pipe(
  share({ resetOnRefCountZero: false })
);
// Now sub2 continues from where sub1 left off (if subscribing within the same tick)

// WHY: By default, share() unsubscribes from the source when refCount hits 0.
// The NEXT subscriber triggers a fresh cold Observable execution.
// For counters, timers, or sequences that should continue, either keep at least
// one subscriber alive or use resetOnRefCountZero: false.
```

## Related Operators

- **`shareReplay(n)`**: Like `share` but with a replay buffer — use for state/cache that late subscribers need
- **`multicast(subject$)`**: Lower-level multicasting primitive; `share` uses this internally
- **`publish()`**: Deprecated in favor of `share` / `shareReplay` — avoid in new code
- **`connectable(source, connector)`**: RxJS 7 replacement for `multicast` + `connect()` pattern
- **`Subject`**: The underlying multicast primitive that `share` wraps

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/share](https://rxjs.dev/api/operators/share)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key teaching points**:
1. `share` = `multicast(Subject) + refCount` — one source execution shared among all active subscribers
2. No replay: late subscribers miss past values — use `shareReplay(1)` for state/caches
3. Resets on refCount=0 by default — next subscriber restarts the cold source
4. Perfect for live event streams (WebSocket, DOM events) with multiple consumers
