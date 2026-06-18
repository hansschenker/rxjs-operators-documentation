# shareReplay

## Identity
- **Name**: shareReplay
- **Category**: Multicasting Operators
- **Type**: Multicast with replay buffer — shares one source subscription among all subscribers and replays buffered emissions to late subscribers
- **Import**:
  ```typescript
  import { shareReplay } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // Config object form (recommended in RxJS 7+)
  function shareReplay<T>(config: ShareReplayConfig): MonoTypeOperatorFunction<T>

  // Shorthand form (sets refCount: false implicitly)
  function shareReplay<T>(
    bufferSize?: number,
    windowTime?: number,
    scheduler?: SchedulerLike
  ): MonoTypeOperatorFunction<T>

  interface ShareReplayConfig {
    bufferSize?: number;    // how many past values to replay; default: Infinity
    windowTime?: number;    // max age of buffered values in ms; default: Infinity
    refCount: boolean;      // REQUIRED in config form
    scheduler?: SchedulerLike;
  }
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable, often cold (HTTP request, expensive computation)

**Output**: `Observable<T>` — a hot Observable that:
1. Maintains a single subscription to the source (shared across all current subscribers)
2. Multicasts each source emission to all current subscribers
3. Replays the last `bufferSize` emissions to each *new* subscriber on subscription

**Transformation**:
The operator wraps a `ReplaySubject` of capacity `bufferSize`. The first subscriber triggers the source subscription. Subsequent subscribers receive the buffered replay immediately on subscription, then live emissions going forward.

**`refCount` — the critical configuration axis**:

| Setting | Source subscribe | Source unsubscribe | Use case |
|---------|-----------------|-------------------|----------|
| `refCount: false` (shorthand default) | On first subscriber | **Never** (until source completes/errors) | Cache a result forever; HTTP requests |
| `refCount: true` | On first subscriber | When count drops to 0 | Share while active; reconnect on re-subscribe |

**Mathematical representation**:
```
Let S be the source Observable emitting v₁, v₂, ..., vₙ
Let buffer = ReplaySubject(bufferSize)
Let subscribers = { s₁, s₂, ..., sₖ } (set of active downstream subscribers)

On first subscriber: S ──subscribes──► buffer ──multicasts──► subscribers
On emission vᵢ from S: buffer stores vᵢ; all current subscribers receive vᵢ
On new subscriber sₙₑw: sₙₑw receives replay of buffered values, then live emissions
On source complete/error: propagated to all current subscribers; buffer preserved (for replay)
```

**Invariants**:
- **Replay on subscribe**: New subscribers always receive buffered values synchronously on subscription, before any live values
- **Single source subscription** (while count > 0 or `refCount: false`)
- **Buffer is per-operator-instance**: The buffer is shared — all subscribers see the same replay values
- **Order preserved**: Replay happens before live emissions; live emissions are in source order

## Marble Diagram

```
Source (cold HTTP):  ----data----|
                     shareReplay(1)

Subscriber A at t=0: ----data----|
Subscriber B at t=5: ----data----|  (waits for source)
Subscriber C at t=8 (after source completes): data|  (replay from buffer, instant)

Source subscribed ONCE. A and B both receive live emission.
C subscribes after completion — receives buffered 'data' immediately.
```

**refCount: true — source unsubscribes when all leave**:
```
Source:        --a--b--c--d--e--...
shareReplay({ bufferSize: 1, refCount: true })

Sub A: t=0  ───a──b──c──X (unsubscribes at c)
Sub B: t=0  ───a──b──c──d──e──...
Sub C: t=9  ──────────────[c]──d──e──  (replays last value c, then live)

If both A and B unsubscribe: source unsubscribes.
Next subscriber re-subscribes to source fresh — no buffer from before.
```

**refCount: false — source stays open**:
```
Source:        --a--b--c--|   (source completes)
shareReplay(1) (refCount: false, default shorthand)

Sub A: t=0  ───a──b──c--|
Sub B: t=9  ──[c]|        (replays last value, then gets completion)
Sub C: t=20 ──[c]|        (replays indefinitely after completion)
```

**Key observation**: `shareReplay(1)` is the most common usage. It ensures late subscribers get the "current value" — a read-only analogue of `BehaviorSubject` for cold sources.

## Behavioral Characteristics

**Subscription**:
- Source subscribed when first subscriber arrives
- `refCount: false`: source subscription lives until source completes/errors, independent of subscriber count
- `refCount: true`: source subscription is reference-counted — released when last subscriber leaves, re-created when next subscriber arrives

**Completion semantics**:
- Source completion is stored in the internal Subject and replayed — late subscribers receive buffered values followed immediately by `complete()`
- `refCount: false` after source completion: new subscribers receive buffer + complete — no new source subscription

**Error handling**:
- Source error propagates to all current subscribers
- `refCount: false`: error is stored; new subscribers receive the error immediately on subscription (dangerous — see pitfalls)
- `refCount: true`: error does NOT replay; next subscriber triggers a fresh source subscription

**Backpressure**:
- None — `ReplaySubject` buffers synchronously; no back-pressure mechanism
- Memory: O(bufferSize × valueSize) — bound carefully for large values

**Hot vs. Cold**:
- Transforms a cold Observable into a hot one
- Classic use case: cold HTTP request → shared, cached, hot stream

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — type is preserved)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * The output is a ConnectableObservable-like hot Observable.
 * Type T flows through unchanged — shareReplay adds sharing, not transformation.
 *
 * ShareReplayConfig.refCount is REQUIRED when using config object form;
 * TypeScript will error if omitted.
 */

import { shareReplay } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Inferred as Observable<UserProfile>
const userProfile$ = ajax.getJSON<UserProfile>('/api/me').pipe(
  shareReplay(1)
);

// Config form — refCount required
const data$ = ajax.getJSON<Data[]>('/api/data').pipe(
  shareReplay({ bufferSize: 1, refCount: false })
);

// Type safety: T is preserved through shareReplay
function withCache<T>(source$: Observable<T>, size = 1): Observable<T> {
  return source$.pipe(
    shareReplay({ bufferSize: size, refCount: false })
  );
  // Return type: Observable<T> — correct
}
```

## Examples

### Basic Usage — Caching an HTTP Request
```typescript
import { shareReplay } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Without shareReplay: every subscriber triggers a new HTTP request
const users$ = ajax.getJSON<User[]>('/api/users');

users$.subscribe(u => renderSidebar(u));  // GET /api/users
users$.subscribe(u => renderTable(u));    // GET /api/users  (duplicate!)

// With shareReplay: one request, shared result
const cachedUsers$ = ajax.getJSON<User[]>('/api/users').pipe(
  shareReplay(1) // buffer last 1 value; refCount: false (shorthand)
);

cachedUsers$.subscribe(u => renderSidebar(u));  // GET /api/users (one request)
cachedUsers$.subscribe(u => renderTable(u));    // receives cached response — no new request
```

### Common Pattern — Angular Service Data Cache
```typescript
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { shareReplay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class UserService {
  // Singleton service = singleton cached Observable
  readonly currentUser$: Observable<UserProfile> = this.http
    .get<UserProfile>('/api/me')
    .pipe(
      shareReplay(1) // cache for the lifetime of the service
    );

  constructor(private http: HttpClient) {}
}

// In any component:
@Component({ ... })
export class HeaderComponent {
  user$ = this.userService.currentUser$; // no HTTP call; uses cache
  constructor(private userService: UserService) {}
}

@Component({ ... })
export class ProfileComponent {
  user$ = this.userService.currentUser$; // same cache; no HTTP call
  constructor(private userService: UserService) {}
}
// HTTP is called exactly once, regardless of how many components use it.
```

### Common Pattern — shareReplay as a Read-Only BehaviorSubject
```typescript
import { BehaviorSubject } from 'rxjs';
import { scan, startWith, shareReplay, map } from 'rxjs/operators';

// Writable state source
const actions$ = new Subject<Action>();

// Derived state — shared, cached, replays current state to late subscribers
const state$ = actions$.pipe(
  scan(reducer, initialState),
  startWith(initialState),
  shareReplay(1) // late subscribers get current state immediately
);

// Multiple consumers, all get current state on subscribe:
state$.pipe(map(s => s.user)).subscribe(renderUserBadge);
state$.subscribe(renderSidebar);

// This is the pattern that replaces BehaviorSubject for derived state:
// - BehaviorSubject: mutable, push-based, requires manual .next()
// - scan + startWith + shareReplay(1): derived from actions, immutable, composable
```

### Common Pattern — refCount: true for Component-Scoped Sharing
```typescript
import { interval } from 'rxjs';
import { shareReplay, tap } from 'rxjs/operators';

// A stream that should be active only while components are subscribed
const livePrice$ = interval(1000).pipe(
  tap(() => console.log('fetching price...')),
  // map(() => fetchPrice()),
  shareReplay({ bufferSize: 1, refCount: true })
);

const sub1 = livePrice$.subscribe(p => console.log('chart:', p));
const sub2 = livePrice$.subscribe(p => console.log('header:', p));
// One 'fetching price...' per second — shared

sub1.unsubscribe(); // count drops to 1 — source continues
sub2.unsubscribe(); // count drops to 0 — SOURCE UNSUBSCRIBES (no more fetches)

const sub3 = livePrice$.subscribe(p => console.log('new:', p));
// re-subscribes to source fresh — 'fetching price...' resumes
// sub3 receives replay of last buffered value first, then live
```

### Edge Case — Error Replay with refCount: false
```typescript
import { throwError, of } from 'rxjs';
import { shareReplay, catchError } from 'rxjs/operators';

// refCount: false (shorthand) stores errors in the buffer
const failing$ = throwError(() => new Error('server down')).pipe(
  catchError(err => { console.log('caught:', err.message); return throwError(() => err); }),
  shareReplay(1)
);

failing$.subscribe({ error: e => console.log('sub1 error:', e.message) });
// Output: caught: server down, sub1 error: server down

failing$.subscribe({ error: e => console.log('sub2 error:', e.message) });
// Output: sub2 error: server down  (error replayed — catchError NOT called again)

// WHY: With refCount: false, errors are stored and replayed.
// Use refCount: true to re-execute the source (and its catchError) on re-subscription.
```

## Common Pitfalls

### Anti-pattern: Misunderstanding `refCount: false` Error Replay
```typescript
import { shareReplay, retry } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ SILENT FAILURE — with refCount: false, errors are replayed to new subscribers
// The source (HTTP request) is NOT retried; the error is replayed from buffer
const userData$ = ajax.getJSON('/api/user').pipe(
  shareReplay(1)  // shorthand = refCount: false
);

userData$.subscribe({ error: e => console.error('first:', e) }); // HTTP error
userData$.subscribe({ error: e => console.error('second:', e) }); // replayed error — no new HTTP request
userData$.subscribe({ error: e => console.error('third:', e) });  // replayed error — still no retry

// ✅ CORRECT — use refCount: true to trigger a fresh source subscription on re-subscribe
const retryableData$ = ajax.getJSON('/api/user').pipe(
  retry(3),
  shareReplay({ bufferSize: 1, refCount: true })
);

// Or: don't share until after error handling
const withErrorHandling$ = ajax.getJSON('/api/user').pipe(
  retry(3),
  catchError(err => of(defaultUser))
).pipe(
  shareReplay(1) // safe to share: source never errors after catchError
);

// WHY: refCount: false means the source subscription is never released.
// After an error, the source has terminated — there is nothing to re-subscribe to.
// The error is stored in the ReplaySubject and replayed to all future subscribers.
// This permanently breaks the shared Observable. Use refCount: true if retries
// are needed, or handle errors before shareReplay.
```

### Anti-pattern: Memory Leak from `refCount: false` on Component-Scoped Streams
```typescript
import { fromEvent } from 'rxjs';
import { shareReplay, map } from 'rxjs/operators';
import { Component, OnDestroy } from '@angular/core';

// ❌ MEMORY LEAK — refCount: false; source never unsubscribes
@Component({ ... })
export class ChartComponent implements OnDestroy {
  // This creates a new Observable on every component instance
  private mouseMoves$ = fromEvent(document, 'mousemove').pipe(
    map((e: MouseEvent) => ({ x: e.clientX, y: e.clientY })),
    shareReplay(1) // shorthand: refCount: false
  );

  ngOnInit() {
    this.mouseMoves$.subscribe(p => this.updateCrosshair(p));
  }

  ngOnDestroy() {
    // Subscribing is cleaned up... but the SOURCE (mousemove listener) stays alive
    // because refCount: false keeps the source subscription open.
    // Destroy 100 instances → 100 live mousemove listeners.
  }
}

// ✅ CORRECT — use refCount: true for component-scoped streams
@Component({ ... })
export class SafeChartComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  private mouseMoves$ = fromEvent(document, 'mousemove').pipe(
    map((e: MouseEvent) => ({ x: e.clientX, y: e.clientY })),
    shareReplay({ bufferSize: 1, refCount: true }) // source released when count → 0
  );

  ngOnInit() {
    this.mouseMoves$.pipe(takeUntil(this.destroy$)).subscribe(p => this.updateCrosshair(p));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    // takeUntil completes the subscription → refCount drops to 0 → source unsubscribes
  }
}

// WHY: refCount: false is designed for singleton/app-lifetime streams (HTTP caches,
// app-level config). For component-scoped streams tied to DOM events or
// subscriptions that should be cleaned up, always use refCount: true paired
// with takeUntil.
```

### Anti-pattern: Using `shareReplay` Without a Buffer for Multicasting
```typescript
import { shareReplay, share } from 'rxjs/operators';
import { interval } from 'rxjs';

// ❌ WRONG TOOL — using shareReplay(0) just to share; no replay benefit
interval(1000).pipe(
  shareReplay(0) // bufferSize 0 — no replay, just multicasting
).subscribe(console.log);

// ✅ CORRECT — use share() for pure multicasting without replay
interval(1000).pipe(
  share() // semantically clear: "I want multicasting, not caching"
).subscribe(console.log);

// WHY: shareReplay(0) technically works for multicasting but communicates
// the wrong intent. share() is the canonical multicast-without-replay operator.
// Code reading shareReplay expects a replay buffer; when none is present,
// share() is clearer and cheaper (no ReplaySubject overhead).
```

### Anti-pattern: Placing `shareReplay` Before Operators That Should Run Per Subscriber
```typescript
import { shareReplay, map, tap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ INCORRECT — tap (analytics) runs once; both subscribers share the same execution
ajax.getJSON<User>('/api/me').pipe(
  shareReplay(1),
  tap(user => analytics.track('profile_viewed', { userId: user.id })) // runs TWICE
).subscribe(renderHeader);
// analytics.track is called for every subscriber downstream of shareReplay

// ✅ CORRECT — analytics tap goes BEFORE shareReplay; executes once
ajax.getJSON<User>('/api/me').pipe(
  tap(user => analytics.track('profile_fetched', { userId: user.id })), // runs ONCE
  shareReplay(1)
).subscribe(renderHeader);

// WHY: shareReplay multicasts the source. Operators placed AFTER shareReplay
// are in subscriber-local pipelines — each subscriber runs them independently.
// Operators placed BEFORE shareReplay are in the shared source pipeline —
// they run once regardless of subscriber count. Put side effects and expensive
// transforms before shareReplay; put subscriber-specific transforms after.
```

## Related Operators

**Same Category (Multicasting)**:
- **`share()`**: Pure multicast — no replay buffer; equivalent to `publish() + refCount()` or `shareReplay({ bufferSize: 0, refCount: true })`; use when late subscribers do not need past values
- **`publishReplay(n).refCount()`**: Older pattern that `shareReplay` supersedes
- **`multicast(subject)`**: Lower-level multicasting with explicit Subject control

**Complementary Operators**:
- **`scan + startWith`**: Build the accumulated state that `shareReplay` then caches and shares
- **`distinctUntilChanged`**: Prevent downstream re-renders when `shareReplay` delivers the same cached value to multiple subscribers
- **`takeUntil`**: Manage component-lifetime subscriptions to `shareReplay({ refCount: true })` streams

**Alternatives by Use Case**:

| Use Case | Instead of `shareReplay` | Use This | Why |
|----------|--------------------------|----------|-----|
| Multicast without caching | `shareReplay(0)` | `share()` | Clearer intent, no ReplaySubject overhead |
| Mutable shared state | `shareReplay(scan(...))` | `BehaviorSubject` | Push-based API is clearer for mutable state |
| Time-windowed replay | `shareReplay(N)` | `shareReplay({ bufferSize: N, windowTime: 5000 })` | windowTime prevents stale value replay |
| HTTP cache with expiry | `shareReplay(1)` | `shareReplay(1) + timer invalidation` | Build explicit invalidation logic |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/shareReplay](https://rxjs.dev/api/operators/shareReplay)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/replay.html](http://reactivex.io/documentation/operators/replay.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/shareReplay.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/shareReplay.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Multicast Cache with Replay Buffer (Cold-to-Hot Conversion)
- **Cognitive Load**: 4/5 — The `refCount` axis is the primary complexity; the error-replay behavior with `refCount: false` is the most dangerous footgun in all of RxJS
- **Usage Frequency**: 5/5 — Present in nearly every Angular service; essential for any multi-consumer observable pipeline
- **Composability**: 4/5 — Must be placed carefully (pre vs. post side-effects); pairs naturally with scan, distinctUntilChanged, takeUntil

**Problem Domain**:
Sharing a single execution of a cold Observable (HTTP request, WebSocket, expensive computation) across multiple subscribers while replaying the most recent state to late subscribers. The primary tool for moving from subscription-per-component to shared reactive state.

**When to Teach**:
Teach after `scan`, `startWith`, and the concept of hot vs. cold Observables. `shareReplay` is best understood as the final step in building a reactive state store: `scan + startWith + shareReplay(1)`.

- **Prerequisites**: `Subject`, `scan`, `startWith`, hot vs. cold Observables
- **Teaches**: Multicasting, reference counting, replay semantics, the refCount footgun
- **Leads to**: NgRx/Redux-Observable, reactive service design, component-state sharing
- **Common with**: `scan`, `startWith`, `distinctUntilChanged`, `takeUntil`, `ajax`, `BehaviorSubject`

**Common Misconceptions**:
1. **"shareReplay(1) always caches safely"** — after an error with `refCount: false`, the error is replayed forever; add error handling before `shareReplay`
2. **"Operators after shareReplay run once"** — they run per-subscriber; only operators before it run once
3. **"refCount: false is the safe default"** — it prevents resource cleanup; `refCount: true` is safer for non-singleton streams
4. **"shareReplay(0) and share() are the same"** — functionally similar but semantically different; prefer `share()` for clarity when no replay is needed
