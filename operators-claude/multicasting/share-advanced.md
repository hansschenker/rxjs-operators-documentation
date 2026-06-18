# share — Advanced Patterns

For `share` fundamentals see the core [share](./share) doc. This page covers `share` internals, the `connect()` operator, `refCount` behavior, and choosing between `share` and `shareReplay`.

---

## What `share` Actually Does

`share()` is shorthand for:

```typescript
import { share, Subject } from 'rxjs';

// share() ≡ pipe(multicast(() => new Subject()), refCount())
// In RxJS 7+, share() is implemented as:
source$.pipe(
  share({
    connector:     () => new Subject(),  // fresh Subject for each "connected" period
    resetOnError:  true,                 // reset when source errors
    resetOnComplete: true,               // reset when source completes
    resetOnRefCountZero: true            // reset when all subscribers unsubscribe
  })
);
```

Understanding these reset behaviors is the key to `share` vs `shareReplay` decisions.

---

## `share` Reset Behaviors

```typescript
import { share } from 'rxjs';

// Default share() — resets on ALL three conditions:
source$.pipe(share());

// share with custom reset behavior:
source$.pipe(
  share({
    resetOnRefCountZero: false // don't reset when last subscriber leaves
    // (like shareReplay without buffer)
  })
);

source$.pipe(
  share({
    resetOnComplete: false // survive source completion — late subscribers still connect
  })
);
```

---

## `share` vs `shareReplay` — The Definitive Comparison

```typescript
import { share, shareReplay } from 'rxjs';

// share() — no buffer, resets on zero subscribers
// Use for: live event streams, WebSockets, user interactions
// Late subscriber gets: no historical values
const clicks$ = fromEvent(button, 'click').pipe(share());

// shareReplay(1) — replays last N values to late subscribers, no reset
// Use for: HTTP requests, config loading, auth state, async initialization
// Late subscriber gets: immediately the last emitted value
const config$ = this.http.get('/api/config').pipe(shareReplay(1));

// shareReplay({ bufferSize: 1, refCount: true }) — replay + reset on zero subs
// Use for: expensive computations that should restart when no one listens
const expensive$ = source$.pipe(
  shareReplay({ bufferSize: 1, refCount: true })
);
```

**Decision rule**:
- Need late subscribers to get current state → `shareReplay(1)`
- Pure multicast of live events (no replay needed) → `share()`
- Need the stream to restart when all subscribers leave → `share()` (or `shareReplay({ refCount: true })`)
- Need the stream to persist even without subscribers → `shareReplay(1)` (default `refCount: false`)

---

## Pattern 1: Multicasting a WebSocket

```typescript
import { share, retry } from 'rxjs';
import { webSocket } from 'rxjs/webSocket';

// share() is the right choice for WebSocket — no replay needed, stream is live:
const messages$ = webSocket<Message>('wss://api.example.com/ws').pipe(
  retry({ delay: (_, n) => timer(Math.min(1000 * 2 ** n, 30_000)) }),
  share() // one WebSocket connection, many subscribers
);

// Route message types to different handlers — same socket:
messages$.pipe(filter(m => m.type === 'price')).subscribe(updateChart);
messages$.pipe(filter(m => m.type === 'alert')).subscribe(showNotification);
messages$.pipe(filter(m => m.type === 'status')).subscribe(updateStatus);
// All three use the SAME WebSocket connection
```

---

## Pattern 2: The `connect()` Operator

`connect()` is the modern replacement for `publish()` + `connect()`. It gives you access to the shared Subject for setup before subscribing:

```typescript
import { connect, merge } from 'rxjs';
import { filter, map } from 'rxjs/operators';

// Split one stream into two, process separately, then merge — ONE subscription:
source$.pipe(
  connect(shared$ =>
    merge(
      shared$.pipe(filter(x => x > 0), map(x => `positive: ${x}`)),
      shared$.pipe(filter(x => x < 0), map(x => `negative: ${x}`))
    )
  )
).subscribe(console.log);
// source$ subscribed ONCE — both branches use the same underlying subscription
```

Without `connect`, two separate pipes would create two subscriptions to `source$`.

---

## Pattern 3: `connect()` for Split Processing

```typescript
import { connect, combineLatest } from 'rxjs';
import { map, filter, scan } from 'rxjs/operators';

// Process a single event stream multiple ways:
userEvents$.pipe(
  connect(events$ =>
    combineLatest({
      // All three branches share ONE subscription to userEvents$:
      clickCount:   events$.pipe(
        filter(e => e.type === 'click'),
        scan(count => count + 1, 0)
      ),
      lastInput:    events$.pipe(
        filter(e => e.type === 'input'),
        map(e => e.value)
      ),
      submissions:  events$.pipe(
        filter(e => e.type === 'submit'),
        scan(count => count + 1, 0)
      )
    })
  )
).subscribe(analytics => sendAnalytics(analytics));
```

---

## Pattern 4: Lazy vs Eager Multicasting

```typescript
import { share, connectable, Subject } from 'rxjs';

// share() — LAZY: only subscribes to source when first subscriber arrives
const lazy$ = expensiveOp$.pipe(share());
// No work done until someone subscribes:
lazy$.subscribe(sub1); // source subscribed NOW
lazy$.subscribe(sub2); // joins existing subscription

// connectable() — EAGER: subscribes to source immediately on .connect()
const hot$ = connectable(expensiveOp$, { connector: () => new Subject() });
const connection = hot$.connect(); // start immediately, even with no subscribers

hot$.subscribe(sub1); // joins already-running stream
hot$.subscribe(sub2);

connection.unsubscribe(); // stop the source
```

---

## Pattern 5: `publish()` Migration to `connect()`

`publish()` was deprecated in RxJS 7. Here's the migration:

```typescript
// ❌ RxJS 6 — publish() + refCount():
source$.pipe(publish(), refCount())  // equivalent to share()

// ❌ RxJS 6 — publish() + connect() for multi-branch:
const subject = new Subject();
const multicasted$ = source$.pipe(multicast(subject));
multicasted$.connect();
multicasted$.pipe(filter(x => x > 0)).subscribe(handlePositive);
multicasted$.pipe(filter(x => x < 0)).subscribe(handleNegative);

// ✅ RxJS 7+ — use connect() for multi-branch:
source$.pipe(
  connect(shared$ =>
    merge(
      shared$.pipe(filter(x => x > 0), tap(handlePositive)),
      shared$.pipe(filter(x => x < 0), tap(handleNegative))
    )
  )
).subscribe();

// ✅ RxJS 7+ — use share() for simple multicast:
const shared$ = source$.pipe(share());
shared$.pipe(filter(x => x > 0)).subscribe(handlePositive);
shared$.pipe(filter(x => x < 0)).subscribe(handleNegative);
```

---

## Pattern 6: Hot Split — Process Branches Independently

```typescript
import { connect, partition } from 'rxjs';
import { mergeMap, catchError } from 'rxjs/operators';

// Split HTTP responses into success/error branches:
this.api.streamResults().pipe(
  connect(results$ => {
    const [success$, failure$] = partition(results$, r => r.status === 'ok');
    return merge(
      success$.pipe(tap(r => this.store.dispatch(loadSuccess(r.data)))),
      failure$.pipe(
        mergeMap(r => this.logger.logError(r.error).pipe(
          tap(() => this.store.dispatch(loadFailure(r.error)))
        ))
      )
    );
  })
).subscribe();
```

---

## Common Pitfalls

### `shareReplay(1)` Without `refCount: true` — Memory Leak

```typescript
// ❌ The subscription to source NEVER closes — even when all consumers unsubscribe
const data$ = this.http.get('/api/data').pipe(
  shareReplay(1) // default refCount: false — holds subscription forever
);

// ✅ For finite sources (HTTP), shareReplay is fine — they complete themselves
// ✅ For infinite sources, use refCount: true to clean up:
const events$ = this.socket.events$.pipe(
  shareReplay({ bufferSize: 1, refCount: true }) // closes when all unsubscribe
);
```

### Reconnect Race with `share()` Reset

```typescript
// ❌ Gap between last subscriber leaving and new one arriving causes re-subscription
// If subscriber A leaves at t=100ms and subscriber B arrives at t=101ms,
// share() has already reset and B triggers a NEW source subscription

const data$ = expensiveQuery$.pipe(share());
setTimeout(() => data$.subscribe(A), 0);
setTimeout(() => { A.unsubscribe(); data$.subscribe(B); }, 1000);
// B gets a fresh request, not the cached result from A's request

// ✅ Use shareReplay(1) when you want to cache across subscriber gaps:
const data$ = expensiveQuery$.pipe(shareReplay(1));
```

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Decision summary**: `share()` for live events (WebSocket, DOM events) — no replay, resets cleanly. `shareReplay(1)` for async initialization (HTTP, config) — late subscribers get the cached value immediately.
