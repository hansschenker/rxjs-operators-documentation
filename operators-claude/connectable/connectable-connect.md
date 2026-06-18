# connectable / connect

The modern RxJS 7 API for controlled multicasting — replacing the deprecated `publish()` and `multicast()`.

---

## `connectable`

### Identity
- **Import**: `import { connectable } from 'rxjs'`
- **Signature**:
  ```typescript
  function connectable<T>(
    source: ObservableInput<T>,
    config?: ConnectableConfig<T>
  ): ConnectableObservable<T>

  interface ConnectableConfig<T> {
    connector:      () => SubjectLike<T>  // factory for the multicasting Subject
    resetOnDisconnect?: boolean           // default: true
  }

  interface ConnectableObservable<T> extends Observable<T> {
    connect(): Subscription
  }
  ```
- **Category**: Connectable Observable — manual multicasting with explicit `connect()`

### Functional Specification

`connectable` wraps a source Observable with a `Subject`-like connector. The source does not start until `.connect()` is called. All subscribers share one subscription to the source; the Subject routes emissions to all current subscribers.

**Lifecycle**:
1. `connectable(source)` — creates the ConnectableObservable; source is NOT yet subscribed
2. Subscribers subscribe to the ConnectableObservable — they connect to the Subject, not the source
3. `.connect()` — starts the source; emissions flow through the Subject to all subscribers
4. When all subscribers unsubscribe, the source subscription is kept alive (manual teardown)
5. `resetOnDisconnect: true` (default) — a new connector Subject is created on next `connect()` call

**`connectable` replaces `publish()`**:
```typescript
// Old (deprecated):
source$.pipe(publish())

// New RxJS 7:
connectable(source$, { connector: () => new Subject() })
```

### Marble Diagram

```
source$:   --1--2--3--4--5--|

const multi$ = connectable(source$, { connector: () => new Subject() })

sub1 subscribes to multi$  → no emissions yet
sub2 subscribes to multi$  → no emissions yet

multi$.connect()           → source starts

multi$:    --1--2--3--4--5--|
sub1 gets:  --1--2--3--4--5--|
sub2 gets:  --1--2--3--4--5--|

sub1 unsubscribes after 3:
sub1 gets:  --1--2--3
sub2 gets:  --1--2--3--4--5--|  (source continues for remaining subscriber)
```

### Examples

```typescript
import { connectable, interval } from 'rxjs';
import { take } from 'rxjs/operators';

const source$ = interval(1000).pipe(take(5));
const multi$  = connectable(source$);

// Two subscribers share one source
multi$.subscribe(v => console.log('Sub A:', v));
multi$.subscribe(v => console.log('Sub B:', v));

// Source starts NOW — both subscribers receive all emissions
const connection = multi$.connect();

// Disconnect manually when done
setTimeout(() => connection.unsubscribe(), 6000);
```

### ReplaySubject Connector
```typescript
import { connectable, Subject, ReplaySubject, timer } from 'rxjs';

// Late subscribers get the last 2 values
const multi$ = connectable(timer(0, 1000), {
  connector: () => new ReplaySubject<number>(2)
});

multi$.connect();

setTimeout(() => {
  // Subscribes 3s late — immediately gets the 2 most recent values
  multi$.subscribe(v => console.log('late subscriber:', v));
}, 3000);
```

---

## `connect` (pipeable)

### Identity
- **Import**: `import { connect } from 'rxjs/operators'`
- **Signature**:
  ```typescript
  function connect<T, O extends ObservableInput<unknown>>(
    selector: (shared: Observable<T>) => O,
    config?: ConnectConfig<T>
  ): OperatorFunction<T, ObservedValueOf<O>>

  interface ConnectConfig<T> {
    connector: () => SubjectLike<T>  // default: () => new Subject<T>()
  }
  ```
- **Category**: Connectable Observable — self-managing multicast within a pipe

### Functional Specification

`connect` is the **pipeable** counterpart to `connectable`. It multicasts the source within a `selector` function — the source is subscribed to once, and the `shared$` Observable inside the selector can be used multiple times without creating multiple subscriptions.

**Why `connect` over `share`**: `share` shares emissions with all downstream subscribers unconditionally. `connect` lets you build a self-contained multicast graph — split the source into multiple branches that combine back together, all within a single subscription.

```typescript
// WITHOUT connect — two subscriptions to the source:
source$.pipe(
  mergeMap(v => combineLatest([of(v), of(v * 2)]))
)

// WITH connect — one subscription, two uses:
source$.pipe(
  connect(shared$ => combineLatest([shared$, shared$.pipe(map(v => v * 2))]))
)
```

### Examples

```typescript
import { of, interval } from 'rxjs';
import { connect, map, filter, take, combineLatestWith } from 'rxjs/operators';

// Split one source into two branches without double-subscribing
interval(1000).pipe(
  take(5),
  connect(shared$ => shared$.pipe(
    combineLatestWith(shared$.pipe(map(v => v * v)))
  ))
).subscribe(([v, vSquared]) => console.log(v, vSquared));
// [0,0], [1,1], [2,4], [3,9], [4,16]

// Filter into two paths and merge results
of(1, 2, 3, 4, 5, 6).pipe(
  connect(shared$ => {
    const evens$ = shared$.pipe(filter(v => v % 2 === 0));
    const odds$  = shared$.pipe(filter(v => v % 2 !== 0), map(v => v * 10));
    return merge(evens$, odds$);
  })
).subscribe(console.log); // 10, 2, 30, 4, 50, 6
```

---

## Multicasting Decision Guide

| Need | Use |
|------|-----|
| Share a source among all current/future subscribers | `share()` |
| Share and replay last N values for late subscribers | `shareReplay(N)` |
| Manual connect/disconnect control | `connectable(source)` |
| Multicast within a pipe (split & rejoin) | `connect(selector)` |
| ~~`publish()`~~ | Deprecated — use `connectable` |
| ~~`multicast()`~~ | Deprecated — use `connectable` |

## Common Pitfalls

### Anti-pattern: Forgetting to `connect()`
```typescript
import { connectable, interval } from 'rxjs';

// ❌ NOTHING HAPPENS — subscribers connected but source never started
const multi$ = connectable(interval(1000));
multi$.subscribe(console.log);
multi$.subscribe(console.log);
// no emissions — connect() was never called

// ✅ CORRECT
const connection = multi$.connect(); // starts the source
// unsubscribe from `connection` to stop the source
setTimeout(() => connection.unsubscribe(), 5000);

// WHY: connectable gives you manual control over when the source starts.
// Forgetting connect() is the most common mistake. If you want automatic
// connection management, use share() or shareReplay() instead.
```

### Anti-pattern: Using `connectable` When `share` Is Simpler
```typescript
import { connectable, Subject } from 'rxjs';

// ❌ OVERENGINEERED for a simple multicast
const multi$ = connectable(apiCall$, { connector: () => new Subject() });
const conn   = multi$.connect();
multi$.subscribe(handlerA);
multi$.subscribe(handlerB);
conn.unsubscribe(); // manual cleanup

// ✅ SIMPLER — share() manages connection lifecycle automatically
import { share } from 'rxjs/operators';
const shared$ = apiCall$.pipe(share());
shared$.subscribe(handlerA);
shared$.subscribe(handlerB);
// Connects on first subscriber, disconnects when all unsubscribe

// WHY: connectable is for cases where you need explicit connect/disconnect
// timing control — e.g., connect before any subscribers exist. For the
// common "share among current subscribers" pattern, share() is cleaner.
```

## Related Operators

- **`share()`**: Automatic connect/disconnect — use for most multicast needs
- **`shareReplay(N)`**: Like `share` but replays N values to late subscribers
- **`Subject`**: The underlying multicasting primitive both operators wrap

## References
- **connectable**: [https://rxjs.dev/api/index/function/connectable](https://rxjs.dev/api/index/function/connectable)
- **connect**: [https://rxjs.dev/api/operators/connect](https://rxjs.dev/api/operators/connect)

---

**`connectable`** — Cognitive Load: 4/5 | Usage: 2/5 | Manual start — use when you need precise control over when the source subscription begins.
**`connect`** — Cognitive Load: 4/5 | Usage: 2/5 | Pipeable multicast graph — use when you need to fork a source into multiple branches within one pipe.
