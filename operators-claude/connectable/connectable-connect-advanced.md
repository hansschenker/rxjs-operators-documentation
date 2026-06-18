# connectable / connect — Advanced Patterns

For fundamentals see the core [connectable / connect](./connectable-connect) doc. This page covers multicasting architecture decisions, the `connect()` operator for self-contained pipelines, reference counting strategies, and the relationship to `share()` / `shareReplay()`.

---

## Mental Model: The Multicasting Spectrum

```typescript
import { connectable, connect, Subject, ReplaySubject } from 'rxjs';
import { share, shareReplay, publish, publishReplay } from 'rxjs/operators';

// The multicasting spectrum from manual to automatic:

// 1. connectable() — manual connect/disconnect (maximum control):
const conn$ = connectable(source$, { connector: () => new Subject() });
const sub   = conn$.connect(); // start multicasting manually
conn$.subscribe(observerA);
conn$.subscribe(observerB);
sub.unsubscribe(); // stop manually

// 2. connect() — self-contained multicasting within a pipe:
source$.pipe(
  connect(shared$ => merge(
    shared$.pipe(filter(isError), map(toAlert)),
    shared$.pipe(filter(isData),  map(toDisplay))
  ))
);

// 3. share() — automatic ref-counted connectable (Subject internally):
const shared$ = source$.pipe(share()); // connects on first sub, disconnects on last

// 4. shareReplay(n) — automatic ref-counted with replay buffer:
const cached$ = source$.pipe(shareReplay(1)); // replays last value to late subscribers
```

---

## Pattern 1: `connectable` for Coordinated Multi-Subscriber Start

Use `connectable` when multiple subscribers must be attached *before* the source starts emitting — otherwise early values are lost:

```typescript
import { connectable, Subject, interval, zip } from 'rxjs';
import { take, map } from 'rxjs/operators';

// Scenario: live auction — all bidders must connect before bidding opens
function startAuction(itemId: string): void {
  const bids$ = webSocket$<Bid>(`wss://auction.example.com/items/${itemId}`);

  const conn$ = connectable(bids$, {
    connector:       () => new Subject<Bid>(),
    resetOnDisconnect: true // re-subscribes to source after all subscribers leave
  });

  // Attach all bidders first:
  const highBid$ = conn$.pipe(
    scan((max, bid) => bid.amount > max.amount ? bid : max)
  );

  const bidHistory$ = conn$.pipe(
    scan((hist, bid) => [...hist, bid], [] as Bid[])
  );

  const bidCount$ = conn$.pipe(
    scan(n => n + 1, 0)
  );

  // Wire up UI:
  highBid$.subscribe(b => updateHighBid(b));
  bidHistory$.subscribe(h => updateHistory(h));
  bidCount$.subscribe(n => updateCounter(n));

  // NOW start — all subscribers are ready, no values lost:
  conn$.connect();
}

// Coordinated start with a Promise-based gate:
async function coordinatedStart(): Promise<void> {
  const source$ = interval(1000).pipe(take(10));
  const conn$   = connectable(source$, { connector: () => new Subject<number>() });

  const results: Record<string, number[]> = { a: [], b: [], c: [] };

  conn$.subscribe(v => results.a.push(v));
  conn$.subscribe(v => results.b.push(v));
  conn$.subscribe(v => results.c.push(v));

  await readySignal$; // wait for external ready signal

  const sub = conn$.connect();
  await lastValueFrom(source$);
  sub.unsubscribe();

  // All three arrays are identical — no subscriber missed any value
  console.assert(JSON.stringify(results.a) === JSON.stringify(results.c));
}
```

---

## Pattern 2: `connect()` for Operator-Level Multicasting

`connect()` creates a multicast scope within a `pipe()` chain — subscribe once to the source, use the shared stream in multiple derived pipelines:

```typescript
import { connect, merge, EMPTY } from 'rxjs';
import { filter, map, tap, catchError, share } from 'rxjs/operators';

// Without connect() — source subscribed TWICE (two HTTP calls):
const withoutConnect$ = httpResponse$.pipe(
  filter(r => r.ok),
  map(r => r.data)
);
const errors$ = httpResponse$.pipe(
  filter(r => !r.ok),
  map(r => r.error)
);
// Subscribing to both = two subscriptions to httpResponse$ = two HTTP requests

// With connect() — source subscribed ONCE, result split into two streams:
const result$ = httpResponse$.pipe(
  connect(response$ => merge(
    response$.pipe(
      filter(r => r.ok),
      map(r => r.data)
    ),
    response$.pipe(
      filter(r => !r.ok),
      tap(r => logError(r.error)),
      map(() => DEFAULT_DATA)
    )
  ))
);

// Real-world: analytics + main data from same WebSocket stream
webSocketMessages$.pipe(
  connect(msgs$ => {
    // Side-channel: send analytics events (fire-and-forget)
    msgs$.pipe(
      filter(m => m.type === 'user_action'),
      mergeMap(m => analyticsService.track(m).pipe(catchError(() => EMPTY)))
    ).subscribe(); // independent subscription within the connect scope

    // Main channel: return processed data
    return msgs$.pipe(
      filter(m => m.type === 'data'),
      map(m => m.payload as DataPayload)
    );
  })
).subscribe(data => renderData(data));
```

---

## Pattern 3: `connectable` with `ReplaySubject` — Controlled Replay

When you need replay behavior but want manual control over when multicasting starts:

```typescript
import { connectable, ReplaySubject } from 'rxjs';

// Replay last 5 values to any subscriber — but only start after explicit connect():
const replayConn$ = connectable(
  expensiveDataStream$,
  {
    connector: () => new ReplaySubject<DataPoint>(5),
    resetOnDisconnect: false // keep buffer even after all subscribers leave
  }
);

// Record mode: fill the buffer before any consumer subscribes
const subscription = replayConn$.connect();

// ... time passes, 10 values emitted ...

// Late subscriber gets last 5 values immediately:
replayConn$.subscribe(v => console.log('late subscriber:', v));

// Another late subscriber also gets the same last 5:
replayConn$.subscribe(v => updateDashboard(v));

// Cleanup when done:
subscription.unsubscribe();

// Practical: pre-load data, serve cached to multiple late-arriving components
@Injectable({ providedIn: 'root' })
class PreloadService {
  private config$ = connectable(
    this.http.get<AppConfig>('/api/config'),
    {
      connector:         () => new ReplaySubject<AppConfig>(1),
      resetOnDisconnect: false
    }
  );

  constructor() {
    // Start loading immediately at service init — before any component subscribes:
    this.config$.connect();
  }

  getConfig(): Observable<AppConfig> {
    return this.config$; // late subscribers get cached value instantly
  }
}
```

---

## Pattern 4: `resetOnDisconnect` — Reconnecting Behavior

Control whether the multicast resets when all subscribers unsubscribe:

```typescript
import { connectable, Subject, interval } from 'rxjs';
import { share, shareReplay } from 'rxjs/operators';

// resetOnDisconnect: true (default) — like share()
// When last subscriber leaves, the connection is torn down.
// Next subscriber triggers a fresh subscription to the source.
const resetConn$ = connectable(counter$, {
  connector:         () => new Subject(),
  resetOnDisconnect: true  // re-subscribes to source when next subscriber arrives
});

// resetOnDisconnect: false — like shareReplay({ refCount: false })
// Connection persists even with no subscribers.
// Useful for "keep alive" streams that should never be torn down.
const persistConn$ = connectable(livePrice$, {
  connector:         () => new Subject(),
  resetOnDisconnect: false // connection lives forever after connect()
});
persistConn$.connect(); // connect once at startup, never disconnects

// Comparison:
// share()                     ≈ connectable + resetOnDisconnect: true + auto ref-count
// shareReplay(n)              ≈ connectable with ReplaySubject(n) + resetOnDisconnect: true + auto ref-count
// shareReplay(n, Infinity, refCount: false)  ≈ connectable with ReplaySubject(n) + resetOnDisconnect: false
```

---

## Pattern 5: `connect()` for Testing Side-Effects

Use `connect()` to attach test probes without creating additional subscriptions:

```typescript
import { connect, tap } from 'rxjs/operators';

// Test harness — attach spy without extra subscription:
function withSpy<T>(
  spy: (value: T) => void
): MonoTypeOperatorFunction<T> {
  return source$ => source$.pipe(
    connect(shared$ => {
      shared$.pipe(tap(spy)).subscribe(); // spy subscription
      return shared$;                     // passthrough
    })
  );
}

// Usage in tests:
const emitted: number[] = [];

source$.pipe(
  withSpy(v => emitted.push(v)),
  map(v => v * 2)
).subscribe(v => console.log(v));

// emitted[] tracks raw values; subscriber sees doubled values
// Only ONE subscription to source$

// Production use: performance monitoring without extra subscriptions:
function withTiming<T>(label: string): MonoTypeOperatorFunction<T> {
  return source$ => source$.pipe(
    connect(shared$ => {
      let count = 0;
      let start = Date.now();

      shared$.pipe(
        tap(() => count++),
        finalize(() => {
          const elapsed = Date.now() - start;
          console.log(`[${label}] ${count} values in ${elapsed}ms`);
        })
      ).subscribe();

      return shared$;
    })
  );
}
```

---

## `connectable` vs `connect()` vs `share()` Decision Guide

```
Need to manually control WHEN multicasting starts?
  → connectable() + explicit .connect()

Need multicasting WITHIN a pipe chain (not exposed to callers)?
  → connect() operator

Need automatic start/stop based on subscriber count?
  → share() or shareReplay()

Need replay for late subscribers AND manual start control?
  → connectable() with ReplaySubject connector

Need to keep the source alive even with no subscribers?
  → connectable() with resetOnDisconnect: false, or shareReplay({ refCount: false })

Need all of the above configured simply?
  → share({ connector, resetOnRefCountZero }) — the full-featured version
```

---

## Common Pitfalls

### Connecting Before Attaching Subscribers

```typescript
// ❌ connect() before subscribing — source starts, first values lost:
const conn$ = connectable(source$, { connector: () => new Subject() });
conn$.connect(); // source starts emitting NOW

conn$.subscribe(v => console.log(v)); // misses values already emitted

// ✅ Attach all subscribers BEFORE calling connect():
const conn$ = connectable(source$, { connector: () => new Subject() });
conn$.subscribe(v => console.log(v)); // ready to receive
conn$.connect(); // now start
```

### Not Unsubscribing the Connection

```typescript
// ❌ Forgetting to unsubscribe the connection — source runs forever:
const conn$ = connectable(interval(1000), { connector: () => new Subject() });
const sub   = conn$.connect();
conn$.subscribe(/* ... */);
// subscription unsubscribed but conn$ connection never closed — interval keeps running

// ✅ Unsubscribe the connection when done:
const conn$ = connectable(interval(1000), { connector: () => new Subject() });
const connSub = conn$.connect();

conn$.pipe(takeUntilDestroyed()).subscribe(/* ... */);

// On cleanup:
onDestroy(() => connSub.unsubscribe());
```

### `connect()` Selector Must Return an Observable

```typescript
// ❌ Forgetting to return from the connect() selector:
source$.pipe(
  connect(shared$ => {
    shared$.subscribe(v => sideEffect(v)); // ← only side effect, no return
    // Returns undefined — pipe breaks
  })
)

// ✅ Always return an Observable from the connect() selector:
source$.pipe(
  connect(shared$ => {
    shared$.subscribe(v => sideEffect(v));
    return shared$; // or a transformed version
  })
)
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `connectable` is the low-level primitive that `share()` and `shareReplay()` are built on — reach for it when you need the explicit connect/disconnect control that automatic ref-counting doesn't give you. `connect()` (the operator, not the method) is the underused gem: it enables clean multicast scoping within a pipe chain, eliminating the common pattern of manually creating a `Subject` and calling `share()` just to split one stream into two derived pipelines.
