# shareReplay — Advanced Patterns

For `shareReplay` fundamentals see the core [shareReplay](./shareReplay) doc. This page covers the memory leak problem, `refCount` behavior, cache invalidation, and `shareReplay` vs `share` vs `BehaviorSubject`.

---

## The Memory Leak Problem

`shareReplay(1)` (shorthand) uses `refCount: false` by default in RxJS 7, which means the inner subscription **never unsubscribes from the source** even when all consumers unsubscribe:

```typescript
// ❌ This leaks — WebSocket stays open forever:
const prices$ = webSocket('/prices').pipe(
  shareReplay(1)  // refCount: false → source subscription never cleaned up
);

const sub1 = prices$.subscribe(renderChart);
const sub2 = prices$.subscribe(updateTable);

sub1.unsubscribe(); // source still active
sub2.unsubscribe(); // source STILL active — leak!
```

---

## `refCount: true` vs `refCount: false`

```typescript
// refCount: false (default shorthand) — subscribe once, stay forever:
source$.pipe(shareReplay(1))
// ✓ Late subscribers always get the last value
// ✗ Source subscription lives even with 0 consumers

// refCount: true — unsubscribe when all consumers leave:
source$.pipe(shareReplay({ bufferSize: 1, refCount: true }))
// ✓ Source cleaned up when nobody subscribes
// ✗ If source completes/errors and refCount hits 0, state resets on next subscribe

// Full config object:
source$.pipe(shareReplay({
  bufferSize: 1,
  refCount:   false,
  windowTime: 30_000  // replay buffer expires after 30s
}))
```

---

## Pattern 1: Application-Level Singleton (Correct Use of `refCount: false`)

For app-wide singletons where you **want** the source to stay alive:

```typescript
@Injectable({ providedIn: 'root' })
export class UserService {
  // Root service lives for app lifetime — refCount: false is correct here:
  readonly currentUser$ = this.auth.authState$.pipe(
    switchMap(auth => auth ? this.api.getUser(auth.uid) : of(null)),
    shareReplay(1)  // fine — service lives as long as app
  );
}
```

---

## Pattern 2: Component-Level Cache (Use `refCount: true`)

For caches tied to a component or short-lived service:

```typescript
@Component({ ... })
export class ProductListComponent implements OnDestroy {
  // Component-scoped — must clean up when component destroys:
  readonly products$ = this.api.getProducts().pipe(
    shareReplay({ bufferSize: 1, refCount: true })
    // When component destroys and all subscriptions end → source unsubscribed
  );
}
```

---

## Pattern 3: `windowTime` — Expire the Cache

For data that should be re-fetched after N milliseconds:

```typescript
import { shareReplay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class CatalogService {
  readonly categories$ = this.api.getCategories().pipe(
    shareReplay({
      bufferSize: 1,
      refCount:   false,
      windowTime: 5 * 60 * 1000  // cached values expire after 5 minutes
    })
  );
  // After 5min: a new subscriber triggers a fresh API call
}
```

---

## Pattern 4: Manual Cache Invalidation

```typescript
import { BehaviorSubject, switchMap } from 'rxjs';
import { shareReplay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class InvalidatableCacheService {
  private invalidate$ = new BehaviorSubject<void>(undefined);

  readonly data$ = this.invalidate$.pipe(
    switchMap(() =>
      this.api.getData().pipe(
        shareReplay(1)  // cache per generation
      )
    ),
    shareReplay(1)  // cache across generations
  );

  invalidate(): void {
    this.invalidate$.next();  // triggers new fetch + new cache
  }
}
```

---

## Pattern 5: `shareReplay` vs `share` Decision

```typescript
// share — no replay buffer:
source$.pipe(share())
// Late subscriber gets NOTHING from before they subscribed
// Subscription shared while there are active subscribers
// ✓ For hot sources where late subscriber gets live updates only (WebSocket, events)

// shareReplay(1) — replay last value:
source$.pipe(shareReplay(1))
// Late subscriber gets the last emitted value immediately
// ✓ For state/data where "current value" matters (user profile, config)

// shareReplay(N) — replay last N values:
source$.pipe(shareReplay(3))
// Late subscriber gets up to 3 historical values
// ✓ For log/event streams where recent history matters (chat, notifications)
```

---

## Pattern 6: Guarding Against Error Caching

`shareReplay` caches errors — all future subscribers immediately receive the error:

```typescript
// ❌ Error cached — every subsequent subscriber gets the error:
const data$ = this.http.get('/api/data').pipe(
  shareReplay(1)
);
data$.subscribe();     // fails: error cached
data$.subscribe();     // immediately gets cached error — no retry!

// ✅ Option A — resetOnError (RxJS 7):
const data$ = this.http.get('/api/data').pipe(
  shareReplay({ bufferSize: 1, refCount: false }),
  // Note: resetOnError not yet in stable API — use catchError instead:
);

// ✅ Option B — catchError before shareReplay:
const data$ = this.http.get('/api/data').pipe(
  catchError(err => {
    logger.error(err);
    return EMPTY;  // don't cache errors; complete gracefully
  }),
  shareReplay(1)
);

// ✅ Option C — retry before shareReplay:
const data$ = this.http.get('/api/data').pipe(
  retry(3),      // retry before sharing
  shareReplay(1) // only cache successful result
);
```

---

## Pattern 7: `shareReplay` for Expensive Derivations

Cache computed pipelines, not just HTTP responses:

```typescript
// Expensive computation shared across multiple consumers:
const processedData$ = rawEvents$.pipe(
  filter(e => e.type === 'RELEVANT'),
  map(e => heavyTransform(e)),            // expensive per-event
  scan((acc, e) => merge(acc, e), {}),    // stateful accumulation
  shareReplay(1)                          // compute once, share result
);

// Multiple components subscribe — heavy pipeline runs once:
processedData$.subscribe(renderChart);
processedData$.subscribe(renderTable);
processedData$.subscribe(updateMetrics);
```

---

## `shareReplay(1)` vs `BehaviorSubject` vs `AsyncSubject`

```typescript
// shareReplay(1) — wraps an existing Observable:
const state$ = expensiveSource$.pipe(shareReplay(1));
// ✓ Source drives the value — passive cache
// ✗ Cannot imperatively set the value

// BehaviorSubject — you control the value:
const state$ = new BehaviorSubject(initialValue);
state$.next(newValue); // imperative push
// ✓ Set value from anywhere
// ✓ Always has a synchronous current value (.getValue())
// ✗ Must manage subscription lifecycle manually

// AsyncSubject — cache the final value:
const result$ = new AsyncSubject<T>();
// Emits only when complete() is called
// ✓ For "do-once" operations where you want the terminal value
```

---

## Diagnosing Memory Leaks from `shareReplay`

Signs of a `shareReplay` memory leak:
1. HTTP requests or WebSocket connections remain open after component destroy
2. Memory grows over time as components mount/unmount
3. Stale data appears after navigation

Diagnostic approach:

```typescript
// Add tap to detect when source is still running after unsubscribe:
const data$ = source$.pipe(
  tap({
    subscribe:   () => console.log('[shareReplay] source subscribed'),
    unsubscribe: () => console.log('[shareReplay] source unsubscribed'),
    finalize:    () => console.log('[shareReplay] source finalized')
  }),
  shareReplay(1)
);
// If you see "source subscribed" but never "source unsubscribed" after
// all consumers unsubscribe → you have the refCount: false memory leak
```

---

## Common Pitfalls

### Using `shareReplay(1)` for Mutable Shared State

```typescript
// ❌ Mutation bypasses shareReplay — all subscribers get stale reference:
const list$ = this.api.getList().pipe(shareReplay(1));
list$.subscribe(list => {
  list.push(newItem);  // mutates cached reference — all subscribers affected!
});

// ✅ Treat shared values as immutable:
list$.subscribe(list => {
  const updated = [...list, newItem]; // new array, not mutation
  this.localState$.next(updated);
});
```

### Expecting `shareReplay` to Re-fetch After Completion

```typescript
// ❌ Once the source completes, shareReplay replays the completion:
const once$ = this.http.get('/api/data').pipe(shareReplay(1));
once$.subscribe(render); // HTTP call made, data received, source completes
once$.subscribe(render); // Gets cached data — no new HTTP call (correct!)

// This is usually what you WANT for HTTP. But if you need re-fetch:
// Use invalidation (Pattern 4) or don't use shareReplay for that use case.
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 4/5
**The rule**: `shareReplay(1)` (shorthand) is correct for **application-level singletons** (root services). For **component-level** caching, use `shareReplay({ bufferSize: 1, refCount: true })` so the source unsubscribes when the component destroys. Always put `retry` or `catchError` **before** `shareReplay` to prevent error caching.
