# tap (advanced) / finalize (advanced)

Advanced patterns for `tap` and `finalize` beyond basic side-effect usage. For the fundamentals, see the core `tap` and `finalize` docs.

---

## `tap` — Advanced Patterns

### Tapping Into Specific Notification Types

```typescript
import { tap } from 'rxjs/operators';

// Full observer form — different action per notification type
source$.pipe(
  tap({
    next:       v  => metrics.recordValue(v),
    error:      e  => metrics.recordError(e),
    complete:   () => metrics.recordComplete(),
    subscribe:  () => metrics.recordSubscribe(),    // RxJS 7+
    unsubscribe:() => metrics.recordUnsubscribe()   // RxJS 7+
  })
).subscribe(handler);
```

### Reusable Debug Operator

```typescript
import { tap } from 'rxjs/operators';
import { MonoTypeOperatorFunction } from 'rxjs';

function debug<T>(tag: string): MonoTypeOperatorFunction<T> {
  return tap({
    next:        v  => console.log(`[${tag}] next:`,     v),
    error:       e  => console.error(`[${tag}] error:`,  e),
    complete:    () => console.log(`[${tag}] complete`),
    subscribe:   () => console.log(`[${tag}] subscribed`),
    unsubscribe: () => console.log(`[${tag}] unsubscribed`)
  });
}

// Use anywhere in a pipeline:
source$.pipe(
  debug('raw'),
  debounceTime(300),
  debug('debounced'),
  switchMap(v => ajax.getJSON(`/api?q=${v}`)),
  debug('response')
).subscribe(render);
```

### Tap for Caching Side-Effects

```typescript
import { tap } from 'rxjs/operators';

// Populate a local cache as values flow through
const cache = new Map<string, User>();

userRequests$.pipe(
  switchMap(id =>
    cache.has(id)
      ? of(cache.get(id)!)
      : ajax.getJSON<User>(`/api/users/${id}`).pipe(
          tap(user => cache.set(id, user)) // populate cache on success
        )
  )
).subscribe(renderUser);
```

### Tap for Performance Measurement

```typescript
import { tap } from 'rxjs/operators';

function measureLatency<T>(label: string): MonoTypeOperatorFunction<T> {
  let start: number;
  return tap({
    subscribe: () => { start = performance.now(); },
    next:      ()  => {
      const ms = performance.now() - start;
      console.log(`[${label}] first emission: ${ms.toFixed(1)}ms`);
    }
  });
}

ajax.getJSON('/api/data').pipe(
  measureLatency('api/data')
).subscribe(console.log);
```

---

## `finalize` — Advanced Patterns

### Finalize vs `tap({ complete, error })`

```typescript
import { finalize, tap } from 'rxjs/operators';

// tap({ complete }) — runs only on normal completion
source$.pipe(
  tap({ complete: () => console.log('completed normally') })
).subscribe();
// Does NOT run on error or unsubscription

// finalize — runs on complete, error, AND unsubscription
source$.pipe(
  finalize(() => console.log('stream ended — any reason'))
).subscribe();
// Guaranteed teardown regardless of how the stream ends
```

### Resource Cleanup Pattern

```typescript
import { finalize } from 'rxjs/operators';

function withCleanup<T>(source$: Observable<T>, cleanup: () => void): Observable<T> {
  return source$.pipe(finalize(cleanup));
}

// WebSocket with guaranteed close
withCleanup(
  fromWebSocket('wss://api.example.com'),
  () => ws.close()
).pipe(
  takeUntil(destroy$)
).subscribe(handleMessage);
// ws.close() called whether takeUntil fires, error occurs, or manual unsub
```

### Finalize for Loading State Management

```typescript
import { finalize } from 'rxjs/operators';
import { BehaviorSubject } from 'rxjs';

const loading$ = new BehaviorSubject(false);

function withLoadingState<T>(source$: Observable<T>): Observable<T> {
  return new Observable<T>(subscriber => {
    loading$.next(true);
    return source$.pipe(
      finalize(() => loading$.next(false)) // always clears loading
    ).subscribe(subscriber);
  });
}

withLoadingState(ajax.getJSON('/api/data')).subscribe({
  next:  data => render(data),
  error: err  => showError(err) // loading clears even on error
});
```

### Finalize Ordering — Runs After `complete`/`error`

```typescript
import { finalize, tap } from 'rxjs/operators';

// finalize runs AFTER the subscriber's complete/error callbacks
of(1, 2, 3).pipe(
  tap({ complete: () => console.log('tap complete') }),
  finalize(() => console.log('finalize'))
).subscribe({
  complete: () => console.log('subscriber complete')
});
// Order: "tap complete", "subscriber complete", "finalize"
// finalize always runs last — use this for cleanup that must follow subscriber teardown
```

---

## Composing tap + finalize for Full Lifecycle Tracking

```typescript
import { tap, finalize } from 'rxjs/operators';

function trackLifecycle<T>(name: string): MonoTypeOperatorFunction<T> {
  let count = 0;
  return (source$: Observable<T>) => source$.pipe(
    tap({
      subscribe:   () => console.log(`[${name}] subscribed`),
      next:        v  => console.log(`[${name}] #${++count}:`, v),
      error:       e  => console.error(`[${name}] error:`, e),
      complete:    () => console.log(`[${name}] completed (${count} values)`)
    }),
    finalize(() => console.log(`[${name}] finalized`))
  );
}

interval(500).pipe(
  take(3),
  trackLifecycle('counter')
).subscribe();
// [counter] subscribed
// [counter] #1: 0
// [counter] #2: 1
// [counter] #3: 2
// [counter] completed (3 values)
// [counter] finalized
```

## References
- [tap](https://rxjs.dev/api/operators/tap)
- [finalize](https://rxjs.dev/api/operators/finalize)

---

**Advanced `tap`**: The `subscribe`/`unsubscribe` hooks (RxJS 7+) enable full lifecycle monitoring — invaluable for debugging subscription leaks.
**Advanced `finalize`**: Runs after subscriber's complete/error, making it the right place for cleanup that must follow all teardown — not before.
