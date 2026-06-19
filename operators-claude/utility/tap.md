# tap

## Identity
- **Name**: tap (formerly `do` in RxJS 5)
- **Category**: Utility Operators
- **Type**: Transparent side-effect observer (pass-through)
- **Import**:
  ```typescript
  import { tap } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { tap } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function tap<T>(
    observerOrNext?: Partial<TapObserver<T>> | ((value: T) => void) | null
  ): MonoTypeOperatorFunction<T>

  // TapObserver interface (RxJS 7+)
  interface TapObserver<T> {
    next:        (value: T) => void;
    error:       (err: any) => void;
    complete:    () => void;
    subscribe:   () => void;    // fires on subscription
    unsubscribe: () => void;    // fires on explicit unsubscribe
    finalize:    () => void;    // fires on complete OR error OR unsubscribe
  }
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable emitting values of type T

**Output**: `Observable<T>` — an identical Observable; every emission, error, and completion from the source passes through unchanged

**Transformation**: None — `tap` is a transparent pass-through. It intercepts each notification (next, error, complete) to execute a side effect, then forwards the notification downstream without modification. The output stream is semantically identical to the source stream.

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let sideEffect: T → void be the callback

Output = S  (identity)

For each vᵢ ∈ S:
  1. Execute sideEffect(vᵢ)     ← side effect happens here
  2. Forward vᵢ to downstream   ← stream is unchanged
```

**Invariants**:
- **Identity**: The output stream is observationally equivalent to the source — same values, same order, same timing, same completion and error behaviour
- **No transformation**: The callback return value is discarded; `tap` never alters the emitted value
- **Error passthrough**: Errors are forwarded after the `error` callback executes — `tap` does not catch or swallow errors
- **Completion passthrough**: Completion is forwarded after the `complete` callback executes
- **Synchronous callbacks**: All `tap` callbacks execute synchronously within the notification cycle before the notification continues downstream

## Marble Diagram

```
Source:   --1-----2-----3-----|
          tap(x => console.log('saw', x))
Result:   --1-----2-----3-----|
          (side effects: logs "saw 1", "saw 2", "saw 3")

Legend:
  - : time unit (10ms)
  1,2,3 : values — identical in source and result
  | : completion
  The result stream is a perfect mirror of the source.
```

**With error callback**:
```
Source:   --1-----2-----#
          tap({ next: log, error: logErr })
Result:   --1-----2-----#
          (logs 1, logs 2, logErr runs before # propagates downstream)
```

**Lifecycle hooks (RxJS 7+)**:
```
Subscription:   tap({ subscribe: () => log('subscribed') })
                ^ fires once at subscription time, before any values

Unsubscribe:    tap({ unsubscribe: () => log('unsubscribed') })
                ^ fires only on explicit unsubscribe, not on complete/error

Finalize:       tap({ finalize: () => log('done') })
                ^ fires on complete, error, OR unsubscribe — use as
                  inline alternative to the finalize() operator
```

**Key observation**: `tap` never changes what the subscriber receives. It is a one-way observation window into the stream — you can see the data and react to it externally, but you cannot alter it from within `tap`.

## Behavioral Characteristics

**Subscription**:
- The `subscribe` callback (if provided) fires synchronously at subscription time, before any source values are requested
- Subscribes to source lazily — only when the output Observable is subscribed to
- Maintains exactly one subscription to the source

**Completion semantics**:
- The `complete` callback fires synchronously when the source completes, before the completion signal reaches downstream
- The `finalize` callback fires on completion, error, or explicit unsubscribe
- `tap` itself never triggers or prevents completion

**Error handling**:
- The `error` callback fires synchronously when the source errors, before the error propagates downstream
- `tap` does **not** catch errors — if the `error` callback throws, the new error replaces the original
- If the `next` callback throws, the error is forwarded downstream and the subscription terminates
- Use `catchError` downstream to recover from errors; `tap` is not an error boundary

**Backpressure**:
- None — `tap` is synchronous and 1-to-1; it cannot buffer, drop, or rate-limit values
- If the `next` callback is slow, it delays downstream delivery synchronously

**Hot vs. Cold**:
- Transparent to hot/cold semantics — `tap` does not alter the multicast nature of the source

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type emitted by both the source and result Observables (unchanged)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>  (MonoTypeOperatorFunction<T>)
 *
 * Type Narrowing:
 *   - None — tap is a MonoTypeOperatorFunction; T in = T out
 *   - The callback return type is void; any return value is ignored
 *
 * Type Safety:
 *   - The next callback is typed as (value: T) => void — T is fully known
 *   - The error callback receives any (typed as unknown in strict mode)
 *   - Attempting to return a value from the callback is a type error (void)
 */

import { of, throwError } from 'rxjs';
import { tap } from 'rxjs/operators';

// T flows through unchanged
const numbers$ = of(1, 2, 3).pipe(
  tap(n => console.log('next:', n)),     // n: number
  tap({ complete: () => console.log('done') })
);
// numbers$: Observable<number> — same T as source

// Observer object with all hooks
of('a', 'b').pipe(
  tap({
    subscribe:   ()    => console.log('subscribed'),
    next:        v     => console.log('next:', v),      // v: string
    error:       err   => console.error('error:', err), // err: any
    complete:    ()    => console.log('complete'),
    unsubscribe: ()    => console.log('unsubscribed'),
    finalize:    ()    => console.log('finalized'),
  })
).subscribe();
// Output:
// subscribed
// next: a
// next: b
// complete
// finalized

// Strict-mode error typing
of(1).pipe(
  tap({
    error: (err: unknown) => {
      if (err instanceof Error) console.log(err.message);
    }
  })
);
```

## Examples

### Basic Usage — Debug Logging
```typescript
import { of } from 'rxjs';
import { tap, map, filter } from 'rxjs/operators';

const pipeline$ = of(1, 2, 3, 4, 5).pipe(
  tap(n => console.log('before filter:', n)),
  filter(n => n % 2 === 0),
  tap(n => console.log('after filter:', n)),
  map(n => n * 10),
  tap(n => console.log('after map:', n))
);

pipeline$.subscribe();
// Output:
// before filter: 1
// before filter: 2
// after filter: 2
// after map: 20
// before filter: 3
// before filter: 4
// after filter: 4
// after map: 40
// before filter: 5
```

### Common Pattern — HTTP Request Lifecycle Logging
```typescript
import { ajax } from 'rxjs/ajax';
import { tap, map, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

interface User { id: number; name: string; }

function fetchUser(id: number) {
  return ajax.getJSON<User>(`https://api.example.com/users/${id}`).pipe(
    tap({
      subscribe:  ()    => console.log(`[${id}] Request started`),
      next:       user  => console.log(`[${id}] Received:`, user.name),
      error:      err   => console.error(`[${id}] Failed:`, err.message),
      complete:   ()    => console.log(`[${id}] Request complete`),
      finalize:   ()    => console.log(`[${id}] Cleaned up`),
    }),
    catchError(err => of(null))
  );
}

fetchUser(1).subscribe();
// Output (success):
// [1] Request started
// [1] Received: Alice
// [1] Request complete
// [1] Cleaned up
```

### Common Pattern — Loading Indicator and Analytics
```typescript
import { fromEvent } from 'rxjs';
import { tap, switchMap, finalize } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const searchInput = document.getElementById('search') as HTMLInputElement;

let isLoading = false;

const results$ = fromEvent(searchInput, 'input').pipe(
  tap(() => {
    isLoading = true;
    showSpinner();
    analytics.track('search_started');
  }),
  switchMap(e => {
    const query = (e.target as HTMLInputElement).value;
    return ajax.getJSON(`/api/search?q=${query}`).pipe(
      tap(results => analytics.track('search_results', { count: results.length })),
      finalize(() => {
        isLoading = false;
        hideSpinner();
      })
    );
  })
);

results$.subscribe(results => renderResults(results));
```

### Common Pattern — Updating External State (Store / Cache)
```typescript
import { ajax } from 'rxjs/ajax';
import { tap, shareReplay } from 'rxjs/operators';

const cache = new Map<number, unknown>();

function getUser(id: number) {
  if (cache.has(id)) {
    return of(cache.get(id));
  }

  return ajax.getJSON(`/api/users/${id}`).pipe(
    tap(user => cache.set(id, user)), // populate cache as a side effect
    shareReplay(1)
  );
}

// tap updates the cache without altering what subscribers receive
getUser(1).subscribe(user => console.log('Got user:', user));
getUser(1).subscribe(user => console.log('Got user again:', user)); // from cache
```

### Edge Cases — Callback Throws, Lifecycle Order
```typescript
import { of, Subject } from 'rxjs';
import { tap } from 'rxjs/operators';

// Edge case 1: next callback throws — error propagates downstream
of(1, 2, 3).pipe(
  tap(n => {
    if (n === 2) throw new Error('tap threw on 2');
  })
).subscribe({
  next:  v   => console.log('next:', v),
  error: err => console.log('error:', err.message),
});
// Output:
// next: 1
// error: tap threw on 2
// (3 is never emitted)

// Edge case 2: unsubscribe vs complete — finalize fires for both, unsubscribe only for explicit cancel
const subject$ = new Subject<number>();
const sub = subject$.pipe(
  tap({
    unsubscribe: () => console.log('unsubscribe hook'),
    complete:    () => console.log('complete hook'),
    finalize:    () => console.log('finalize hook'),
  })
).subscribe();

subject$.next(1);
sub.unsubscribe(); // explicit cancel
// Output:
// unsubscribe hook
// finalize hook   ← finalize fires on unsubscribe too

const sub2 = subject$.pipe(
  tap({
    unsubscribe: () => console.log('unsubscribe hook'),
    complete:    () => console.log('complete hook'),
    finalize:    () => console.log('finalize hook'),
  })
).subscribe();

subject$.complete();
// Output:
// complete hook
// finalize hook   ← finalize fires on complete
// (unsubscribe hook does NOT fire on natural completion)
```

## Common Pitfalls

### Anti-pattern: Using `tap` to Transform Values
```typescript
import { of } from 'rxjs';
import { tap, map } from 'rxjs/operators';

let transformed: number;

// ❌ INCORRECT — trying to "transform" via a shared variable
of(1, 2, 3).pipe(
  tap(n => { transformed = n * 10; }), // sets external variable
  map(() => transformed)               // reads it back — fragile!
).subscribe(console.log);
// Happens to work synchronously, but is:
// - Invisible to the type system
// - Broken if any async operator is introduced
// - A maintenance hazard

// ✅ CORRECT — use map for transformation
of(1, 2, 3).pipe(
  map(n => n * 10)
).subscribe(console.log);
// Output: 10, 20, 30

// WHY: tap's callback return value is always discarded. It cannot
// transform the stream. map is the correct tool for transformation;
// tap is only for side effects that do not affect downstream values.
```

### Anti-pattern: Mutating the Emitted Value Inside `tap`
```typescript
import { of } from 'rxjs';
import { tap } from 'rxjs/operators';

interface Config { debug: boolean; retries: number; }

// ❌ INCORRECT — mutating the object inside tap
of<Config>({ debug: false, retries: 3 }).pipe(
  tap(cfg => {
    cfg.debug = true; // mutates the original object!
  })
).subscribe(cfg => console.log(cfg));
// Output: { debug: true, retries: 3 }
// The mutation "works" but is a hidden side effect that surprises
// any other subscriber sharing the same object reference.

// ✅ CORRECT — if you need to change the value, use map
of<Config>({ debug: false, retries: 3 }).pipe(
  map(cfg => ({ ...cfg, debug: true })) // new object, original untouched
).subscribe(cfg => console.log(cfg));

// WHY: tap is meant to observe, not modify. Mutating inside tap makes
// the pipeline's behaviour invisible — the type signature still shows
// Observable<Config>, but the config object has been silently changed.
// Mutations also cause subtle bugs when the observable is multicasted.
```

### Anti-pattern: Three-Argument Form (Deprecated)
```typescript
import { of } from 'rxjs';
import { tap } from 'rxjs/operators';

// ❌ DEPRECATED — three separate arguments (RxJS 6 style, removed in RxJS 7)
of(1, 2, 3).pipe(
  tap(
    v   => console.log('next:', v),
    err => console.error('error:', err),
    ()  => console.log('complete')
  )
);

// ✅ CORRECT — pass an observer object (RxJS 7+)
of(1, 2, 3).pipe(
  tap({
    next:     v   => console.log('next:', v),
    error:    err => console.error('error:', err),
    complete: ()  => console.log('complete'),
  })
);

// WHY: The three-argument form was deprecated in RxJS 7 for consistency
// with the Observer interface. The object form is explicit, readable,
// and supports the additional lifecycle hooks (subscribe, unsubscribe, finalize).
```

### Anti-pattern: Heavy Synchronous Work in `tap`
```typescript
import { interval } from 'rxjs';
import { tap, take } from 'rxjs/operators';

// ❌ INCORRECT — blocking synchronous work in tap delays every emission
interval(100).pipe(
  take(10),
  tap(n => {
    heavySynchronousComputation(n); // blocks for 50ms per emission
  })
).subscribe(console.log);
// Emissions are delayed by 50ms each — the stream is effectively throttled
// by the side effect, which is invisible to readers of the pipeline

// ✅ CORRECT — offload heavy work or use a scheduler
interval(100).pipe(
  take(10),
  tap(n => {
    // Queue the work so it doesn't block emission
    queueMicrotask(() => heavySynchronousComputation(n));
  })
).subscribe(console.log);

// Or: do the heavy work downstream where it's explicit
interval(100).pipe(
  take(10),
  map(n => ({ n, result: heavySynchronousComputation(n) }))
).subscribe(({ n, result }) => handleResult(n, result));

// WHY: tap's callback is synchronous and blocks the notification cycle.
// Heavy work in tap delays all downstream operators, making the
// pipeline's performance characteristics surprising and hard to diagnose.
```

### Performance: Removing `tap` in Production Builds
**When this matters**:
Debug `tap` calls left in production add call-stack overhead for every emission, even when the callback is a no-op logger.

**What to do**:
```typescript
// Development-only tap using an environment flag
import { tap } from 'rxjs/operators';
import { identity } from 'rxjs';

const debugTap = <T>(label: string) =>
  process.env.NODE_ENV === 'development'
    ? tap<T>(v => console.log(`[${label}]`, v))
    : identity; // identity operator: no-op pass-through

source$.pipe(
  debugTap('before-filter'),
  filter(isValid),
  debugTap('after-filter')
);
```

## Related Operators

**Same Category (Utility)**:
- **`finalize`**: Executes a callback when the stream ends (complete, error, or unsubscribe) — equivalent to `tap({ finalize })` but more explicit as a standalone operator; prefer `finalize()` when cleanup is the only concern
- **`delay`**: Shifts emission timing without altering values — another pass-through, but temporal
- **`timeout`**: Errors if the source goes silent — observes timing rather than values
- **`repeat`**: Re-subscribes on completion — alters stream lifecycle rather than observing it

**Complementary Operators**:
- **`map`**: The transformation counterpart to `tap` — if you need to change the value, use `map`; if you need to observe it, use `tap`
- **`filter`**: Pair `tap` before `filter` to log all values, and after `filter` to log only passing values
- **`catchError`**: Place `tap({ error })` before `catchError` to log the raw error before recovery
- **`shareReplay`**: Use `tap` to populate a cache when combined with `shareReplay` for memoisation

**Alternatives by Use Case**:

| Use Case | Instead of `tap` | Use This | Why |
|----------|-----------------|----------|-----|
| Transform values | `tap(v => { x = f(v) })` then `map(() => x)` | `map(v => f(v))` | Explicit, type-safe, no shared state |
| Cleanup on end | `tap({ finalize })` | `finalize(() => cleanup())` | Clearer intent as a dedicated operator |
| Error recovery | `tap({ error })` | `catchError(err => ...)` | `tap` observes; `catchError` handles |
| Conditional logging | `tap(v => { if (cond) log(v) })` | `tap(v => cond && log(v))` | Same — either form is fine |

**Migration Notes**:
```typescript
// RxJS 5 — operator was named `do`
import { do } from 'rxjs/add/operator/do'; // RxJS 5 (not valid JS — `do` is reserved)

// RxJS 6 — renamed to tap, three-argument form
source$.pipe(tap(nextFn, errorFn, completeFn))

// RxJS 7+ — observer object form preferred
source$.pipe(tap({ next: nextFn, error: errorFn, complete: completeFn }))
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/tap](https://rxjs.dev/api/operators/tap)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/do.html](http://reactivex.io/documentation/operators/do.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/tap.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/tap.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Transparent Observer Strategy (Non-intrusive Side-Effect Injection)
- **Cognitive Load**: 2/5 — Simple concept, but the lifecycle hooks (subscribe/unsubscribe/finalize) and the distinction from `map` require deliberate understanding
- **Usage Frequency**: 5/5 — Essential for debugging, logging, and analytics in every production pipeline
- **Composability**: 5/5 — Can be placed anywhere in a pipeline without affecting downstream behaviour

**Problem Domain**:
Observing stream events to produce side effects (logging, metrics, UI state updates, cache population, analytics) without altering what downstream operators or subscribers receive. The transparent pass-through guarantee makes `tap` safe to add or remove anywhere for debugging.

**When to Teach**:
Immediately after `map` and `filter` — students need `tap` to debug their first pipelines.

- **Prerequisites**: Observable creation, subscribe, `map`, `filter`
- **Teaches**: The separation of concerns between observation (tap) and transformation (map); the concept of pure vs. impure pipeline steps; stream lifecycle (subscribe/complete/error/unsubscribe)
- **Leads to**: `finalize` (focused cleanup), `catchError` (error handling), understanding hot/cold and multicast implications of shared state in callbacks
- **Common with**: `map`, `filter`, `catchError` — `tap` is the "printf debugging" of reactive pipelines

**Common Misconceptions**:
1. **"tap can transform values"** — the callback return value is discarded; use `map` to transform
2. **"tap catches errors"** — the `error` callback observes the error but does not catch it; use `catchError` to recover
3. **"complete and finalize are the same"** — `complete` fires only on natural completion; `finalize` fires on complete, error, and unsubscribe
4. **"tap is only for logging"** — it is the correct operator for any side effect (cache updates, analytics, loading spinners) as long as the stream value must remain unchanged
