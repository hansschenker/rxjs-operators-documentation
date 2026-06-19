# forkJoin

## Identity
- **Name**: forkJoin
- **Category**: Combination Operators (Join Creation)
- **Type**: Parallel completion combinator — waits for all sources to complete, then emits their final values as a tuple or dictionary
- **Import**:
  ```typescript
  import { forkJoin } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // Dictionary form (recommended — named properties, better TypeScript inference)
  function forkJoin<T extends Record<string, ObservableInput<any>>>(
    sources: T
  ): Observable<{ [K in keyof T]: ObservedValueOf<T[K]> }>

  // Array form
  function forkJoin<A extends readonly unknown[]>(
    sources: readonly [...{ [K in keyof A]: ObservableInput<A[K]> }]
  ): Observable<A>

  // Spread form (deprecated in favour of array/dict forms)
  function forkJoin<T>(...sources: ObservableInput<T>[]): Observable<T[]>
  ```

## Functional Specification

**Input**: Multiple `ObservableInput<T>` sources (array, dictionary object, or spread arguments)

**Output**: `Observable<T[]>` or `Observable<{ [key]: T }>` — emits exactly once when ALL sources complete, with the last value emitted by each source

**Transformation**: Subscribes to all sources simultaneously. Waits until every source has completed. On the final completion, emits a single array/object containing the last value from each source, then completes. If any source never completes, `forkJoin` never emits.

**Semantics**: The reactive analogue of `Promise.all()`.

**Mathematical representation**:
```
Let S₁, S₂, ..., Sₙ be sources that each complete
Let last(Sᵢ) = last value emitted by Sᵢ before completion

forkJoin([S₁, ..., Sₙ]) = Observable that:
  1. Subscribes to all Sᵢ simultaneously
  2. Buffers last emitted value from each Sᵢ
  3. On final Sᵢ completion: emits [last(S₁), last(S₂), ..., last(Sₙ)], then completes

Special cases:
  Any Sᵢ errors                → forkJoin errors (other sources are unsubscribed)
  Any Sᵢ completes without emitting → forkJoin completes without emitting (!)
```

**Invariants**:
- **Exactly one emission**: Always emits at most once (zero times if any source is empty or any errors)
- **Simultaneity**: All source subscriptions begin at the same time
- **Last-value semantics**: Only the final value of each source is captured
- **Completion required**: Sources that never complete cause forkJoin to hang forever

## Marble Diagram

```
S1:       ----a----b----c----|
S2:       -------x----------y----|
S3:       --------1--2--3--4-----|
          forkJoin([S1, S2, S3])
Result:   -----------------------[c, y, 4]-|

All three complete; last values captured: c from S1, y from S2, 4 from S3.
Single emission of array, then completion.
```

**Error propagation**:
```
S1:       ----a----b----c----|
S2:       -------#
          forkJoin([S1, S2])
Result:   -------#

S2 errors → forkJoin errors immediately, S1 is unsubscribed.
c is never captured.
```

**Empty source kills the result**:
```
S1:       ----a----|
S2:       ---------|  (completes without emitting)
          forkJoin([S1, S2])
Result:   ---------|  (completes without emitting — no array!)

S2 emitted nothing → forkJoin emits nothing.
```

**Key observation**: `forkJoin` is `Promise.all()` for Observables — use it when you need results from multiple independent async operations and only care about the final outcome of each.

## Behavioral Characteristics

**Subscription**:
- All sources subscribed simultaneously when `forkJoin`'s output is subscribed
- Each source runs concurrently — no sequential waiting
- Only the last value from each source is retained; intermediate values are discarded

**Completion semantics**:
- Emits once when the last source completes
- If a source completes without emitting, the entire `forkJoin` completes without emitting
- If sources complete at different times, forkJoin waits for the slowest

**Error handling**:
- First error from any source propagates immediately
- All other source subscriptions are unsubscribed on error
- Unlike `Promise.allSettled`, there is no built-in "collect all outcomes" mode — errors must be handled inside each source stream

**Backpressure**:
- Stores one value per source at any time (the latest)
- Memory: O(number of sources) — last-value buffer per source

**Hot vs. Cold**:
- Works with both; most useful with cold sources (HTTP requests, promises)
- With hot sources that never complete: `forkJoin` hangs forever — add `take(1)` or `first()` to make them finite

## Type System Integration

```typescript
/**
 * Dictionary form provides the best TypeScript experience — each key maps to
 * the concrete type of its source, preserving the full type structure.
 *
 * Array form uses a const-asserted tuple for exact positional types.
 *
 * Promise sources are valid ObservableInput — forkJoin wraps them automatically.
 */

import { forkJoin, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

interface User    { id: number; name: string; }
interface Product { id: number; title: string; price: number; }
interface Order   { id: number; userId: number; items: number[]; }

// Dictionary form — best type inference
forkJoin({
  user:    ajax.getJSON<User>('/api/user/1'),
  product: ajax.getJSON<Product>('/api/product/42'),
  order:   ajax.getJSON<Order>('/api/order/99'),
}).subscribe(({ user, product, order }) => {
  // TypeScript knows: user: User, product: Product, order: Order
  console.log(user.name, product.title, order.items);
});

// Array form — positional typing via tuple inference
forkJoin([
  ajax.getJSON<User>('/api/user/1'),
  ajax.getJSON<Product>('/api/product/42'),
] as const).subscribe(([user, product]) => {
  // TypeScript knows: user: User, product: Product
});

// Promise sources — automatically converted
forkJoin({
  config: fetch('/api/config').then(r => r.json()) as Promise<Config>,
  flags:  ajax.getJSON<FeatureFlags>('/api/flags'),
}).subscribe(({ config, flags }) => {
  // config: Config, flags: FeatureFlags
});
```

## Examples

### Basic Usage — Parallel HTTP Requests
```typescript
import { forkJoin } from 'rxjs';
import { ajax } from 'rxjs/ajax';

interface Dashboard {
  user: User;
  notifications: Notification[];
  settings: Settings;
}

// Three parallel requests — all three must complete before the page renders
forkJoin({
  user:          ajax.getJSON<User>('/api/me'),
  notifications: ajax.getJSON<Notification[]>('/api/notifications'),
  settings:      ajax.getJSON<Settings>('/api/settings'),
}).subscribe({
  next: ({ user, notifications, settings }) => {
    renderDashboard({ user, notifications, settings });
  },
  error: err => showError('Failed to load dashboard', err),
});
// All three HTTP requests fire simultaneously; result arrives when the slowest completes.
```

### Common Pattern — Error Isolation per Source
```typescript
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// By default, any error kills the whole forkJoin.
// Wrap each source in catchError to return a fallback instead.

forkJoin({
  user:    ajax.getJSON<User>('/api/me'),
  widgets: ajax.getJSON<Widget[]>('/api/widgets').pipe(
    catchError(() => of([] as Widget[])) // empty array on failure
  ),
  config:  ajax.getJSON<Config>('/api/config').pipe(
    catchError(() => of({ theme: 'light' } as Config)) // defaults on failure
  ),
}).subscribe(({ user, widgets, config }) => {
  // user must succeed (no catchError); widgets and config have fallbacks
  renderApp({ user, widgets, config });
});
```

### Common Pattern — Loading State Wrapper
```typescript
import { forkJoin, of } from 'rxjs';
import { map, startWith, catchError } from 'rxjs/operators';

type AsyncResult<T> =
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; error: unknown };

function asAsyncResult<T>(source$: Observable<T>): Observable<AsyncResult<T>> {
  return source$.pipe(
    map(data => ({ status: 'success' as const, data })),
    catchError(error => of({ status: 'error' as const, error })),
    startWith({ status: 'loading' as const })
  );
}

// Use with a combineLatest-style approach for per-stream loading states
// (or use forkJoin when you only need the final result and want unified loading)

forkJoin({
  user: ajax.getJSON<User>('/api/me'),
  data: ajax.getJSON<Data[]>('/api/data'),
}).pipe(
  map(result => ({ status: 'success' as const, ...result })),
  startWith({ status: 'loading' as const }),
  catchError(err => of({ status: 'error' as const, error: err }))
).subscribe(state => {
  switch (state.status) {
    case 'loading': showSpinner(); break;
    case 'success': renderPage(state); break;
    case 'error':   showError(state.error); break;
  }
});
```

### Common Pattern — Parameterized Parallel Fetches
```typescript
import { forkJoin, from } from 'rxjs';
import { mergeMap, toArray } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Fetch a fixed list of IDs in parallel
const productIds = [1, 2, 3, 4, 5];

forkJoin(
  productIds.map(id => ajax.getJSON<Product>(`/api/products/${id}`))
).subscribe(products => {
  // products: Product[]  — array indexed the same as productIds
  renderProductGrid(products);
});

// Fetch a dynamic list (map + reduce pattern)
function loadProfiles(ids: number[]): Observable<Record<number, UserProfile>> {
  if (ids.length === 0) return of({});

  return forkJoin(
    Object.fromEntries(ids.map(id => [id, ajax.getJSON<UserProfile>(`/api/users/${id}`)]))
  ) as Observable<Record<number, UserProfile>>;
}
```

### Edge Cases — Empty Array, Never-Completing Source, Single Source
```typescript
import { forkJoin, NEVER, of, timer } from 'rxjs';

// Edge case 1: empty array — completes immediately without emitting
forkJoin([]).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete'),
});
// Output: complete  (no next emission)

// Edge case 2: never-completing source — forkJoin hangs forever
forkJoin([of(1), NEVER]).subscribe(console.log);
// No output ever — NEVER never completes, so forkJoin never emits
// Fix: add take(1) to make NEVER finite

// Edge case 3: source completes without emitting — forkJoin emits nothing
import { EMPTY } from 'rxjs';
forkJoin([of(1, 2, 3), EMPTY]).subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('done'),
});
// Output: done  (EMPTY emits nothing → no combined result)

// Edge case 4: single source
forkJoin([timer(1000)]).subscribe(([t]) => console.log('elapsed timer:', t));
// Output: elapsed timer: 0  (timer(1000) emits 0 then completes)
```

## Common Pitfalls

### Anti-pattern: Using `forkJoin` with Long-Lived or Infinite Observables
```typescript
import { forkJoin, interval, timer } from 'rxjs';
import { take } from 'rxjs/operators';

// ❌ BROKEN — interval never completes; forkJoin never emits
forkJoin([
  ajax.getJSON('/api/data'),
  interval(1000) // ← never completes
]).subscribe(console.log);
// No output ever. The ajax request completes, but interval never does.

// ✅ CORRECT — make the long-lived source finite with take()
forkJoin([
  ajax.getJSON('/api/data'),
  interval(1000).pipe(take(5)) // completes after 5 ticks
]).subscribe(([data, lastTick]) => console.log(data, lastTick));

// ✅ CORRECT — use first() to convert "first emission" pattern
const currentUser$ = userStore$.pipe(first()); // hot store → finite single-value

forkJoin({
  user:    currentUser$,
  product: ajax.getJSON('/api/product/1'),
}).subscribe(({ user, product }) => process(user, product));

// WHY: forkJoin requires all sources to complete. Observables like interval,
// fromEvent, or BehaviorSubject never complete on their own. Add take(1),
// first(), or take(N) to make them finite before passing to forkJoin.
```

### Anti-pattern: Expecting `forkJoin` to Behave Like `Promise.allSettled`
```typescript
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';

// ❌ WRONG ASSUMPTION — forkJoin errors if ANY source errors (like Promise.all, not Promise.allSettled)
forkJoin([
  ajax.getJSON('/api/a'),
  ajax.getJSON('/api/b'), // fails with 500
  ajax.getJSON('/api/c'),
]).subscribe({
  next:  results => processAll(results), // only fires if ALL succeed
  error: err => console.error(err)       // fires on first failure; b and c may not have run
});

// ✅ CORRECT — wrap each source to collect outcomes (like Promise.allSettled)
type Settled<T> = { status: 'fulfilled'; value: T } | { status: 'rejected'; reason: unknown };

function settle<T>(source$: Observable<T>): Observable<Settled<T>> {
  return source$.pipe(
    map(value => ({ status: 'fulfilled' as const, value })),
    catchError(reason => of({ status: 'rejected' as const, reason }))
  );
}

forkJoin([
  settle(ajax.getJSON('/api/a')),
  settle(ajax.getJSON('/api/b')),
  settle(ajax.getJSON('/api/c')),
]).subscribe(results => {
  results.forEach(r => {
    if (r.status === 'fulfilled') process(r.value);
    else console.warn('request failed:', r.reason);
  });
});

// WHY: forkJoin propagates the first error and cancels remaining subscriptions —
// equivalent to Promise.all. If you need all results regardless of individual
// failures (Promise.allSettled semantics), wrap each source in a catchError
// that returns a settled-result value instead of propagating the error.
```

### Anti-pattern: Using `forkJoin` When Sequential Ordering Matters
```typescript
import { forkJoin, concat, of } from 'rxjs';

// ❌ WRONG TOOL — forkJoin for sequential operations that depend on each other
forkJoin([
  createUser(userData),
  sendWelcomeEmail(userData.email) // must run AFTER user is created
]).subscribe(console.log);
// Both fire simultaneously — email sent before user exists in DB!

// ✅ CORRECT — use concat or switchMap for sequential async operations
import { switchMap } from 'rxjs/operators';

createUser(userData).pipe(
  switchMap(user => sendWelcomeEmail(user.email).pipe(
    map(emailResult => ({ user, emailResult }))
  ))
).subscribe(({ user, emailResult }) => onSuccess(user));

// ✅ ALSO CORRECT — forkJoin for truly independent parallel operations
forkJoin([
  sendAnalyticsEvent('user.created'),
  updateUserDirectory(user),
  invalidateCache('users')
  // All independent — none depends on the others' results
]).subscribe(() => console.log('all side effects complete'));

// WHY: forkJoin subscribes to all sources simultaneously. Operations that depend
// on each other's results must be sequenced with switchMap, concatMap, or concat —
// not parallelized with forkJoin.
```

## Related Operators

**Same Category (Combination / Join Creation)**:
- **`combineLatest`**: Emits on *every* source emission after all have emitted once — use for reactive derived state that should update live; not for one-shot parallel requests
- **`zip`**: Pairs emissions by index — emits after each source has emitted N times; rarely the right choice for HTTP
- **`merge`**: Forwards each emission as it arrives without waiting for completion — use for concurrent stream merging, not final-value collection
- **`race`**: Emits from whichever source emits first, unsubscribes the rest — use for timeout races

**Complementary Operators**:
- **`catchError`**: Handle per-source errors for `Promise.allSettled`-style collection
- **`startWith`**: Add a loading-state emission before `forkJoin` resolves (on the outer pipe)
- **`take(1)` / `first()`**: Convert long-lived Observables to finite single-value sources for use inside `forkJoin`

**Alternatives by Use Case**:

| Use Case | Instead of `forkJoin` | Use This | Why |
|----------|----------------------|----------|-----|
| Live reactive combination | `forkJoin` | `combineLatest` | combineLatest emits on each change, not just final |
| Sequential async steps | `forkJoin` | `switchMap` / `concatMap` | Sequential operations need ordering guarantees |
| Collect all outcomes | `forkJoin` | `forkJoin` + per-source `catchError` | Error isolation via settle() wrapper |
| Infinite sources | `forkJoin` | `forkJoin` + `first()` / `take(1)` | Make sources finite first |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/forkJoin](https://rxjs.dev/api/index/function/forkJoin)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/zip.html](http://reactivex.io/documentation/operators/zip.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/observable/forkJoin.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/observable/forkJoin.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Parallel Completion Gate (Promise.all for Observables)
- **Cognitive Load**: 3/5 — The empty-source and never-completing footguns, plus error semantics vs. Promise.allSettled, require explicit teaching
- **Usage Frequency**: 5/5 — The canonical operator for parallel HTTP requests in Angular and similar frameworks
- **Composability**: 4/5 — Clean API; requires care with long-lived sources and error isolation

**Teaching Sequence**:
- **Prerequisites**: `Observable`, `ajax`, `catchError`, `take(1)`, `first()`
- **Teaches**: Parallel subscriptions, join semantics, last-value capture, error isolation patterns
- **Leads to**: `combineLatest` (live join), `zip` (indexed join), reactive dashboard patterns
- **Common with**: `catchError`, `startWith`, `take(1)`, `first()`, `ajax`
