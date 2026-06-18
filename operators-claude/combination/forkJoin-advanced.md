# forkJoin — Advanced Patterns

For `forkJoin` fundamentals, see the core [forkJoin](./forkJoin) doc. This page covers error handling, partial failures, timeouts, and the comparison with `combineLatest`.

---

## The Core Problem: One Failure Kills Everything

`forkJoin` subscribes to all sources and waits for all to complete. If **any source errors**, the entire `forkJoin` errors immediately — other in-flight requests are cancelled and their results are lost.

```typescript
// ❌ One 404 means we get NOTHING
forkJoin({
  user:   this.http.get('/api/users/1'),  // succeeds
  orders: this.http.get('/api/orders'),    // 404
  prefs:  this.http.get('/api/prefs')     // succeeds
}).subscribe({
  next:  data  => render(data),
  error: e     => showError(e) // user and prefs results lost!
});
```

---

## Pattern 1: Tolerant forkJoin (Partial Failures)

Wrap each source with `catchError` to provide fallback values:

```typescript
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';

forkJoin({
  user:   this.http.get<User>('/api/users/1').pipe(
    catchError(() => of(null))
  ),
  orders: this.http.get<Order[]>('/api/orders').pipe(
    catchError(() => of([] as Order[]))
  ),
  prefs:  this.http.get<Prefs>('/api/prefs').pipe(
    catchError(() => of(DEFAULT_PREFS))
  )
}).subscribe(({ user, orders, prefs }) => {
  // Always called — nulls/defaults for failed requests
  render(user, orders, prefs);
});
```

---

## Pattern 2: forkJoin with Individual Error Tracking

```typescript
type Result<T> =
  | { ok: true;  value: T }
  | { ok: false; error: string };

function safe<T>(source$: Observable<T>): Observable<Result<T>> {
  return source$.pipe(
    map(value => ({ ok: true, value } as Result<T>)),
    catchError(err => of({ ok: false, error: err.message } as Result<T>))
  );
}

forkJoin({
  user:   safe(this.http.get<User>('/api/users/1')),
  orders: safe(this.http.get<Order[]>('/api/orders')),
  prefs:  safe(this.http.get<Prefs>('/api/prefs'))
}).subscribe(results => {
  if (results.user.ok)   renderUser(results.user.value);
  if (results.orders.ok) renderOrders(results.orders.value);
  else                   showOrdersError(results.orders.error);
});
```

---

## Pattern 3: forkJoin with Per-Request Timeout

```typescript
import { forkJoin } from 'rxjs';
import { timeout, catchError, of } from 'rxjs/operators';

forkJoin({
  fastData: fastApi$.pipe(
    timeout(2000),
    catchError(() => of(null)) // timeout = null fallback
  ),
  slowData: slowApi$.pipe(
    timeout(10_000),
    catchError(() => of(DEFAULT_SLOW)) // different timeout per request
  )
}).subscribe(({ fastData, slowData }) => render(fastData, slowData));
```

---

## Pattern 4: Dynamic forkJoin from Array

```typescript
import { forkJoin, from } from 'rxjs';

// When the number of requests isn't known at compile time:
const ids = ['1', '2', '3', '4', '5'];

forkJoin(
  ids.map(id =>
    this.http.get<Item>(`/api/items/${id}`).pipe(
      catchError(() => of(null))
    )
  )
).subscribe(items => {
  const loaded  = items.filter((item): item is Item => item !== null);
  const failed  = ids.filter((_, i) => items[i] === null);
  render(loaded);
  if (failed.length) logMissing(failed);
});
```

---

## Pattern 5: forkJoin as Initialization Gate

Run all bootstrap requests before rendering anything:

```typescript
@Injectable({ providedIn: 'root' })
export class AppInitService implements APP_INITIALIZER {
  init(): Observable<void> {
    return forkJoin({
      config:      this.config.load(),
      featureFlags:this.flags.load(),
      translations:this.i18n.load('en')
    }).pipe(
      tap(({ config, featureFlags, translations }) => {
        this.store.dispatch(AppActions.initSuccess({ config, featureFlags }));
        this.i18n.setTranslations(translations);
      }),
      map(() => undefined),
      catchError(err => {
        console.error('App initialization failed:', err);
        return of(undefined); // let app continue with defaults
      })
    );
  }
}
```

---

## `forkJoin` vs `combineLatest` — The Key Distinction

```typescript
// forkJoin: waits for ALL to COMPLETE, emits once
// Use for: HTTP requests, one-shot operations
forkJoin({ a: req1$, b: req2$ }).subscribe(({ a, b }) => {
  // Called ONCE when both complete
});

// combineLatest: emits whenever any source emits (after all have emitted at least once)
// Use for: live state, ongoing streams
combineLatest({ a: state1$, b: state2$ }).subscribe(({ a, b }) => {
  // Called whenever state1$ OR state2$ changes
});
```

**Rule**: Use `forkJoin` for "fetch all data once." Use `combineLatest` for "react to any state change."

---

## Pattern 6: Parallel with Sequential Dependency

```typescript
// First: run two independent requests in parallel
// Then: use both results for a third request
forkJoin({
  user: this.http.get<User>('/api/me'),
  config: this.http.get<Config>('/api/config')
}).pipe(
  switchMap(({ user, config }) =>
    // Now use both results for the next request:
    this.http.get<Dashboard>(`/api/dashboard?role=${user.role}&theme=${config.theme}`)
  )
).subscribe(renderDashboard);
```

---

## Common Pitfalls

### `forkJoin` on Never-Completing Observables

```typescript
// ❌ HANGS FOREVER — interval never completes
forkJoin([
  interval(1000).pipe(take(5)), // completes ✓
  interval(500)                 // never completes ✗
]).subscribe(console.log);      // never emits

// ✅ Ensure all sources complete with take/first/last:
forkJoin([
  interval(1000).pipe(take(5)),
  interval(500).pipe(take(10)) // both complete
]).subscribe(console.log);
// WHY: forkJoin waits for all sources to complete before emitting.
// Any source that never completes means forkJoin never emits.
```

### Forgetting `catchError` Makes forkJoin All-or-Nothing

See Pattern 1 above — always wrap individual sources with `catchError` if partial failures are acceptable.

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 3/5
**Key rule**: `forkJoin` is perfectly suited for parallel HTTP requests. Its Achilles heel is error handling — always decide upfront whether one failure should kill all results (no `catchError`) or each should fail independently (per-source `catchError`).
