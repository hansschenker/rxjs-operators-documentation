# first / last — Advanced Patterns

For fundamentals see the core [first / last](./first-last) doc. This page covers conditional selection, race patterns, timeout-with-first-valid, and the differences from `take(1)`, `takeLast(1)`, and `filter`.

---

## Mental Model

```typescript
import { first, last } from 'rxjs/operators';

// first() — emit first value, then complete. Error if stream completes empty.
// first(predicate) — emit first MATCHING value, then complete.
// first(predicate, default) — emit default if no match before completion.

// last() — emit last value on completion. Error if stream completes empty.
// last(predicate) — emit last MATCHING value on completion.
// last(predicate, default) — emit default if no match.
```

---

## Pattern 1: First Valid Response (Race Multiple Sources)

Emit the first non-error response from any source:

```typescript
import { first, catchError, merge, filter } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

function firstSuccessful<T>(sources: Observable<T>[]): Observable<T> {
  return merge(
    ...sources.map(s$ => s$.pipe(catchError(() => EMPTY)))
  ).pipe(
    first() // take whichever source responds first without error
  );
}

// Primary + backup API, take whichever responds first:
firstSuccessful([
  this.primaryApi.getUser(id),
  this.backupApi.getUser(id)
]).subscribe(user => renderUser(user));
```

---

## Pattern 2: First Matching Value with Default

```typescript
import { first, map } from 'rxjs/operators';

interface Config { featureFlags: Record<string, boolean>; }

// Get a feature flag, default to false if not found:
config$.pipe(
  first(
    cfg => cfg.featureFlags['new_checkout'] !== undefined,
    { featureFlags: { new_checkout: false } } as Config
  ),
  map(cfg => cfg.featureFlags['new_checkout'])
).subscribe(enabled => toggleNewCheckout(enabled));

// First error-free value (skip null/undefined):
stream$.pipe(
  first(v => v != null, null)
).subscribe(v => {
  if (v !== null) renderValue(v);
  else            showEmpty();
});
```

---

## Pattern 3: `first` with Timeout

Emit first value within a deadline, fallback otherwise:

```typescript
import { first, timeout, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

function firstWithin<T>(
  source$:    Observable<T>,
  ms:         number,
  fallback:   T
): Observable<T> {
  return source$.pipe(
    first(),
    timeout({
      each: ms,
      with: () => of(fallback)
    })
  );
}

// Wait up to 2s for WebSocket connection, use REST fallback:
firstWithin(
  wsMessages$.pipe(filter(m => m.type === 'CONNECTED')),
  2000,
  { type: 'CONNECTED', via: 'rest' }
).subscribe(conn => initConnection(conn));
```

---

## Pattern 4: `last` for Aggregating Stream Results

Collect the final accumulated state from a finite stream:

```typescript
import { last, scan } from 'rxjs/operators';

interface ProcessResult { processed: number; failed: number; items: unknown[]; }

// Process a batch and get only the final summary:
from(batch).pipe(
  mergeMap(item =>
    processItem(item).pipe(
      map(result => ({ success: true, result })),
      catchError(() => of({ success: false, result: null }))
    ),
    5 // 5 concurrent
  ),
  scan((acc, { success, result }) => ({
    processed: acc.processed + (success ? 1 : 0),
    failed:    acc.failed    + (success ? 0 : 1),
    items:     success ? [...acc.items, result] : acc.items
  }), { processed: 0, failed: 0, items: [] } as ProcessResult),
  last() // only care about final totals, not intermediate progress
).subscribe(summary => showBatchSummary(summary));
```

---

## Pattern 5: Conditional `last` — Final Error-Free Value

```typescript
import { last, catchError } from 'rxjs/operators';

// Get last successfully-parsed message before stream errors:
messageStream$.pipe(
  scan((acc, raw) => {
    try    { return { value: JSON.parse(raw), error: null }; }
    catch  { return { ...acc, error: raw }; }
  }, { value: null, error: null }),
  filter(s => s.value !== null),
  map(s => s.value),
  last(null, null) // last valid message, or null if none
).subscribe(lastGood => {
  if (lastGood) recoverFromLastGoodState(lastGood);
});
```

---

## Pattern 6: `first` as One-Shot Initializer

Initialize a service exactly once from the first emission:

```typescript
import { first, shareReplay } from 'rxjs/operators';

class ConfigService {
  // Load config once, cache for all subscribers:
  private config$ = this.http.get<Config>('/api/config').pipe(
    first(),          // complete after first response
    shareReplay(1)    // cache for all future subscribers
  );

  get<K extends keyof Config>(key: K): Observable<Config[K]> {
    return this.config$.pipe(map(cfg => cfg[key]));
  }
}

// Ensure router waits for config before first navigation:
const configLoaded$ = inject(ConfigService).get('routePermissions').pipe(first());

bootstrapApplication(AppComponent, {
  providers: [
    provideRouter(routes, withEnabledBlockingInitialNavigation())
  ]
}).then(() => configLoaded$.subscribe());
```

---

## `first` vs `take(1)` vs `filter` + `take(1)`

```typescript
// take(1) — take first value, no error on empty stream:
source$.pipe(take(1))
// ✓ Safe on empty streams (just completes)
// ✗ No predicate; no default

// first() — same as take(1) but ERRORS on empty stream:
source$.pipe(first())
// ✓ Signals missing-data as an error (useful for assertions)
// ✗ Requires catchError to handle empty case

// first(predicate) — first MATCHING value, errors if none match before completion:
source$.pipe(first(v => v > 10))
// ✓ Inline predicate + auto-complete
// ✗ Errors if no match — use first(predicate, default) for safety

// filter + take(1) — same as first(predicate) but never errors:
source$.pipe(filter(v => v > 10), take(1))
// ✓ Safe on no-match (just never emits if condition never true on infinite streams)
// ✗ More verbose; no default value
```

**Decision**: Use `take(1)` when you just need the first value and can't guarantee a match. Use `first(predicate, default)` when you want inline matching with a safe fallback. Use `last()` only on finite/bounded streams.

---

## Common Pitfalls

### `first()` Errors on Empty Streams

```typescript
// ❌ first() throws EmptyError if source completes without emitting:
EMPTY.pipe(first()).subscribe({
  next:  v   => console.log(v),
  error: err => console.log(err) // EmptyError!
});

// ✅ Provide a default, or use take(1):
EMPTY.pipe(first(null, 'default')).subscribe(v => console.log(v)); // 'default'
EMPTY.pipe(take(1)).subscribe(v => console.log(v));                 // (nothing)
```

### `last()` on Infinite Stream — Never Emits

```typescript
// ❌ interval() never completes — last() never emits:
interval(100).pipe(last()).subscribe(v => console.log(v)); // never fires

// ✅ Bound the stream first:
interval(100).pipe(take(10), last()).subscribe(v => console.log(v)); // 9
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `first(predicate, default)` is the safest form — it handles empty streams and no-match gracefully. The most common production use is initialization: "give me the first config value / auth state / feature flag, then I'm done."
