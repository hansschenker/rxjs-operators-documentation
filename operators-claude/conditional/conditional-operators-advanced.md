# Conditional Operators — Advanced Patterns

For fundamentals see the core [conditional operators](./conditional-operators) doc. This page covers production patterns for `defaultIfEmpty`, `isEmpty`, `every`, and `sequenceEqual` — including stream validation pipelines, empty-state handling, and assertion-style operators.

---

## Quick Reference

```typescript
import { defaultIfEmpty, isEmpty, every, sequenceEqual } from 'rxjs/operators';

// defaultIfEmpty(fallback) — emit fallback if source completes without emitting:
EMPTY.pipe(defaultIfEmpty('no results')).subscribe(console.log); // 'no results'
of(1, 2, 3).pipe(defaultIfEmpty('no results')).subscribe(console.log); // 1, 2, 3

// isEmpty() — emit true if source completes empty, false on first emission:
EMPTY.pipe(isEmpty()).subscribe(console.log);   // true
of(1).pipe(isEmpty()).subscribe(console.log);   // false

// every(predicate) — emit true if ALL values satisfy predicate, else false:
of(2, 4, 6).pipe(every(n => n % 2 === 0)).subscribe(console.log); // true
of(2, 3, 6).pipe(every(n => n % 2 === 0)).subscribe(console.log); // false (short-circuits at 3)

// sequenceEqual(comparator$) — emit true if both sequences are identical:
sequenceEqual(of(1, 2, 3))(of(1, 2, 3)).subscribe(console.log); // true
sequenceEqual(of(1, 2, 4))(of(1, 2, 3)).subscribe(console.log); // false
```

---

## Pattern 1: Empty-State UI Handling

`defaultIfEmpty` and `isEmpty` together handle the full empty-state UX pattern:

```typescript
import { defaultIfEmpty, isEmpty, share, switchMap } from 'rxjs/operators';

interface SearchResult { id: string; title: string; }
const EMPTY_RESULTS: SearchResult[] = [];

// Search results with empty-state indicator:
const results$ = searchQuery$.pipe(
  debounceTime(300),
  switchMap(q => q.length < 2 ? of(EMPTY_RESULTS) : searchApi(q)),
  share()
);

const hasResults$ = results$.pipe(
  map(results => results.length > 0)
);

// Or use isEmpty on a stream of individual items:
const itemStream$ = searchQuery$.pipe(
  debounceTime(300),
  switchMap(q => searchItems$(q)), // emits individual items
  share()
);

const noResults$ = itemStream$.pipe(
  isEmpty() // true if no items emitted before completion
);

// Template-driven:
combineLatest([results$, hasResults$]).pipe(
  takeUntilDestroyed()
).subscribe(([results, hasResults]) => {
  resultList.hidden   = !hasResults;
  emptyState.hidden   = hasResults;
  resultList.innerHTML = results.map(renderItem).join('');
});

// With defaultIfEmpty for simpler pipelines:
searchItems$(query).pipe(
  toArray(),
  defaultIfEmpty([] as SearchResult[]),
  takeUntilDestroyed()
).subscribe(results => {
  if (results.length === 0) showEmptyState();
  else renderResults(results);
});
```

---

## Pattern 2: Stream Validation with `every`

Use `every` as a stream assertion — validate that all emitted values meet a contract:

```typescript
import { every, tap, share } from 'rxjs/operators';

// Validate all API responses are well-formed before processing:
function validateStream<T>(
  source$: Observable<T>,
  predicate: (v: T) => boolean,
  errorMessage: string
): Observable<T> {
  const shared$ = source$.pipe(share());

  // Side-channel validation — doesn't affect the main stream:
  shared$.pipe(
    every(predicate),
    filter(allValid => !allValid),
    takeUntilDestroyed()
  ).subscribe(() => {
    console.error(`Stream validation failed: ${errorMessage}`);
    reportToMonitoring(errorMessage);
  });

  return shared$;
}

// Validate user permissions before processing batch:
const userBatch$ = loadUserBatch().pipe(
  every(user => user.id && user.email && user.role)
);

userBatch$.subscribe(allValid => {
  if (!allValid) throw new Error('Batch contains invalid users');
  processBatch(users);
});

// Verify data integrity in a synchronization pipeline:
const syncedItems$ = remoteItems$.pipe(share());

combineLatest([
  syncedItems$.pipe(toArray()),
  syncedItems$.pipe(every(item => item.syncedAt !== null))
]).subscribe(([items, allSynced]) => {
  if (!allSynced) reportSyncIncomplete(items.filter(i => !i.syncedAt));
  else markSyncComplete();
});
```

---

## Pattern 3: `every` for Form-Level Validation

Validate all form fields reactively before enabling submit:

```typescript
import { every, combineLatest, map } from 'rxjs/operators';

interface FieldState { value: string; valid: boolean; touched: boolean; }

// All fields must be valid and touched before submit is enabled:
const formFields = {
  email$:    new BehaviorSubject<FieldState>({ value: '', valid: false, touched: false }),
  password$: new BehaviorSubject<FieldState>({ value: '', valid: false, touched: false }),
  name$:     new BehaviorSubject<FieldState>({ value: '', valid: false, touched: false }),
};

// combineLatest emits an array — use every() on that array:
const formValid$ = combineLatest(Object.values(formFields)).pipe(
  map(fields => fields.every(f => f.valid && f.touched))
);

formValid$.pipe(takeUntilDestroyed()).subscribe(valid => {
  submitButton.disabled = !valid;
});

// Alternative: validate a stream of individual field states:
const fieldUpdates$ = merge(
  ...Object.values(formFields).map((f$, i) => f$.pipe(map(v => ({ ...v, index: i }))))
);

fieldUpdates$.pipe(
  scan((allFields, update) => {
    const next = [...allFields];
    next[update.index] = update;
    return next;
  }, [] as FieldState[]),
  map(fields => fields.length === 3 && fields.every(f => f.valid))
).subscribe(valid => {
  submitButton.disabled = !valid;
});
```

---

## Pattern 4: `sequenceEqual` for Replay Verification

Verify that a recorded sequence is replayed correctly — useful in testing, undo/redo, and synchronization:

```typescript
import { sequenceEqual, zip, map } from 'rxjs/operators';

// Verify an undo/redo replay produces the same state sequence:
function verifyReplay(
  original$:  Observable<AppState>,
  replayed$:  Observable<AppState>
): Observable<boolean> {
  return original$.pipe(
    sequenceEqual(replayed$, (a, b) => JSON.stringify(a) === JSON.stringify(b))
  );
}

// Assert that two event streams contain the same events in the same order:
const recordedEvents$ = from(localStorage.getItem('recorded-session')
  ? JSON.parse(localStorage.getItem('recorded-session')!) as UserEvent[]
  : []
);

const replayedEvents$ = replaySession();

recordedEvents$.pipe(
  sequenceEqual(replayedEvents$, (a, b) => a.type === b.type && a.payload === b.payload)
).subscribe(matches => {
  if (matches) console.log('Replay verified ✓');
  else console.error('Replay diverged — session recording may be corrupted');
});

// Compare two independent data sources for consistency:
const dbQuery$  = database.query('SELECT * FROM orders ORDER BY id');
const cacheRead$ = cache.getAllOrders();

from(dbQuery$).pipe(
  sequenceEqual(from(cacheRead$), (a, b) => a.id === b.id && a.status === b.status)
).subscribe(inSync => {
  if (!inSync) triggerCacheInvalidation();
});
```

---

## Pattern 5: `defaultIfEmpty` for Fallback Chains

Chain multiple data sources with `defaultIfEmpty` as the fallback mechanism:

```typescript
import { defaultIfEmpty, switchMap, EMPTY } from 'rxjs/operators';

// Try cache → network → default:
function getData$(key: string): Observable<Data> {
  return cache.get$(key).pipe(
    // If cache misses (completes empty), try network:
    defaultIfEmpty(null as Data | null),
    switchMap(cached => {
      if (cached) return of(cached);
      return network.fetch$(key).pipe(
        tap(data => cache.set(key, data)),
        defaultIfEmpty(DEFAULT_DATA) // network also failed → use default
      );
    })
  );
}

// Feature flag with fallback:
const featureFlags$ = remoteConfig.get$('feature_flags').pipe(
  defaultIfEmpty(LOCAL_FEATURE_FLAGS) // use local flags if remote unavailable
);

// User preferences with defaults:
const preferences$ = userStore.getPreferences$(userId).pipe(
  defaultIfEmpty(DEFAULT_PREFERENCES),
  map(prefs => ({ ...DEFAULT_PREFERENCES, ...prefs })) // merge with defaults
);
```

---

## Pattern 6: Building a Stream Assertion Utility

Combine conditional operators into reusable assertion helpers for observable pipelines:

```typescript
import { every, isEmpty, defaultIfEmpty } from 'rxjs/operators';

// Assert stream is non-empty:
function assertNonEmpty<T>(message = 'Expected non-empty stream'): MonoTypeOperatorFunction<T> {
  return source$ => {
    const shared$ = source$.pipe(share());

    shared$.pipe(
      isEmpty(),
      filter(empty => empty),
      takeUntilDestroyed()
    ).subscribe(() => { throw new Error(message); });

    return shared$;
  };
}

// Assert all values satisfy a predicate:
function assertAll<T>(
  predicate: (v: T) => boolean,
  message = 'Not all values satisfied predicate'
): MonoTypeOperatorFunction<T> {
  return source$ => {
    const shared$ = source$.pipe(share());

    shared$.pipe(
      every(predicate),
      filter(ok => !ok),
      takeUntilDestroyed()
    ).subscribe(() => { throw new Error(message); });

    return shared$;
  };
}

// Usage in a data pipeline:
productStream$.pipe(
  assertNonEmpty('Product stream must not be empty'),
  assertAll(p => p.price > 0, 'All products must have positive price'),
  assertAll(p => p.id !== undefined, 'All products must have an ID'),
  map(p => transformProduct(p))
).subscribe(renderProduct);
```

---

## `isEmpty` vs `every` vs `defaultIfEmpty` — Choosing the Right Tool

```typescript
// isEmpty() — "did anything come through?"
// Returns boolean. Useful for showing/hiding empty-state UI.
// Emits false on FIRST value (short-circuit); emits true only on completion.
stream$.pipe(isEmpty()).subscribe(empty => toggleEmptyState(empty));

// every(pred) — "do ALL values satisfy a condition?"
// Returns boolean. Useful for validation before processing.
// Emits false on FIRST non-matching value (short-circuit); emits true only on completion.
stream$.pipe(every(v => v.valid)).subscribe(allValid => proceed(allValid));

// defaultIfEmpty(fallback) — "give me a value even if the stream was empty"
// Returns T (same type). Useful for fallback chains and guaranteed emission.
// Passes through all values unchanged; only emits fallback on empty completion.
stream$.pipe(defaultIfEmpty(FALLBACK)).subscribe(v => use(v));

// sequenceEqual(other$) — "are these two streams identical?"
// Returns boolean. Useful for verification and testing.
// Must consume both streams completely before emitting result.
a$.pipe(sequenceEqual(b$)).subscribe(equal => report(equal));
```

---

## Common Pitfalls

### `sequenceEqual` Hangs on Infinite Streams

```typescript
// ❌ sequenceEqual waits for BOTH streams to complete — never resolves on infinite streams:
interval(100).pipe(
  sequenceEqual(interval(100))
).subscribe(console.log); // never emits

// ✅ Bound both streams before comparing:
interval(100).pipe(take(10),
  sequenceEqual(interval(100).pipe(take(10)))
).subscribe(console.log); // true (after 10 emissions from both)
```

### `every` Short-Circuits — Later Values Never Checked

```typescript
// ⚠️ every() emits false and completes on first non-matching value:
of(1, 2, 'oops', 4, 5).pipe(
  every(v => typeof v === 'number')
  // Emits false when it hits 'oops', then COMPLETES
  // Values 4 and 5 are NEVER processed
)

// If you need all values processed regardless:
// Use filter + count / toArray instead of every()
of(1, 2, 'oops', 4, 5).pipe(
  toArray(),
  map(arr => arr.every(v => typeof v === 'number'))
)
```

### `defaultIfEmpty` Does Not Catch Errors

```typescript
// ❌ Expecting defaultIfEmpty to handle errors:
erroringSource$.pipe(
  defaultIfEmpty('fallback') // ERROR: doesn't catch errors, only empty completion
).subscribe({
  next: v   => console.log(v),
  error: err => console.log(err) // still fires on error
});

// ✅ Use catchError for error fallback:
erroringSource$.pipe(
  catchError(() => of('error-fallback')),
  defaultIfEmpty('empty-fallback') // for the empty completion case
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: These four operators are rarely the center of a pipeline — they're validators and safety nets. `defaultIfEmpty` is the most practically useful (fallback chains, empty-state handling). `every` is the reactive equivalent of `Array.prototype.every` — elegant for batch validation but beware the short-circuit behavior. `sequenceEqual` is mainly useful in testing and verification contexts. `isEmpty` is the most concise way to power an empty-state UI toggle.
