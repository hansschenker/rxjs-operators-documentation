# find / findIndex — Advanced Patterns

For fundamentals see the core [find / findIndex](./find-findIndex) doc. This page covers search-across-streams patterns, race-based lookup, conditional activation, and comparisons with `first`, `filter`, and `single`.

---

## Mental Model

```typescript
import { find, findIndex } from 'rxjs/operators';

// find(predicate) — emit first value satisfying predicate, then complete
// Returns undefined if source completes without a match (unlike first() which errors)
of(1, 2, 3, 4, 5).pipe(find(n => n > 3)).subscribe(console.log); // 4

// findIndex(predicate) — emit the 0-based INDEX of the first matching value
of('a', 'b', 'c', 'd').pipe(findIndex(v => v === 'c')).subscribe(console.log); // 2

// No match — emits undefined/-1 (not an error):
EMPTY.pipe(find(v => v > 0)).subscribe(console.log);      // undefined
EMPTY.pipe(findIndex(v => v > 0)).subscribe(console.log); // -1
```

**Key distinction from `first(predicate)`**:

| | No match on empty/no-match stream |
|---|---|
| `find(pred)` | Emits `undefined` |
| `findIndex(pred)` | Emits `-1` |
| `first(pred)` | **Throws EmptyError** |
| `first(pred, default)` | Emits `default` |

Use `find` when absence is a valid result. Use `first(pred, default)` when you want a typed fallback. Use `first(pred)` when absence signals a programming error.

---

## Pattern 1: Safe Property Lookup in State Streams

Locate an entity in a state stream without throwing if it's missing:

```typescript
import { find, findIndex, map, switchMap } from 'rxjs/operators';

interface Product { id: string; name: string; price: number; inStock: boolean; }

// Find specific product — undefined if not loaded yet:
const targetProduct$ = productList$.pipe(
  switchMap(products =>
    from(products).pipe(find(p => p.id === targetId))
  )
);

// With fallback for missing product:
const product$ = productList$.pipe(
  map(products => products.find(p => p.id === targetId) ?? DEFAULT_PRODUCT)
);

// Find index for virtual scroll positioning:
const productIndex$ = productList$.pipe(
  switchMap(list =>
    from(list).pipe(findIndex(p => p.id === scrollToId))
  ),
  filter((idx): idx is number => idx !== -1)
);

productIndex$.subscribe(idx => virtualScroll.scrollToIndex(idx));

// Find first out-of-stock item to show a warning:
const firstOutOfStock$ = productStream$.pipe(
  find(p => !p.inStock)
);

firstOutOfStock$.subscribe(product => {
  if (product) showOutOfStockWarning(product);
  // undefined means all products are in stock — no warning needed
});
```

---

## Pattern 2: Race Lookup Across Multiple Sources

Find the first matching item from any of several sources:

```typescript
import { merge, find, share, filter } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// Search cache, then DB, take whichever finds the item first:
function findUser$(userId: string): Observable<User | undefined> {
  return merge(
    cache.getUsers$().pipe(
      find(u => u.id === userId),
      filter((u): u is User => u !== undefined) // only propagate hits
    ),
    database.streamUsers$().pipe(
      find(u => u.id === userId),
      filter((u): u is User => u !== undefined)
    )
  ).pipe(
    take(1) // take whichever source finds it first
  );
}

// Find first responding service:
function findHealthyService$(services: ServiceConfig[]): Observable<ServiceConfig> {
  return merge(
    ...services.map(svc =>
      checkHealth$(svc).pipe(
        find(healthy => healthy),
        filter(Boolean),
        map(() => svc),
        catchError(() => EMPTY) // skip unreachable services
      )
    )
  ).pipe(
    take(1)
  );
}
```

---

## Pattern 3: `findIndex` for Sorted Insert Position

Use `findIndex` to find where to insert a new element into a sorted list:

```typescript
import { findIndex, map, withLatestFrom } from 'rxjs/operators';

interface SortedList<T> { items: T[]; compareFn: (a: T, b: T) => number; }

// Find insertion index in a sorted stream:
function insertionIndex$<T>(
  sortedList$: Observable<T[]>,
  newItem$:    Observable<T>,
  compareFn:   (a: T, b: T) => number
): Observable<number> {
  return newItem$.pipe(
    withLatestFrom(sortedList$),
    switchMap(([newItem, list]) =>
      from(list).pipe(
        findIndex(existing => compareFn(newItem, existing) <= 0),
        map(idx => idx === -1 ? list.length : idx) // -1 means append at end
      )
    )
  );
}

// Usage — insert new price update into sorted price list:
const insertAt$ = insertionIndex$(
  sortedPrices$,
  newPrice$,
  (a, b) => a.value - b.value
);

insertAt$.pipe(
  withLatestFrom(sortedPrices$, newPrice$)
).subscribe(([idx, prices, price]) => {
  const updated = [...prices.slice(0, idx), price, ...prices.slice(idx)];
  updatePriceList(updated);
});
```

---

## Pattern 4: Conditional Activation with `find`

Trigger an action exactly once when a specific condition is first met:

```typescript
import { find, tap, switchMap, share } from 'rxjs/operators';

// One-shot tutorial: trigger only when user first encounters an empty state
const userState$ = store.select(selectUserState).pipe(share());

userState$.pipe(
  find(state => state.itemCount === 0 && !state.tutorialSeen),
  filter(Boolean),
  switchMap(() => this.tutorialService.showFirstTimeGuide()),
  tap(() => store.dispatch(UserActions.markTutorialSeen()))
).subscribe();

// First error in a batch — show toast, then let errors continue silently:
const batchResults$ = from(batchItems).pipe(
  mergeMap(item => processItem(item).pipe(
    map(result => ({ item, result, error: null })),
    catchError(err => of({ item, result: null, error: err }))
  )),
  share()
);

// Show toast for first error only:
batchResults$.pipe(
  find(r => r.error !== null)
).subscribe(r => {
  if (r) showErrorToast(`First batch error: ${r.error.message}`);
});

// Process all results regardless:
batchResults$.subscribe(r => updateProgress(r));
```

---

## Pattern 5: `findIndex` for Tab / Step Navigation

Find the current step's position in a wizard or tab list:

```typescript
import { findIndex, map, distinctUntilChanged, combineLatest } from 'rxjs/operators';

interface WizardStep { id: string; label: string; valid: boolean; }

const steps$       = store.select(selectWizardSteps);
const currentId$   = store.select(selectCurrentStepId);

// Current step index — drives progress bar:
const currentIndex$ = combineLatest([steps$, currentId$]).pipe(
  switchMap(([steps, currentId]) =>
    from(steps).pipe(
      findIndex(s => s.id === currentId),
    )
  ),
  distinctUntilChanged()
);

// Progress percentage:
const progress$ = combineLatest([currentIndex$, steps$.pipe(map(s => s.length))]).pipe(
  map(([idx, total]) => idx === -1 ? 0 : Math.round((idx / (total - 1)) * 100))
);

// Can navigate next — current step is valid:
const canNext$ = combineLatest([steps$, currentIndex$]).pipe(
  map(([steps, idx]) => idx >= 0 && idx < steps.length - 1 && steps[idx]?.valid)
);

// First invalid step — for "submit" validation highlighting:
const firstInvalidIndex$ = steps$.pipe(
  switchMap(steps => from(steps).pipe(findIndex(s => !s.valid))),
  distinctUntilChanged()
);

firstInvalidIndex$.subscribe(idx => {
  if (idx !== -1) highlightStep(idx);
});
```

---

## Pattern 6: Type-Safe `find` with Type Guards

Use `find` with a type-guard predicate for type-narrowed results:

```typescript
import { find, filter } from 'rxjs/operators';

type Event =
  | { type: 'click';   target: HTMLElement }
  | { type: 'keydown'; key: string }
  | { type: 'scroll';  deltaY: number };

// Type-narrowing with find:
const firstClick$ = eventStream$.pipe(
  find((e): e is Extract<Event, { type: 'click' }> => e.type === 'click')
);
// firstClick$ is Observable<Extract<Event, { type: 'click' }> | undefined>
// TypeScript knows .target is HTMLElement inside the subscription

// With filter for non-undefined guarantee:
const firstClickDefined$ = eventStream$.pipe(
  find((e): e is Extract<Event, { type: 'click' }> => e.type === 'click'),
  filter((e): e is Extract<Event, { type: 'click' }> => e !== undefined)
);
// Now Observable<Extract<Event, { type: 'click' }>> — no undefined

// Find first user with admin role (type-narrowed):
interface RegularUser { role: 'user';  name: string }
interface AdminUser   { role: 'admin'; name: string; adminLevel: number }
type User = RegularUser | AdminUser;

users$.pipe(
  switchMap(list => from(list).pipe(
    find((u): u is AdminUser => u.role === 'admin')
  )),
  filter((u): u is AdminUser => u !== undefined)
).subscribe(admin => {
  console.log(admin.adminLevel); // TypeScript knows this exists
});
```

---

## `find` vs `first` vs `filter` + `take(1)` — Decision Guide

```typescript
// find(pred)                  — first match, undefined if none, COMPLETES after find
source$.pipe(find(v => v > 5))
// ✓ Safe on no-match (undefined, not error)
// ✓ Completes after first match
// ✗ undefined is hard to distinguish from "stream ended with no match" at the type level

// first(pred, default)        — first match with fallback, ERRORS if no default and no match
source$.pipe(first(v => v > 5, -1))
// ✓ Typed fallback value
// ✓ Signals missing data as an error without default
// ✗ More verbose

// filter(pred) + take(1)      — first match, no emission if no match (silent)
source$.pipe(filter(v => v > 5), take(1))
// ✓ Never emits on no-match (just completes silently)
// ✓ Safe for infinite streams (won't block waiting for completion)
// ✗ Silent no-match — caller can't distinguish "no match" from "stream ended"

// single(pred)                — exactly one match, errors on 0 or 2+
source$.pipe(single(v => v > 5))
// ✓ Assertion: exactly one match expected
// ✗ Errors on duplicate matches — not for "find first"
```

---

## Common Pitfalls

### Using `find` on Infinite Streams Without `takeUntil`

```typescript
// ⚠️ find() on an infinite stream waits forever if no match:
interval(1000).pipe(
  find(n => n > 1_000_000) // won't complete until 1,000,001 seconds pass
).subscribe(v => console.log(v));

// ✅ Add a timeout or takeUntil for safety:
interval(1000).pipe(
  find(n => n > 100),
  timeout({ each: 200_000, with: () => of(undefined) })
).subscribe(v => {
  if (v !== undefined) found(v);
  else                 notFound();
});
```

### Confusing `findIndex` Result `-1` with a Valid Index

```typescript
// ❌ Treating -1 as a valid array index:
findIndex$.subscribe(idx => {
  const item = myArray[idx]; // myArray[-1] is undefined — silent bug!
  processItem(item);         // processes undefined
});

// ✅ Guard against -1:
findIndex$.pipe(
  filter(idx => idx !== -1)
).subscribe(idx => {
  const item = myArray[idx]; // guaranteed to exist
  processItem(item);
});
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: `find` and `findIndex` are the "safe" alternatives to `first(predicate)` — they never error on an empty or no-match stream. The most common production use is locating items in streamed lists (sorted insert, virtual scroll positioning, wizard step navigation) and conditional one-shot activation (show tutorial once, first-error toast). The critical decision: reach for `find` when absence is a valid result you want to handle; reach for `first(pred, default)` when absence means "use this fallback value"; reach for `first(pred)` when absence is a programming error.
