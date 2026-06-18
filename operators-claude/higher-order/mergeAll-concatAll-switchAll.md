# mergeAll / concatAll / switchAll

## Identity

These three operators flatten an `Observable<Observable<T>>` (a higher-order Observable) into a plain `Observable<T>`. They are the `*All` counterparts of `mergeMap`, `concatMap`, and `switchMap`.

| | `mergeAll` | `concatAll` | `switchAll` |
|---|---|---|---|
| **Import** | `import { mergeAll } from 'rxjs/operators'` | `import { mergeAll } from 'rxjs/operators'` | `import { switchAll } from 'rxjs/operators'` |
| **Concurrency** | All inner Observables subscribed concurrently | One at a time, sequentially | Only latest inner Observable active |
| **Equivalent to** | `mergeMap(x => x)` | `concatMap(x => x)` | `switchMap(x => x)` |

```typescript
function mergeAll<T>(concurrent?: number): OperatorFunction<ObservableInput<T>, T>
function concatAll<T>(): OperatorFunction<ObservableInput<T>, T>
function switchAll<T>(): OperatorFunction<ObservableInput<T>, T>
```

## Functional Specification

**When to use `*All` vs `*Map`**:
- Use `mergeMap/concatMap/switchMap` when the projection function is inline: `source$.pipe(mergeMap(v => transform(v)))`
- Use `*All` when the outer Observable already emits Observables: `source$.pipe(map(v => transform(v)), mergeAll())`

**`mergeAll(concurrent?)`**: Subscribes to each inner Observable as it arrives. All active inner Observables run concurrently. Optional `concurrent` limits active subscriptions.

**`concatAll()`**: Queues inner Observables. Only subscribes to the next one after the current one completes. Preserves order.

**`switchAll()`**: Subscribes to each new inner Observable and immediately unsubscribes from the previous one. Only one inner Observable is active at any time — the latest.

## Marble Diagrams

```
Outer:  --A-----------B-----------|
         |             |
         A: --1--2--|   B: --3--4--|

mergeAll():  --1--2-----3--4--|     (A and B run concurrently after B arrives)

concatAll(): --1--2--------3--4--|  (B waits for A to complete)

switchAll(): --1--2--------3--4--|  (coincidentally same here if no overlap)

Outer:  --A-------B---|
         |         |
         A: --1--2--3--4--|   (long-running)
         B: --x--y--|

switchAll():  --1--2---x--y--|   (B arrives → unsubscribe A, subscribe B)
mergeAll():   --1--2---x--3--y--4--|  (A and B run concurrently, merged)
concatAll():  --1--2--3--4--x--y--|  (B waits until A completes)
```

## Type System Integration

```typescript
import { of, interval } from 'rxjs';
import { map, mergeAll, concatAll, switchAll, take } from 'rxjs/operators';

// Higher-order Observable — Observable<Observable<number>>
const outerOfInners$ = of(1, 2, 3).pipe(
  map(n => interval(100 * n).pipe(take(3), map(i => `${n}:${i}`)))
);
// outerOfInners$: Observable<Observable<string>>

outerOfInners$.pipe(mergeAll())
// Observable<string> — concurrent
outerOfInners$.pipe(concatAll())
// Observable<string> — sequential
outerOfInners$.pipe(switchAll())
// Observable<string> — latest only
```

## Examples

### Basic Usage — `*Map` vs `*All` Equivalence
```typescript
import { of, timer } from 'rxjs';
import { map, mergeAll, mergeMap, concatAll, concatMap } from 'rxjs/operators';

// These are equivalent:
of(1, 2, 3).pipe(
  mergeMap(n => timer(n * 100))
).subscribe(console.log);

of(1, 2, 3).pipe(
  map(n => timer(n * 100)),  // produces Observable<Observable<number>>
  mergeAll()                  // flattens
).subscribe(console.log);

// Use *All when the projection is done elsewhere (e.g., in a service)
function toTimer(n: number): Observable<number> {
  return timer(n * 100);
}

of(1, 2, 3).pipe(
  map(toTimer),  // map returns Observable<Observable<number>>
  mergeAll()
).subscribe(console.log);
```

### Common Pattern — `switchAll` for Search (Classic Use Case)
```typescript
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged, switchAll } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Each keystroke produces a search Observable; switchAll keeps only the latest
fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),
  distinctUntilChanged(),
  map(query => ajax.getJSON<Result[]>(`/api/search?q=${query}`)),
  switchAll()  // cancel previous search when new query arrives
).subscribe(renderResults);

// This is equivalent to the more idiomatic:
// .pipe(debounceTime(300), distinctUntilChanged(), switchMap(query => ajax...))
// Both approaches are valid; switchAll is useful when the inner Observable
// is computed in a separate step.
```

### Common Pattern — `concatAll` for Ordered Async Sequences
```typescript
import { of, from } from 'rxjs';
import { map, concatAll } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Execute migrations in strict order — each must complete before next starts
const migrations = ['001_create_users', '002_add_roles', '003_seed_data'];

from(migrations).pipe(
  map(name => ajax.post(`/api/migrations/${name}`)),
  concatAll() // run strictly in sequence
).subscribe({
  next:     res  => console.log('migration done:', res),
  error:    err  => console.error('migration failed:', err),
  complete: ()   => console.log('all migrations complete')
});
```

### Common Pattern — `mergeAll` for Parallel Batches
```typescript
import { from } from 'rxjs';
import { map, mergeAll, toArray } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const userIds = [1, 2, 3, 4, 5];

// Fetch all users in parallel, collect results
from(userIds).pipe(
  map(id => ajax.getJSON<User>(`/api/users/${id}`)),
  mergeAll(3), // max 3 concurrent requests
  toArray()
).subscribe(users => renderTable(users));
```

## Common Pitfalls

### Anti-pattern: Using `concatAll` When Inner Observable Never Completes
```typescript
import { interval } from 'rxjs';
import { map, concatAll } from 'rxjs/operators';

// ❌ BLOCKED FOREVER — first inner Observable never completes
of('a', 'b', 'c').pipe(
  map(letter => interval(100).pipe(map(n => `${letter}:${n}`))),
  concatAll() // subscribes to interval('a') — never completes — 'b' and 'c' never start
).subscribe(console.log);
// Output: a:0, a:1, a:2, a:3, ... (only 'a' stream, forever)

// ✅ CORRECT — make inner Observables finite, or use mergeAll
import { take } from 'rxjs/operators';
of('a', 'b', 'c').pipe(
  map(letter => interval(100).pipe(take(3), map(n => `${letter}:${n}`))),
  concatAll()
).subscribe(console.log);
// a:0, a:1, a:2, b:0, b:1, b:2, c:0, c:1, c:2

// WHY: concatAll subscribes to inner Observables one at a time and waits for
// each to complete before subscribing to the next. An infinite inner Observable
// permanently blocks all subsequent inner Observables. Always ensure inner
// Observables complete (use take, takeUntil, or a finite source).
```

### Anti-pattern: Choosing `*All` When `*Map` Is More Readable
```typescript
import { of } from 'rxjs';
import { map, mergeAll, mergeMap } from 'rxjs/operators';

// ❌ UNNECESSARILY VERBOSE — split map + mergeAll when mergeMap is clearer
of(1, 2, 3).pipe(
  map(n => fetchData(n)),
  mergeAll()
).subscribe(console.log);

// ✅ IDIOMATIC — mergeMap is the standard one-step form
of(1, 2, 3).pipe(
  mergeMap(n => fetchData(n))
).subscribe(console.log);

// WHY: mergeMap(fn) = map(fn) + mergeAll() in one operator.
// Prefer *Map for inline projections. Use *All only when the source
// already emits Observables (e.g., a service returns Observable<Observable<T>>),
// or when the projection needs to be in a separate map() step for readability.
```

## Operator Selection Guide

```
Source emits Observables (Observable<Observable<T>>)?
  → Need cancellation of previous inner on new outer?  → switchAll()
  → Need strict sequential execution (order matters)?  → concatAll()
  → Need max concurrency or full parallelism?          → mergeAll(n)

Source emits plain values and you project them to Observables?
  → switchMap / concatMap / mergeMap  (preferred over map + *All)
```

## Related Operators

- **`mergeMap`**: `map(fn) + mergeAll()` combined — the idiomatic form for inline projections
- **`concatMap`**: `map(fn) + concatAll()` combined
- **`switchMap`**: `map(fn) + switchAll()` combined
- **`exhaustMap`**: Like `mergeAll` but ignores new inner Observables while one is active (4th flattening strategy)
- **`combineLatestAll`**: Combines all inner Observables with `combineLatest` semantics

## References
- **RxJS mergeAll**: [https://rxjs.dev/api/operators/mergeAll](https://rxjs.dev/api/operators/mergeAll)
- **RxJS concatAll**: [https://rxjs.dev/api/operators/concatAll](https://rxjs.dev/api/operators/concatAll)
- **RxJS switchAll**: [https://rxjs.dev/api/operators/switchAll](https://rxjs.dev/api/operators/switchAll)

---

**`mergeAll`** — Cognitive Load: 2/5 | Usage: 3/5 | Concurrent flattening; use `mergeMap` for inline projections.
**`concatAll`** — Cognitive Load: 2/5 | Usage: 3/5 | Sequential flattening; blocks on infinite inner Observables.
**`switchAll`** — Cognitive Load: 2/5 | Usage: 3/5 | Latest-only flattening; the `switchMap` equivalent for pre-mapped Observables.
**Teaching sequence**: After `mergeMap`, `concatMap`, `switchMap` — these are the same strategies applied to an already-higher-order stream.
