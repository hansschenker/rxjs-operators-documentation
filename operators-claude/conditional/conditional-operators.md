# Conditional Operators

## `defaultIfEmpty`

### Identity
- **Import**: `import { defaultIfEmpty } from 'rxjs/operators'`
- **Signature**: `defaultIfEmpty<T, R>(defaultValue: R): OperatorFunction<T, T | R>`
- **Category**: Conditional — emits a default value if the source completes without emitting

### Functional Specification

If the source Observable completes without emitting any `next` value, `defaultIfEmpty` emits `defaultValue` and then completes. If the source emits at least one value, it passes all values through unchanged.

```
Source emits nothing: --|        →  default-value|
Source emits values:  --a--b--|  →  --a--b--|      (unchanged)
```

### Examples

```typescript
import { EMPTY, of } from 'rxjs';
import { defaultIfEmpty, filter } from 'rxjs/operators';

// Common use: guard against empty search results
of(1, 2, 3, 4).pipe(
  filter(v => v > 10),         // nothing passes
  defaultIfEmpty('no results')
).subscribe(console.log); // 'no results'

// With typed fallback
of<User>().pipe(                // empty Observable
  defaultIfEmpty({ id: 0, name: 'Guest' } as User)
).subscribe(user => console.log(user.name)); // 'Guest'

// Real-world: API returns empty array → use placeholder
fetchItems().pipe(
  switchMap(items => from(items)),    // flatten array to stream
  defaultIfEmpty(PLACEHOLDER_ITEM)   // shown if no items
).subscribe(renderItem);
```

### Type System
```typescript
// Return type is T | R — TypeScript preserves both possibilities
of(1, 2).pipe(defaultIfEmpty(0)).subscribe((v: number) => {}); // number | number = number
of<string>().pipe(defaultIfEmpty(null)).subscribe((v: string | null) => {}); // string | null
```

### Pitfall
```typescript
// ❌ WRONG — using defaultIfEmpty as a catch-all for undefined
source$.pipe(
  map(v => v?.nested),           // may be undefined
  defaultIfEmpty('fallback')     // does NOT trigger on undefined!
).subscribe(console.log);
// defaultIfEmpty only triggers when the stream emits NOTHING, not when
// individual emissions are undefined/null

// ✅ CORRECT — use map with nullish coalescing for per-value defaults
source$.pipe(
  map(v => v?.nested ?? 'fallback')
).subscribe(console.log);

// WHY: defaultIfEmpty reacts to a MISSING completion (empty stream),
// not to individual missing values within the stream.
```

---

## `isEmpty`

### Identity
- **Import**: `import { isEmpty } from 'rxjs/operators'`
- **Signature**: `isEmpty<T>(): OperatorFunction<T, boolean>`
- **Category**: Conditional — emits `true` if source completes without emitting; `false` on first emission

### Functional Specification

Emits `true` and completes if the source completes without emitting any `next` value. Emits `false` and completes as soon as the source emits its first value (source is then unsubscribed).

```
Source:  --|           isEmpty: true|
Source:  --a--b--|     isEmpty: false|  (a triggers immediate false; b never seen)
Source:  NEVER         isEmpty: (never emits — source never completes or emits)
```

### Examples

```typescript
import { EMPTY, of, Subject } from 'rxjs';
import { isEmpty } from 'rxjs/operators';

EMPTY.pipe(isEmpty()).subscribe(console.log); // true
of(1, 2, 3).pipe(isEmpty()).subscribe(console.log); // false

// Real-world: conditional rendering based on stream content
searchResults$.pipe(
  toArray(),
  mergeMap(results => of(results).pipe(isEmpty()))
).subscribe(empty => {
  if (empty) showEmptyState();
  else hideEmptyState();
});

// Simpler version:
searchResults$.pipe(
  isEmpty()
).subscribe(empty => toggleEmptyState(empty));
```

### Pitfall
```typescript
import { Subject } from 'rxjs';
import { isEmpty } from 'rxjs/operators';

// ❌ SURPRISE — isEmpty requires source completion to emit true
const subject = new Subject<number>();
subject.pipe(isEmpty()).subscribe(console.log); // nothing logged yet

// Only after complete() will it emit true (if nothing was next'd):
subject.complete(); // logs: true

// If it never completes → isEmpty never emits
// WHY: isEmpty needs to observe the full stream to decide "empty".
// For a live Subject, consider using take(1) with a timeout instead.
```

---

## `every`

### Identity
- **Import**: `import { every } from 'rxjs/operators'`
- **Signature**: `every<T>(predicate: (value: T, index: number, source: Observable<T>) => boolean): OperatorFunction<T, boolean>`
- **Category**: Conditional — emits `true` if ALL emissions satisfy the predicate; `false` on first failure

### Functional Specification

Emits `false` (and completes) as soon as one source value fails the predicate — the source is unsubscribed immediately. Emits `true` only after the source completes with all values passing.

```
Source: --1--2--3--|     every(v => v > 0): true|
Source: --1--2---3--|    every(v => v < 2): false| (2 fails → early exit)
Source: EMPTY:--|        every(p):          true|   (vacuously true)
```

### Examples

```typescript
import { of } from 'rxjs';
import { every } from 'rxjs/operators';

of(2, 4, 6, 8).pipe(every(v => v % 2 === 0)).subscribe(console.log); // true
of(2, 4, 5, 8).pipe(every(v => v % 2 === 0)).subscribe(console.log); // false (5 fails)

// Validate all items before submitting a batch
submittedItems$.pipe(
  toArray(),
  mergeMap(items => from(items).pipe(every(isValid)))
).subscribe(allValid => {
  if (allValid) submitBatch();
  else showValidationError();
});
```

### Comparison Table

| Operator | Emits | When |
|---|---|---|
| `every(p)` | `boolean` | All pass → true on complete; first fail → false |
| `some(p)` | N/A | No native `some` — use `find(p).pipe(map(v => v !== undefined))` |
| `find(p)` | `T \| undefined` | First matching value; undefined if none |
| `filter(p)` | `T` (multiple) | Every matching value |
| `defaultIfEmpty(v)` | `T \| R` | v only if source was empty |

---

## `sequenceEqual`

### Identity
- **Import**: `import { sequenceEqual } from 'rxjs/operators'`
- **Signature**: `sequenceEqual<T>(compareTo: Observable<T>, comparator?: (a: T, b: T) => boolean): OperatorFunction<T, boolean>`
- **Category**: Conditional — emits `true` if source and `compareTo` emit identical sequences

### Functional Specification

Buffers emissions from both the source and `compareTo`. When both complete, emits `true` if they emitted the same values in the same order; `false` otherwise. Also emits `false` early if a mismatch is detected before either completes.

```typescript
import { of } from 'rxjs';
import { sequenceEqual } from 'rxjs/operators';

of(1, 2, 3).pipe(sequenceEqual(of(1, 2, 3))).subscribe(console.log); // true
of(1, 2, 3).pipe(sequenceEqual(of(1, 2, 4))).subscribe(console.log); // false
of(1, 2).pipe(sequenceEqual(of(1, 2, 3))).subscribe(console.log);    // false (different lengths)

// With custom comparator for objects
of({ id: 1 }, { id: 2 }).pipe(
  sequenceEqual(of({ id: 1 }, { id: 2 }), (a, b) => a.id === b.id)
).subscribe(console.log); // true
```

---

## Related Operators

- **`find` / `findIndex`**: First value matching a predicate (see Filtering)
- **`filter`**: All values matching a predicate (see Filtering)
- **`toArray`**: Collect all values — useful before `every` or `isEmpty` checks on finite streams
- **`defaultIfEmpty`** + **`switchMap`**: Common pattern for "empty stream → fallback Observable"

## References
- [defaultIfEmpty](https://rxjs.dev/api/operators/defaultIfEmpty)
- [isEmpty](https://rxjs.dev/api/operators/isEmpty)
- [every](https://rxjs.dev/api/operators/every)
- [sequenceEqual](https://rxjs.dev/api/operators/sequenceEqual)

---

**`defaultIfEmpty`** — Cognitive Load: 1/5 | Usage: 4/5 | Most used of the group — guard against empty streams.
**`isEmpty`** — Cognitive Load: 1/5 | Usage: 3/5 | Requires completion to return true.
**`every`** — Cognitive Load: 2/5 | Usage: 2/5 | Early-exits on first failure; vacuously true on empty source.
**`sequenceEqual`** — Cognitive Load: 2/5 | Usage: 1/5 | Buffers both streams — avoid on long or infinite sources.
