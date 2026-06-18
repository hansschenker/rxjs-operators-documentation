# toArray / count

## Identity

| | `toArray` | `count` |
|---|---|---|
| **Import** | `import { toArray } from 'rxjs/operators'` | `import { count } from 'rxjs/operators'` |
| **Signature** | `toArray<T>(): OperatorFunction<T, T[]>` | `count<T>(predicate?): OperatorFunction<T, number>` |
| **Category** | Mathematical / Aggregate | Mathematical / Aggregate |
| **Output** | Single `T[]` on source completion | Single `number` on source completion |

```typescript
function toArray<T>(): OperatorFunction<T, T[]>

function count<T>(
  predicate?: (value: T, index: number) => boolean
): OperatorFunction<T, number>
```

## Functional Specification

Both operators are **completion-required** — like `reduce` and `last`, they buffer until source completes, then emit a single value.

**`toArray()`**: Collects all emitted values into an array. Emits `[]` if source completes without emitting.

**`count(predicate?)`**:
- Without predicate: counts all values
- With predicate: counts only values for which `predicate(value, index)` returns `true`
- Emits `0` if source completes without emitting (or no values match predicate)

**Equivalences**:
```
toArray() ≡ reduce((acc, v) => [...acc, v], [])
count()   ≡ reduce((n) => n + 1, 0)
count(p)  ≡ filter(p).pipe(count())
```

## Marble Diagrams

```
Source:  --1--2--3--4--|

toArray():   -----------[1,2,3,4]|
count():     -----------4|
count(n => n % 2 === 0):  -----------2|  (2 and 4 are even)

Source:  --|   (empty)
toArray():   []|
count():     0|
```

## Type System Integration

```typescript
import { of, EMPTY } from 'rxjs';
import { toArray, count } from 'rxjs/operators';

// toArray — T[] inferred
of(1, 2, 3).pipe(toArray()).subscribe((arr: number[]) => console.log(arr)); // [1,2,3]
EMPTY.pipe(toArray()).subscribe((arr: number[]) => console.log(arr));        // []

// count — always Observable<number>
of('a', 'b', 'c').pipe(count()).subscribe((n: number) => console.log(n));   // 3
of(1, 2, 3, 4, 5).pipe(
  count(n => n > 3)
).subscribe((n: number) => console.log(n)); // 2  (4 and 5)
```

## Examples

### Basic Usage
```typescript
import { of, interval } from 'rxjs';
import { toArray, count, take } from 'rxjs/operators';

// toArray — batch collect
of('apple', 'banana', 'cherry').pipe(toArray()).subscribe(console.log);
// ['apple', 'banana', 'cherry']

// count — total items
of(1, 2, 3, 4, 5).pipe(count()).subscribe(console.log); // 5

// count with predicate
of(1, 2, 3, 4, 5).pipe(count(n => n % 2 === 0)).subscribe(console.log); // 2

// Finite interval — collect all ticks
interval(100).pipe(take(5), toArray()).subscribe(console.log);
// [0, 1, 2, 3, 4]
```

### Common Pattern — Batch Processing
```typescript
import { from } from 'rxjs';
import { toArray, mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Fetch all items in parallel, collect results when all done
const ids = [1, 2, 3, 4, 5];

from(ids).pipe(
  mergeMap(id => ajax.getJSON<User>(`/api/users/${id}`)),
  toArray()          // wait for all fetches, collect into single array
).subscribe((users: User[]) => renderTable(users));
```

### Common Pattern — Validate a Complete Stream
```typescript
import { from } from 'rxjs';
import { toArray, count } from 'rxjs/operators';

// Verify all records were processed
from(csvRows).pipe(
  toArray()
).subscribe(rows => {
  console.log(`Processed ${rows.length} rows`);
  if (rows.length !== expectedCount) throw new Error('row count mismatch');
});

// Just need the count? count() is lighter than toArray().length
from(csvRows).pipe(
  count(row => row.isValid)
).subscribe(validCount => console.log(`${validCount} valid rows`));
```

### Edge Case — `toArray()` vs `reduce` vs `scan`
```typescript
import { of } from 'rxjs';
import { toArray, reduce, scan } from 'rxjs/operators';

// toArray — get all values as array (no accumulator logic)
of(1, 2, 3).pipe(toArray()).subscribe(console.log);    // [1, 2, 3]

// reduce — custom aggregation, emits on complete
of(1, 2, 3).pipe(reduce((a, v) => a + v, 0)).subscribe(console.log); // 6

// scan — running aggregation, emits on each value
of(1, 2, 3).pipe(scan((a, v) => [...a, v], [] as number[])).subscribe(console.log);
// [1]  →  [1,2]  →  [1,2,3]   (three emissions, not one)
```

## Common Pitfalls

### Anti-pattern: Using `toArray()` on Infinite Sources
```typescript
import { interval } from 'rxjs';
import { toArray } from 'rxjs/operators';

// ❌ BROKEN — interval never completes; toArray never emits, leaks memory
interval(100).pipe(toArray()).subscribe(console.log); // nothing, ever

// ✅ CORRECT — make the source finite before toArray
import { take } from 'rxjs/operators';
interval(100).pipe(take(10), toArray()).subscribe(console.log); // [0,1,...,9]

// WHY: toArray (like reduce, last, count) requires source completion to know
// when the collection is "done." Never use these operators on infinite sources.
```

### Anti-pattern: `toArray().pipe(map(arr => arr.length))` Instead of `count()`
```typescript
import { of } from 'rxjs';
import { toArray, count } from 'rxjs/operators';
import { map } from 'rxjs/operators';

// ❌ WASTEFUL — allocates an array just to get its length
of(1, 2, 3, 4, 5).pipe(
  toArray(),
  map(arr => arr.length)
).subscribe(console.log); // 5

// ✅ CORRECT — count() never allocates an array
of(1, 2, 3, 4, 5).pipe(count()).subscribe(console.log); // 5

// WHY: toArray holds every value in memory. count() only tracks a running
// integer. For streams with thousands of items, this is a meaningful difference.
```

## Related Operators

- **`reduce(fn, seed)`**: Custom terminal aggregation — use when you need more than a count or array
- **`scan(fn, seed)`**: Running aggregation — emits on each value (does not require completion)
- **`last()`**: Emits only the final value (not all values)
- **`bufferTime / bufferCount`**: Collect values into arrays on a time/count cadence (not requiring completion)
- **`first()`**: Symmetric to `last()` — emits the first value

## References
- **RxJS toArray**: [https://rxjs.dev/api/operators/toArray](https://rxjs.dev/api/operators/toArray)
- **RxJS count**: [https://rxjs.dev/api/operators/count](https://rxjs.dev/api/operators/count)

---

**`toArray`** — Cognitive Load: 1/5 | Usage: 4/5 | The infinite-source footgun is the key anti-pattern.
**`count`** — Cognitive Load: 1/5 | Usage: 3/5 | Prefer over `toArray().pipe(map(a => a.length))` for memory efficiency.
