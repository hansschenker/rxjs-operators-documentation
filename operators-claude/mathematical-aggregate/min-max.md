# min / max

## Identity

| | `min` | `max` |
|---|---|---|
| **Import** | `import { min } from 'rxjs/operators'` | `import { max } from 'rxjs/operators'` |
| **Signature** | `min<T>(comparer?: (x: T, y: T) => number): OperatorFunction<T, T>` | `max<T>(comparer?: (x: T, y: T) => number): OperatorFunction<T, T>` |
| **Category** | Mathematical / Aggregate | Mathematical / Aggregate |
| **Emits** | Single smallest value on source completion | Single largest value on source completion |

```typescript
function min<T>(comparer?: (x: T, y: T) => number): OperatorFunction<T, T>
function max<T>(comparer?: (x: T, y: T) => number): OperatorFunction<T, T>
```

## Functional Specification

Both `min` and `max` are **aggregate operators** — they buffer all source emissions and emit a single result when the source completes. They are equivalent to `reduce` with a comparison accumulator.

**Without comparer**: Uses JavaScript's `<` / `>` operators — works correctly for numbers and strings.

**With comparer**: A function `(x, y) => number` where negative means x < y, 0 means equal, positive means x > y — same convention as `Array.prototype.sort`.

**Invariants**:
- Requires source completion — does not emit on infinite sources
- Emits exactly one value
- On empty source: emits `undefined` (no TypeScript error, but value is undefined at runtime)

**Equivalent to**:
```typescript
// min is equivalent to:
source$.pipe(reduce((a, b) => a < b ? a : b))

// max is equivalent to:
source$.pipe(reduce((a, b) => a > b ? a : b))
```

## Marble Diagram

```
Source: --3--1--4--1--5--9--2--6--|

min(): ----------------------------1|   (emits on completion)
max(): ----------------------------9|   (emits on completion)

Source: --|   (empty)
min(): ----|  (emits undefined — handle carefully)
```

## Examples

### Basic Usage — Numbers and Strings
```typescript
import { of } from 'rxjs';
import { min, max } from 'rxjs/operators';

of(3, 1, 4, 1, 5, 9, 2, 6).pipe(min()).subscribe(console.log); // 1
of(3, 1, 4, 1, 5, 9, 2, 6).pipe(max()).subscribe(console.log); // 9

of('banana', 'apple', 'cherry').pipe(min()).subscribe(console.log); // 'apple'
of('banana', 'apple', 'cherry').pipe(max()).subscribe(console.log); // 'cherry'
```

### Common Pattern — Objects with Custom Comparer
```typescript
import { from } from 'rxjs';
import { min, max } from 'rxjs/operators';

interface Product { name: string; price: number }

const products: Product[] = [
  { name: 'Widget', price: 9.99 },
  { name: 'Gadget', price: 24.99 },
  { name: 'Doohickey', price: 4.99 }
];

from(products).pipe(
  min((a, b) => a.price - b.price)
).subscribe(p => console.log('cheapest:', p.name));  // 'Doohickey'

from(products).pipe(
  max((a, b) => a.price - b.price)
).subscribe(p => console.log('priciest:', p.name));  // 'Gadget'
```

### Common Pattern — From HTTP Response
```typescript
import { ajax } from 'rxjs/ajax';
import { mergeMap, min, max, from } from 'rxjs/operators';

ajax.getJSON<Score[]>('/api/scores').pipe(
  mergeMap(scores => from(scores)),
  max((a, b) => a.value - b.value)
).subscribe(topScore => showHighScore(topScore));
```

### Common Pattern — When `reduce` Is More Readable
```typescript
import { of } from 'rxjs';
import { min, reduce } from 'rxjs/operators';

// These are equivalent — prefer whichever reads more clearly:
of(3, 1, 4, 1, 5).pipe(min()).subscribe(console.log);

of(3, 1, 4, 1, 5).pipe(
  reduce((a, b) => a < b ? a : b)
).subscribe(console.log);

// Use reduce when you need additional context (e.g., index of min):
of(3, 1, 4, 1, 5).pipe(
  reduce(
    (acc, v, i) => v < acc.min ? { min: v, index: i } : acc,
    { min: Infinity, index: -1 }
  )
).subscribe(({ min, index }) => console.log(`min: ${min} at index ${index}`));
```

## Common Pitfalls

### Anti-pattern: Using on Infinite Sources
```typescript
import { interval } from 'rxjs';
import { min } from 'rxjs/operators';

// ❌ HANGS — interval never completes; min never emits
interval(100).pipe(min()).subscribe(console.log); // nothing ever

// ✅ CORRECT — make the source finite first
import { take } from 'rxjs/operators';
interval(100).pipe(take(10), min()).subscribe(console.log); // 0

// WHY: min and max buffer all values and emit only on completion.
// On infinite sources, they buffer forever and never emit.
```

### Anti-pattern: Ignoring Empty Source
```typescript
import { EMPTY } from 'rxjs';
import { min } from 'rxjs/operators';

// ❌ SURPRISE — emits undefined on empty source (no TypeScript error)
EMPTY.pipe(min()).subscribe(v => console.log(v)); // undefined

// ✅ CORRECT — guard with defaultIfEmpty
import { defaultIfEmpty } from 'rxjs/operators';
EMPTY.pipe(
  min(),
  defaultIfEmpty(0)
).subscribe(v => console.log(v)); // 0

// WHY: min/max have no "nothing to compare" guard. On an empty source
// they emit undefined, which TypeScript won't catch at compile time.
```

## Related Operators

- **`reduce((a, b) => a < b ? a : b)`**: Manual min — use when you need the index or additional context
- **`scan`**: Running min/max without waiting for completion: `scan((a, b) => a < b ? a : b)`
- **`toArray()`**: Collect all values then use `Math.min(...values)` — familiar but less composable
- **`count()`**: Other aggregate — count emissions

## References
- [min](https://rxjs.dev/api/operators/min)
- [max](https://rxjs.dev/api/operators/max)

---

**`min`** — Cognitive Load: 1/5 | Usage: 2/5 | Requires completion; provide a comparer for non-primitive types.
**`max`** — Cognitive Load: 1/5 | Usage: 2/5 | Same semantics as min, inverted direction.
**Key teaching point**: Both are `reduce` shortcuts. Use `reduce` directly when you need more than just the extreme value (e.g., the index, or additional aggregated context).
