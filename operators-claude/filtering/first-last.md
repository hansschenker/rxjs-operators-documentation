# first / last

## Identity

| | `first` | `last` |
|---|---|---|
| **Import** | `import { first } from 'rxjs/operators'` | `import { last } from 'rxjs/operators'` |
| **Signature** | `first<T, D>(predicate?, defaultValue?): OperatorFunction<T, T\|D>` | `last<T, D>(predicate?, defaultValue?): OperatorFunction<T, T\|D>` |
| **Category** | Filtering Operators | Filtering Operators |
| **Type** | Emits first matching value, then completes | Emits last matching value on source completion |

```typescript
function first<T, D = T>(
  predicate?: ((value: T, index: number, source: Observable<T>) => boolean) | null,
  defaultValue?: D
): OperatorFunction<T, T | D>

function last<T, D = T>(
  predicate?: ((value: T, index: number, source: Observable<T>) => boolean) | null,
  defaultValue?: D
): OperatorFunction<T, T | D>
```

## Functional Specification

**`first(predicate?, defaultValue?)`**:
- Without predicate: equivalent to `take(1)` but errors on empty source
- With predicate: emits first value satisfying the predicate, then completes; errors if none found
- With `defaultValue`: emits `defaultValue` instead of erroring on empty/no-match

**`last(predicate?, defaultValue?)`**:
- Without predicate: equivalent to `takeLast(1)` — emits final value on source completion
- Requires source to complete (like `reduce`) — hangs on infinite sources
- With predicate: emits last value satisfying the predicate on completion
- With `defaultValue`: emits `defaultValue` instead of erroring when no match found

**Error semantics** (`EmptyError`):
```
Source emits nothing  + no defaultValue → EmptyError
No value matches predicate + no defaultValue → EmptyError
```

## Marble Diagram

```
Source:   --a--b--c--d--|

first():  --a|                    (first value, immediate completion)
last():   ---------------d|       (waits for source to complete, emits last)

first(v => v > 'b'):  -----c|    (first value passing predicate)
last(v => v < 'c'):   ---------------b|  (last value passing predicate, emitted at completion)

first() on EMPTY:  #  (EmptyError)
take(1) on EMPTY:  |  (silent completion — key difference from first())
```

## Type System Integration

```typescript
import { of, EMPTY } from 'rxjs';
import { first, last } from 'rxjs/operators';

// Without predicate — T preserved
of(1, 2, 3).pipe(first()).subscribe((v: number) => console.log(v)); // 1
of(1, 2, 3).pipe(last()).subscribe((v: number) => console.log(v));  // 3

// With defaultValue — output is T | D
of<number>().pipe(first(null, -1)).subscribe((v: number) => console.log(v)); // -1
// Output type: Observable<number>  (D = number = T here; no union widening)

// defaultValue of different type — union
EMPTY.pipe(first(null, 'none' as const))
// Output: Observable<never | 'none'> → Observable<'none'>

// With predicate: same type
of(1, 2, 3, 4).pipe(
  first(n => n > 2)
).subscribe((v: number) => console.log(v)); // 3
```

## Examples

### Basic Usage
```typescript
import { of, from, EMPTY } from 'rxjs';
import { first, last } from 'rxjs/operators';

// first() — immediate, then complete
of(10, 20, 30).pipe(first()).subscribe(console.log); // 10

// last() — waits for completion
of(10, 20, 30).pipe(last()).subscribe(console.log);  // 30

// With predicate
of(1, 2, 3, 4, 5).pipe(first(n => n % 2 === 0)).subscribe(console.log); // 2
of(1, 2, 3, 4, 5).pipe(last(n => n % 2 === 0)).subscribe(console.log);  // 4

// defaultValue prevents EmptyError
EMPTY.pipe(first(null, 'fallback')).subscribe(console.log); // fallback
EMPTY.pipe(last(null, 'fallback')).subscribe(console.log);  // fallback
```

### Common Pattern — `first()` for One-Shot Subscriptions
```typescript
import { BehaviorSubject, forkJoin } from 'rxjs';
import { first } from 'rxjs/operators';

const user$ = new BehaviorSubject<User | null>(null);

// Snapshot current value — first() is more expressive than take(1)
// when "an empty source is a bug"
user$.pipe(first(u => u !== null)).subscribe(user => {
  console.log('first authenticated user:', user!.name);
});

// forkJoin with live stores — first() makes them finite
forkJoin({
  user:  user$.pipe(first()),
  theme: themeStore$.pipe(first()),
}).subscribe(({ user, theme }) => bootstrap(user, theme));
```

### Common Pattern — `last()` for Batch Results
```typescript
import { from } from 'rxjs';
import { scan, last } from 'rxjs/operators';

// Process a stream of events, get the final accumulated state
from(eventLog).pipe(
  scan(reducer, initialState),
  last()  // same as reduce(reducer, initialState) — get final state only
).subscribe(finalState => saveCheckpoint(finalState));
```

### Edge Cases — `first()` vs `take(1)` on Empty Sources
```typescript
import { EMPTY } from 'rxjs';
import { first, take } from 'rxjs/operators';

// take(1) — completes silently on empty source
EMPTY.pipe(take(1)).subscribe({
  complete: () => console.log('take(1): done with no value')
});
// Output: take(1): done with no value

// first() — errors on empty source (EmptyError)
EMPTY.pipe(first()).subscribe({
  error: e => console.log('first(): error:', e.name) // EmptyError
});
// Output: first(): error: EmptyError

// When to use which:
// take(1): "I'll take the first value if one arrives — empty is fine"
// first():  "I expect at least one value — empty is a bug"
```

## Common Pitfalls

### Anti-pattern: `last()` on Never-Completing Sources
```typescript
import { interval } from 'rxjs';
import { last } from 'rxjs/operators';

// ❌ BROKEN — interval never completes; last() never emits
interval(100).pipe(last()).subscribe(console.log);
// No output ever.

// ✅ CORRECT — make the source finite first
interval(100).pipe(
  take(5),
  last()
).subscribe(console.log); // 4  (last of 0,1,2,3,4)

// WHY: last() requires source completion to know which value was "last."
// Use only with finite, completing sources.
```

### Anti-pattern: Ignoring `EmptyError` From `first()`
```typescript
import { Subject } from 'rxjs';
import { first } from 'rxjs/operators';

const events$ = new Subject<string>();

// ❌ MISSING ERROR HANDLER — EmptyError crashes silently in some environments
events$.pipe(first()).subscribe(v => console.log(v));
events$.complete(); // Subject completes without emitting → EmptyError thrown!

// ✅ CORRECT — handle error or use defaultValue
events$.pipe(first(null, null)).subscribe(v => {
  if (v !== null) console.log(v);
  else console.log('no events before completion');
});

// WHY: first() throws EmptyError if the source completes or (with predicate)
// if no value matches. Always handle the error case or provide a defaultValue
// when the empty case is possible.
```

## Related Operators

- **`take(1)`**: Like `first()` without predicate, but completes silently on empty sources
- **`takeLast(n)`**: Symmetric to `last()` — emits last N values on completion
- **`find(predicate)`**: Like `first(predicate)` but emits `undefined` instead of erroring when not found
- **`elementAt(index)`**: Emits value at a specific index position
- **`filter`**: General predicate filtering — use when you want all matching values, not just first/last

## References
- **RxJS first**: [https://rxjs.dev/api/operators/first](https://rxjs.dev/api/operators/first)
- **RxJS last**: [https://rxjs.dev/api/operators/last](https://rxjs.dev/api/operators/last)

---

## Additional Notes for rxjs-strategies Integration

**`first`** — Cognitive Load: 2/5 | Usage: 4/5 | The key teaching point is EmptyError vs take(1) silent completion.
**`last`** — Cognitive Load: 2/5 | Usage: 3/5 | The key teaching point is completion-required (like reduce).
**Common with**: `BehaviorSubject`, `forkJoin`, `take`, `EMPTY`, `Subject`
