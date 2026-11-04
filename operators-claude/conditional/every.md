# every

## Identity
- **Name**: every
- **Category**: Conditional Operators
- **Type**: Universal-quantifier reduction — emits one boolean stating whether all source values satisfy a predicate
- **Import**:
  ```typescript
  import { every } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function every<T>(
    predicate: (value: T, index: number, source: Observable<T>) => boolean,
    thisArg?: any   // deprecated — bind the predicate instead
  ): OperatorFunction<T, boolean>
  ```

## Functional Specification

**Input**: `Observable<T>` — a finite source whose values are to be tested

**Output**: `Observable<boolean>` — exactly one boolean, then completion

**Transformation**:
Applies `predicate` to each value as it arrives. The first `false` result emits `false` downstream, completes the output, and unsubscribes the source — no further values are inspected. If the source completes without any `false`, the output emits `true` and completes. An empty source emits `true` (vacuous truth), exactly like `Array.prototype.every`.

**Mathematical representation**:
```
every(p)(S) = { ∀ v ∈ S : p(v) }  ++ complete()

Evaluation:
  ∃ vᵢ : ¬p(vᵢ)   → emit false at vᵢ, unsubscribe S       (short-circuit)
  otherwise       → emit true when S completes
  |S| = 0         → emit true                             (vacuously)
```

**Invariants**:
- **Exactly one emission** in every non-error outcome
- **Short-circuit on failure**: `false` is emitted at the failing value, never later
- **`true` requires completion**: it cannot be known until the source ends
- **Predicate index is the source index**: `index` counts every source value, starting at 0

## Marble Diagram

```
Source:   --2--4--6--8--|
          every(v => v % 2 === 0)
Result:   --------------(true|)

All values passed; true is emitted at completion.
```

**One value fails — short-circuit**:
```
Source:   --2--4--5--8--|
          every(v => v % 2 === 0)
Result:   --------(false|)

5 fails; false is emitted immediately; 8 is never requested.
```

**Empty source**:
```
Source:   --|
          every(v => v > 100)
Result:   --(true|)

No counterexample exists — vacuously true.
```

**Key observation**: `every` is asymmetric in time. A negative answer arrives as early as possible; a positive answer arrives as late as possible. That asymmetry is what makes it safe on long finite streams (it stops at the first violation) and useless on infinite ones (it can never confirm).

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately
- Unsubscribes early on the first predicate failure

**Completion semantics**:
- Completes immediately after emitting its single boolean
- On an infinite source where every value passes, the output never emits

**Error handling**:
- Source errors propagate immediately; no boolean is emitted
- A throwing predicate is caught and forwarded as an error notification

**Backpressure**:
- O(1) state; no buffering
- Consumes values as fast as the source produces them

**Hot vs. Cold**:
- Cold sources: a fresh evaluation per subscription
- Hot sources: evaluates only values after subscription; a `Subject` that never completes can only ever yield `false`

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source value type
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<boolean>
 *
 * Type Narrowing:
 *   None. A type-guard predicate does not narrow the output — the output
 *   is always boolean. Use filter(guard) when a narrowed stream is needed.
 *
 * Type Safety:
 *   The predicate must return boolean; truthy/falsy non-boolean returns
 *   are a compile error under strict settings.
 */

import { from } from 'rxjs';
import { every } from 'rxjs/operators';

interface LineItem { sku: string; qty: number }

const cart: LineItem[] = [
  { sku: 'A', qty: 2 },
  { sku: 'B', qty: 1 },
];

// Output type is Observable<boolean>, regardless of T
const allInStock$ = from(cart).pipe(
  every((item: LineItem, index: number) => item.qty > 0 && index < 100)
);

allInStock$.subscribe((ok: boolean) => console.log('all positive:', ok));
// Output: all positive: true
```

## Examples

### Basic Usage — Check That All Values Pass
```typescript
import { of } from 'rxjs';
import { every } from 'rxjs/operators';

of(1, 2, 3, 4, 5).pipe(
  every(n => n > 0)
).subscribe(result => console.log('All positive?', result));
// Output: All positive? true

of(2, 4, 5, 6).pipe(
  every(n => n % 2 === 0)
).subscribe(result => console.log('All even?', result));
// Output: All even? false   (short-circuits at 5)
```

### Common Pattern — Validate a Batch Before Submitting
```typescript
import { from } from 'rxjs';
import { every } from 'rxjs/operators';

interface Response { status: number; body: string }

const responses: Response[] = [
  { status: 200, body: 'OK' },
  { status: 201, body: 'Created' },
  { status: 200, body: 'OK' },
];

from(responses).pipe(
  every(r => r.status >= 200 && r.status < 300)
).subscribe(allOk => {
  console.log(allOk ? 'All requests succeeded' : 'Some requests failed');
});
// Output: All requests succeeded
```

### Common Pattern — Form-Wide Validity From Per-Field Streams
```typescript
import { from, of } from 'rxjs';
import { every, mergeMap, toArray } from 'rxjs/operators';

interface Field { name: string; valid: boolean }

const fields$ = of<Field[]>([
  { name: 'email', valid: true },
  { name: 'age',   valid: false },
]);

fields$.pipe(
  mergeMap(fields => from(fields).pipe(every(f => f.valid)))
).subscribe(formValid => console.log('submit enabled:', formValid));
// Output: submit enabled: false
```

### Edge Cases — Empty Source, Index Argument, Throwing Predicate
```typescript
import { EMPTY, of } from 'rxjs';
import { every } from 'rxjs/operators';

// Edge case 1: empty source → true (vacuous truth)
EMPTY.pipe(every(() => false)).subscribe(console.log);
// Output: true

// Edge case 2: index is available and counts every source value
of('a', 'b', 'c').pipe(
  every((v, i) => v.charCodeAt(0) === 97 + i)
).subscribe(console.log);
// Output: true   (a→0, b→1, c→2 in order)

// Edge case 3: a throwing predicate becomes an error, not a false
of(1, 2).pipe(
  every(n => { if (n === 2) throw new Error('bad'); return true; })
).subscribe({
  next:  v   => console.log('next', v),
  error: err => console.log('error:', err.message),
});
// Output: error: bad
```

## Common Pitfalls

### Anti-pattern: Forgetting That an Empty Source Is `true`
```typescript
import { from, of } from 'rxjs';
import { every } from 'rxjs/operators';

interface Permission { granted: boolean }

// ❌ INCORRECT — no permissions at all is treated as "all granted"
function canProceed(perms: Permission[]) {
  return from(perms).pipe(every(p => p.granted));
}
canProceed([]).subscribe(console.log);
// Output: true   (probably not the intent)

// ✅ CORRECT — guard emptiness explicitly when it has meaning
function canProceedSafely(perms: Permission[]) {
  return perms.length === 0
    ? of(false)
    : from(perms).pipe(every(p => p.granted));
}
canProceedSafely([]).subscribe(console.log);
// Output: false

// WHY: every() follows mathematical convention — there is no counterexample
// in an empty set. When "nothing to check" should fail, test for emptiness
// before or alongside every().
```

### Anti-pattern: Using `every` on an Infinite Stream
```typescript
import { interval } from 'rxjs';
import { every, take } from 'rxjs/operators';

// ❌ INCORRECT — interval never completes; true can never be emitted
interval(500).pipe(
  every(n => n >= 0)
).subscribe(console.log);
// (no output, ever)

// ✅ CORRECT — bound the source so completion is possible
interval(500).pipe(
  take(5),
  every(n => n >= 0)
).subscribe(console.log);
// Output: true   (after 5 ticks)

// WHY: A positive answer needs proof that no future value fails, and that
// proof only exists at completion. Only a negative answer can arrive early.
```

### Anti-pattern: Using `every` to Filter
```typescript
import { of } from 'rxjs';
import { every, filter } from 'rxjs/operators';

// ❌ INCORRECT — expecting the passing values back
of(1, 2, 3).pipe(
  every(n => n > 1)
).subscribe(console.log);
// Output: false   (a single boolean — the values are gone)

// ✅ CORRECT — filter keeps the values; every only judges them
of(1, 2, 3).pipe(
  filter(n => n > 1)
).subscribe(console.log);
// Output: 2, 3

// WHY: every() is a reduction to one boolean. If you need the values, use
// filter; if you need both the values and the verdict, use toArray and
// check the array.
```

## Related Operators

**Same Category (Boolean Reductions)**:
- **`isEmpty()`**: Decides whether any value arrived at all; also decided at completion for `true`
- **`sequenceEqual(other)`**: Compares two streams for equality; also short-circuits on the first mismatch
- **`defaultIfEmpty(v)`**: Not a boolean, but shares the completion-time decision

**Complementary Operators**:
- **`filter(predicate)`**: Keeps values instead of judging them
- **`find(predicate)`**: The existential counterpart — emits the first match (or `undefined`); `find(p).pipe(map(v => v !== undefined))` is RxJS's "some"
- **`toArray()`**: Collect first when both the verdict and the values are needed

**Alternatives by Use Case**:

| Use Case | Instead of `every` | Use This | Why |
|----------|--------------------|----------|-----|
| "Does any value match?" | `every(v => !p(v))` + negate | `find(p)` + `map` | Direct existential check |
| Keep the passing values | `every(p)` | `filter(p)` | `every` discards values |
| Reject empty as a failure | `every(p)` | `isEmpty()` + `every(p)` | `every` is vacuously true on empty |
| Static array already in hand | `from(arr).pipe(every(p))` | `arr.every(p)` | No stream needed |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/every](https://rxjs.dev/api/operators/every)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/all.html](http://reactivex.io/documentation/operators/all.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/every.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/every.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Universal Quantifier Reduction
- **Cognitive Load**: 2/5 — Mirrors `Array.prototype.every`; the vacuous-truth rule and completion requirement are the only surprises
- **Usage Frequency**: 2/5 — Useful for batch validation, but most predicate checks are expressed with `filter`
- **Composability**: 3/5 — Terminates the stream into one boolean, so it sits at the end of a pipeline rather than in the middle

**Teaching Sequence**:
- **Prerequisites**: `filter`, completion semantics, `Array.prototype.every`
- **Teaches**: Short-circuit evaluation in streams, vacuous truth, reduction operators that need completion
- **Leads to**: `isEmpty`, `sequenceEqual`, `reduce`, `find`
- **Common with**: `filter`, `find`, `toArray`, `take`, `from`
