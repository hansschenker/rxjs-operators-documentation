# isEmpty

## Identity
- **Name**: isEmpty
- **Category**: Conditional Operators
- **Type**: Emptiness test — emits one boolean stating whether the source completed without ever emitting
- **Import**:
  ```typescript
  import { isEmpty } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function isEmpty<T>(): OperatorFunction<T, boolean>
  ```

## Functional Specification

**Input**: `Observable<T>` — any source

**Output**: `Observable<boolean>` — exactly one boolean, then completion

**Transformation**:
Waits for the first notification that settles the question. A `next` from the source means the stream is not empty: `false` is emitted, the output completes, and the source is unsubscribed. A `complete` from the source with no prior `next` means the stream is empty: `true` is emitted and the output completes. The source's values themselves are never forwarded.

**Mathematical representation**:
```
isEmpty()(S) = { |S| = 0 }  ++ complete()

Timing:
  first next of S     → emit false, unsubscribe S     (immediate)
  complete, no next   → emit true                     (at completion)
```

**Invariants**:
- **Exactly one emission** in every non-error outcome
- **`false` is immediate**: emitted on the first source value
- **`true` requires completion**: cannot be known earlier
- **Values are consumed, not forwarded**: the output type is `boolean` only

## Marble Diagram

```
Source:   --|
          isEmpty()
Result:   --(true|)

Completed with no values — empty.
```

**Source emits**:
```
Source:   --a--b--c--|
          isEmpty()
Result:   --(false|)

'a' settles the question; b and c are never requested.
```

**Infinite, silent source**:
```
Source:   -------------------
          isEmpty()
Result:   -------------------

Neither a value nor completion ever arrives; isEmpty never emits.
```

**Key observation**: `isEmpty` is the mirror image of `every`: a *negative* answer (`false`, "not empty") is instant, a *positive* answer (`true`) waits for completion. It is cheap and safe on any source that either emits quickly or completes; it is useless on a source that does neither.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately
- Unsubscribes the source as soon as the first value arrives

**Completion semantics**:
- Completes right after its single boolean
- On a never-emitting, never-completing source the output stays silent forever

**Error handling**:
- Source errors propagate immediately; no boolean is emitted
- No errors of its own

**Backpressure**:
- O(1) state; no buffering
- Never holds more than one notification's worth of work

**Hot vs. Cold**:
- Cold: evaluated per subscription against the full cold sequence
- Hot: only emissions after subscription count; a hot source that already emitted before subscription still reads as empty until it emits again or completes

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source value type (consumed, never forwarded)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<boolean>
 *
 * Type Narrowing: none — T is erased from the output.
 *
 * Type Safety:
 *   Because the output is always boolean, downstream code cannot
 *   accidentally depend on the source values. If values are also needed,
 *   use toArray() and inspect length instead.
 */

import { of } from 'rxjs';
import { filter, isEmpty } from 'rxjs/operators';

interface Order { id: number; paid: boolean }

const orders$ = of<Order>(
  { id: 1, paid: true },
  { id: 2, paid: true }
);

// Observable<Order> → Observable<boolean>
const hasUnpaid$ = orders$.pipe(
  filter(o => !o.paid),
  isEmpty()
);

hasUnpaid$.subscribe((noneUnpaid: boolean) => {
  console.log('all orders paid:', noneUnpaid);
});
// Output: all orders paid: true
```

## Examples

### Basic Usage — Test Whether a Stream Emits
```typescript
import { EMPTY, of } from 'rxjs';
import { isEmpty } from 'rxjs/operators';

EMPTY.pipe(isEmpty()).subscribe(result => console.log('Is empty?', result));
// Output: Is empty? true

of(1, 2, 3).pipe(isEmpty()).subscribe(result => console.log('Is empty?', result));
// Output: Is empty? false
```

### Common Pattern — Did the Filter Leave Anything?
```typescript
import { of } from 'rxjs';
import { filter, isEmpty } from 'rxjs/operators';

const items = [1, 3, 5, 7];

of(...items).pipe(
  filter(n => n % 2 === 0),
  isEmpty()
).subscribe(empty => {
  console.log(empty ? 'No even numbers found' : 'Found at least one even number');
});
// Output: No even numbers found
```

### Common Pattern — Toggle an Empty-State Placeholder
```typescript
import { from } from 'rxjs';
import { isEmpty } from 'rxjs/operators';

function renderResults(results: string[], placeholder: HTMLElement) {
  from(results).pipe(
    isEmpty()
  ).subscribe(empty => {
    placeholder.hidden = !empty;
  });
}

const el = document.createElement('div');
renderResults([], el);
console.log('placeholder visible:', !el.hidden);
// Output: placeholder visible: true

renderResults(['result-1'], el);
console.log('placeholder visible:', !el.hidden);
// Output: placeholder visible: false
```

### Edge Cases — Early Unsubscribe, Late Completion, Errors
```typescript
import { interval, Subject, throwError } from 'rxjs';
import { isEmpty, tap } from 'rxjs/operators';

// Edge case 1: false is emitted on the first value and the source is released
interval(100).pipe(
  tap(v => console.log('source produced', v)),
  isEmpty()
).subscribe(empty => console.log('empty:', empty));
// Output: source produced 0, empty: false   (interval unsubscribed — no 1, 2, ...)

// Edge case 2: true waits for completion
const s$ = new Subject<string>();
s$.pipe(isEmpty()).subscribe(empty => console.log('subject empty:', empty));
// (nothing yet)
s$.complete();
// Output: subject empty: true

// Edge case 3: an error is not "empty" — it propagates
throwError(() => new Error('boom')).pipe(isEmpty()).subscribe({
  next:  v   => console.log(v),
  error: err => console.log('error:', err.message),
});
// Output: error: boom
```

## Common Pitfalls

### Anti-pattern: Expecting the Values Back
```typescript
import { of } from 'rxjs';
import { isEmpty, toArray } from 'rxjs/operators';

// ❌ INCORRECT — isEmpty consumes the stream; 1, 2, 3 are gone
of(1, 2, 3).pipe(isEmpty()).subscribe(empty => {
  console.log('empty?', empty);
  // no way to reach the values from here
});

// ✅ CORRECT — collect, then inspect
of(1, 2, 3).pipe(toArray()).subscribe(arr => {
  console.log('empty?', arr.length === 0);
  console.log('values:', arr);
});
// Output: empty? false, values: [1, 2, 3]

// WHY: isEmpty answers one yes/no question and discards everything else.
// When the values matter, toArray (finite sources) or defaultIfEmpty
// (substitute a sentinel) are the right tools.
```

### Anti-pattern: Waiting for `true` on a Live Subject
```typescript
import { Subject, timer } from 'rxjs';
import { isEmpty, takeUntil } from 'rxjs/operators';

const events$ = new Subject<Event>();

// ❌ INCORRECT — a Subject that never completes can never report "empty"
events$.pipe(isEmpty()).subscribe(empty => console.log('no events:', empty));
// (silent forever unless an event arrives, in which case: false)

// ✅ CORRECT — give the question a deadline
events$.pipe(
  takeUntil(timer(5000)),
  isEmpty()
).subscribe(empty => console.log('no events in 5s:', empty));
// Output after 5s with no events: no events in 5s: true

// WHY: "empty" is a statement about the whole stream, which only exists
// once the stream ends. Bound the stream, then ask.
```

### Anti-pattern: Using `isEmpty` When a Fallback Value Is the Goal
```typescript
import { of } from 'rxjs';
import { filter, isEmpty, defaultIfEmpty, mergeMap } from 'rxjs/operators';

const source$ = of(1, 3, 5);

// ❌ INCORRECT — two subscriptions and manual branching just to supply a default
source$.pipe(
  filter(n => n % 2 === 0),
  isEmpty(),
  mergeMap(empty => empty ? of(-1) : source$.pipe(filter(n => n % 2 === 0)))
).subscribe(console.log);
// Output: -1   (works, but re-runs the source)

// ✅ CORRECT — defaultIfEmpty expresses the intent in one step
source$.pipe(
  filter(n => n % 2 === 0),
  defaultIfEmpty(-1)
).subscribe(console.log);
// Output: -1

// WHY: isEmpty reports; defaultIfEmpty substitutes. Choose by what the
// consumer needs — a verdict or a value.
```

## Related Operators

**Same Category (Boolean Reductions)**:
- **`every(predicate)`**: The dual — `false` is immediate, `true` waits for completion; empty source is `true`
- **`sequenceEqual(other)`**: Boolean comparison of two streams
- **`defaultIfEmpty(v)`**: Substitutes a value instead of reporting

**Complementary Operators**:
- **`filter`**: The usual reason a stream might turn out empty
- **`count()`**: Emits how many values arrived — a numeric generalization of `isEmpty`
- **`takeUntil` / `take`**: Bound a live source so completion, and therefore `true`, becomes possible
- **`throwIfEmpty`**: Convert emptiness into an error rather than a boolean

**Alternatives by Use Case**:

| Use Case | Instead of `isEmpty` | Use This | Why |
|----------|----------------------|----------|-----|
| Need a fallback value | `isEmpty()` + branching | `defaultIfEmpty(v)` | One operator, no re-subscription |
| Need the count, not just zero/non-zero | `isEmpty()` | `count()` | Same cost, more information |
| Empty is an error | `isEmpty()` + `throwError` | `throwIfEmpty()` | Direct error path |
| Need the values too | `isEmpty()` | `toArray()` | Check `length` and keep the data |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/isEmpty](https://rxjs.dev/api/operators/isEmpty)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/contains.html](http://reactivex.io/documentation/operators/contains.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/isEmpty.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/isEmpty.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Emptiness Predicate
- **Cognitive Load**: 1/5 — A single yes/no question; the only subtlety is that `true` needs completion
- **Usage Frequency**: 3/5 — Common for empty-state UI and post-filter checks
- **Composability**: 3/5 — Terminal reduction to a boolean; typically the last operator before `subscribe`

**Teaching Sequence**:
- **Prerequisites**: `filter`, completion semantics, `EMPTY`
- **Teaches**: Early vs. completion-time answers, consuming reductions, the difference between "no values" and "error"
- **Leads to**: `every`, `defaultIfEmpty`, `throwIfEmpty`, `count`
- **Common with**: `filter`, `defaultIfEmpty`, `toArray`, `takeUntil`, `EMPTY`
