# distinctUntilChanged

## Identity
- **Name**: distinctUntilChanged
- **Category**: Filtering Operators
- **Type**: Consecutive duplicate filter
- **Import**:
  ```typescript
  import { distinctUntilChanged } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { distinctUntilChanged } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function distinctUntilChanged<T, K>(
    comparator?: (previous: K, current: K) => boolean,
    keySelector?: (value: T) => K
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable that may emit consecutive duplicate values

**Output**: `Observable<T>` — an Observable that suppresses any emission that is equal to the immediately preceding emission

**Transformation**: Maintains a reference to the most recently emitted value. Each new emission is compared to that reference using the comparator (default: `===`). If equal, the emission is silently dropped. If not equal, it is forwarded and becomes the new reference. The first emission is always forwarded (no previous value exists).

**Mathematical representation**:
```
Let S be the source Observable emitting values v₀, v₁, v₂, ...
Let eq: (K, K) → boolean be the comparator (default: ===)
Let key: T → K be the keySelector (default: identity)

v₀ is always forwarded.

For i > 0:
  vᵢ is forwarded if eq(key(vᵢ₋₁), key(vᵢ)) = false
  vᵢ is suppressed if eq(key(vᵢ₋₁), key(vᵢ)) = true
```

**Invariants**:
- **First-emission always passes**: No previous value exists; the first emission is unconditionally forwarded
- **Only consecutive pairs compared**: Non-consecutive duplicates (e.g. `1,2,1`) each pass — only back-to-back repeats are suppressed
- **Order preserved**: Forwarded emissions appear in source order
- **Synchronous**: Comparison and forwarding happen synchronously per emission
- **Stateful**: Holds exactly one value in memory — the last forwarded emission

## Marble Diagram

```
Source:   --1--1--2--2--2--1--|
          distinctUntilChanged()
Result:   --1-----2-----------1--|

Legend:
  - : time unit (10ms)
  1,2 : emitted values
  | : completion
  Second 1 suppressed (same as previous).
  Second and third 2 suppressed (same as previous).
  Final 1 passes (different from previous value 2).
```

**Non-consecutive duplicates both pass**:
```
Source:   --1--2--1--|
          distinctUntilChanged()
Result:   --1--2--1--|

All three pass — no two consecutive values are equal.
```

**With keySelector on objects**:
```
Source:   --{id:1,v:'a'}--{id:1,v:'b'}--{id:2,v:'a'}--|
          distinctUntilChanged(_, x => x.id)
Result:   --{id:1,v:'a'}------------------{id:2,v:'a'}--|

Second object suppressed: id is still 1.
Third object passes: id changed to 2.
```

**Key observation**: `distinctUntilChanged` filters *change* — it answers "did the value actually change?" — not "has this value been seen before?"

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily on output subscription
- Holds one internal reference: the last forwarded value (or its key)
- The reference is initialized on first emission, not at subscription time

**Completion semantics**:
- Source completion propagates immediately
- The stored reference is released on unsubscription

**Error handling**:
- Source errors propagate immediately, bypassing the comparator
- If the `comparator` or `keySelector` throws, the error is forwarded downstream and the subscription terminates

**Backpressure**:
- None — synchronous, 1-or-0-per-emission; no buffering
- Memory is O(1): one stored reference regardless of stream length

**Hot vs. Cold**:
- Transparent to hot/cold semantics
- With a multicasted (hot) source, each subscriber maintains its own independent stored reference — late subscribers start fresh

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type emitted by both source and result (MonoTypeOperatorFunction)
 *   K - The key type used for comparison (defaults to T when no keySelector)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>  (values are forwarded unchanged)
 *
 * Type Narrowing:
 *   - No narrowing — T in = T out
 *   - keySelector maps T → K; comparator receives K, not T
 *   - Without keySelector: K = T (comparator receives the full value)
 *
 * Type Safety:
 *   - comparator is typed as (previous: K, current: K) => boolean
 *   - keySelector is typed as (value: T) => K
 *   - Mismatched K between comparator and keySelector is a compile error
 */

import { of } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

// Default: reference equality (===)
of(1, 1, 2, 2, 1).pipe(
  distinctUntilChanged()
).subscribe(console.log); // 1, 2, 1

// Custom comparator: deep equality for primitives within tolerance
of(1.0, 1.01, 1.05, 2.0).pipe(
  distinctUntilChanged((prev, curr) => Math.abs(prev - curr) < 0.1)
).subscribe(console.log); // 1.0, 2.0

// keySelector: compare by a single property — T = User, K = number
interface User { id: number; name: string; }

of<User>(
  { id: 1, name: 'Alice' },
  { id: 1, name: 'Alicia' }, // same id → suppressed
  { id: 2, name: 'Bob' }
).pipe(
  distinctUntilChanged(undefined, user => user.id)
  // comparator defaults to === on the key (number)
).subscribe(u => console.log(u.name));
// Output: Alice, Bob

// keySelector + custom comparator: T = Product, K = { price: number }
interface Product { name: string; price: number; category: string; }

of<Product>(
  { name: 'A', price: 9.99,  category: 'books' },
  { name: 'B', price: 10.01, category: 'books' }, // price within $0.10 → suppressed
  { name: 'C', price: 15.00, category: 'books' }
).pipe(
  distinctUntilChanged(
    (prev, curr) => Math.abs(prev - curr) < 0.10,
    p => p.price
  )
).subscribe(p => console.log(p.name));
// Output: A, C
```

## Examples

### Basic Usage — Suppressing Consecutive Duplicates
```typescript
import { of } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

// Primitive values — uses === by default
of(1, 1, 2, 3, 3, 3, 2, 1).pipe(
  distinctUntilChanged()
).subscribe(v => console.log(v));
// Output: 1, 2, 3, 2, 1

// Strings
of('a', 'a', 'b', 'a', 'a').pipe(
  distinctUntilChanged()
).subscribe(v => console.log(v));
// Output: a, b, a
```

### Common Pattern — Search Input Pipeline
```typescript
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const input = document.getElementById('search') as HTMLInputElement;

fromEvent(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),
  distinctUntilChanged(),   // skip if same query as last search
  switchMap(q => ajax.getJSON(`/api/search?q=${q}`))
).subscribe(renderResults);

// User types "rxjs", pauses → search fires
// User clicks elsewhere and back, pauses again with "rxjs" still in the box
// → debounceTime fires again but distinctUntilChanged suppresses the duplicate
// → no wasted HTTP request
```

### Common Pattern — Object Comparison with `keySelector`
```typescript
import { BehaviorSubject } from 'rxjs';
import { distinctUntilChanged, map } from 'rxjs/operators';

interface AppState {
  userId: number;
  theme: 'light' | 'dark';
  sidebarOpen: boolean;
}

const state$ = new BehaviorSubject<AppState>({
  userId: 1,
  theme: 'light',
  sidebarOpen: false,
});

// React only when userId changes — ignore unrelated state updates
const userId$ = state$.pipe(
  distinctUntilChanged((a, b) => a === b, s => s.userId)
);

// Equivalently with map first:
const userId2$ = state$.pipe(
  map(s => s.userId),
  distinctUntilChanged()
);

userId$.subscribe(s => console.log('User changed:', s.userId));

state$.next({ userId: 1, theme: 'dark',  sidebarOpen: false }); // suppressed — userId unchanged
state$.next({ userId: 2, theme: 'dark',  sidebarOpen: false }); // passes — userId changed
state$.next({ userId: 2, theme: 'light', sidebarOpen: true  }); // suppressed — userId unchanged
// Output: User changed: 1  (initial), User changed: 2
```

### Common Pattern — Custom Deep Comparator
```typescript
import { Subject } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

interface Bounds { x: number; y: number; width: number; height: number; }

const resize$ = new Subject<Bounds>();

resize$.pipe(
  distinctUntilChanged(
    (prev, curr) =>
      prev.width  === curr.width  &&
      prev.height === curr.height &&
      prev.x      === curr.x      &&
      prev.y      === curr.y
  )
).subscribe(b => recalculateLayout(b));

// Fires recalculateLayout only when bounds actually changed,
// not every time the resize$ subject emits the same dimensions.
```

### Edge Cases — First Emission, Object References, Empty Source
```typescript
import { of, EMPTY, Subject } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

// Edge case 1: first emission always passes
of(42).pipe(distinctUntilChanged()).subscribe(console.log);
// Output: 42  (no previous value to compare)

// Edge case 2: object reference equality (default ===)
const obj = { x: 1 };
of(obj, obj, { x: 1 }).pipe(
  distinctUntilChanged()
).subscribe(v => console.log(v));
// Output: { x: 1 }, { x: 1 }
// — first { x: 1 } is a new reference even though value is "equal"
// Use keySelector or custom comparator for value equality on objects

// Edge case 3: empty source
EMPTY.pipe(distinctUntilChanged()).subscribe({
  complete: () => console.log('complete')
});
// Output: complete  (no emissions, comparator never called)

// Edge case 4: comparator throws
of(1, 2, 3).pipe(
  distinctUntilChanged((prev, curr) => {
    if (curr === 2) throw new Error('comparator error');
    return prev === curr;
  })
).subscribe({ next: console.log, error: e => console.log('error:', e.message) });
// Output: 1, error: comparator error
```

## Common Pitfalls

### Anti-pattern: Confusing `distinctUntilChanged` with `distinct`
```typescript
import { of } from 'rxjs';
import { distinctUntilChanged, distinct } from 'rxjs/operators';

// ❌ WRONG expectation: thinking distinctUntilChanged removes ALL duplicates
of(1, 2, 1, 2, 1).pipe(
  distinctUntilChanged()
).subscribe(console.log);
// Output: 1, 2, 1, 2, 1  — all five pass!
// No two consecutive values are equal, so nothing is suppressed.

// ✅ CORRECT for "emit each value only once ever": use distinct()
of(1, 2, 1, 2, 1).pipe(
  distinct()
).subscribe(console.log);
// Output: 1, 2  — seen values are remembered for the stream lifetime

// WHY: distinctUntilChanged only compares adjacent pairs.
// It prevents re-emitting the same value twice in a row —
// ideal for change detection. distinct() tracks all seen values —
// ideal for deduplication over a finite set. For long-lived streams,
// distinct() has unbounded memory growth; distinctUntilChanged is O(1).
```

### Anti-pattern: Object Reference Equality Trap
```typescript
import { Subject } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

interface Config { debug: boolean; timeout: number; }

const config$ = new Subject<Config>();

// ❌ INCORRECT — every emission is a new object reference, so === always false
config$.pipe(
  distinctUntilChanged() // compares references, not values
).subscribe(cfg => applyConfig(cfg));

config$.next({ debug: true,  timeout: 3000 });
config$.next({ debug: true,  timeout: 3000 }); // logically same, but new object!
// applyConfig is called twice — distinctUntilChanged did nothing

// ✅ CORRECT — compare by relevant properties
config$.pipe(
  distinctUntilChanged(
    (a, b) => a.debug === b.debug && a.timeout === b.timeout
  )
).subscribe(cfg => applyConfig(cfg));

// Or extract a stable key:
config$.pipe(
  distinctUntilChanged(undefined, cfg => `${cfg.debug}:${cfg.timeout}`)
).subscribe(cfg => applyConfig(cfg));

// WHY: The default comparator uses ===, which is reference equality.
// New object literals are always new references even if values are identical.
// Provide a custom comparator or keySelector for structural equality on objects.
```

### Anti-pattern: Placing `distinctUntilChanged` Before `debounceTime`
```typescript
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged } from 'rxjs/operators';

const input = document.getElementById('search') as HTMLInputElement;
const value$ = fromEvent(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);

// ❌ SUBOPTIMAL — distinctUntilChanged before debounceTime
// filters consecutive duplicate keystrokes, but the real goal is
// to suppress duplicate *debounced* queries
value$.pipe(
  distinctUntilChanged(), // fires on every non-duplicate keystroke
  debounceTime(300),
).subscribe(search);

// ✅ CORRECT — distinctUntilChanged after debounceTime
// suppresses cases where the user pauses, then types and deletes
// back to the same query they last searched for
value$.pipe(
  debounceTime(300),
  distinctUntilChanged(), // fires only when debounced value actually changed
).subscribe(search);

// WHY: Placed before debounceTime, distinctUntilChanged compares every
// individual keystroke — rarely useful. After debounceTime it compares
// the settled query values, preventing duplicate API calls when the
// user returns to a previously searched term.
```

### Anti-pattern: Using `distinctUntilChanged` as a Change Guard on Mutations
```typescript
import { BehaviorSubject } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

interface Item { id: number; count: number; }

const item$ = new BehaviorSubject<Item>({ id: 1, count: 0 });

// ❌ DANGEROUS — mutating the object held in BehaviorSubject
item$.pipe(
  distinctUntilChanged()
).subscribe(item => console.log('count:', item.count));

const current = item$.getValue();
current.count++;       // mutates the object in-place
item$.next(current);   // emits the same reference

// Output: count: 0  (only initial emission)
// distinctUntilChanged sees same reference → suppresses the update!
// The subscriber never learns the count changed.

// ✅ CORRECT — always emit a new object reference
item$.pipe(
  distinctUntilChanged((a, b) => a.count === b.count)
).subscribe(item => console.log('count:', item.count));

item$.next({ ...item$.getValue(), count: item$.getValue().count + 1 });
// Output: count: 0, count: 1
// Or: don't mutate — always spread/clone before next()
```

## Related Operators

**Same Category (Filtering)**:
- **`distinct`**: Filters out values already seen *anywhere* in the stream lifetime — use for deduplication over a finite, bounded set. O(N) memory vs O(1) for `distinctUntilChanged`.
- **`distinctUntilKeyChanged`**: Shorthand for `distinctUntilChanged(undefined, x => x[key])` — use when comparing by a single string property name
- **`filter`**: General predicate filtering — use when the suppression condition involves more than just equality to the previous value
- **`debounceTime`**: Always pair with `distinctUntilChanged` in search pipelines

**Complementary Operators**:
- **`debounceTime`**: Rate-limit then deduplicate — the canonical search pattern
- **`map`**: Extract a sub-value before `distinctUntilChanged` as an alternative to `keySelector`
- **`shareReplay(1)`**: Combined with `distinctUntilChanged`, avoids re-triggering downstream for unchanged derived state

**Alternatives by Use Case**:

| Use Case | Instead of `distinctUntilChanged` | Use This | Why |
|----------|----------------------------------|----------|-----|
| Deduplicate all values ever seen | `distinctUntilChanged()` | `distinct()` | Tracks full history, not just adjacent |
| Compare by single property name | `distinctUntilChanged(_, x => x.prop)` | `distinctUntilKeyChanged('prop')` | Shorter, same semantics |
| Suppress based on complex condition | `distinctUntilChanged(comparator)` | `filter(predicate)` | If condition isn't equality |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/distinctUntilChanged](https://rxjs.dev/api/operators/distinctUntilChanged)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/distinct.html](http://reactivex.io/documentation/operators/distinct.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/distinctUntilChanged.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/distinctUntilChanged.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Change Detection Gate (Consecutive Equality Filter)
- **Cognitive Load**: 2/5 — The "only consecutive pairs" semantics is the one subtlety; otherwise intuitive
- **Usage Frequency**: 5/5 — Appears in virtually every state-management and search pipeline
- **Composability**: 5/5 — Slots naturally after `debounceTime`, `map`, `BehaviorSubject`

**Problem Domain**:
Preventing downstream re-execution when the meaningful value has not changed, even if a new emission was produced. Used in state selectors (Redux-style), search pipelines, and any derived computation that is expensive to rerun unnecessarily.

**When to Teach**:
Teach immediately after `debounceTime` — the two operators are almost always used together in search and form patterns.

- **Prerequisites**: `map`, `filter`, `debounceTime`
- **Teaches**: Stateful filtering, reference vs. structural equality, the keySelector pattern for derived keys
- **Leads to**: `distinct` (full history), `distinctUntilKeyChanged` (shorthand), state selectors
- **Common with**: `debounceTime`, `switchMap`, `map`, `BehaviorSubject`

**Common Misconceptions**:
1. **"distinctUntilChanged removes all duplicates"** — only consecutive ones; use `distinct()` for lifetime deduplication
2. **"Works on objects by value"** — default is `===` (reference); always provide a comparator or keySelector for objects
3. **"Should go before debounceTime"** — almost always goes after, to compare settled debounced values
4. **"Mutating and re-emitting the same reference triggers it"** — it does not; mutation is invisible to `===`
