# scan

## Identity
- **Name**: scan
- **Category**: Mathematical/Aggregate Operators
- **Type**: Running accumulator — emits each intermediate accumulated value (streaming `Array.reduce`)
- **Import**:
  ```typescript
  import { scan } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // With seed — T (source) and A (accumulator) may differ
  function scan<V, A>(
    accumulator: (acc: A, value: V, index: number) => A,
    seed: A
  ): OperatorFunction<V, A>

  // Without seed — A = V; first value is used as initial accumulator
  function scan<V>(
    accumulator: (acc: V, value: V, index: number) => V
  ): OperatorFunction<V, V>
  ```

## Functional Specification

**Input**: `Observable<V>` — a source Observable emitting values of type V

**Output**: `Observable<A>` — an Observable emitting the running accumulated value after each source emission

**Transformation**:
- **With seed**: `acc` starts as `seed`. On each source emission `v`, computes `acc = accumulator(acc, v, index)` and emits the new `acc`. Every source emission produces exactly one output emission.
- **Without seed**: First source emission is emitted as-is (becomes the initial `acc`). Starting from the second emission, `accumulator(acc, v, index)` is computed and emitted. N source emissions → N output emissions in both cases.

**Relationship to `Array.reduce`**:
```
[1, 2, 3, 4].reduce((acc, v) => acc + v, 0)  // → 10  (final value only)

of(1, 2, 3, 4).pipe(
  scan((acc, v) => acc + v, 0)
)  // → 1, 3, 6, 10  (all intermediate values emitted)
```

**Mathematical representation**:
```
Let S = v₁, v₂, v₃, ..., vₙ
Let f = accumulator function
Let a₀ = seed

Output:
  a₁ = f(a₀, v₁, 0)  →  emit a₁
  a₂ = f(a₁, v₂, 1)  →  emit a₂
  ...
  aₙ = f(aₙ₋₁, vₙ, n-1)  →  emit aₙ

(Without seed: v₁ emitted as a₀; accumulator first called at index=1)
```

**Invariants**:
- **N → N mapping** (with or without seed): every source emission produces exactly one output emission
- **Synchronous accumulation**: accumulator is called synchronously per emission
- **Stateful**: holds the current accumulated value in memory; O(size of accumulated value)
- **Emission before completion**: values emitted during the stream, not only at completion (unlike `reduce`)

## Marble Diagram

```
Source:   --1--2--3--4--|
          scan((acc, v) => acc + v, 0)
Result:   --1--3--6--10--|

acc starts at 0.
Emission 1: f(0, 1) = 1  → emit 1
Emission 2: f(1, 2) = 3  → emit 3
Emission 3: f(3, 3) = 6  → emit 6
Emission 4: f(6, 4) = 10 → emit 10
```

**Array accumulation**:
```
Source:   --a--b--c--|
          scan((acc, v) => [...acc, v], [])
Result:   --[a]--[a,b]--[a,b,c]--|

Growing array emitted at each step.
```

**Without seed — first value passes through**:
```
Source:   --1--2--3--|
          scan((acc, v) => acc + v)    // no seed
Result:   --1--3--6--|

Index 0: value 1 emitted directly as initial acc.
Index 1: f(1, 2, 1) = 3 → emit 3.
Index 2: f(3, 3, 2) = 6 → emit 6.
```

**Key observation**: `scan` is the operator that turns a stream of *events* into a stream of *state*. It is the reactive equivalent of a state machine accumulator.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily when output is subscribed
- Holds exactly one internal value: the current accumulated state

**Completion semantics**:
- Source completion propagates immediately after the last accumulated emission
- If source emits no values (and seed provided): the output completes without emitting
- If source emits no values (no seed): the output completes without emitting

**Error handling**:
- Source error propagates immediately
- If the accumulator function throws, the error is forwarded downstream and the subscription terminates

**Backpressure**:
- None — synchronous, one-to-one; no buffering beyond the current accumulator state

**Hot vs. Cold**:
- Transparent — works with both hot and cold sources
- With hot sources, accumulated state reflects all values received since subscription (not from the beginning of the hot source's lifetime)
- Each subscriber gets an independent accumulator starting from the seed

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   V - Source value type
 *   A - Accumulator (output) type — may differ from V when seed is provided
 *
 * Input Type:  Observable<V>
 * Output Type: Observable<A>
 *
 * Without seed: A = V (inferred); accumulator must be (V, V) => V
 * With seed: A is inferred from the seed type; V and A may differ
 *
 * index parameter: zero-based for "with seed" (0 on first call),
 *                  starts at 1 for "without seed" (0 is the emitted first value)
 */

import { of } from 'rxjs';
import { scan } from 'rxjs/operators';

// V = number, A = number (seed provided, same type)
of(1, 2, 3, 4).pipe(
  scan((acc: number, v: number) => acc + v, 0)
).subscribe(console.log); // 1, 3, 6, 10

// V = string, A = string[] (different types — seed required)
of('a', 'b', 'c').pipe(
  scan((acc: string[], v: string) => [...acc, v], [] as string[])
).subscribe(v => console.log(v)); // ['a'], ['a','b'], ['a','b','c']

// V = Action, A = AppState (Redux-style reducer)
interface AppState { count: number; lastAction: string; }
type Action = { type: 'INCREMENT' } | { type: 'DECREMENT' } | { type: 'RESET' };

const initialState: AppState = { count: 0, lastAction: 'INIT' };

function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'INCREMENT': return { count: state.count + 1, lastAction: 'INCREMENT' };
    case 'DECREMENT': return { count: state.count - 1, lastAction: 'DECREMENT' };
    case 'RESET':     return { ...initialState, lastAction: 'RESET' };
  }
}

const actions$ = new Subject<Action>();
const state$: Observable<AppState> = actions$.pipe(
  scan(reducer, initialState),
  startWith(initialState)
);
// TypeScript infers state$ as Observable<AppState> — strongly typed
```

## Examples

### Basic Usage — Running Sum, Count, Min/Max
```typescript
import { of } from 'rxjs';
import { scan } from 'rxjs/operators';

// Running sum
of(1, 2, 3, 4, 5).pipe(
  scan((acc, v) => acc + v, 0)
).subscribe(console.log);
// Output: 1, 3, 6, 10, 15

// Running count (ignores value)
of('a', 'b', 'c', 'd').pipe(
  scan(count => count + 1, 0)
).subscribe(console.log);
// Output: 1, 2, 3, 4

// Running minimum
of(5, 3, 8, 1, 9).pipe(
  scan((min, v) => Math.min(min, v))   // no seed — first value (5) is initial min
).subscribe(console.log);
// Output: 5, 3, 3, 1, 1
```

### Common Pattern — Building a Redux-Style State Store
```typescript
import { Subject } from 'rxjs';
import { scan, startWith, shareReplay, distinctUntilChanged, map } from 'rxjs/operators';

interface CartItem { id: number; name: string; qty: number; price: number; }
interface CartState { items: CartItem[]; total: number; }

type CartAction =
  | { type: 'ADD';    item: CartItem }
  | { type: 'REMOVE'; id: number }
  | { type: 'CLEAR' };

const initialCart: CartState = { items: [], total: 0 };

function cartReducer(state: CartState, action: CartAction): CartState {
  switch (action.type) {
    case 'ADD': {
      const items = [...state.items, action.item];
      return { items, total: items.reduce((t, i) => t + i.qty * i.price, 0) };
    }
    case 'REMOVE': {
      const items = state.items.filter(i => i.id !== action.id);
      return { items, total: items.reduce((t, i) => t + i.qty * i.price, 0) };
    }
    case 'CLEAR':
      return initialCart;
  }
}

const dispatch$ = new Subject<CartAction>();

// Full state stream — shared, cached, late subscribers get current state
const cart$ = dispatch$.pipe(
  scan(cartReducer, initialCart),
  startWith(initialCart),
  shareReplay(1)
);

// Derived selectors
const total$ = cart$.pipe(
  map(s => s.total),
  distinctUntilChanged()
);

const itemCount$ = cart$.pipe(
  map(s => s.items.length),
  distinctUntilChanged()
);

// Usage
cart$.subscribe(s => renderCart(s));
total$.subscribe(t => renderTotal(t));
itemCount$.subscribe(n => renderBadge(n));

dispatch$.next({ type: 'ADD', item: { id: 1, name: 'Widget', qty: 2, price: 9.99 } });
dispatch$.next({ type: 'ADD', item: { id: 2, name: 'Gadget', qty: 1, price: 19.99 } });
// cart$: { items: [{...}], total: 19.98 }, { items: [{...},{...}], total: 39.97 }
```

### Common Pattern — Collecting Events Into an Array
```typescript
import { fromEvent } from 'rxjs';
import { scan, debounceTime, map } from 'rxjs/operators';

const clickStream$ = fromEvent(document, 'click');

// Collect last 5 click positions
clickStream$.pipe(
  map((e: MouseEvent) => ({ x: e.clientX, y: e.clientY })),
  scan(
    (history, point) => [...history.slice(-4), point], // keep last 5
    [] as { x: number; y: number }[]
  )
).subscribe(history => drawTrail(history));

// Group events into batches of 10
clickStream$.pipe(
  scan(
    (batch, event) => batch.length < 10 ? [...batch, event] : [event],
    [] as MouseEvent[]
  )
).subscribe(batch => batch.length === 10 && processBatch(batch));
```

### Common Pattern — Tracking UI State (Toggle, Multi-Select)
```typescript
import { fromEvent, merge } from 'rxjs';
import { scan, map, distinctUntilChanged } from 'rxjs/operators';

// Toggle sidebar
const toggleBtn = document.getElementById('sidebar-toggle')!;
const sidebarOpen$ = fromEvent(toggleBtn, 'click').pipe(
  scan(isOpen => !isOpen, false) // boolean toggle — no value needed
);
sidebarOpen$.subscribe(open => setSidebarVisibility(open));

// Multi-select: track selected IDs
type SelectAction = { type: 'toggle'; id: number } | { type: 'clear' };
const selection$ = new Subject<SelectAction>();

const selectedIds$ = selection$.pipe(
  scan((selected: Set<number>, action) => {
    if (action.type === 'clear') return new Set<number>();
    const next = new Set(selected);
    if (next.has(action.id)) next.delete(action.id); else next.add(action.id);
    return next;
  }, new Set<number>()),
  map(s => [...s].sort((a, b) => a - b)),
  distinctUntilChanged((a, b) => a.join() === b.join())
);
```

### Edge Cases — Empty Source, Single Value, Accumulator Error
```typescript
import { EMPTY, of, throwError } from 'rxjs';
import { scan } from 'rxjs/operators';

// Edge case 1: empty source with seed — completes without emitting
EMPTY.pipe(
  scan((acc, v) => acc + v, 0)
).subscribe({ next: console.log, complete: () => console.log('complete') });
// Output: complete  (no emissions)

// Edge case 2: single value with seed — emits once
of(42).pipe(
  scan((acc, v) => acc + v, 10)
).subscribe(console.log);
// Output: 52  (10 + 42)

// Edge case 3: accumulator throws — error propagates
of(1, 2, 0, 3).pipe(
  scan((acc, v) => {
    if (v === 0) throw new Error('zero not allowed');
    return acc + v;
  }, 0)
).subscribe({ next: console.log, error: e => console.log('error:', e.message) });
// Output: 1, 3, error: zero not allowed

// Edge case 4: index parameter
of('a', 'b', 'c').pipe(
  scan((acc, v, i) => [...acc, `${i}:${v}`], [] as string[])
).subscribe(console.log);
// Output: ['0:a'], ['0:a','1:b'], ['0:a','1:b','2:c']
```

## Common Pitfalls

### Anti-pattern: Mutating the Accumulator Instead of Returning a New Value
```typescript
import { Subject } from 'rxjs';
import { scan } from 'rxjs/operators';

const events$ = new Subject<string>();

// ❌ INCORRECT — mutating acc in place; same reference returned each time
events$.pipe(
  scan((acc: string[], v: string) => {
    acc.push(v); // mutates the array
    return acc;  // same reference
  }, [])
).subscribe(list => console.log([...list]));

// distinctUntilChanged after this would NEVER suppress — reference never changes
// components using OnPush change detection won't re-render

// ✅ CORRECT — return a new value every time
events$.pipe(
  scan((acc: string[], v: string) => [...acc, v], [])
).subscribe(list => console.log(list));

// WHY: Mutating and returning the same reference makes the accumulator lie about
// whether the state has changed. Operators that compare by reference (like
// distinctUntilChanged) and frameworks that rely on immutability for change
// detection (Angular OnPush, React, etc.) will break silently. Always return
// a new object or array from the accumulator.
```

### Anti-pattern: Using `scan` When `reduce` is Sufficient
```typescript
import { of } from 'rxjs';
import { scan, last, reduce } from 'rxjs/operators';

// ❌ SUBOPTIMAL — scan + last when only the final value is needed
of(1, 2, 3, 4, 5).pipe(
  scan((acc, v) => acc + v, 0),
  last()
).subscribe(console.log);
// Output: 15

// ✅ CORRECT — use reduce() when only the final accumulated value matters
of(1, 2, 3, 4, 5).pipe(
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log);
// Output: 15

// WHY: scan emits every intermediate value. If you only need the final result
// (like Array.reduce), use reduce(). scan + last is valid but communicates
// the wrong intent — reduce makes the goal explicit.
// Use scan when intermediate values drive UI, derived state, or other streams.
```

### Anti-pattern: Accumulating State That Should Be Computed Reactively
```typescript
import { Subject, combineLatest } from 'rxjs';
import { scan, map } from 'rxjs/operators';

// ❌ FRAGILE — tracking derived state manually inside scan
interface Item { id: number; price: number; qty: number; }
type State = { items: Item[]; total: number; itemCount: number; avgPrice: number; };

const actions$ = new Subject<Item>();
const state$ = actions$.pipe(
  scan((state: State, item: Item): State => ({
    items: [...state.items, item],
    total: state.total + item.price * item.qty,
    itemCount: state.itemCount + 1,
    avgPrice: (state.total + item.price * item.qty) / (state.itemCount + 1),
  }), { items: [], total: 0, itemCount: 0, avgPrice: 0 })
);

// ✅ CORRECT — scan holds minimal state; derive secondary values in map/combineLatest
const items$ = actions$.pipe(
  scan((items: Item[], item: Item) => [...items, item], [])
);

const derived$ = items$.pipe(
  map(items => ({
    items,
    total:      items.reduce((t, i) => t + i.price * i.qty, 0),
    itemCount:  items.length,
    avgPrice:   items.length
      ? items.reduce((t, i) => t + i.price, 0) / items.length
      : 0,
  }))
);

// WHY: Accumulating computed values inside scan duplicates computation and
// makes the reducer harder to test and reason about. Keep accumulated state
// minimal (the source-of-truth items array), then derive computed values
// using map. This mirrors the Redux principle of keeping state normalized
// and selectors pure.
```

### Anti-pattern: Forgetting `startWith` When Using `scan` with `combineLatest`
```typescript
import { combineLatest, Subject } from 'rxjs';
import { scan, startWith, map } from 'rxjs/operators';

const price$ = new Subject<number>();
const quantity$ = new Subject<number>();

// ❌ BROKEN — combineLatest waits for BOTH to emit before producing
// scan streams don't emit until the first action arrives;
// the combined stream never starts
const orderTotal$ = combineLatest([
  price$.pipe(scan((_, p) => p)),
  quantity$.pipe(scan((_, q) => q)),
]).pipe(
  map(([p, q]) => p * q)
);

// Until both price$ and quantity$ emit, orderTotal$ emits nothing.
// If the UI depends on an initial value, it renders nothing.

// ✅ CORRECT — use startWith to provide initial values
const orderTotal2$ = combineLatest([
  price$.pipe(scan((_, p) => p), startWith(0)),
  quantity$.pipe(scan((_, q) => q), startWith(1)),
]).pipe(
  map(([p, q]) => p * q)
);
// Immediately emits 0 * 1 = 0; updates as price$ and quantity$ emit

// WHY: combineLatest requires all input streams to have emitted at least once.
// scan streams only emit when their source emits. startWith seeds each scan
// stream with an initial value, satisfying combineLatest's requirement and
// producing a sensible initial output.
```

## Related Operators

**Same Category (Mathematical/Aggregate)**:
- **`reduce`**: Like `scan` but emits only the final accumulated value after source completes — use when intermediate state is not needed
- **`count`**: Counts source emissions — equivalent to `reduce(() => acc + 1, 0)` but semantically explicit
- **`min` / `max`**: Find the minimum/maximum value — equivalent to `reduce` with a comparator
- **`sum`** (not built-in): `reduce((acc, v) => acc + v, 0)` — implement with reduce or scan + last

**Complementary Operators**:
- **`startWith(initialState)`**: Emit an initial value before `scan` starts accumulating — essential with `combineLatest`
- **`shareReplay(1)`**: Cache the current accumulated state for late subscribers — the final step in a reactive store
- **`distinctUntilChanged`**: Suppress downstream re-renders when accumulated state hasn't changed by reference
- **`map`**: Derive computed values from accumulated state without storing them in the accumulator

**Alternatives by Use Case**:

| Use Case | Instead of `scan` | Use This | Why |
|----------|-------------------|----------|-----|
| Only need final result | `scan + last` | `reduce` | Clearer intent; reduce emits once on completion |
| Simple toggle | `scan(x => !x, false)` | `scan(x => !x, false)` | This IS the right approach |
| Accumulate then combine | `scan` alone | `scan + combineLatest + startWith` | startWith enables combineLatest compatibility |
| Mutable external state | `scan` + mutation | `scan` + immutable update | Mutation breaks change detection and operators |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/scan](https://rxjs.dev/api/operators/scan)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/scan.html](http://reactivex.io/documentation/operators/scan.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/scan.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/scan.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Running State Accumulator (Streaming Reducer)
- **Cognitive Load**: 3/5 — The seed/no-seed distinction and immutability requirement are the primary stumbling blocks; the pattern itself (running total) is intuitive
- **Usage Frequency**: 4/5 — Core to any reactive state management pattern; less visible than map/filter but present in every non-trivial RxJS application
- **Composability**: 5/5 — Composes perfectly with startWith, shareReplay, map, distinctUntilChanged to form a complete reactive state store

**Problem Domain**:
Turning a stream of *events* or *actions* into a stream of *state*. The foundational operator for implementing Redux-style reducers, event sourcing, running statistics, multi-select UIs, and any scenario where state depends on the history of events.

**When to Teach**:
Teach as the bridge between "stream of events" and "stream of state". Show the progression: scan alone → scan + startWith → scan + startWith + shareReplay(1) → full reactive store.

- **Prerequisites**: `map`, `filter`, `reduce`, `Subject`
- **Teaches**: Stateful streaming, immutability in accumulator, the reducer pattern, event sourcing
- **Leads to**: `shareReplay`, `startWith`, `combineLatest`, reactive state management patterns
- **Common with**: `startWith`, `shareReplay`, `distinctUntilChanged`, `map`, `Subject`, `BehaviorSubject`

**Common Misconceptions**:
1. **"scan only emits at the end"** — that's `reduce`; scan emits on every source emission
2. **"I can mutate the accumulator"** — mutation breaks reference equality checks and change detection; always return a new value
3. **"The index starts at 1"** — with seed, index starts at 0; without seed, the first emission is passed through and index starts at 1 for the accumulator call
4. **"scan + last is the same as reduce"** — functionally equivalent but `reduce` is clearer and avoids unnecessary intermediate emissions
