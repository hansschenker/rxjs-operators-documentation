# map

## Identity
- **Name**: map
- **Category**: Transformation Operators
- **Type**: Synchronous projection operator (1-to-1 transformation)
- **Import**: 
  ```typescript
  import { map } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { map } from 'rxjs';
  ```
- **Signature**: 
  ```typescript
  function map<T, R>(
    project: (value: T, index: number) => R,
    thisArg?: any
  ): OperatorFunction<T, R>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable emitting values of type T

**Output**: `Observable<R>` — an Observable emitting transformed values of type R

**Transformation**: Applies a projection function to each emission from the source Observable synchronously, forwarding the transformed value to the output stream. Every source emission produces exactly one output emission. The cardinality of the stream is preserved (N inputs → N outputs).

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let project: (T, number) → R be the projection function

Output = { project(v₁, 0), project(v₂, 1), project(v₃, 2), ... }

Formally: map(f) ≡ ∀ vᵢ ∈ S → f(vᵢ, i) ∈ Output
```

**Invariants**:
- **1-to-1 cardinality**: Every source emission produces exactly one output emission — no emissions are added or dropped
- **Order preserved**: Output emissions appear in the same order as source emissions
- **Synchronous execution**: The projection function executes synchronously within the same tick; `map` does not introduce asynchrony
- **No subscription side effects**: `map` subscribes to and unsubscribes from source in lockstep with the output subscriber
- **Index monotonically increases**: The `index` parameter increments by 1 for each source emission, starting at 0, regardless of value type or timing

## Marble Diagram

```
Source:   --1-----2-----3-----|
              map(x => x * 10)
Result:   --10----20----30----|

Legend:
  - : time unit (10ms)
  1,2,3 : source values
  10,20,30 : transformed values (x * 10)
  | : completion
```

**Key observation**: Each source emission is transformed immediately and synchronously. Timing of emissions is identical between source and result — `map` introduces zero delay.

**With index parameter**:
```
Source:   --a-----b-----c-----|
              map((x, i) => `${i}:${x}`)
Result:   --0:a---1:b---2:c---|
```

**Error propagation**:
```
Source:   --1-----2-----#
              map(x => x * 10)
Result:   --10----20----#

Note: The project function never runs after source errors.
```

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily — only when the output Observable is subscribed to
- Maintains exactly one subscription to the source for the lifetime of the output subscription
- Unsubscribes from source immediately when the output subscriber unsubscribes

**Completion semantics**:
- Source completion propagates immediately to the output — no buffering or delay
- If the source is empty (completes without emitting), the result completes immediately without emitting
- The `project` function is never called after source completion or error

**Error handling**:
- Errors from the source propagate immediately to the output subscriber without transformation
- If the `project` function throws, the error is caught and forwarded to the output subscriber as an Observable error, then the subscription terminates
- No built-in recovery; use `catchError` downstream for error handling

**Backpressure**:
- None — `map` is 1-to-1 and synchronous; it cannot buffer or drop values
- The output emits at exactly the same rate as the source
- There is no memory accumulation from `map` itself

**Hot vs. Cold**:
- Transparent to hot/cold semantics — `map` does not alter the multicast or unicast nature of the source
- With a hot source, late subscribers miss past emissions just as they would without `map`

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type emitted by the source Observable
 *   R - The type emitted by the result Observable, inferred from the return type of `project`
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<R>
 *
 * Type Narrowing:
 *   - R is inferred from the project function's return type — no annotation needed in most cases
 *   - When T and R differ, the type system enforces correct project function shape
 *   - Discriminated union transformations narrow correctly through the projection
 *
 * Type Safety:
 *   - Compile-time verification that project accepts T and returns R
 *   - No implicit any — TypeScript infers R from the lambda body
 *   - The index parameter is always typed as number
 */

import { of } from 'rxjs';
import { map } from 'rxjs/operators';

// Basic inference: T = number, R = string
const numbers$ = of(1, 2, 3);
const strings$ = numbers$.pipe(
  map(n => n.toString()) // R inferred as string
);
// strings$: Observable<string>

// Object transformation: T = { name: string }, R = string
interface User { id: number; name: string; age: number; }
const users$ = of<User>({ id: 1, name: 'Alice', age: 30 });
const names$ = users$.pipe(
  map(user => user.name) // R inferred as string
);
// names$: Observable<string>

// Discriminated union transformation
type Action =
  | { type: 'INCREMENT'; amount: number }
  | { type: 'DECREMENT'; amount: number }
  | { type: 'RESET' };

type ActionLabel = 'increment' | 'decrement' | 'reset';

const action$ = of<Action>({ type: 'INCREMENT', amount: 5 });

const label$ = action$.pipe(
  map((action): ActionLabel => {
    switch (action.type) {
      case 'INCREMENT': return 'increment';
      case 'DECREMENT': return 'decrement';
      case 'RESET':     return 'reset';
    }
  })
);
// label$: Observable<ActionLabel>

// Index parameter usage: T = string, R = { index: number; value: string }
const items$ = of('a', 'b', 'c');
const indexed$ = items$.pipe(
  map((value, index) => ({ index, value }))
);
// indexed$: Observable<{ index: number; value: string }>
```

## Examples

### Basic Usage — Numeric Transformation
```typescript
import { of } from 'rxjs';
import { map } from 'rxjs/operators';

const celsius$ = of(0, 20, 37, 100);

const fahrenheit$ = celsius$.pipe(
  map(c => (c * 9) / 5 + 32)
);

fahrenheit$.subscribe(f => console.log(`${f}°F`));
// Output:
// 32°F
// 68°F
// 98.6°F
// 212°F
```

### Common Pattern — Transforming HTTP Responses
```typescript
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';

interface ApiUser {
  id: number;
  login: string;
  avatar_url: string;
  public_repos: number;
}

interface User {
  id: number;
  username: string;
  avatarUrl: string;
  repoCount: number;
}

const user$ = ajax.getJSON<ApiUser>('https://api.github.com/users/hansschenker').pipe(
  map((apiUser): User => ({
    id:         apiUser.id,
    username:   apiUser.login,
    avatarUrl:  apiUser.avatar_url,
    repoCount:  apiUser.public_repos,
  }))
);

user$.subscribe(user => {
  console.log(`Username: ${user.username}`);
  console.log(`Repos: ${user.repoCount}`);
});
// Output:
// Username: hansschenker
// Repos: 42
```

### Common Pattern — Deriving State for the UI
```typescript
import { BehaviorSubject } from 'rxjs';
import { map } from 'rxjs/operators';

interface CartState {
  items: Array<{ name: string; price: number; quantity: number }>;
  discount: number; // as a fraction: 0.1 = 10%
}

const cart$ = new BehaviorSubject<CartState>({
  items: [
    { name: 'RxJS Book', price: 39.99, quantity: 2 },
    { name: 'TypeScript Handbook', price: 29.99, quantity: 1 },
  ],
  discount: 0.1,
});

const summary$ = cart$.pipe(
  map(cart => {
    const subtotal = cart.items.reduce(
      (sum, item) => sum + item.price * item.quantity,
      0
    );
    const total = subtotal * (1 - cart.discount);
    return {
      itemCount: cart.items.reduce((n, i) => n + i.quantity, 0),
      subtotal: subtotal.toFixed(2),
      total: total.toFixed(2),
    };
  })
);

summary$.subscribe(s => {
  console.log(`${s.itemCount} items — subtotal $${s.subtotal}, total $${s.total}`);
});
// Output: 3 items — subtotal $109.97, total $98.97
```

### Edge Cases — Throwing Inside the Project Function
```typescript
import { of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

// Edge case 1: project throws — error propagates downstream
const unsafe$ = of('hello', '', 'world').pipe(
  map(str => {
    if (str.length === 0) throw new Error('Empty string not allowed');
    return str.toUpperCase();
  })
);

unsafe$.subscribe({
  next:  v   => console.log(v),
  error: err => console.log('Error:', err.message),
});
// Output:
// HELLO
// Error: Empty string not allowed
// (world is never processed)

// Edge case 2: recover from a throwing project with catchError
const safe$ = of('hello', '', 'world').pipe(
  map(str => {
    if (str.length === 0) throw new Error('Empty string not allowed');
    return str.toUpperCase();
  }),
  catchError(err => {
    console.warn('Recovered:', err.message);
    return of('[EMPTY]');
  })
);

safe$.subscribe(v => console.log(v));
// Output:
// HELLO
// Recovered: Empty string not allowed
// [EMPTY]
// (world is still not processed — catchError replaces the stream)

// Edge case 3: empty source
of<number>().pipe(
  map(n => n * 2)
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('Completed'),
});
// Output: Completed  (project never called)
```

### Advanced Pattern — Building a Projection Pipeline
```typescript
import { fromEvent } from 'rxjs';
import { map, filter, distinctUntilChanged } from 'rxjs/operators';

interface MousePosition { x: number; y: number; }
interface Quadrant { label: 'I' | 'II' | 'III' | 'IV'; x: number; y: number; }

const centerX = window.innerWidth  / 2;
const centerY = window.innerHeight / 2;

// Chain maps to build up a richer type step by step
const quadrant$ = fromEvent<MouseEvent>(document, 'mousemove').pipe(
  map((e): MousePosition => ({ x: e.clientX, y: e.clientY })),
  map(({ x, y }): Quadrant => ({
    label: x >= centerX
      ? (y <= centerY ? 'I' : 'IV')
      : (y <= centerY ? 'II' : 'III'),
    x,
    y,
  })),
  filter(q => q.label === 'I' || q.label === 'III'), // diagonal only
  distinctUntilChanged((a, b) => a.label === b.label),
  map(q => `Entered quadrant ${q.label} at (${q.x}, ${q.y})`)
);

quadrant$.subscribe(msg => console.log(msg));
// Output (example):
// Entered quadrant I at (812, 234)
// Entered quadrant III at (400, 560)
```

## Common Pitfalls

### Anti-pattern: Using `map` for Side Effects
```typescript
import { of } from 'rxjs';
import { map, tap } from 'rxjs/operators';

// ❌ INCORRECT — side effect inside map
of(1, 2, 3).pipe(
  map(n => {
    console.log('Processing:', n); // side effect!
    return n * 2;
  })
).subscribe(console.log);

// ✅ CORRECT — use tap for side effects, map for transformation only
of(1, 2, 3).pipe(
  tap(n => console.log('Processing:', n)),
  map(n => n * 2)
).subscribe(console.log);

// WHY: map is meant to be a pure function. Side effects in map are
// hard to reason about, violate the principle of least surprise, and
// make pipelines difficult to test. tap is the dedicated side-effect
// operator — its intent is clear and it doesn't affect the value stream.
```

### Anti-pattern: Using `map` for Async Operations
```typescript
import { of } from 'rxjs';
import { map, mergeMap } from 'rxjs/operators';

// ❌ INCORRECT — project returns a Promise, result is Observable<Promise<User>>
const wrapped$ = of(1, 2, 3).pipe(
  map(id => fetch(`/api/users/${id}`).then(r => r.json()))
  // result type: Observable<Promise<User>> — not Observable<User>!
);

wrapped$.subscribe(promise => {
  // You receive a Promise, not a User — this is almost never intended
  console.log(promise); // Promise { <pending> }
});

// ✅ CORRECT — use mergeMap (or switchMap/concatMap) to flatten async ops
import { mergeMap } from 'rxjs/operators';

const users$ = of(1, 2, 3).pipe(
  mergeMap(id => fetch(`/api/users/${id}`).then(r => r.json()))
  // result type: Observable<User> — correctly flattened
);

users$.subscribe(user => console.log(user));

// WHY: map applies a synchronous projection. When the project function
// returns a Promise or Observable, map wraps it as-is — it does NOT
// subscribe to or await it. Use mergeMap, switchMap, or concatMap
// whenever the project function returns an async value.
```

### Anti-pattern: Mutating the Source Object
```typescript
import { of } from 'rxjs';
import { map } from 'rxjs/operators';

interface Item { id: number; count: number; }

// ❌ INCORRECT — mutating the original object
of<Item>({ id: 1, count: 5 }).pipe(
  map(item => {
    item.count += 1; // mutates the input!
    return item;     // returns the same reference
  })
).subscribe(console.log);

// ✅ CORRECT — return a new object
of<Item>({ id: 1, count: 5 }).pipe(
  map(item => ({ ...item, count: item.count + 1 }))
).subscribe(console.log);
// Output: { id: 1, count: 6 }

// WHY: Mutating input objects breaks referential transparency,
// makes debugging harder (the original value is gone), and can cause
// subtle bugs when the same source object is shared across multiple
// subscribers or used after the pipeline completes.
```

### Anti-pattern: Redundant Chained Maps
```typescript
import { of } from 'rxjs';
import { map } from 'rxjs/operators';

// ❌ INEFFICIENT — three separate operator calls for one logical transformation
of(1, 2, 3).pipe(
  map(n => n * 2),
  map(n => n + 1),
  map(n => `Value: ${n}`)
).subscribe(console.log);

// ✅ PREFERRED — compose into a single map when transformations are related
of(1, 2, 3).pipe(
  map(n => `Value: ${n * 2 + 1}`)
).subscribe(console.log);
// Output:
// Value: 3
// Value: 5
// Value: 7

// WHY: Each operator in a pipe adds a small overhead. When transformations
// form a single logical step, composing them into one map is cleaner.
// Exception: keep maps separate when each step has distinct semantic
// meaning or when intermediate values need to be reused (e.g., via tap).
```

### Performance: Expensive Projections on High-Frequency Sources
**When this matters**:
- Source emits >60 times/second (e.g., mousemove, scroll, animation frames)
- The project function is computationally expensive (e.g., JSON serialization, sorting)

**What to do**:
```typescript
import { fromEvent } from 'rxjs';
import { map, throttleTime, distinctUntilChanged } from 'rxjs/operators';

const scroll$ = fromEvent(window, 'scroll');

// Without rate limiting: project runs on every scroll event (could be 100+/s)
const expensive$ = scroll$.pipe(
  map(() => computeExpensiveLayout()) // called every scroll tick
);

// With rate limiting: project runs at most every 16ms (~60fps)
const efficient$ = scroll$.pipe(
  throttleTime(16),
  map(() => computeExpensiveLayout()),
  distinctUntilChanged() // skip if layout unchanged
);
```

## Related Operators

**Same Category (Transformation)**:
- **`scan`**: Like `map` but accumulates state across emissions — use when the output depends on previous values (running total, accumulated state)
- **`reduce`**: Like `scan` but only emits the final accumulated value on completion — use for aggregate operations over a finite stream
- **`pluck`** *(deprecated in RxJS 8)*: A specialized `map` for extracting a nested property — replace with `map(x => x.prop)`
- **`mapTo`** *(deprecated in RxJS 8)*: A specialized `map` that always returns the same constant — replace with `map(() => constant)`
- **`pairwise`**: Emits pairs of consecutive values — use when the transformation depends on the current and previous emission

**Complementary Operators**:
- **`filter`**: Pair with `map` to transform-then-filter or filter-then-transform depending on intent
- **`tap`**: Use alongside `map` for side effects — `tap` observes, `map` transforms
- **`mergeMap` / `switchMap` / `concatMap`**: Use instead of `map` when the project function returns an Observable or Promise
- **`distinctUntilChanged`**: Follow `map` with this when the projection may produce the same value repeatedly and downstream should only react to changes

**Alternatives by Use Case**:

| Use Case | Instead of `map` | Use This | Why |
|----------|-----------------|----------|-----|
| Async transformation | `map(x => fetch(x))` | `mergeMap(x => fetch(x))` | Flattens Promises/Observables |
| Side effects only | `map(x => { log(x); return x; })` | `tap(x => log(x))` | Expresses intent, doesn't transform |
| Property extraction | `map(x => x.name)` | `map(x => x.name)` | ✓ Correct — pluck is deprecated |
| Running accumulation | `map(...)` chained | `scan((acc, x) => ...)` | Carries state across emissions |
| Constant replacement | `map(() => 'done')` | `map(() => 'done')` | ✓ Correct — mapTo is deprecated |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/map](https://rxjs.dev/api/operators/map)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/map.html](http://reactivex.io/documentation/operators/map.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/map.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/map.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Pure Projection Strategy (Synchronous Value Transformation)
- **Cognitive Load**: 1/5 — Identical mental model to `Array.prototype.map`; no asynchrony, no higher-order concepts
- **Usage Frequency**: 5/5 — Present in virtually every RxJS pipeline; the most fundamental transformation operator
- **Composability**: 5/5 — Composes freely with every other operator; the universal adapter between type domains

**Problem Domain**:
Transforming the shape or type of each emission in a stream without altering its timing, count, or order. Converts between data representations (API responses → domain models, raw events → structured data, primitives → display strings).

**When to Teach**:
First operator after `Observable` subscription basics. `map` is the entry point to the transformation category and the ideal first operator because it shares its mental model with `Array.prototype.map`.

- **Prerequisites**: Observable creation, subscribe/unsubscribe, understanding that operators return new Observables
- **Teaches**: The concept of operator pipelines, pure functional transformation, type safety in reactive streams
- **Leads to**: `filter` (same mental model as `Array.prototype.filter`), then `scan`/`reduce`, then higher-order operators (`mergeMap` etc.)
- **Common with**: `filter`, `tap`, `distinctUntilChanged` — the "everyday pipeline" quartet

**Common Misconceptions**:
1. **"map can handle Promises"** — `map` wraps the Promise as a value; use `mergeMap` to await it
2. **"map can drop or add emissions"** — `map` is strictly 1-to-1; use `filter` to drop, `mergeMap` returning multiple values to add
3. **"I need tap for logging inside map"** — keep `map` pure and use `tap` before or after for logging
