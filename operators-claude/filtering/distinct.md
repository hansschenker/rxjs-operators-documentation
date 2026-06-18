# distinct

## Identity

- **Name**: distinct
- **Category**: Filtering Operators
- **Type**: Global uniqueness filter — emits values only if they have not been seen before in the entire stream
- **Import**:
  ```typescript
  import { distinct } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function distinct<T, K>(
    keySelector?: (value: T) => K,
    flushes?: Observable<any>
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**`distinct` vs `distinctUntilChanged`**:

| | `distinct` | `distinctUntilChanged` |
|---|---|---|
| Tracks | ALL previously seen values | Only the PREVIOUS value |
| Memory | Grows with unique values seen (Set) | O(1) — one value |
| Use when | Globally unique across entire stream | Consecutive duplicate suppression |
| Example | Events replayed from history, IDs | UI state changes, keystrokes |

**`keySelector`**: Extract a key for comparison instead of comparing the full value. The key is stored in the Set. Useful for objects where identity-by-property matters.

**`flushes`**: An Observable that, when it emits, clears the internal Set — allowing previously-seen values to pass through again. Use for long-running streams where the "seen" set would grow unbounded.

**Invariants**:
- Uses a `Set` internally — `===` equality for primitives; reference equality for objects
- Memory grows proportionally to distinct values — be careful with high-cardinality streams
- Without `flushes`, the Set is never cleared

## Marble Diagram

```
Source:   --1--2--1--3--2--4--|

distinct():
Result:   --1--2-----3-----4--|  (1 and 2 on second appearance are dropped)

distinctUntilChanged():
Result:   --1--2--1--3--2--4--|  (all pass — none are CONSECUTIVE duplicates)

distinct(x => x.id):
Source: --{id:1,v:'a'}--{id:2,v:'b'}--{id:1,v:'c'}--|
Result: --{id:1,v:'a'}--{id:2,v:'b'}--|   (second id:1 dropped regardless of v)

distinct() with flushes$:
--1--2--[flush]--1--2--|
Result: --1--2-----------1--2--|  (Set cleared on flush; 1,2 pass again)
```

## Type System Integration

```typescript
import { of } from 'rxjs';
import { distinct } from 'rxjs/operators';

// Primitives — Set<T> comparison
of(1, 2, 1, 3, 2).pipe(distinct()).subscribe((v: number) => console.log(v));
// 1, 2, 3

// Objects — keySelector extracts comparable key
interface Event { id: string; payload: unknown }
of<Event>(
  { id: 'e1', payload: 'a' },
  { id: 'e2', payload: 'b' },
  { id: 'e1', payload: 'c' }  // duplicate id
).pipe(
  distinct(e => e.id)
).subscribe((e: Event) => console.log(e));
// { id: 'e1', payload: 'a' }
// { id: 'e2', payload: 'b' }
```

## Examples

### Basic Usage
```typescript
import { of, from } from 'rxjs';
import { distinct } from 'rxjs/operators';

// Deduplicate a stream of IDs
from([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]).pipe(
  distinct()
).subscribe(console.log); // 3, 1, 4, 5, 9, 2, 6

// Object dedup by key
const clicks = [
  { userId: 'u1', event: 'click' },
  { userId: 'u2', event: 'click' },
  { userId: 'u1', event: 'hover' }, // u1 already seen
];
from(clicks).pipe(
  distinct(c => c.userId)
).subscribe(console.log);
// { userId: 'u1', event: 'click' }
// { userId: 'u2', event: 'click' }
```

### Common Pattern — Deduplicate Replayed Events
```typescript
import { Subject, merge } from 'rxjs';
import { distinct, scan } from 'rxjs/operators';

// Event sourcing: replay historical events, then live events
// Historical events may duplicate live events that arrived before replay completed
const history$ = replayHistoricalEvents(); // Observable<Event>
const live$    = liveEventStream$;         // Observable<Event>

merge(history$, live$).pipe(
  distinct(event => event.id)  // drop duplicate IDs regardless of order
).subscribe(applyEvent);
```

### Common Pattern — `flushes` to Bound Memory
```typescript
import { interval, Subject } from 'rxjs';
import { distinct, map } from 'rxjs/operators';

const flush$ = new Subject<void>();

// Clear the seen-set every hour for a long-running stream
// (allows values to reappear after the flush)
longRunningStream$.pipe(
  distinct(item => item.id, flush$)
).subscribe(processItem);

// Flush every hour to prevent unbounded Set growth
interval(60 * 60 * 1000).subscribe(() => flush$.next());
```

## Common Pitfalls

### Anti-pattern: `distinct()` on Object Streams Without `keySelector`
```typescript
import { of } from 'rxjs';
import { distinct } from 'rxjs/operators';

// ❌ ALL PASS — distinct uses reference equality for objects
of({ id: 1 }, { id: 2 }, { id: 1 }).pipe(
  distinct()
).subscribe(console.log);
// { id: 1 }, { id: 2 }, { id: 1 }  — all three emitted!
// Each object literal is a NEW reference — Set never finds a duplicate

// ✅ CORRECT — provide a keySelector for value-based comparison
of({ id: 1 }, { id: 2 }, { id: 1 }).pipe(
  distinct(obj => obj.id)
).subscribe(console.log);
// { id: 1 }, { id: 2 }  — second { id: 1 } dropped ✓

// WHY: JavaScript Set uses SameValueZero comparison (like ===).
// Two different object literals { id: 1 } and { id: 1 } are different references
// and are NOT equal by ===. Always provide a keySelector for objects.
```

### Anti-pattern: `distinct()` Without `flushes` on Unbounded Streams
```typescript
import { fromEvent } from 'rxjs';
import { distinct } from 'rxjs/operators';

// ❌ MEMORY LEAK — Set grows without bound over the lifetime of the app
fromEvent<CustomEvent>(document, 'dataEvent').pipe(
  distinct(e => e.detail.id)  // Set keeps every seen ID forever
).subscribe(processEvent);
// After thousands of events: large Set in memory, never GC'd

// ✅ CORRECT — flush periodically for long-running streams
import { interval } from 'rxjs';
const hourlyFlush$ = interval(3_600_000);

fromEvent<CustomEvent>(document, 'dataEvent').pipe(
  distinct(e => e.detail.id, hourlyFlush$)
).subscribe(processEvent);

// WHY: distinct() never removes values from its internal Set unless flushed.
// For long-running streams with high cardinality, provide a flushes Observable
// to periodically clear the Set and bound memory usage.
```

## Related Operators

- **`distinctUntilChanged`**: O(1) consecutive-duplicate suppression — use for state streams
- **`distinctUntilKeyChanged`**: Property-based consecutive dedup — specialized form of `distinctUntilChanged`
- **`filter`**: Per-value predicate — re-evaluates every value; no memory of past
- **`bufferCount`/`toArray`**: Collect all values to dedup synchronously (array-based alternative for finite streams)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/distinct](https://rxjs.dev/api/operators/distinct)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 3/5
**Key teaching points**:
1. Global uniqueness via a Set — memory grows with distinct values seen
2. Always use `keySelector` for objects (reference equality won't work)
3. Use `flushes` for long-running streams to bound Set memory growth
4. `distinct` ≠ `distinctUntilChanged` — tracks ALL seen values, not just previous
