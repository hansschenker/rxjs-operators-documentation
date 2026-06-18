# pairwise

## Identity

- **Name**: pairwise
- **Category**: Transformation Operators
- **Type**: Sliding window of size 2 — emits `[previous, current]` on each emission after the first
- **Import**:
  ```typescript
  import { pairwise } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function pairwise<T>(): OperatorFunction<T, [T, T]>
  ```

## Functional Specification

**Input**: Stream of `T` values.

**Output**: Stream of `[T, T]` tuples. Each tuple is `[previous value, current value]`. The first value from the source is buffered but not emitted. Emission begins on the second source value.

**Invariants**:
- Always emits one fewer value than the source
- Buffer size is always exactly 2 (the last two values seen)
- The previous value in each tuple is always the value that immediately preceded the current
- Equivalent to `bufferCount(2, 1).pipe(filter(buf => buf.length === 2))`

## Marble Diagram

```
Source:   --1--2--3--4--5--|

pairwise():
Result:   -----[1,2]--[2,3]--[3,4]--[4,5]--|

          1 is buffered (no emission)
          2 arrives → emit [1,2]
          3 arrives → emit [2,3]  (window slides)
          4 arrives → emit [3,4]
          5 arrives → emit [4,5]
          source completes → result completes

Single-value source:
Source:   --a--|
pairwise():
Result:   -----|   (no pairs possible — completes with no emissions)
```

## Type System Integration

```typescript
import { of } from 'rxjs';
import { pairwise } from 'rxjs/operators';

// Output type is [T, T]
of(1, 2, 3, 4).pipe(
  pairwise()
).subscribe(([prev, curr]: [number, number]) => {
  console.log(`prev=${prev}, curr=${curr}`);
});
// prev=1, curr=2
// prev=2, curr=3
// prev=3, curr=4

// Works with any type
of('a', 'b', 'c').pipe(pairwise())
// Observable<[string, string]>
```

## Examples

### Basic Usage — Detect Changes
```typescript
import { of } from 'rxjs';
import { pairwise, map } from 'rxjs/operators';

of(10, 20, 15, 25, 20).pipe(
  pairwise(),
  map(([prev, curr]) => ({ from: prev, to: curr, delta: curr - prev }))
).subscribe(console.log);
// { from: 10, to: 20, delta: 10 }
// { from: 20, to: 15, delta: -5 }
// { from: 15, to: 25, delta: 10 }
// { from: 25, to: 20, delta: -5 }
```

### Common Pattern — Velocity / Mouse Delta
```typescript
import { fromEvent } from 'rxjs';
import { pairwise, map } from 'rxjs/operators';

fromEvent<MouseEvent>(document, 'mousemove').pipe(
  pairwise(),
  map(([prev, curr]) => ({
    dx: curr.clientX - prev.clientX,
    dy: curr.clientY - prev.clientY,
    speed: Math.sqrt(
      Math.pow(curr.clientX - prev.clientX, 2) +
      Math.pow(curr.clientY - prev.clientY, 2)
    )
  }))
).subscribe(velocity => updateCursor(velocity));
```

### Common Pattern — Route Change Detection
```typescript
import { Router, NavigationEnd } from '@angular/router';
import { filter, pairwise, map } from 'rxjs/operators';

// Track navigation transitions: where the user came from
router.events.pipe(
  filter(e => e instanceof NavigationEnd),
  pairwise(),
  map(([prev, curr]) => ({
    from: (prev as NavigationEnd).urlAfterRedirects,
    to:   (curr as NavigationEnd).urlAfterRedirects
  }))
).subscribe(({ from, to }) => analytics.trackNavigation(from, to));
```

### Edge Case — Consecutive Equality Check
```typescript
import { fromEvent } from 'rxjs';
import { pairwise, filter, map } from 'rxjs/operators';

// Detect when a value actually changes (alternative to distinctUntilChanged)
statusUpdates$.pipe(
  pairwise(),
  filter(([prev, curr]) => prev.code !== curr.code), // only real changes
  map(([prev, curr]) => ({ from: prev.code, to: curr.code }))
).subscribe(change => logStatusTransition(change));
// vs distinctUntilChanged: pairwise gives you BOTH values; distinctUntilChanged
// only gives you the new value (no access to what it changed FROM)
```

## Common Pitfalls

### Anti-pattern: Not Accounting for the "Missing First Emission"
```typescript
import { of } from 'rxjs';
import { pairwise } from 'rxjs/operators';

// ❌ SURPRISE — only 2 pairs from 3 values; first value is always dropped
of(1, 2, 3).pipe(pairwise()).subscribe(console.log);
// [1, 2]
// [2, 3]
// (not three pairs — 1 is consumed by the buffer with no output)

// If you need the first value to be part of the first pair,
// use startWith to inject a synthetic "previous" value
import { startWith } from 'rxjs/operators';
of(1, 2, 3).pipe(
  startWith(0),       // inject a "previous" before the stream starts
  pairwise()
).subscribe(console.log);
// [0, 1]
// [1, 2]
// [2, 3]

// WHY: pairwise needs TWO values to form its first pair. The first value
// from the source is always held in the buffer with no corresponding
// "previous." Use startWith(initialValue) before pairwise if you need
// a pair for the very first emission.
```

### Anti-pattern: Using `pairwise` When `distinctUntilChanged` Suffices
```typescript
import { pairwise, filter, map, distinctUntilChanged } from 'rxjs/operators';

// ❌ VERBOSE — using pairwise just to skip unchanged values
source$.pipe(
  pairwise(),
  filter(([prev, curr]) => prev !== curr),
  map(([, curr]) => curr)
).subscribe(console.log);

// ✅ SIMPLER — distinctUntilChanged is exactly this pattern
source$.pipe(
  distinctUntilChanged()
).subscribe(console.log);

// WHY: Use pairwise when you need BOTH previous and current values.
// If you only need to suppress consecutive duplicates, distinctUntilChanged
// is simpler and more expressive.
```

## Related Operators

- **`bufferCount(2, 1)`**: Generalization of pairwise — sliding window of any size
- **`distinctUntilChanged`**: Skip consecutive duplicates — simpler when you only need the current value
- **`scan((prev, curr) => ...)`**: When you need to accumulate state across ALL previous values, not just the last
- **`withLatestFrom`**: Combine a trigger with the latest value from another stream (asymmetric)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/pairwise](https://rxjs.dev/api/operators/pairwise)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching point**: pairwise emits N-1 pairs for N source values — the first value is always consumed silently. Use `startWith(initial)` before `pairwise()` if you need the first value to be paired.
