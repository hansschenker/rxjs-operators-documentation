# pairwise

**Category**: Transformation  
**Import**: `import { pairwise } from 'rxjs';`

## Description

Groups pairs of consecutive emissions together and emits them as a two-element array `[previous, current]`. The Nth emission from the source causes the output to emit `[(N-1)th, Nth]`.

Because each emission requires a previous value, `pairwise` does not emit for the first source value. It starts emitting on the second source emission. This makes it useful for computing deltas, detecting direction changes, or comparing consecutive states.

## Signature

```typescript
function pairwise<T>(): OperatorFunction<T, [T, T]>
```

## Parameters

None.

## Return Type

`OperatorFunction<T, [T, T]>` — emits `[T, T]` arrays containing consecutive value pairs. Emits one fewer value than the source.

## Marble Diagram

```
Source: --a--b--c--d--|
           pairwise()
Output: -----[a,b]--[b,c]--[c,d]--|
        (no emission for first value 'a')
```

## Examples

### Example 1: Calculate distance between consecutive mouse clicks

```typescript
import { fromEvent, pairwise, map } from 'rxjs';

fromEvent<PointerEvent>(document, 'click').pipe(
  pairwise(),
  map(([prev, curr]) => {
    const dx = curr.clientX - prev.clientX;
    const dy = curr.clientY - prev.clientY;
    return Math.sqrt(dx * dx + dy * dy);
  })
).subscribe(distance => {
  console.log(`Distance from last click: ${distance.toFixed(1)}px`);
});
```

### Example 2: Detect price movement direction in a live feed

```typescript
import { Subject, pairwise, map } from 'rxjs';

const price$ = new Subject<number>();

price$.pipe(
  pairwise(),
  map(([prev, curr]) => ({
    prev,
    curr,
    change: curr - prev,
    direction: curr > prev ? 'up' : curr < prev ? 'down' : 'flat',
  }))
).subscribe(tick => {
  console.log(`${tick.prev} → ${tick.curr} (${tick.direction}: ${tick.change > 0 ? '+' : ''}${tick.change})`);
});

price$.next(100);
price$.next(102);  // → up: +2
price$.next(101);  // → down: -1
price$.next(101);  // → flat: 0
```

### Example 3: Detect route changes in a navigation stream

```typescript
import { Subject, pairwise, filter } from 'rxjs';

const route$ = new Subject<string>();

route$.pipe(
  pairwise(),
  filter(([from, to]) => from !== to) // Only distinct transitions
).subscribe(([from, to]) => {
  console.log(`Navigated from "${from}" to "${to}"`);
});

route$.next('/home');
route$.next('/about');   // Navigated from "/home" to "/about"
route$.next('/about');   // Filtered out (same route)
route$.next('/contact'); // Navigated from "/about" to "/contact"
```

## Common Pitfalls

- **No first emission**: `pairwise` emits nothing for the first source value. If your stream only ever emits once, `pairwise` produces no output. Use `startWith(initialValue)` before `pairwise` if you need to pair the first value with a known starting state.
- **N-1 output values**: A source that emits N values produces N-1 pairs. Keep this in mind for finite sources where you expect a specific number of outputs.
- **Reference equality for objects**: If the source emits objects, `prev` and `curr` are distinct references even if their contents are equal. Pairwise does not perform deep comparison.

## Related Operators

- `bufferCount(2, 1)` — generalization of `pairwise` for windows of arbitrary size
- `scan` — accumulate state across consecutive values with full control over the accumulation logic
- `distinctUntilChanged` — filters out consecutive duplicate values (conceptually inverse: `pairwise` highlights changes, `distinctUntilChanged` suppresses them)
- `withLatestFrom` — combine the current source value with the latest value from another Observable
