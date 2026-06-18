# concatWith

**Category**: Combination  
**Import**: `import { concatWith } from 'rxjs';`

## Description

`concatWith` is the pipeable equivalent of the `concat` creation operator. It emits all values from the source Observable, and then, once the source completes, subscribes to each additional Observable provided as an argument — one at a time, in order — emitting all of their values before moving on to the next. The output Observable completes only after the last provided source completes.

`concat(a$, b$, c$)` is exactly equivalent to `a$.pipe(concatWith(b$, c$))`. Use `concatWith` when you want to append one or more streams after the current stream in a readable pipe chain.

## Signature

```typescript
function concatWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| otherSources | `...ObservableInputTuple<A>` | One or more Observable inputs to subscribe to in sequence after the source completes. |

## Return Type

`OperatorFunction<T, T | A[number]>` — An Observable that emits all values from the source, then all values from each additional source in order.

## Marble Diagram

```
Source A: --1--2--3--|
Source B:             --4--5--|
Source C:                      --6--|
          concatWith(B, C)
Output:   --1--2--3----4--5----6--|
```

## Examples

### Example 1: Show a loading message, then emit results, then show a completion message

```typescript
import { of, delay, concatWith } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const loading$ = of({ status: 'loading', data: null });
const done$ = of({ status: 'done', data: null });

loading$.pipe(
  concatWith(
    ajax.getJSON<User[]>('/api/users').pipe(
      map(data => ({ status: 'success', data }))
    )
  ),
  concatWith(done$)
).subscribe(state => updateUI(state));
// { status: 'loading' }
// { status: 'success', data: [...] }
// { status: 'done' }
```

### Example 2: Listen for one click, then switch to tracking mouse moves

```typescript
import { fromEvent, map, take, concatWith } from 'rxjs';

const clicks$ = fromEvent(document, 'click');
const moves$ = fromEvent(document, 'mousemove');

// Record the click position, then record all subsequent mouse movements
clicks$.pipe(
  map(e => ({ type: 'click', x: (e as MouseEvent).clientX, y: (e as MouseEvent).clientY })),
  take(1),
  concatWith(
    moves$.pipe(
      map(e => ({ type: 'move', x: (e as MouseEvent).clientX, y: (e as MouseEvent).clientY }))
    )
  )
).subscribe(point => drawPoint(point));
```

### Example 3: Running an animation sequence in phases

```typescript
import { of, timer, concatWith, map, switchMap } from 'rxjs';

// Phase 1: fade in over 300ms, Phase 2: hold for 2s, Phase 3: fade out over 300ms
const fadeIn$ = timer(0, 16).pipe(
  map((_, i) => Math.min(i / 18, 1)), // opacity 0 → 1 over ~300ms
  take(19)
);
const hold$ = timer(2000).pipe(map(() => 1));
const fadeOut$ = timer(0, 16).pipe(
  map((_, i) => Math.max(1 - i / 18, 0)), // opacity 1 → 0 over ~300ms
  take(19)
);

fadeIn$.pipe(
  concatWith(hold$, fadeOut$)
).subscribe(opacity => {
  element.style.opacity = String(opacity);
});
```

## Common Pitfalls

- **Source must complete before the next Observable is subscribed**: If the source Observable never completes (e.g. an unending event stream), the additional sources will never be subscribed to. Use `take` or `takeUntil` to ensure the source completes if needed.
- **Later sources wait regardless of timing**: There is no timeout between sources. If the first source completes after 10 seconds, the second source starts at exactly that point. Plan for the total duration to be the sum of all individual durations.
- **Not for parallel work**: `concatWith` is strictly sequential. For parallel subscriptions, use `mergeWith`. For "latest wins" behaviour, use `switchAll`.

## Related Operators

- `concat` — creation operator equivalent; takes a static list of Observable inputs without a pipe source
- `mergeWith` — subscribes to all sources concurrently rather than sequentially
- `startWith` — a shorthand for prepending synchronous values at the beginning of a stream
- `endWith` — a shorthand for appending synchronous values at the end of a stream
