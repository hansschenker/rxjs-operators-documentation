# takeWhile

**Category**: Filtering  
**Import**: `import { takeWhile } from 'rxjs';`

## Description

`takeWhile` emits values from the source Observable as long as each value satisfies a given predicate function. The moment a value fails the predicate, the output Observable completes — that failing value is dropped by default, but can be included by setting the `inclusive` option to `true`.

Unlike `filter`, which continues to watch the stream after a value is rejected, `takeWhile` terminates the subscription as soon as the predicate first returns `false`.

## Signature

```typescript
function takeWhile<T>(
  predicate: (value: T, index: number) => boolean,
  inclusive?: boolean
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number) => boolean` | A function that evaluates each emitted value. Emission continues as long as this returns `true`. |
| inclusive | `boolean` | Optional (default `false`). When `true`, the value that caused the predicate to return `false` is also emitted before completion. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits values while the predicate holds, then completes.

## Marble Diagram

```
Source: --1--2--3--4--5--|
        takeWhile(x => x < 4)
Output: --1--2--3|

Source: --1--2--3--4--5--|
        takeWhile(x => x < 4, true)   (inclusive)
Output: --1--2--3--4|
```

## Examples

### Example 1: Track mouse position while button is held

```typescript
import { fromEvent } from 'rxjs';
import { takeWhile, map } from 'rxjs';

const pointerMove$ = fromEvent<PointerEvent>(document, 'pointermove');

pointerMove$.pipe(
  takeWhile(ev => ev.buttons === 1), // left mouse button held
  map(ev => ({ x: ev.clientX, y: ev.clientY }))
).subscribe({
  next: pos => console.log('Dragging at:', pos),
  complete: () => console.log('Button released, tracking stopped')
});
```

### Example 2: Read from a stream until a sentinel value

```typescript
import { Subject } from 'rxjs';
import { takeWhile } from 'rxjs';

const messages$ = new Subject<string>();

messages$.pipe(
  takeWhile(msg => msg !== 'STOP')
).subscribe({
  next: msg => console.log('Message:', msg),
  complete: () => console.log('Received stop signal')
});

messages$.next('hello');
messages$.next('world');
messages$.next('STOP');   // causes completion, not emitted
messages$.next('after');  // never received

// Logs: Message: hello, Message: world, Received stop signal
```

### Example 3: Include the terminating value with `inclusive: true`

```typescript
import { interval } from 'rxjs';
import { takeWhile } from 'rxjs';

interval(200).pipe(
  takeWhile(n => n < 5, true) // emit 0,1,2,3,4,5 then complete
).subscribe({
  next: n => console.log(n),
  complete: () => console.log('Done')
});
// Logs: 0, 1, 2, 3, 4, 5, Done
```

## Common Pitfalls

- **Confusing `takeWhile` with `filter`**: `takeWhile` completes the stream permanently when the predicate fails. Use `filter` if you want to simply drop non-matching values and keep listening.
- **Forgetting `inclusive`**: By default, the value that fails the predicate is dropped. If your protocol requires emitting that final boundary value (e.g., a "DONE" sentinel), set `inclusive: true`.
- **Index resets on each subscription**: The `index` parameter is per-subscription. Resubscribing to the same Observable starts the index at `0` again.

## Related Operators

- `takeUntil` — completes based on an external Observable notifier
- `take` — emits exactly N values regardless of their content
- `filter` — drops values without terminating the stream
- `skipWhile` — the complement: skips values while the predicate holds
