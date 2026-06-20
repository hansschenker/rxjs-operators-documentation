# debounce

**Category**: Filtering  
**Import**: `import { debounce } from 'rxjs';`

## Description

`debounce` delays each source emission and only forwards it if no new value arrives before the duration Observable (returned by `durationSelector`) emits. When a new source value arrives, the previous pending emission is cancelled and a new duration Observable is created for the new value.

This is the dynamic-duration counterpart of `debounceTime`. The duration Observable is created fresh from each new source value, enabling variable debounce windows. If the source completes while a value is pending, that value is emitted before completion.

Like `debounceTime`, this is a rate-limiting operator that introduces a delay — output emissions do not occur at the same time as source emissions.

## Signature

```typescript
function debounce<T>(durationSelector: (value: T) => ObservableInput<any>): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| durationSelector | `(value: T) => ObservableInput<any>` | A function called with each source value that returns an Observable (or Promise, etc.) defining the debounce window. The value is emitted only when this inner Observable emits without a new source value arriving first. |

## Return Type

`MonoTypeOperatorFunction<T>` — debounced emissions using a dynamic duration.

## Marble Diagram

```
Source:   --a--b-----c--d--|
Duration: --|      --|--|
          debounce(durationSelector)
Output:   ----b------c--d--|
          (a cancelled by b; b emitted when duration fires)
```

## Examples

### Example 1: Variable debounce that grows with each rapid emission

```typescript
import { fromEvent, scan, interval } from 'rxjs';
import { debounce } from 'rxjs';

const clicks$ = fromEvent(document, 'click');

clicks$.pipe(
  scan(count => count + 1, 0),      // count clicks
  debounce(count => interval(count * 100)) // longer wait after more clicks
).subscribe(count => console.log('Settled at click count:', count));
```

### Example 2: Debounce with a Promise-based duration

```typescript
import { Subject } from 'rxjs';
import { debounce } from 'rxjs';

const search$ = new Subject<string>();

search$.pipe(
  debounce(() => new Promise(resolve => setTimeout(resolve, 300)))
).subscribe(query => {
  console.log('Search for:', query);
  // Trigger API call
});

search$.next('r');
search$.next('rx');
search$.next('rxj');
search$.next('rxjs'); // Only this one fires after 300ms of silence
```

### Example 3: Dynamic debounce based on user input length

```typescript
import { fromEvent } from 'rxjs';
import { debounce, map, interval } from 'rxjs';

const input = document.getElementById('search') as HTMLInputElement;

fromEvent(input, 'input').pipe(
  map(ev => (ev.target as HTMLInputElement).value),
  debounce(query => {
    // Short queries debounce longer (user probably still typing)
    const ms = query.length < 3 ? 500 : 200;
    return interval(ms);
  })
).subscribe(query => console.log('Searching:', query));
```

## Common Pitfalls

- **Every source value cancels the previous pending emission**: Only the most recent value can ever be emitted from a burst. This means rapid streams can lose many values.
- **Source completion flushes the pending value**: If the source completes while a value is being debounced, that value is emitted before the completion notification.
- **Dynamic duration is evaluated per value**: A new inner Observable is created for every source value. Ensure `durationSelector` is efficient and does not create expensive subscriptions.

## Related Operators

- `debounceTime` — like `debounce` with a fixed millisecond duration
- `audit` — emits the most recent value when the duration fires (not on silence)
- `throttle` — emits the first value in a window (leading edge)
- `sample` — emits the most recent value when a notifier fires
