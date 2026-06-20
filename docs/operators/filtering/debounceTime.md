# debounceTime

**Category**: Filtering  
**Import**: `import { debounceTime } from 'rxjs';`

## Description

`debounceTime` delays each source emission and only forwards it if no newer value arrives within the `dueTime` window. Whenever a new value arrives before the timer expires, the timer resets and the previous pending value is discarded. The most recent value is emitted only after the source has been silent for `dueTime` milliseconds.

This is the idiomatic operator for implementing search-as-you-type, input field validation, and other scenarios where you want to react only once the user has paused. If the source completes while a value is pending, that value is emitted before the completion notification.

## Signature

```typescript
function debounceTime<T>(dueTime: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| dueTime | `number` | The silence duration in milliseconds. The value is emitted only after this many milliseconds pass without a new source emission. |
| scheduler | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. Useful for testing with `TestScheduler`. |

## Return Type

`MonoTypeOperatorFunction<T>` — debounced emissions; only the latest value after each silence period.

## Marble Diagram

```
Source:  --a-b-c-----------d-e--|
         debounceTime(3)
Output:  -----------c-----------e|

Time:    0 1 2 3 4 5 6 7 8 9 ...
Source:  a b c
Timers:  [---] [---] [---]       <-- resets on each new value
Output:            c             <-- only after 3 units of silence
```

## Examples

### Example 1: Search-as-you-type with debounce

```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, map, distinctUntilChanged } from 'rxjs';

const searchInput = document.getElementById('search') as HTMLInputElement;

fromEvent(searchInput, 'input').pipe(
  debounceTime(300),
  map(ev => (ev.target as HTMLInputElement).value),
  distinctUntilChanged()
).subscribe(query => {
  console.log('Searching for:', query);
  // Call API here
});
```

### Example 2: Auto-save form after user stops typing

```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, map } from 'rxjs';

const textarea = document.getElementById('notes') as HTMLTextAreaElement;

fromEvent(textarea, 'input').pipe(
  debounceTime(1000),
  map(ev => (ev.target as HTMLTextAreaElement).value)
).subscribe(content => {
  console.log('Auto-saving:', content.substring(0, 20) + '...');
  // POST to /api/save
});
```

### Example 3: Debounce resize events for expensive calculations

```typescript
import { fromEvent } from 'rxjs';
import { debounceTime } from 'rxjs';

fromEvent(window, 'resize').pipe(
  debounceTime(200)
).subscribe(() => {
  console.log('Window dimensions:', window.innerWidth, 'x', window.innerHeight);
  // Re-run expensive layout calculations
});
```

## Common Pitfalls

- **Rapid bursts drop all intermediate values**: Only the last value in a burst is emitted. If you need every value but at a controlled rate, use `throttleTime` or `auditTime` instead.
- **Introduces a delay**: Even when only one value arrives, it will always be delayed by `dueTime`. This affects perceived responsiveness in time-critical UIs.
- **Source completion emits the pending value**: If the source completes before the debounce timer fires, the pending value is emitted synchronously before the completion notification.
- **Use `distinctUntilChanged` after debounce**: Pairing `debounceTime` with `distinctUntilChanged` avoids firing when the value did not actually change (e.g., user types and deletes back to the same text).

## Related Operators

- `debounce` — like `debounceTime` but with a dynamic duration Observable per value
- `throttleTime` — emits immediately on the first value, then ignores for `duration`
- `auditTime` — emits the most recent value after a fixed window (not on silence)
- `sampleTime` — samples the source at regular periodic intervals
