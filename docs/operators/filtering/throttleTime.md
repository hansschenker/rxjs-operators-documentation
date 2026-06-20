# throttleTime

**Category**: Filtering  
**Import**: `import { throttleTime } from 'rxjs';`

## Description

`throttleTime` emits a source value, then suppresses subsequent source values for `duration` milliseconds. This is the fixed-duration, time-based version of `throttle`.

The default behavior (`{ leading: true, trailing: false }`) emits the first value of each burst and discards the rest during the window. The `ThrottleConfig` options allow configuring leading and/or trailing emission:

- `leading: true` — emit the first value when the window opens (default)
- `trailing: true` — emit the last value when the window closes
- Both can be `true` to emit both edges

## Signature

```typescript
function throttleTime<T>(
  duration: number,
  scheduler?: SchedulerLike,
  config?: ThrottleConfig
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| duration | `number` | Time in milliseconds to suppress source values after an emission. |
| scheduler | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. |
| config | `ThrottleConfig` | Optional. `{ leading?: boolean, trailing?: boolean }`. Defaults to `{ leading: true, trailing: false }`. |

## Return Type

`MonoTypeOperatorFunction<T>` — rate-limited emissions with at most one emission per `duration` window (or two if both `leading` and `trailing` are `true`).

## Marble Diagram

```
Source:  --a-b-c---------d-e-f--|
         throttleTime(3)
Output:  --a-------------d------|
         (leading edge: first in each window)

         throttleTime(3, asyncScheduler, { leading: false, trailing: true })
Output:  ------c--------------f-|
         (trailing edge: last in each window)

Time:    0 1 2 3 4 5 6 7 8 9 10
Source:  a b c       d e f
Windows: [--3--]      [--3--]
Leading: a            d
Trailing:    c              f
```

## Examples

### Example 1: Limit button click rate

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime } from 'rxjs';

const button = document.getElementById('submit-btn')!;

fromEvent(button, 'click').pipe(
  throttleTime(2000)
).subscribe(() => {
  console.log('Submit triggered');
  // API call happens at most once every 2 seconds
});
```

### Example 2: Throttle scroll events for smooth performance

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, map } from 'rxjs';

fromEvent(window, 'scroll').pipe(
  throttleTime(100),
  map(() => window.scrollY)
).subscribe(y => {
  console.log('Scroll position:', y);
  // Update sticky header visibility
});
```

### Example 3: Capture both leading and trailing edges

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime } from 'rxjs';

// Track rapid keystrokes: emit the first key pressed AND the last key released
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  throttleTime(300, undefined, { leading: true, trailing: true })
).subscribe(ev => console.log('Key event:', ev.key));

// If user types "hello" quickly:
// leading:  'h' emitted immediately
// trailing: 'o' emitted 300ms after 'h'
```

## Common Pitfalls

- **Default drops all but the first value in a window**: Intermediate values are permanently lost. Use `trailing: true` if you need the last value in a burst.
- **`throttleTime` vs `debounceTime`**: `throttleTime` emits the first value immediately and silences for `duration`; `debounceTime` waits until the source is silent for `dueTime` before emitting (so rapid bursts delay all output).
- **Scheduler matters for tests**: In unit tests, use `TestScheduler` and pass it as the second argument to make time-based assertions deterministic.

## Related Operators

- `throttle` — like `throttleTime` but with a dynamic duration Observable
- `auditTime` — always emits the most recent value at the end of the window
- `debounceTime` — emits only after a silence period of `dueTime`
- `sampleTime` — samples at a fixed clock rate regardless of source activity
