# throttle

**Category**: Filtering  
**Import**: `import { throttle } from 'rxjs';`

## Description

`throttle` emits a source value, then ignores subsequent source values for a duration determined by an Observable returned by `durationSelector`. When the duration Observable emits or completes, the throttle window ends and the next source value can be emitted.

By default (`{ leading: true, trailing: false }`), the first value in each window is emitted and subsequent values are suppressed. The `leading` and `trailing` config options give full control:

- `leading: true` — emit the first value in the window (default)
- `trailing: true` — emit the last value in the window when it ends
- Both can be `true` simultaneously, causing both the first and last value to be emitted

## Signature

```typescript
function throttle<T>(
  durationSelector: (value: T) => ObservableInput<any>,
  config?: ThrottleConfig
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| durationSelector | `(value: T) => ObservableInput<any>` | A function called with each emitted value that returns an Observable defining the suppression window. |
| config | `ThrottleConfig` | Optional. `{ leading?: boolean, trailing?: boolean }`. Defaults to `{ leading: true, trailing: false }`. |

## Return Type

`MonoTypeOperatorFunction<T>` — rate-limited emissions from the source.

## Marble Diagram

```
Source:   --a--b--c--------d--e--|
Duration: -----|           -----|
          throttle(durationSelector)   (leading: true, trailing: false)
Output:   --a--------------d---------|

          throttle(durationSelector, { leading: false, trailing: true })
Output:   -------c--------------e---|
```

## Examples

### Example 1: Limit click rate to at most one click per second

```typescript
import { fromEvent, interval } from 'rxjs';
import { throttle } from 'rxjs';

fromEvent(document, 'click').pipe(
  throttle(() => interval(1000))
).subscribe(ev => console.log('Click processed:', ev));

// The first click is always processed; subsequent clicks within 1s are suppressed
```

### Example 2: Throttle with both leading and trailing edges

```typescript
import { Subject, interval } from 'rxjs';
import { throttle } from 'rxjs';

const events$ = new Subject<string>();

events$.pipe(
  throttle(() => interval(500), { leading: true, trailing: true })
).subscribe(ev => console.log('Event:', ev));

events$.next('A'); // emitted immediately (leading)
events$.next('B'); // suppressed
events$.next('C'); // suppressed
// After 500ms: 'C' is emitted (trailing — most recent during window)
```

### Example 3: Dynamic throttle window based on event type

```typescript
import { fromEvent, interval, timer } from 'rxjs';
import { throttle, map } from 'rxjs';

const clicks$ = fromEvent<MouseEvent>(document, 'click');

clicks$.pipe(
  throttle(ev => {
    // Double-clicks get a longer cooldown
    return ev.detail === 2 ? timer(2000) : timer(500);
  }),
  map(ev => `${ev.detail === 2 ? 'Double' : 'Single'} click`)
).subscribe(console.log);
```

## Common Pitfalls

- **Default is leading edge only**: By default, only the first value in each window is emitted. Intermediate values are permanently dropped. Set `trailing: true` if you need the last value.
- **Duration Observable is created from the emitted value**: The `durationSelector` receives the value that was just emitted (or suppressed, for trailing). Ensure it returns a consistent Observable type.
- **`trailing: true` without `leading: true` skips the first value**: With `{ leading: false, trailing: true }`, the first value in a window is held until the window ends, delaying all output.

## Related Operators

- `throttleTime` — like `throttle` with a fixed millisecond duration
- `audit` — always emits the last value (equivalent to `{ leading: false, trailing: true }`)
- `debounce` — emits only after a silence period
- `sample` — emits the most recent value when a notifier fires
