# audit

**Category**: Filtering  
**Import**: `import { audit } from 'rxjs';`

## Description

`audit` ignores source values for a duration determined by an Observable returned by a `durationSelector` function. When the duration Observable emits, the most recent source value (if any arrived during the silence window) is emitted. Then the process repeats for the next source value.

The key behavioral difference from `throttle` is about *which* value is emitted: `throttle` emits the **first** value in a window (leading edge); `audit` emits the **last** value in a window (trailing edge, like `auditTime`). Both suppress intermediate values.

If the source completes while a duration is active and a value is pending, that last value is emitted before completion.

## Signature

```typescript
function audit<T>(durationSelector: (value: T) => ObservableInput<any>): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| durationSelector | `(value: T) => ObservableInput<any>` | A function called with each source value that returns an Observable (or Promise, etc.) defining the silence duration. When this inner Observable emits, the most recent source value is forwarded. |

## Return Type

`MonoTypeOperatorFunction<T>` — rate-limited emissions, always the most recent value at the end of each window.

## Marble Diagram

```
Source:   --a--b--c--------d--e--|
Duration: --------|        ------|
          audit(durationSelector)
Output:   ---------c            e|
          (most recent value when duration fires)
```

## Examples

### Example 1: Rate-limit click events (emit most recent)

```typescript
import { fromEvent, interval } from 'rxjs';
import { audit } from 'rxjs';

const clicks$ = fromEvent(document, 'click');

clicks$.pipe(
  audit(() => interval(1000))
).subscribe(ev => console.log('Audited click:', ev));

// If user clicks rapidly, only the most recent click per 1-second window is emitted
```

### Example 2: Dynamic duration based on the value

```typescript
import { Subject, interval } from 'rxjs';
import { audit } from 'rxjs';

const priority$ = new Subject<number>();

priority$.pipe(
  audit(priority => interval(priority * 100)) // higher priority = shorter window
).subscribe(p => console.log('Priority processed:', p));

priority$.next(5);  // window = 500ms
priority$.next(3);  // window = 300ms
priority$.next(1);  // window = 100ms
```

### Example 3: Smooth out rapid scroll events

```typescript
import { fromEvent, animationFrames } from 'rxjs';
import { audit, map } from 'rxjs';

const scroll$ = fromEvent(window, 'scroll');

scroll$.pipe(
  audit(() => animationFrames()), // align with animation frame
  map(() => window.scrollY)
).subscribe(y => {
  // Update UI at most once per animation frame
  document.getElementById('scroll-indicator')!.textContent = `Scroll: ${y}px`;
});
```

## Common Pitfalls

- **First value is not emitted immediately**: Unlike `throttle` (with `leading: true`), `audit` always waits for the duration Observable to emit before forwarding any value. The first value in a burst is held until the duration fires.
- **Values during the duration window are dropped**: Only the most recent value when the timer fires is emitted. If you need all values but rate-limited, consider `bufferTime` or `windowTime`.
- **If source completes before duration**: The last pending value is emitted before the completion notification is forwarded.

## Related Operators

- `auditTime` — like `audit` with a fixed millisecond duration
- `throttle` — emits the first value in each window (leading edge)
- `throttleTime` — `throttle` with a fixed duration
- `debounce` — only emits after a silence period (no activity, not periodic)
- `sample` — emits the most recent value when a notifier fires
