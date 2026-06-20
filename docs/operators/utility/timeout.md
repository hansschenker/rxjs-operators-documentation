# timeout

**Category**: Utility  
**Import**: `import { timeout } from 'rxjs';`

## Description

`timeout` errors (or switches to a fallback observable) if the source does not emit a value within a specified time budget. It is the most flexible timing-guard in RxJS, supporting both simple millisecond thresholds and rich configuration objects.

The configuration object accepts:
- `first`: maximum time (ms or `Date`) to wait for the first value
- `each`: maximum time (ms) allowed between any two consecutive values
- `with`: a factory function returning a fallback observable to switch to instead of throwing a `TimeoutError`
- `scheduler`: defaults to `asyncScheduler`
- `meta`: arbitrary metadata attached to the `TimeoutError` for diagnostics

Without a `with` factory, a `TimeoutError` is thrown. You can inspect `error.info` (`{ seen, lastValue, meta }`) inside a `catchError` handler to understand what triggered the timeout.

## Signature

```typescript
// Simple milliseconds threshold (equivalent to `{ each: ms }`)
function timeout<T>(each: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>

// Simple Date threshold (equivalent to `{ first: date }`)
function timeout<T>(first: Date, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>

// Full configuration without fallback — emits TimeoutError
function timeout<T, M = unknown>(config: Omit<TimeoutConfig<T, any, M>, 'with'>): OperatorFunction<T, T>

// Full configuration with fallback observable
function timeout<T, O extends ObservableInput<unknown>, M = unknown>(
  config: TimeoutConfig<T, O, M> & { with: (info: TimeoutInfo<T, M>) => O }
): OperatorFunction<T, T | ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| config | `number \| Date \| TimeoutConfig` | Time budget in ms, a specific `Date`, or a full config object. |
| scheduler | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. Only used with the simple overloads. |

### `TimeoutConfig` properties

| Property | Type | Description |
|----------|------|-------------|
| `each` | `number` | Max ms between any two values (or first value if `first` is not set). |
| `first` | `number \| Date` | Max ms (or exact `Date`) before the first value must arrive. |
| `with` | `(info: TimeoutInfo<T, M>) => ObservableInput` | Factory returning a fallback observable on timeout. |
| `scheduler` | `SchedulerLike` | Scheduler for timers. Defaults to `asyncScheduler`. |
| `meta` | `M` | Arbitrary metadata included in the `TimeoutError`. |

## Return Type

`OperatorFunction<T, T | ObservedValueOf<O>>` — mirrors the source until a timeout condition is met, then either errors with `TimeoutError` or switches to the observable returned by `with`.

## Marble Diagram

```
Source:  --a--------b--...(silent)...
         timeout({ each: 5000 })
Output:  --a--------b--X  (TimeoutError after 5s of silence)
```

## Examples

### Example 1: Error if source is silent for too long

```typescript
import { interval, timeout, catchError, of } from 'rxjs';

const slowSource$ = interval(2000); // emits every 2s

slowSource$.pipe(
  timeout({ each: 3000 }), // allow up to 3s between values
  catchError(err => {
    console.error('Timed out:', err.message);
    return of(-1);
  })
).subscribe(console.log);
```

### Example 2: Switch to a fallback observable on timeout

```typescript
import { interval, timeout } from 'rxjs';

const slow$ = interval(2000);
const fast$ = interval(500);

slow$.pipe(
  timeout({
    each: 1500,
    with: (info) => {
      console.log(`Timed out after ${info.seen} values. Switching to fast$.`);
      return fast$;
    }
  })
).subscribe(console.log);
```

### Example 3: Require first value within 5 seconds, then allow up to 3 seconds between values

```typescript
import { timer, timeout, expand } from 'rxjs';

const getRandomTime = () => Math.round(Math.random() * 6000);

const source$ = timer(getRandomTime()).pipe(
  expand(() => timer(getRandomTime()))
);

source$.pipe(
  timeout({ first: 5000, each: 3000 })
).subscribe({
  next: n => console.log('Value:', n),
  error: err => console.error('Timeout:', err.info)
});
```

## Common Pitfalls

- **`first` vs `each`**: `first` applies only to the initial value. `each` applies to every subsequent value (or the first too, when `first` is not provided). Mixing them incorrectly leads to unexpected behavior.
- **Providing neither `first` nor `each`**: This throws a `TypeError` at runtime. At least one must be specified.
- **Error information**: Without `with`, the thrown `TimeoutError` has an `info` property (`{ seen, lastValue, meta }`). Always use `instanceof TimeoutError` to distinguish timeout errors from other errors in `catchError`.
- **`timeoutWith` is deprecated**: Prefer the `with` option inside the `timeout` configuration object instead.

## Related Operators

- `timeoutWith` — deprecated; use `timeout({ with: ... })` instead
- `catchError` — handle the `TimeoutError` gracefully
- `delay` — shift emissions without erroring
- `debounceTime` — wait for a quiet period but don't error on silence
