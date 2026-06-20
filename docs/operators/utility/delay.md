# delay

**Category**: Utility  
**Import**: `import { delay } from 'rxjs';`

## Description

`delay` time-shifts every emission from the source Observable by a fixed duration. If passed a `number`, each emitted value is held for that many milliseconds before being forwarded downstream; the relative time intervals between values are preserved. If passed a `Date`, the entire start of the observable execution is delayed until that point in time.

This operator is useful when you need to simulate latency, stagger requests, or hold off a UI reaction until a short pause has elapsed. Unlike `observeOn`, `delay` does not reschedule error notifications — errors pass through immediately.

## Signature

```typescript
function delay<T>(due: number | Date, scheduler: SchedulerLike = asyncScheduler): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| due | `number \| Date` | The delay in milliseconds (a `number`) or the exact `Date` at which to begin emitting. |
| scheduler | `SchedulerLike` | The scheduler used to manage the delay timers. Defaults to `asyncScheduler`. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable emitting the same values as the source, each shifted by the specified delay.

## Marble Diagram

```
Source:  --a----b--c--|
         delay(2000)
Output:  ------a----b--c--|
                           (each value shifted ~2s to the right)
```

## Examples

### Example 1: Simulate network latency in tests

```typescript
import { of, delay } from 'rxjs';

function fakeApiCall(id: number) {
  return of({ id, data: 'result' }).pipe(delay(500));
}

fakeApiCall(42).subscribe(response => {
  console.log('Received after 500ms:', response);
});
// Received after 500ms: { id: 42, data: 'result' }
```

### Example 2: Debounce a UI notification banner

```typescript
import { Subject, delay } from 'rxjs';

const saveSuccess$ = new Subject<string>();

saveSuccess$.pipe(
  delay(300) // give the UI 300ms to settle before showing feedback
).subscribe(msg => showBanner(msg));

function showBanner(message: string) {
  console.log('Banner:', message);
}

saveSuccess$.next('Changes saved!');
```

### Example 3: Delay start until a specific date

```typescript
import { interval, delay, take } from 'rxjs';

const launchDate = new Date('2030-01-01T00:00:00Z');

interval(1000).pipe(
  take(5),
  delay(launchDate)
).subscribe(n => console.log('Post-launch tick:', n));
```

## Common Pitfalls

- **Errors are not delayed**: Unlike `observeOn`, `delay` passes error notifications through immediately without applying the delay. If you need all notifications (including errors) delayed, use `observeOn(asyncScheduler, delayMs)`.
- **Infinite observables and `Date` delay**: Using a `Date` in the past has no effect — the observable subscribes immediately.
- **Memory buildup**: If the source emits faster than the delay period, buffered values accumulate in memory. Combine with operators like `throttleTime` if backpressure is a concern.

## Related Operators

- `delayWhen` — delay each value by a duration determined by a per-value observable
- `throttleTime` — drop values that arrive too quickly rather than buffering them
- `debounceTime` — wait for a quiet period before forwarding the latest value
- `observeOn` — reschedule all notifications (including errors) onto a scheduler
