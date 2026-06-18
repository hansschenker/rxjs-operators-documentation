# delay / delayWhen

## Identity

| | `delay` | `delayWhen` |
|---|---|---|
| **Import** | `import { delay } from 'rxjs/operators'` | `import { delayWhen } from 'rxjs/operators'` |
| **Signature** | `delay<T>(due, scheduler?): MonoTypeOperatorFunction<T>` | `delayWhen<T>(delayDurationSelector, subscriptionDelay?): MonoTypeOperatorFunction<T>` |
| **Category** | Utility Operators | Utility Operators |
| **Delay type** | Fixed duration (ms) or absolute Date | Per-value dynamic duration (Observable-based) |

```typescript
function delay<T>(
  due: number | Date,
  scheduler?: SchedulerLike
): MonoTypeOperatorFunction<T>

function delayWhen<T>(
  delayDurationSelector: (value: T, index: number) => Observable<any>,
  subscriptionDelay?: Observable<any>
): MonoTypeOperatorFunction<T>
```

## Functional Specification

**`delay(due)`**: Shifts the entire Observable timeline by a fixed duration. Every value and the completion notification are delayed by `due` milliseconds relative to when they were originally emitted. Errors are NOT delayed — they pass through immediately.

**`delayWhen(delayDurationSelector)`**: Per-value dynamic delay. For each value `v`, calls `delayDurationSelector(v, index)` which returns an Observable. The value `v` is emitted downstream when that Observable emits its first value (or completes). This allows each value to have its own independent delay duration.

**Key invariants for `delay`**:
- Relative timing between values is preserved (values maintain their spacing)
- Completion is also delayed
- Source errors pass through immediately (not delayed)
- Unsubscribing during the delay window cancels pending emissions

**Key invariants for `delayWhen`**:
- Each value gets its own delay Observable — they run concurrently
- The delay Observable's first emission (or completion) triggers the value
- Values can overtake each other if their delay durations differ
- The `subscriptionDelay` parameter delays the subscription to the source itself

## Marble Diagrams

```
Source:  --a-----b--c--|

delay(20ms):
Result:  ----a-----b--c--|     (entire timeline shifted right by 20ms)

Note: spacing between a, b, c is preserved; completion also delayed.
Source errors pass through immediately:

Source:  --a--#
delay(20ms):
Result:  ----a--#    (a is delayed, error is NOT delayed — passes through at once)

delayWhen(v => timer(v * 100)):  (each value delays itself proportionally)

Source:  (a=1)(b=2)(c=3) all synchronous

delayWhen result:  --a(100ms)----b(200ms)------c(300ms)|
                   values arrive in order (a=100ms, b=200ms, c=300ms)

delayWhen(v => timer(v.priority === 'high' ? 0 : 500)):
                   high-priority values emitted immediately;
                   low-priority values delayed 500ms
```

## Behavioral Characteristics

**Scheduler**: `delay` uses `asyncScheduler` by default. Pass `animationFrameScheduler` for visual animations or `VirtualTimeScheduler` for tests.

**Concurrency**: `delayWhen` runs all per-value delay Observables concurrently. Values can arrive out of order if delays differ.

**Memory**: All delayed values are held in a buffer until their delay expires. For high-throughput sources with large delays, this can grow.

## Type System Integration

```typescript
import { of, timer } from 'rxjs';
import { delay, delayWhen } from 'rxjs/operators';

// delay — type preserved, same T in/out
of(1, 2, 3).pipe(delay(1000)).subscribe((v: number) => console.log(v));

// delayWhen — type preserved; the delay Observable's type is discarded
of('fast', 'slow').pipe(
  delayWhen(s => timer(s === 'fast' ? 0 : 2000))
).subscribe((v: string) => console.log(v));
// 'fast' emitted at ~0ms, 'slow' emitted at ~2000ms
```

## Examples

### Basic — Fixed Delay
```typescript
import { of, fromEvent } from 'rxjs';
import { delay } from 'rxjs/operators';

// Delay all values by 1 second
of('hello', 'world').pipe(delay(1000)).subscribe(console.log);
// (after 1s) hello
// (after 1s) world   (both arrive 1s after their original emission)

// Delay until a specific time (Date)
const fiveMinutesFromNow = new Date(Date.now() + 5 * 60 * 1000);
notifications$.pipe(delay(fiveMinutesFromNow)).subscribe(showNotification);
```

### Common Pattern — UX Debounce Feedback (Delayed Spinner)
```typescript
import { of, EMPTY, timer } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

// Show loading spinner only if request takes > 200ms
// (avoids spinner flash for fast responses)
function withDelayedSpinner<T>(source$: Observable<T>): Observable<T> {
  const spinnerTimer$ = timer(200).pipe(
    switchMap(() => {
      showSpinner();
      return EMPTY;
    })
  );

  return new Observable<T>(subscriber => {
    const spinnerSub = spinnerTimer$.subscribe();
    return source$.subscribe({
      next: v => { spinnerSub.unsubscribe(); hideSpinner(); subscriber.next(v); },
      error: e => { spinnerSub.unsubscribe(); hideSpinner(); subscriber.error(e); },
      complete: () => subscriber.complete()
    });
  });
}
```

### Common Pattern — Retry Backoff With `delayWhen`
```typescript
import { timer, throwError } from 'rxjs';
import { retryWhen, delayWhen, mergeMap } from 'rxjs/operators';

function withExponentialBackoff<T>(maxRetries = 3) {
  return (source$: Observable<T>) => source$.pipe(
    retryWhen(errors$ =>
      errors$.pipe(
        mergeMap((err, attempt) => {
          if (attempt >= maxRetries) return throwError(() => err);
          return of(err);
        }),
        delayWhen((_, attempt) => timer(Math.pow(2, attempt) * 1000))
        // attempt 0 → wait 1s, attempt 1 → wait 2s, attempt 2 → wait 4s
      )
    )
  );
}

apiCall$.pipe(withExponentialBackoff(3)).subscribe(handleResult);
```

### `delayWhen` — Priority Queue Pattern
```typescript
import { Subject, timer } from 'rxjs';
import { delayWhen } from 'rxjs/operators';

interface Task { id: number; priority: 'high' | 'medium' | 'low'; }

const tasks$ = new Subject<Task>();
const DELAY_MAP = { high: 0, medium: 500, low: 2000 };

tasks$.pipe(
  delayWhen(task => timer(DELAY_MAP[task.priority]))
).subscribe(task => processTask(task));

tasks$.next({ id: 1, priority: 'low' });    // processed at ~2000ms
tasks$.next({ id: 2, priority: 'high' });   // processed at ~0ms — arrives FIRST
tasks$.next({ id: 3, priority: 'medium' }); // processed at ~500ms
```

## Common Pitfalls

### Anti-pattern: Expecting `delay` to Delay Errors
```typescript
import { throwError } from 'rxjs';
import { delay } from 'rxjs/operators';

// ❌ MISCONCEPTION — errors are NOT delayed
throwError(() => new Error('oops')).pipe(
  delay(5000)
).subscribe({
  next: v => console.log(v),
  error: e => console.log('error:', e.message) // fires IMMEDIATELY, not after 5s
});
// Output: error: oops  (immediate, not delayed)

// WHY: delay only defers next() and complete() notifications.
// Errors bypass the delay buffer and propagate immediately.
// This is by design — errors should be visible as soon as possible.
// If you need to delay an error, use delayWhen or materialize/dematerialize.
```

### Anti-pattern: Using `delay` in Tests (Real Time)
```typescript
import { fakeAsync, tick } from '@angular/core/testing';
import { delay } from 'rxjs/operators';

// ❌ SLOW TEST — real 1000ms wait
it('emits after delay', (done) => {
  of(42).pipe(delay(1000)).subscribe(v => {
    expect(v).toBe(42);
    done();
  });
}); // takes 1 real second!

// ✅ CORRECT — pass a scheduler for virtual time testing
import { TestScheduler } from 'rxjs/testing';

it('emits after delay', () => {
  const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
  scheduler.run(({ cold, expectObservable }) => {
    const source = cold('a', { a: 42 });
    const result = source.pipe(delay(1000, scheduler));
    expectObservable(result).toBe('1000ms a', { a: 42 });
  });
}); // instant!

// WHY: delay uses asyncScheduler by default — real wall-clock time.
// For tests, inject a VirtualTimeScheduler or use TestScheduler.run()
// so tests execute in virtual time without real waiting.
```

## Related Operators

- **`debounceTime(ms)`**: Delays emission and cancels if another value arrives within the window
- **`throttleTime(ms)`**: Rate-limits by passing one value per window
- **`timer(ms)`**: Creation operator — single emission after a delay
- **`timeout(ms)`**: Errors if no emission arrives within the window (complement of delay)
- **`auditTime(ms)`**: Emits the latest value from a source after a silent period

## References
- **RxJS delay**: [https://rxjs.dev/api/operators/delay](https://rxjs.dev/api/operators/delay)
- **RxJS delayWhen**: [https://rxjs.dev/api/operators/delayWhen](https://rxjs.dev/api/operators/delayWhen)

---

**`delay`** — Cognitive Load: 1/5 | Usage: 3/5 | Key gotcha: errors bypass the delay.
**`delayWhen`** — Cognitive Load: 3/5 | Usage: 2/5 | Key use case: exponential backoff, priority scheduling.
**Teaching sequence**: After `timer` and `debounceTime` — they all involve time, but delay shifts the timeline rather than gating it.
