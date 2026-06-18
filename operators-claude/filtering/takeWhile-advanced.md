# takeWhile — Advanced Patterns

For `takeWhile` fundamentals see the core [takeWhile](./takeWhile) doc. This page covers the `inclusive` flag, state machine teardown, condition derived from external streams, and `takeWhile` vs `takeUntil` tradeoffs.

---

## The `inclusive` Flag — The Most Important Option

Without `inclusive: true`, the value that fails the predicate is discarded. With it, that value is emitted before the stream completes:

```typescript
import { takeWhile } from 'rxjs/operators';

// Without inclusive (default):
// Source: 1--2--3--4--5--|
// takeWhile(x => x < 4):   1--2--3--|    ← 4 discarded

// With inclusive:
// takeWhile(x => x < 4, true): 1--2--3--4|   ← 4 emitted then complete
```

```typescript
// Load data until sentinel value received:
dataStream$.pipe(
  takeWhile(item => item.type !== 'END_OF_STREAM', true) // emit END_OF_STREAM, then complete
).subscribe({
  next:     item => item.type !== 'END_OF_STREAM' && processItem(item),
  complete: () => finalize()
});
```

---

## Pattern 1: Progress Bar Until 100%

```typescript
import { interval } from 'rxjs';
import { scan, map, takeWhile } from 'rxjs/operators';

const progress$ = interval(100).pipe(
  scan(pct => Math.min(pct + Math.random() * 5, 100), 0),
  takeWhile(pct => pct < 100, true) // inclusive: emit 100 then complete
);

progress$.subscribe({
  next:     pct => updateProgressBar(pct),
  complete: () => showSuccess()
});
```

---

## Pattern 2: Poll Until Condition Met

```typescript
import { timer } from 'rxjs';
import { switchMap, takeWhile, tap } from 'rxjs/operators';

function pollUntilComplete(jobId: string): Observable<JobStatus> {
  return timer(0, 2000).pipe(
    switchMap(() => this.api.getJobStatus(jobId)),
    takeWhile(
      status => status.state !== 'complete' && status.state !== 'failed',
      true  // emit final status (complete/failed) before completing
    ),
    tap(status => {
      if (status.progress) updateProgress(status.progress);
    })
  );
}

pollUntilComplete('job-123').subscribe({
  next: status => {
    if (status.state === 'complete') showResult(status.result);
    if (status.state === 'failed')  showError(status.error);
  }
});
```

---

## Pattern 3: Retry Budget — Stop Retrying After N Failures

```typescript
import { throwError, defer } from 'rxjs';
import { catchError, scan, takeWhile, switchMap } from 'rxjs/operators';

function withRetryBudget<T>(
  factory: () => Observable<T>,
  maxAttempts: number
): Observable<T> {
  return defer(factory).pipe(
    catchError((err, attempt$) =>
      of(null).pipe(
        scan(count => count + 1, 0),
        takeWhile(count => count < maxAttempts),
        switchMap(count => {
          logger.warn(`Attempt ${count + 1}/${maxAttempts} failed`);
          return timer(1000 * count); // backoff
        }),
        switchMap(() => attempt$)     // retry
      )
    )
  );
}
```

---

## Pattern 4: State Machine — Run Until Terminal State

```typescript
import { BehaviorSubject } from 'rxjs';
import { takeWhile, map } from 'rxjs/operators';

type AppState = 'idle' | 'loading' | 'success' | 'error';

const TERMINAL_STATES: AppState[] = ['success', 'error'];

const state$ = new BehaviorSubject<AppState>('idle');

// Run pipeline until terminal state reached:
state$.pipe(
  takeWhile(
    s => !TERMINAL_STATES.includes(s),
    true  // emit the terminal state before completing
  )
).subscribe({
  next: state => {
    switch (state) {
      case 'idle':    showIdle();    break;
      case 'loading': showSpinner(); break;
      case 'success': showResult();  break;
      case 'error':   showError();   break;
    }
  },
  complete: () => cleanupResources()
});
```

---

## Pattern 5: Countdown Timer

```typescript
import { interval } from 'rxjs';
import { map, takeWhile } from 'rxjs/operators';

function countdown(seconds: number): Observable<number> {
  return interval(1000).pipe(
    map(i => seconds - i - 1),     // seconds remaining
    takeWhile(remaining => remaining >= 0, true) // inclusive: emit 0
  );
}

countdown(30).subscribe({
  next:     remaining => updateCountdown(remaining),
  complete: () => onTimeExpired()
});
```

---

## Pattern 6: `takeWhile` with External Condition

When the "while" condition depends on another stream, compose with `withLatestFrom`:

```typescript
import { withLatestFrom, filter, takeWhile } from 'rxjs/operators';

const featureEnabled$ = new BehaviorSubject(true);

// Only process events while feature is enabled:
userActions$.pipe(
  withLatestFrom(featureEnabled$),
  takeWhile(([, enabled]) => enabled),     // stop when feature disabled
  map(([action]) => action)                // unwrap
).subscribe(processAction);

// Note: this STOPS the stream when disabled. To PAUSE (resume later), use:
userActions$.pipe(
  withLatestFrom(featureEnabled$),
  filter(([, enabled]) => enabled),        // skip when disabled, stream continues
  map(([action]) => action)
).subscribe(processAction);
```

---

## `takeWhile` vs `takeUntil` vs `filter`

```typescript
// takeWhile — checks each value against a predicate; stops when false:
source$.pipe(takeWhile(x => x < 10))
// ✓ Condition is derived from the values themselves
// ✗ Once it stops, it's done — can't restart

// takeUntil — stops when an external notifier emits:
source$.pipe(takeUntil(stop$))
// ✓ Condition is external (user action, lifecycle event, timer)
// ✓ More decoupled — stop signal can come from anywhere

// filter — keeps values matching predicate, stream continues:
source$.pipe(filter(x => x < 10))
// ✓ Skips non-matching values but stream keeps going
// ✓ More values may still come after a non-matching one

// When to use which:
// Values contain their own "stop" signal  → takeWhile
// External event signals completion       → takeUntil
// Some values are irrelevant but more come → filter
```

---

## Combining `takeWhile` with `share` for Multi-Subscriber Scenarios

```typescript
// When multiple subscribers share a stream with takeWhile,
// ensure the source is shared to avoid multiple subscriptions:

const sharedSource$ = source$.pipe(
  share()
);

// Multiple consumers:
sharedSource$.pipe(takeWhile(x => x < 100)).subscribe(consumerA);
sharedSource$.pipe(takeWhile(x => x < 50)).subscribe(consumerB);
// consumerB stops at 50, consumerA at 100 — they share the source subscription
```

---

## Common Pitfalls

### Missing `inclusive: true` for Terminal Value Processing

```typescript
// ❌ Without inclusive — terminal value silently discarded:
statusStream$.pipe(
  takeWhile(s => s.status !== 'done')
).subscribe({
  next:     renderStatus,
  complete: () => handleDone() // s.status === 'done' was never processed!
});

// ✅ With inclusive — terminal value emitted, then stream completes:
statusStream$.pipe(
  takeWhile(s => s.status !== 'done', true)
).subscribe({
  next: s => {
    if (s.status === 'done') handleDone(s.result); // handle it in next
    else                     renderStatus(s);
  }
});
```

### `takeWhile` Predicate Has Side Effects

```typescript
// ❌ Side effects in predicate run on the discarded value too:
source$.pipe(
  takeWhile(x => {
    logger.log('Checking:', x); // logs even for the stopping value
    return x < 10;
  })
)

// ✅ Move side effects to tap:
source$.pipe(
  tap(x => logger.log('Checking:', x)),
  takeWhile(x => x < 10)
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**The one thing to remember**: `takeWhile(pred, true)` — the second argument `inclusive: true` emits the boundary value before completing. Without it, the value that causes the stream to stop is silently discarded. Any time you need to act on the "stop" condition (display final state, finalize, send the terminal event), use `inclusive: true`.
