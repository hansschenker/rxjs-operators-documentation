# repeat — Advanced Patterns

For `repeat` fundamentals, see the core [repeat](./repeat) doc. This page covers polling, conditional restart, and the comparison with `retry`.

---

## `repeat` vs `retry` — The Core Distinction

```typescript
// repeat: re-subscribes after COMPLETE
// retry:  re-subscribes after ERROR

of(1, 2, 3).pipe(repeat(3)).subscribe(console.log);
// 1, 2, 3, 1, 2, 3, 1, 2, 3 (completes 3 times, then final complete)

throwError(() => new Error()).pipe(retry(3)).subscribe({ error: () => {} });
// errors 3 times, then final error propagates
```

---

## Pattern 1: Simple Polling

```typescript
import { repeat, switchMap, delay } from 'rxjs/operators';
import { timer } from 'rxjs';

// Poll every 5 seconds:
this.api.getStatus().pipe(
  delay(5000),         // wait 5s after each response before re-subscribing
  repeat()             // repeat forever (until unsubscribed)
).subscribe(status => updateStatusBar(status));
```

A cleaner polling pattern uses `timer` directly:

```typescript
timer(0, 5000).pipe(
  switchMap(() => this.api.getStatus())
).subscribe(updateStatusBar);
// Emits immediately, then every 5s
// switchMap cancels in-flight request if next tick arrives (prevents overlap)
```

---

## Pattern 2: `repeat` with Delay (RxJS 7)

RxJS 7 added `delay` to the `repeat` config:

```typescript
import { repeat } from 'rxjs/operators';

// Built-in delay between repeats:
this.api.getStatus().pipe(
  repeat({ delay: 5000 }) // 5s between completions
).subscribe(updateStatus);

// Dynamic delay via function:
this.api.getStatus().pipe(
  repeat({
    delay: (count) => timer(1000 * count) // increasing delay: 1s, 2s, 3s...
  })
).subscribe(updateStatus);
```

---

## Pattern 3: Finite Polling with Count

```typescript
import { repeat } from 'rxjs/operators';

// Poll exactly N times:
this.api.waitForJob(jobId).pipe(
  repeat({ count: 10, delay: 2000 }) // try every 2s, max 10 times
).subscribe({
  next:     status => handleStatus(status),
  complete: ()     => console.log('Max retries reached')
});
```

---

## Pattern 4: Poll Until Condition Met

```typescript
import { repeat, takeWhile, last } from 'rxjs/operators';

// Keep polling until job completes:
this.api.getJobStatus(jobId).pipe(
  repeat({ delay: 3000 }),
  takeWhile(status => status.state !== 'complete', true) // inclusive last
).subscribe({
  next:     s  => updateProgressBar(s.progress),
  complete: () => showJobComplete()
});
```

---

## Pattern 5: Conditional Repeat

```typescript
import { repeat } from 'rxjs/operators';

// Repeat only if a condition is true:
this.api.fetchBatch().pipe(
  repeat({
    delay: (count, lastValue) =>
      lastValue?.hasMore
        ? timer(100)   // more data — fetch next batch quickly
        : NEVER        // no more data — stop repeating
  })
).subscribe(batch => processBatch(batch));
```

---

## Pattern 6: Retry Then Repeat (Resilient Polling)

Combine `retry` (for errors) with `repeat` (for completion):

```typescript
import { retry, repeat, catchError, of } from 'rxjs/operators';

this.api.getStatus().pipe(
  retry({ count: 3, delay: 1000 }),    // retry up to 3x on error
  catchError(() => of({ error: true })), // fallback after retries exhausted
  repeat({ delay: 5000 })              // keep polling every 5s
).subscribe(status => {
  if ('error' in status) showWarning();
  else                   updateUI(status);
});
```

---

## Pattern 7: WebSocket Reconnect

```typescript
import { webSocket } from 'rxjs/webSocket';
import { repeat, retry, delay } from 'rxjs/operators';

const ws$ = webSocket('wss://api.example.com/ws').pipe(
  retry({                              // reconnect on error
    delay: (_, attempt) => timer(Math.min(1000 * 2 ** attempt, 30_000))
  }),
  repeat({ delay: 0 })                 // reconnect on clean close
);

ws$.pipe(takeUntilDestroyed(this.destroyRef))
  .subscribe(handleMessage);
```

---

## `repeat` vs `timer` + `switchMap` — Which to Use

| Approach | When to use |
|---|---|
| `timer(0, interval).pipe(switchMap(fn))` | Fixed interval polling; cancels in-flight on next tick |
| `source$.pipe(repeat({ delay: ms }))` | Re-subscribe N ms *after source completes*; waits for each response |
| `source$.pipe(repeat({ count: N }))` | Finite repetitions |

**Key difference**: `timer`-based polling starts the next poll at a fixed clock interval regardless of response time. `repeat({ delay })` waits for the request to complete, then waits the delay — total cycle = request time + delay.

---

## Common Pitfalls

### `repeat` on Never-Completing Observables

```typescript
// ❌ NEVER REPEATS — interval never completes
interval(1000).pipe(repeat(3)).subscribe(console.log);
// Just 0, 1, 2, 3... forever — repeat has no effect

// ✅ repeat only works on sources that complete
of(1, 2, 3).pipe(repeat(3)).subscribe(console.log); // 1,2,3 three times
// WHY: repeat re-subscribes after complete. A source that never completes
// means repeat's trigger never fires.
```

### Polling Overlap with `repeat` + No Concurrency Guard

```typescript
// ❌ POTENTIAL OVERLAP — if response takes > delay, next poll starts before current finishes
slowApi$.pipe(
  repeat({ delay: 1000 })
).subscribe();
// If slowApi$ takes 3s: polls at 0s, 4s, 8s... (waits for completion + 1s)
// This is actually fine for repeat — it waits for complete before re-subscribing

// vs timer-based: polls at exactly 0s, 1s, 2s, 3s...
// timer(0, 1000).pipe(switchMap(() => slowApi$)) cancels previous if slow
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `repeat` is `retry`'s counterpart — one handles success loops, the other error recovery. For polling, `repeat({ delay: ms })` (wait after completion) and `timer(0, ms).pipe(switchMap(...))` (fixed interval) have different timing semantics — choose based on whether you want "time since last response" or "time since last start."
