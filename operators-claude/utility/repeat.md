# repeat

## Identity

- **Name**: repeat
- **Category**: Utility Operators
- **Type**: Completion-driven resubscription — resubscribes to the source when it completes, creating a repeating sequence
- **Import**:
  ```typescript
  import { repeat } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // Simple form
  function repeat<T>(count?: number): MonoTypeOperatorFunction<T>

  // Config form (RxJS 7+)
  function repeat<T>(config: RepeatConfig): MonoTypeOperatorFunction<T>

  interface RepeatConfig {
    count?: number;   // max repetitions (default: Infinity)
    delay?: number | ((count: number) => ObservableInput<any>);
  }
  ```

## Functional Specification

**Concept**: `repeat` is the completion-driven counterpart to `retry`. Where `retry` resubscribes on **error**, `repeat` resubscribes on **completion**.

| | `repeat` | `retry` |
|---|---|---|
| Triggers on | Source **completion** | Source **error** |
| `count` param | Repetitions after first run | Retry attempts after first error |
| `delay` | Wait between repetitions | Wait between retries |
| Use for | Polling, animation loops, repeating sequences | Error recovery |

**`count` semantics**:
```
repeat()      → repeat forever (Infinity)
repeat(0)     → never repeat (same as no repeat — completes after first run)
repeat(3)     → repeat 3 times after first completion (4 total executions)
```

**`delay` in RepeatConfig**:
- `delay: 1000` — fixed 1000ms gap between repetitions
- `delay: (count) => timer(count * 1000)` — increasing delay between repetitions

**Errors are NOT caught** — if the source errors, the error passes through and `repeat` does not resubscribe.

## Marble Diagram

```
Source:  --1--2--|  (completes)

repeat(2):
Result:  --1--2----1--2----1--2--|
          run 1    run 2    run 3 (original + 2 repetitions)

repeat({ count: 2, delay: 100 }):
Result:  --1--2--(100ms)--1--2--(100ms)--1--2--|
                  gap           gap

repeat() on a never-completing source:
Source:  --1--2--3...  (never completes)
Result:  --1--2--3...  (same — repeat never triggers without completion)

repeat() vs retry() on erroring source:
Source:  --1--#  (errors)
repeat(3):  --1--#  (error passes through — repeat ignores errors)
retry(3):   --1----1----1----1--#  (resubscribes 3 times)
```

## Type System Integration

```typescript
import { of, timer } from 'rxjs';
import { repeat, map } from 'rxjs/operators';

// Type preserved
of('ping').pipe(
  repeat({ count: 3, delay: 1000 })
).subscribe((v: string) => console.log(v));
// 'ping' (immediately), 'ping' (after 1s), 'ping' (after 2s), 'ping' (after 3s)
// complete

// Infinite repeat with delay — polling
of(null).pipe(
  repeat({ delay: 5000 })
).subscribe(() => fetchStatus()); // fetches every 5 seconds
```

## Examples

### Basic Usage — Repeat a Finite Sequence
```typescript
import { of } from 'rxjs';
import { repeat } from 'rxjs/operators';

of(1, 2, 3).pipe(repeat(2)).subscribe(console.log);
// 1, 2, 3, 1, 2, 3, 1, 2, 3  (original + 2 repetitions = 3 total runs)

of('tick').pipe(repeat(3)).subscribe(console.log);
// tick, tick, tick, tick  (4 total)
```

### Common Pattern — HTTP Polling
```typescript
import { of, timer } from 'rxjs';
import { repeat, switchMap, takeUntil } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const stop$ = new Subject<void>();

// Poll an endpoint every 10 seconds
of(null).pipe(
  switchMap(() => ajax.getJSON<Status>('/api/status')),
  repeat({ delay: 10_000 }),
  takeUntil(stop$)
).subscribe({
  next: status => updateStatusDisplay(status),
  error: err => handlePollError(err)
});

// Stop polling
stop$.next();
```

### Common Pattern — Animation / Game Loop
```typescript
import { animationFrameScheduler, of } from 'rxjs';
import { repeat, timestamp, map } from 'rxjs/operators';

// Game loop: repeat a single frame computation on every animation frame
of(null).pipe(
  repeat(),  // repeat forever
  timestamp(animationFrameScheduler),
  map(({ timestamp: t }) => t)
).subscribe(time => renderFrame(time));

// More explicit animation frame loop
import { interval } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

interval(0, animationFrameScheduler).pipe(
  takeUntil(gameOver$)
).subscribe(() => updateGameState());
```

### Common Pattern — Repeating With Progressive Delay
```typescript
import { of, timer } from 'rxjs';
import { repeat, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

let pollCount = 0;

// Start polling quickly, then back off over time
ajax.getJSON<JobStatus>('/api/job/123').pipe(
  repeat({
    delay: (count) => {
      // count is the number of completions so far (1-based)
      const ms = Math.min(count * 1000, 30_000); // cap at 30s
      console.log(`Next poll in ${ms}ms`);
      return timer(ms);
    }
  })
).subscribe(status => {
  if (status.done) stopPolling();
});
```

## Common Pitfalls

### Anti-pattern: `repeat()` on a Never-Completing Source
```typescript
import { interval } from 'rxjs';
import { repeat } from 'rxjs/operators';

// ❌ NO EFFECT — interval never completes, repeat never triggers
interval(1000).pipe(repeat(3)).subscribe(console.log);
// Same as interval(1000) alone — 0, 1, 2, 3, ... forever
// The repeat(3) never fires because interval never completes

// ✅ CORRECT — make the source finite first
import { take } from 'rxjs/operators';
interval(1000).pipe(
  take(3),     // complete after 3 values
  repeat(2)    // then repeat twice more
).subscribe(console.log);
// 0, 1, 2, 0, 1, 2, 0, 1, 2  (3 runs of 3 values)

// WHY: repeat triggers on SOURCE COMPLETION. If the source never completes
// (interval, fromEvent, Subject, WebSocket), repeat never runs.
// Always ensure the source completes before expecting repeat to act.
```

### Anti-pattern: Confusing `repeat` With `retry` for Error Recovery
```typescript
import { throwError, of } from 'rxjs';
import { repeat, retry, catchError } from 'rxjs/operators';

// ❌ WRONG — using repeat to handle errors (it doesn't)
throwError(() => new Error('oops')).pipe(
  repeat(3) // does nothing for errors — error passes through immediately
).subscribe({ error: e => console.log('error:', e.message) });
// error: oops  (repeat never triggered — source errored, not completed)

// ✅ CORRECT — retry for error recovery, repeat for completion-based loops
throwError(() => new Error('oops')).pipe(
  retry(3)  // resubscribes on error up to 3 times
).subscribe({ error: e => console.log('final error:', e.message) });

// Combining both: retry errors, then repeat the recovered stream
ajax.getJSON('/api/data').pipe(
  retry(3),     // retry up to 3 times on error
  repeat(5)     // repeat the entire successful sequence 5 times
).subscribe(console.log);

// WHY: repeat only reacts to COMPLETION signals. retry only reacts to ERROR
// signals. They are complementary — for a stream that can both error and
// complete, compose them: retry first (inner), repeat outer.
```

## Related Operators

- **`retry`**: Symmetric counterpart — resubscribes on error instead of completion
- **`interval`**: Often a simpler alternative for fixed-rate polling (`interval(5000).pipe(switchMap(fetchData))`)
- **`timer`**: Cleaner for "fetch immediately, then every N seconds" patterns
- **`takeUntil`**: Required companion for infinite `repeat()` in component contexts
- **`expand`**: Recursive resubscription with value feedback — more powerful than `repeat` for sequences that evolve

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/repeat](https://rxjs.dev/api/operators/repeat)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching points**:
1. `repeat` triggers on COMPLETION — not error, not next. Source must complete.
2. `retry` = error recovery; `repeat` = completion-driven loops — they compose cleanly
3. For polling, `interval(ms).pipe(switchMap(fetchData))` is often simpler than `of(null).pipe(switchMap(fetchData), repeat({ delay: ms }))`
