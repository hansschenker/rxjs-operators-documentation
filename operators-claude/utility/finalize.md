# finalize

## Identity
- **Name**: finalize
- **Category**: Utility Operators
- **Type**: Teardown side-effect runner — executes a callback when the Observable terminates for any reason
- **Import**:
  ```typescript
  import { finalize } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function finalize<T>(callback: () => void): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — any source Observable

**Output**: `Observable<T>` — mirrors the source exactly; the only addition is the callback runs on any termination path

**Transformation**: Subscribes to the source and forwards all next/error/complete notifications unchanged. When the subscription terminates — whether by source completion, source error, or subscriber unsubscription — the `callback` is called exactly once.

**The three termination paths**:

| Termination | `tap({ complete })` runs? | `tap({ error })` runs? | `finalize` runs? |
|-------------|--------------------------|------------------------|-----------------|
| Source completes normally | ✅ | ❌ | ✅ |
| Source errors | ❌ | ✅ | ✅ |
| Subscriber unsubscribes | ❌ | ❌ | ✅ ← **only finalize catches this** |

**Mathematical representation**:
```
finalize(cb)(source) = source  (values/errors/completion pass through unchanged)
Side effect: cb() called once when the subscription tears down, regardless of reason.
```

**Invariants**:
- **Exactly one call**: `callback` is called exactly once per subscription, on any termination
- **After termination**: callback runs after the complete/error notification has been delivered to downstream
- **No parameters**: callback receives no arguments — it cannot distinguish completion from error from unsubscription
- **Non-blocking**: if callback throws, the error propagates but teardown has already happened
- **Type-transparent**: T passes through unchanged

## Marble Diagram

```
Source completes normally:
Source: --a--b--c--|
        finalize(() => console.log('done'))
Result: --a--b--c--|
                   ↑ callback fires here (after completion delivered downstream)

Source errors:
Source: --a--b--#
        finalize(() => console.log('done'))
Result: --a--b--#
                ↑ callback fires here (after error delivered downstream)

Subscriber unsubscribes at t=2:
Source: --a--b--c--|  (never completes from subscriber's perspective)
Sub:    --a--✂️
            ↑ unsubscribe here
Callback fires on unsubscription — tap({ complete }) would NOT fire here
```

**Key observation**: `finalize` is the Observable equivalent of `try/finally` — it guarantees cleanup code runs regardless of the success/failure/cancellation path. Use it to hide spinners, release resources, or update loading flags.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source; adds a teardown action to the inner subscription

**Completion semantics**:
- Complete notification delivered to downstream first, then callback runs

**Error handling**:
- Error delivered to downstream first, then callback runs
- If callback itself throws, the error propagates; teardown has already happened

**Unsubscription**:
- When the downstream subscriber unsubscribes: source is unsubscribed, then callback runs
- This is the critical case — the only way to run cleanup on mid-stream cancellation

**Backpressure**:
- None — transparent pass-through

**Ordering relative to `tap`**:
```typescript
source$.pipe(
  tap({ complete: () => console.log('tap complete') }),
  finalize(() => console.log('finalize'))
)
// On complete: "tap complete" then "finalize"
// On error:    "finalize" only (tap complete doesn't fire)
// On unsub:    "finalize" only (tap complete doesn't fire)
```

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — type preserved)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * finalize adds no transformation — T in = T out.
 * The callback is () => void — no parameters, no return value.
 */

import { interval, Subject } from 'rxjs';
import { takeUntil, finalize, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface Data { id: number; value: string; }

// finalize does not change the type
const data$: Observable<Data> = ajax.getJSON<Data>('/api/data').pipe(
  finalize(() => setLoading(false))
); // still Observable<Data>

// Multiple finalize operators stack (all run, inner-to-outer on teardown)
interval(100).pipe(
  finalize(() => console.log('inner finalize')),
  takeUntil(new Subject()),
  finalize(() => console.log('outer finalize'))
).subscribe();
```

## Examples

### Basic Usage — Loading State Management
```typescript
import { finalize } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

function loadData(): Observable<Data> {
  setLoading(true);

  return ajax.getJSON<Data>('/api/data').pipe(
    finalize(() => setLoading(false)) // always runs: success, error, or cancel
  );
}

loadData().subscribe({
  next:  data => renderData(data),
  error: err  => showError(err),
  // setLoading(false) called in ALL three cases
});
```

### Common Pattern — Resource Cleanup
```typescript
import { fromEvent } from 'rxjs';
import { finalize, takeUntil } from 'rxjs/operators';

function openWebSocket(url: string, destroy$: Observable<void>): Observable<MessageEvent> {
  const ws = new WebSocket(url);

  return fromEvent<MessageEvent>(ws, 'message').pipe(
    takeUntil(destroy$),
    finalize(() => {
      // Guaranteed cleanup — runs on destroy$ fire, error, or manual unsubscribe
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    })
  );
}
```

### Common Pattern — Analytics / Audit Trail
```typescript
import { finalize, tap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

function trackedRequest<T>(url: string, label: string): Observable<T> {
  const startTime = performance.now();

  return ajax.getJSON<T>(url).pipe(
    tap({
      next:  data => analytics.track(`${label}.success`, { data }),
      error: err  => analytics.track(`${label}.error`,   { err }),
    }),
    finalize(() => {
      const duration = performance.now() - startTime;
      analytics.track(`${label}.duration`, { ms: duration });
      // Duration logged regardless of success/error/cancel
    })
  );
}
```

### Common Pattern — Spinner with `switchMap` Cancellation
```typescript
import { Subject, fromEvent } from 'rxjs';
import { switchMap, finalize, startWith } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const searchInput = document.getElementById('search') as HTMLInputElement;

fromEvent(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query => {
    setSearching(true);
    return ajax.getJSON<Result[]>(`/api/search?q=${query}`).pipe(
      finalize(() => setSearching(false))
      // finalize inside switchMap: runs when THIS inner Observable terminates
      // If a new query arrives, switchMap cancels the previous inner Observable
      // → finalize runs for the cancelled request too (spinner hidden correctly)
    );
  })
).subscribe(results => renderResults(results));
```

### Common Pattern — Stacking `finalize` Operators
```typescript
import { interval } from 'rxjs';
import { take, finalize, tap } from 'rxjs/operators';

// Multiple finalize operators execute in order (inner → outer)
interval(100).pipe(
  take(3),
  tap({ complete: () => console.log('tap: complete') }),
  finalize(() => console.log('finalize 1')), // inner
  finalize(() => console.log('finalize 2'))  // outer
).subscribe({
  complete: () => console.log('subscriber: complete')
});

// Output order:
// tap: complete
// subscriber: complete
// finalize 1
// finalize 2
// (finalize runs after downstream has received the notification)
```

### Edge Cases — Error in Callback, Unsubscription Timing
```typescript
import { of, throwError } from 'rxjs';
import { finalize } from 'rxjs/operators';

// Edge case 1: callback throws — error propagates after teardown
of(1, 2, 3).pipe(
  finalize(() => { throw new Error('cleanup failed'); })
).subscribe({
  next:     v => console.log(v),
  error:    e => console.log('error:', e.message),
  complete: () => console.log('complete'),
});
// Output: 1, 2, 3, complete, error: cleanup failed
// complete fired first (before finalize callback ran)

// Edge case 2: finalize distinguishes nothing — callback has no params
of(1).pipe(finalize(() => console.log('done'))).subscribe();
throwError(() => new Error('oops')).pipe(finalize(() => console.log('done'))).subscribe({ error: () => {} });
// Both output: done — callback cannot tell if it was success or failure
// Use tap({ complete, error }) if you need to distinguish
```

## Common Pitfalls

### Anti-pattern: Using `tap({ complete })` When Cancellation Must Also Clean Up
```typescript
import { interval, Subject } from 'rxjs';
import { takeUntil, tap, finalize } from 'rxjs/operators';

const destroy$ = new Subject<void>();

// ❌ INCOMPLETE — tap({ complete }) does NOT fire on unsubscription
interval(1000).pipe(
  takeUntil(destroy$),
  tap({ complete: () => setLoading(false) }) // only fires on natural completion!
).subscribe();

destroy$.next(); // takeUntil causes unsubscription — tap({ complete }) NEVER fires
// setLoading(false) is never called → spinner stuck!

// ✅ CORRECT — finalize fires on ALL termination paths including unsubscription
interval(1000).pipe(
  takeUntil(destroy$),
  finalize(() => setLoading(false)) // fires on complete, error, AND unsubscription
).subscribe();

destroy$.next(); // finalize fires → setLoading(false) called correctly

// WHY: tap({ complete }) only fires on the 'complete' notification.
// takeUntil (and any unsubscription) bypasses the 'complete' path — it terminates
// the subscription without emitting a 'complete' notification to tap.
// finalize is called by the subscription's teardown logic, which runs on ALL
// termination paths. Use finalize for any cleanup that must be guaranteed.
```

### Anti-pattern: Using `finalize` When `tap({ complete, error })` Is More Appropriate
```typescript
import { ajax } from 'rxjs/ajax';
import { finalize, tap } from 'rxjs/operators';

// ❌ CONFLATING — using finalize for notifications that should distinguish success/failure
ajax.getJSON('/api/data').pipe(
  finalize(() => {
    // This runs on both success and error — we can't tell which
    analytics.track('request.ended'); // loses context
  })
).subscribe();

// ✅ CORRECT — use tap for success/error specific logic; finalize for unconditional cleanup
ajax.getJSON('/api/data').pipe(
  tap({
    next:  data => analytics.track('request.success', { data }),
    error: err  => analytics.track('request.error',   { err }),
  }),
  finalize(() => setLoading(false)) // cleanup — doesn't need to know why
).subscribe();

// WHY: finalize receives no parameters — it cannot distinguish completion from error.
// Use tap for conditional logic (success vs. failure analytics, error logging,
// success notifications). Use finalize only for unconditional cleanup (hide spinner,
// release lock, decrement counter) that must happen on ALL termination paths.
```

### Anti-pattern: Placing `finalize` Before Operators That Transform Completion
```typescript
import { ajax } from 'rxjs/ajax';
import { finalize, retry } from 'rxjs/operators';

// ❌ FIRES TOO EARLY — finalize before retry runs on each failed attempt
ajax.getJSON('/api/data').pipe(
  finalize(() => setLoading(false)), // fires after each retry attempt, not just final
  retry(3)
).subscribe();
// setLoading(false) called after attempt 1 errors, then 2, then 3
// Spinner hidden on first failure even though retries are ongoing!

// ✅ CORRECT — finalize AFTER retry (and any other operators that re-subscribe)
ajax.getJSON('/api/data').pipe(
  retry(3),
  finalize(() => setLoading(false)) // fires only when entire retry chain ends
).subscribe();

// WHY: finalize is triggered by the teardown of the Observable it's applied to.
// Placing it before retry means it fires each time the retried Observable
// terminates (after each attempt). Place finalize at the outermost position
// where "done for real" is defined.
```

## Related Operators

**Same Category (Utility)**:
- **`tap`**: Observe next/error/complete notifications without handling them — use when you need to distinguish which notification fired or react to the value
- **`tap({ error })`**: Like finalize for errors only — receives the error value; finalize does not
- **`tap({ complete })`**: Like finalize for normal completion only — does not fire on error or unsubscription

**Complementary Operators**:
- **`takeUntil`**: The primary cause of mid-stream unsubscription — always pair with `finalize` for cleanup inside `takeUntil`-bounded streams
- **`retry`**: Each retry attempt triggers finalize if placed before retry — put finalize after retry
- **`switchMap`**: Each cancelled inner Observable triggers finalize inside switchMap — use to hide per-request spinners

**Alternatives by Use Case**:

| Use Case | Instead of `finalize` | Use | Why |
|----------|-----------------------|-----|-----|
| Cleanup only on success | `finalize` | `tap({ complete })` | Only fires on completion |
| Cleanup only on error | `finalize` | `tap({ error })` | Receives error value |
| Cleanup only on unsub | `finalize` | `finalize` with guard | No alternative — only finalize catches this |
| Log request duration | `finalize` | `finalize` | Always runs; perfect for timing |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/finalize](https://rxjs.dev/api/operators/finalize)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/finalize.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/finalize.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Universal Teardown Hook (Observable `finally`)
- **Cognitive Load**: 2/5 — Simple concept; the key teaching point is that tap({ complete }) does NOT fire on unsubscription — only finalize does
- **Usage Frequency**: 4/5 — Essential wherever resources need cleanup: loading states, WebSockets, timers, locks
- **Composability**: 5/5 — Transparent pass-through; stacks cleanly; correct placement relative to retry is the main pitfall

**Teaching Sequence**:
- **Prerequisites**: `tap`, `takeUntil`, Observable teardown/unsubscription
- **Teaches**: The three termination paths, finalize vs. tap distinction, teardown ordering with retry
- **Common with**: `takeUntil`, `switchMap`, `retry`, `tap`, `ajax`
