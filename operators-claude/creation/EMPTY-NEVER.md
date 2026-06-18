# EMPTY / NEVER

## Identity

| | `EMPTY` | `NEVER` |
|---|---|---|
| **Import** | `import { EMPTY } from 'rxjs'` | `import { NEVER } from 'rxjs'` |
| **Type** | `Observable<never>` | `Observable<never>` |
| **Category** | Creation — Constants | Creation — Constants |
| **Emits** | Nothing — completes immediately | Nothing — never completes, never errors |

```typescript
const EMPTY: Observable<never>  // singleton, pre-created
const NEVER: Observable<never>  // singleton, pre-created
```

## Functional Specification

**`EMPTY`**: An Observable that completes synchronously on subscription without emitting any value. The reactive equivalent of an empty array `[]` or a resolved `Promise<void>`.

**`NEVER`**: An Observable that subscribes but never emits, never completes, and never errors. It stays subscribed forever, doing nothing. The reactive equivalent of an infinite Promise that never resolves.

**Mathematical representation**:
```
EMPTY = Observable that emits: complete  (synchronous, no next/error)
NEVER = Observable that emits: (nothing, ever)

EMPTY is equivalent to: of()   or  from([])
NEVER is equivalent to: new Observable(() => {})  (empty teardown, no emissions)
```

## Marble Diagram

```
EMPTY:   |          (completes immediately, synchronous)

NEVER:   ---------  (infinite silence — no completion, no error, no values)
```

**Common usage context**:
```
catchError(_ => EMPTY):   source--a--b--#     →  source--a--b--|
                           error suppressed; stream completes

race(ajax$, NEVER):       Only ajax$ can win; NEVER never emits to compete
```

## Type System Integration

```typescript
/**
 * Both EMPTY and NEVER are typed as Observable<never>.
 * Observable<never> is assignable to Observable<T> for any T — it produces no values,
 * so it cannot violate type constraints.
 *
 * In union types, Observable<never> is absorbed:
 *   Observable<User | never> = Observable<User>
 *
 * This makes EMPTY safe to return from catchError:
 *   source$.pipe(catchError(_ => EMPTY))  → Observable<T>  (not Observable<T | never>)
 */

import { EMPTY, NEVER, of } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';

// EMPTY in catchError — type preserved
const safe$: Observable<User> = getUser().pipe(
  catchError(_ => EMPTY) // Observable<never> → absorbed into Observable<User>
);

// switchMap returning EMPTY — skips emission
of(1, 2, null, 3).pipe(
  switchMap(v => v === null ? EMPTY : of(v * 10)) // Observable<number>
).subscribe(console.log); // 10, 20, 30  (null skipped via EMPTY)
```

## Examples

### `EMPTY` — Suppress Errors, Skip Values, Short-Circuit
```typescript
import { EMPTY, of, merge } from 'rxjs';
import { catchError, switchMap, mergeMap } from 'rxjs/operators';

// 1. Suppress errors — silently complete instead
source$.pipe(
  catchError(err => {
    console.warn('suppressed:', err.message);
    return EMPTY; // stream ends cleanly
  })
).subscribe({ complete: () => console.log('done') });

// 2. Skip null/undefined in switchMap
of(1, null, 2, undefined, 3).pipe(
  switchMap(v => v == null ? EMPTY : of(v))
).subscribe(console.log); // 1, 2, 3

// 3. Resilient merge — one failing source doesn't kill the merge
const sources = ['/api/a', '/api/b', '/api/c'].map(url =>
  ajax.getJSON(url).pipe(catchError(() => EMPTY))
);
merge(...sources).subscribe(processResult);
// Failed sources drop out silently; others continue
```

### `NEVER` — Placeholder, Testing, Race Patterns
```typescript
import { NEVER, race, timer } from 'rxjs';
import { map } from 'rxjs/operators';

// 1. Timeout pattern — NEVER as the "no timeout" placeholder
function withOptionalTimeout<T>(
  source$: Observable<T>,
  timeoutMs?: number
): Observable<T> {
  const timeout$ = timeoutMs
    ? timer(timeoutMs).pipe(map(() => { throw new Error('timeout'); }))
    : NEVER; // if no timeout specified, NEVER races (never wins)

  return race(source$, timeout$);
}

// 2. Conditionally disable a stream
const feature$ = isFeatureEnabled()
  ? liveData$
  : NEVER; // feature disabled → stream is silent forever

// 3. In tests — a Subject that should never emit
import { Subject } from 'rxjs';
const notifier$ = NEVER; // placeholder for a trigger that shouldn't fire in this test
source$.pipe(takeUntil(notifier$)).subscribe(results.push.bind(results));
// takeUntil(NEVER) = source$ runs to its own completion uninterrupted
```

### Common Pattern — Conditional `EMPTY` / `NEVER` Guards
```typescript
import { EMPTY, NEVER, defer } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// Skip processing when a condition is false
userAction$.pipe(
  switchMap(action =>
    canProcess(action) ? processAction(action) : EMPTY
  )
).subscribe(handleResult);

// Pause a stream entirely when offline
const data$ = defer(() =>
  navigator.onLine ? fetchData() : NEVER
);
```

## Common Pitfalls

### Anti-pattern: `EMPTY` When `NEVER` Is Needed (and Vice Versa)
```typescript
import { EMPTY, NEVER } from 'rxjs';
import { combineLatest } from 'rxjs';

// ❌ WRONG — EMPTY in combineLatest completes the combined stream immediately
combineLatest([
  userStream$,
  EMPTY // "optional" second source — but EMPTY completes combineLatest at once!
]).subscribe(console.log); // completes immediately, no output

// ✅ CORRECT — NEVER to "hold" a slot in combineLatest without completing it
// (though this is rarely the right solution — consider startWith or BehaviorSubject)
combineLatest([
  userStream$,
  NEVER // never emits → combineLatest never fires (both must have emitted)
]).subscribe(console.log); // also no output, but at least doesn't complete

// ✅ BEST — startWith to provide an initial value
combineLatest([
  userStream$,
  optionalStream$.pipe(startWith(null))
]).subscribe(([user, optional]) => render(user, optional));

// WHY: EMPTY completes the stream it's part of — it is a terminal signal.
// NEVER is purely passive — it never triggers anything.
// In combineLatest, EMPTY kills the combination; NEVER freezes it.
// Use EMPTY to end gracefully; NEVER to be permanently inactive.
```

### Anti-pattern: Silent Error Suppression With EMPTY
```typescript
import { EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';

// ❌ DANGEROUS — all errors silently dropped
criticalDataStream$.pipe(
  catchError(_ => EMPTY) // errors discarded without logging
).subscribe(handleData);

// ✅ CORRECT — log before suppressing
criticalDataStream$.pipe(
  catchError(err => {
    console.error('data stream error:', err);
    errorTracker.record(err);
    return EMPTY; // suppress after logging
  })
).subscribe(handleData);

// WHY: EMPTY completes the stream normally — the subscriber's complete() fires
// as if nothing went wrong. For critical streams, always log before returning
// EMPTY so that errors are visible in monitoring/alerting.
```

## Related Operators / Constants

- **`of()`**: Creates a new completing Observable each call; `EMPTY` is a singleton pre-built instance
- **`NEVER`** vs `new Observable(() => {})`: Equivalent; NEVER is the pre-built singleton
- **`throwError(() => err)`**: Complement — immediately errors instead of completing
- **`catchError(_ => EMPTY)`**: The canonical error-suppression pattern
- **`takeUntil(NEVER)`**: Effectively disables takeUntil — source runs to natural completion

## References
- **RxJS EMPTY**: [https://rxjs.dev/api/index/const/EMPTY](https://rxjs.dev/api/index/const/EMPTY)
- **RxJS NEVER**: [https://rxjs.dev/api/index/const/NEVER](https://rxjs.dev/api/index/const/NEVER)

---

## Additional Notes for rxjs-strategies Integration

**`EMPTY`** — Cognitive Load: 1/5 | Usage: 5/5 | The "silent completion" sentinel; paired with catchError in virtually every resilient stream
**`NEVER`** — Cognitive Load: 1/5 | Usage: 3/5 | The "permanent silence" sentinel; essential for conditional activation and timeout race patterns
**Teaching Sequence**: Teach alongside `of` and `throwError` as the three completion-state constants: `of(v)` = value + complete, `EMPTY` = complete only, `NEVER` = nothing, `throwError(e)` = error only
