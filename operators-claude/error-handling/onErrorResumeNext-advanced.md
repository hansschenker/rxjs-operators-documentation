# onErrorResumeNext — Advanced Patterns

> **Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
> **Teaching Sequence**: After `catchError` and `retry` — introduces silent-error sequencing as a deliberate design choice

---

## Advanced Behavioral Model

`onErrorResumeNext` (static) and `onErrorResumeNextWith` (pipeable) share one rule: **any terminal signal — error or completion — advances to the next source**. The only terminal signal that actually terminates the operator is when all sources are exhausted.

```
Source A:  --1--2--#          (errors at #)
Source B:  --3--4--|          (completes normally)
Source C:  --5--|             (completes normally)

onErrorResumeNext(A, B, C):
           --1--2--3--4--5--|
                   ^
              error swallowed, B subscribed immediately
```

Key invariant: **errors are invisible to downstream**. No `catchError`, no error notification, no retry — the error simply vanishes.

---

## Type System Integration

```typescript
import { onErrorResumeNext } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs/operators';

// Static form: union of all source types
const a$ = of(1, 2, 3);           // Observable<number>
const b$ = of('x', 'y');          // Observable<string>
const result$ = onErrorResumeNext(a$, b$);
// result$: Observable<number | string>

// Pipeable form: T | A[number] union
const piped$ = source$.pipe(
  onErrorResumeNextWith(of('fallback'))
);
// piped$: Observable<SourceType | string>

// With ObservableInput — accepts Promises too
const withPromise$ = onErrorResumeNext(
  fetch('/api/primary').then(r => r.json()),
  fetch('/api/backup').then(r => r.json())
);
// ObservableInput<T> wraps Promise<T> transparently
```

---

## Advanced Patterns

### 1. Best-Effort Resource Cleanup Chain

When tearing down multiple resources, individual failures must not block subsequent cleanup steps. `onErrorResumeNext` guarantees every step is attempted.

```typescript
import { onErrorResumeNext, from } from 'rxjs';

function teardown(session: Session): Observable<void> {
  return onErrorResumeNext(
    from(session.flushPendingWrites()),   // may fail if DB is down
    from(session.revokeTokens()),         // may fail if auth server is down
    from(session.closeSockets()),         // always attempt
    from(session.writeAuditLog()),        // always attempt
  );
}

// All four steps are always attempted, even if step 1 throws.
// Errors are silently dropped — log them before this chain if needed.
teardown(session).subscribe({
  complete: () => console.log('cleanup finished'),
});
```

### 2. Cascading Fallback Data Sources

Try primary, then secondary, then tertiary — always returning the best available data without exposing the failure chain to consumers.

```typescript
import { onErrorResumeNext, from, of } from 'rxjs';
import { map, filter, take } from 'rxjs/operators';

function getUserProfile(userId: string): Observable<UserProfile> {
  return onErrorResumeNext(
    // 1. Live API — errors if offline
    http.get<UserProfile>(`/api/users/${userId}`),

    // 2. IndexedDB cache — errors if cache miss
    from(idbCache.get<UserProfile>(`user:${userId}`)).pipe(
      map(v => { if (!v) throw new Error('miss'); return v; })
    ),

    // 3. SessionStorage snapshot — may be stale
    of(JSON.parse(sessionStorage.getItem(`user:${userId}`) ?? 'null')).pipe(
      filter((v): v is UserProfile => v !== null)
    ),

    // 4. Skeleton / anonymous default — always succeeds
    of(anonymousProfile(userId)),
  ).pipe(
    take(1), // stop at first emission
  );
}
```

**WHY `take(1)`**: Without it, all sources run to completion and all their values reach downstream. `take(1)` turns this into a "first successful value" pattern.

### 3. Parallel-Try with onErrorResumeNext

Combine with `race` or `merge` to attempt multiple sources simultaneously and fall back sequentially only on failure.

```typescript
import { onErrorResumeNext, race, timer } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// Race two endpoints; if both fail, fall back to cache
function fetchWithFallback<T>(urls: string[]): Observable<T> {
  const live$ = race(urls.map(url => http.get<T>(url)));
  const cache$ = from(localCache.get<T>('last-known'));

  return onErrorResumeNext(live$, cache$).pipe(take(1));
}
```

### 4. Logging Errors Before Silent Discard

`onErrorResumeNext` swallows errors with no hooks. Wrap sources in `catchError` first to log before discarding.

```typescript
import { onErrorResumeNext, EMPTY } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';

function withLogging<T>(
  source$: Observable<T>,
  label: string
): Observable<T> {
  return source$.pipe(
    catchError(err => {
      console.error(`[${label}] error:`, err);
      return EMPTY; // onErrorResumeNext sees "completed", moves on
    }),
  );
}

onErrorResumeNext(
  withLogging(primarySource$, 'primary'),
  withLogging(secondarySource$, 'secondary'),
  withLogging(tertiarySource$, 'tertiary'),
).subscribe(handleValue);
```

### 5. Combining Static and Pipeable Forms

The static form sequences independent sources; the pipeable form extends the current stream.

```typescript
import { onErrorResumeNext } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs/operators';

// Static: orchestrate a batch of independent tasks
const batchResult$ = onErrorResumeNext(...tasks.map(t => t.execute()));

// Pipeable: extend a single stream with a fallback continuation
userEvents$.pipe(
  onErrorResumeNextWith(
    sessionTimeoutFallback$,
    guestModeStream$,
  )
).subscribe(handleEvent);
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — forgetting take(1) in a "first-value" pattern
onErrorResumeNext(primary$, secondary$, tertiary$).subscribe(handler);
// If primary$ emits 3 values before erroring, secondary$ emits 2,
// tertiary$ emits 1 — handler sees ALL 6 values, not just the first.

// ✅ CORRECT
onErrorResumeNext(primary$, secondary$, tertiary$).pipe(
  take(1)
).subscribe(handler);
// WHY: take(1) completes after the first emission, giving "first wins" semantics.


// ❌ INCORRECT — using onErrorResumeNext when errors need handling
function fetchData() {
  return onErrorResumeNext(apiCall$, of(null));
}
// Caller has no way to know if apiCall$ failed. null looks like valid data.

// ✅ CORRECT — use catchError when callers need to distinguish failure
function fetchData() {
  return apiCall$.pipe(
    catchError(err => {
      reportError(err);
      return of(null); // explicit fallback, caller knows it's a fallback
    })
  );
}
// WHY: onErrorResumeNext is for "always continue" orchestration,
// not for per-error recovery logic visible to callers.


// ❌ INCORRECT — assuming errors surface somewhere
onErrorResumeNext(
  riskyOperation1$,
  riskyOperation2$,
).subscribe({
  error: e => console.error('error:', e), // never called
  complete: () => console.log('done'),
});

// ✅ CORRECT — wrap sources to capture errors before discard
onErrorResumeNext(
  riskyOperation1$.pipe(catchError(e => { log(e); return EMPTY; })),
  riskyOperation2$.pipe(catchError(e => { log(e); return EMPTY; })),
).subscribe({ complete: () => console.log('done') });
// WHY: The error handler on subscribe() is never invoked by onErrorResumeNext.
```

---

## Operator Comparison: Silent Error Sequencers

| Operator | Error behavior | Complete behavior | Values from all sources |
|---|---|---|---|
| `concat` | Propagates, stops | Moves to next | Yes (sequentially) |
| `catchError` | Replaces with fallback | Passes through | Caller controls |
| `onErrorResumeNext` | **Silently ignored** | Moves to next | Yes |
| `merge` | Propagates | Independent | Yes (concurrent) |

**Choose `onErrorResumeNext` when**: you have N independent tasks that must all be attempted but whose individual failures are acceptable and should not interrupt the sequence.

---

## Related Operators

- **`catchError`** — intercept and recover from specific errors; error is visible and handleable
- **`retry` / `retryWhen`** — retry the same source on error, rather than moving to the next source
- **`concat`** — sequence sources but propagate errors
- **`forkJoin`** — run sources in parallel, requires all to complete successfully
- **`race`** — subscribe to multiple sources concurrently, keep only the first to emit
