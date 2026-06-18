# onErrorResumeNext

## Identity

- **Name**: `onErrorResumeNext`
- **Category**: Error Handling Operators
- **Type**: Error-ignoring sequencer — continues to the next source when any source errors or completes
- **Import**:
  ```typescript
  import { onErrorResumeNext } from 'rxjs';           // static creation form
  import { onErrorResumeNextWith } from 'rxjs/operators'; // pipeable form
  ```
- **Signature**:
  ```typescript
  function onErrorResumeNext<T extends readonly unknown[]>(
    ...sources: [...ObservableInputTuple<T>]
  ): Observable<T[number]>

  function onErrorResumeNextWith<T, A extends readonly unknown[]>(
    ...sources: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

`onErrorResumeNext` subscribes to sources in sequence — like `concat` — but treats **both errors and completions as "move to next source"** signals. Errors are silently swallowed; the operator immediately subscribes to the next source.

**Mental model**: "Always continue, regardless of success or failure."

**Comparison with similar operators**:

| Operator | On error | On complete | Use when |
|---|---|---|---|
| `concat` | Propagates | Moves to next | Sequence must be error-free |
| `catchError + concat` | Recovers with fallback | Moves to next | Handle specific errors |
| `onErrorResumeNext` | **Silently ignores, moves to next** | Moves to next | All errors ignorable, best-effort sequence |

**When to use**:
- Best-effort fallback chains where you always want the next source tried
- Cleanup sequences where individual steps may fail
- Loading from multiple optional sources (cache → network → default)

## Marble Diagram

```
Source A:  --1--2--#(err)
Source B:  --3--4--|
Source C:  --5--|

concat(A, B, C):
Result:    --1--2--#  ← error stops everything

onErrorResumeNext(A, B, C):
Result:    --1--2----3--4----5--|
           A errors → immediately subscribe to B
           B completes → immediately subscribe to C
           C completes → outer completes

onErrorResumeNext(A, B, C) where A completes normally:
A:  --1--|
B:  --2--|
C:  --3--|
Result: --1----2----3--|  (same as concat — no difference when no errors)
```

## Type System Integration

```typescript
import { onErrorResumeNext, of, throwError } from 'rxjs';

// All sources must share a compatible type
const result$ = onErrorResumeNext(
  throwError(() => new Error('first fails')), // Observable<never>
  of(1, 2, 3),                               // Observable<number>
  throwError(() => new Error('third fails')), // Observable<never>
  of(4, 5)                                   // Observable<number>
);
// result$: Observable<number>
// Emits: 1, 2, 3, 4, 5

// Pipeable form:
import { onErrorResumeNextWith } from 'rxjs/operators';
throwError(() => new Error('primary failed')).pipe(
  onErrorResumeNextWith(of('fallback'))
).subscribe(console.log); // 'fallback'
```

## Examples

### Basic Usage — Fallback Chain
```typescript
import { onErrorResumeNext } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Try primary, then CDN, then static fallback — errors at any step are ignored
onErrorResumeNext(
  ajax.getJSON<Config>('/api/config'),          // try server
  ajax.getJSON<Config>('/cdn/config.json'),     // try CDN
  ajax.getJSON<Config>('/static/config.json'),  // try static
).subscribe({
  next:  config => applyConfig(config),
  complete: () => console.log('tried all sources')
});
// If server returns 404 → tries CDN → if CDN times out → tries static
```

### Common Pattern — Best-Effort Cleanup
```typescript
import { onErrorResumeNext, defer } from 'rxjs';

// Run cleanup steps — continue even if individual steps fail
function cleanup(sessionId: string) {
  return onErrorResumeNext(
    defer(() => revokeToken(sessionId)),     // may fail if already revoked
    defer(() => clearUserCache(sessionId)),  // may fail if already clear
    defer(() => logSessionEnd(sessionId)),   // best-effort audit log
  );
}

// All three are attempted regardless of individual failures
cleanup(currentSession).subscribe({
  complete: () => console.log('cleanup done (best-effort)')
});
```

### Common Pattern — Pipeable Fallback (`onErrorResumeNextWith`)
```typescript
import { fromFetch } from 'rxjs/fetch';
import { onErrorResumeNextWith } from 'rxjs/operators';
import { of } from 'rxjs';

// Primary request with silent fallback to default
fromFetch<UserPrefs>('/api/preferences', {
  selector: r => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json() as Promise<UserPrefs>;
  }
}).pipe(
  onErrorResumeNextWith(of(DEFAULT_PREFS)) // silently use defaults on any error
).subscribe(prefs => applyPreferences(prefs));
```

## Common Pitfalls

### Anti-pattern: Using `onErrorResumeNext` When You Need Error Visibility
```typescript
import { onErrorResumeNext } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// ❌ SILENT — errors disappear without a trace
onErrorResumeNext(
  ajax.getJSON('/api/critical-data'),
  of(null) // fallback
).subscribe(data => {
  if (data === null) console.log('no data'); // you know it failed, but not why
});

// ✅ VISIBLE — log errors before falling back
import { catchError } from 'rxjs/operators';
ajax.getJSON('/api/critical-data').pipe(
  catchError(err => {
    console.error('critical-data failed:', err); // preserve error visibility
    return of(null);
  })
).subscribe(data => {
  if (data === null) console.log('no data');
});

// WHY: onErrorResumeNext discards all error information. In production code,
// silent error swallowing makes debugging very difficult. Prefer catchError
// when the error source matters, even if you ultimately recover from it.
```

### Anti-pattern: Confusing `onErrorResumeNext` with `retry`
```typescript
import { onErrorResumeNext, of } from 'rxjs';

// ❌ WRONG MENTAL MODEL — not a retry; it subscribes to DIFFERENT sources
onErrorResumeNext(
  failingSource$,
  failingSource$,  // this is a separate subscription, not a retry
  failingSource$
); // tries the same source 3 times but each is a NEW subscription

// ✅ CORRECT — use retry for "same source, try again"
import { retry } from 'rxjs/operators';
failingSource$.pipe(retry(3)).subscribe();

// Use onErrorResumeNext for "different fallback sources in sequence"
onErrorResumeNext(primarySource$, secondarySource$, defaultSource$).subscribe();

// WHY: Each source in onErrorResumeNext is a distinct Observable.
// For "same source, retry N times" semantics, use retry().
```

## Related Operators

- **`catchError`**: Handles specific errors with visibility — preferred when you need to know what failed
- **`retry`**: Re-subscribes to the SAME source — for "try again" semantics
- **`concat`**: Sequential subscription, but propagates errors — use when errors should stop the sequence
- **`iif`**: Conditional source selection at subscription time

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/onErrorResumeNext](https://rxjs.dev/api/index/function/onErrorResumeNext)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key teaching points**:
1. Errors are silently swallowed — use only when error visibility is genuinely unnecessary
2. Both errors AND completions trigger "move to next source" — it's not error-only behavior
3. Prefer `catchError` for visibility; reach for `onErrorResumeNext` only for true best-effort chains
