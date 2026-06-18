# exhaustMap

## Identity

- **Name**: exhaustMap
- **Category**: Transformation Operators
- **Type**: Ignore-while-busy higher-order flattener — maps source emissions to inner Observables, silently dropping new emissions while an inner is active
- **Import**:
  ```typescript
  import { exhaustMap } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function exhaustMap<T, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O
  ): OperatorFunction<T, ObservedValueOf<O>>
  ```

## Functional Specification

For each source emission, `exhaustMap` checks if an inner Observable is currently active. If one is active, the new source emission is **silently dropped**. If none is active, it subscribes to `project(value)`.

**Mental model**: "I'm busy — come back later."

**Invariants**:
- At most one inner Observable is active at any time
- Source emissions that arrive while an inner is active are **dropped** — not queued, not delayed
- Unlike `concatMap` (queues) and `switchMap` (cancels), `exhaustMap` simply ignores new inputs while busy

**Four flattening strategies**:

| Operator | When busy with an inner… | Use when |
|---|---|---|
| `mergeMap` | Starts another concurrently | Order doesn't matter, parallelism wanted |
| `concatMap` | Queues it | Order matters, can't drop any |
| `switchMap` | Cancels current, starts new | Only the latest result matters |
| `exhaustMap` | **Drops it** | Current work must not be interrupted |

## Marble Diagram

```
Source:    --a---b-c-d---e---|
               \   ✗ ✗   \
               (b,c,d dropped — inner a still active)

Inner a:   ----A1--A2|
Inner e:              ----E1--E2|

exhaustMap(project):
Result:    ------A1--A2------E1--E2--|
           (b, c, d silently dropped)

Timing detail:
  a emits → inner a starts
  b emits → inner a still active → b dropped
  c emits → inner a still active → c dropped
  d emits → inner a still active → d dropped
  inner a completes ← now idle
  e emits → idle → inner e starts
```

## Behavioral Characteristics

**Subscription**: At most one inner Observable subscribed at any time.

**Completion**: Completes after source completes AND the last active inner completes.

**Error handling**: First error from source or inner propagates immediately.

**Backpressure**: Natural backpressure — excess source emissions are dropped, preventing queue accumulation. This is exhaustMap's key advantage over `concatMap` for high-frequency sources.

## Type System Integration

```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Button click → HTTP request; subsequent clicks ignored while request in flight
fromEvent(document.getElementById('submit')!, 'click').pipe(
  exhaustMap(() => ajax.post<SaveResult>('/api/save', formData()))
).subscribe((result: SaveResult) => showSuccess(result));
```

## Examples

### Basic Usage — Debounced Button Submit
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const saveBtn = document.getElementById('save')!;

fromEvent(saveBtn, 'click').pipe(
  exhaustMap(() =>
    ajax.post('/api/save', collectFormData()).pipe(
      tap(() => showToast('Saved!'))
    )
  )
).subscribe({
  error: err => showError(err.message)
});
// Double-clicking the button fires only ONE request.
// Subsequent clicks are silently dropped until the save completes.
```

### Common Pattern — Login / Authentication Flow
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const loginBtn = document.getElementById('login')!;

fromEvent(loginBtn, 'click').pipe(
  exhaustMap(() => {
    const credentials = getFormCredentials();
    return ajax.post<AuthToken>('/api/auth/login', credentials).pipe(
      map(resp => resp.response)
    );
  })
).subscribe({
  next:  token => storeToken(token),
  error: err   => showLoginError(err)
});
// Multiple rapid login clicks → only the first request is made
```

### Common Pattern — Polling That Can't Overlap
```typescript
import { timer } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Poll every 5s, but skip the tick if previous poll is still running
timer(0, 5000).pipe(
  exhaustMap(() => ajax.getJSON<Status>('/api/status'))
).subscribe(status => updateStatusBar(status));
// If /api/status takes >5s, the next tick is skipped — no overlapping requests
```

### Edge Case — All Emissions Dropped
```typescript
import { of } from 'rxjs';
import { exhaustMap, delay } from 'rxjs/operators';

// Source emits very fast; only first gets through
of(1, 2, 3, 4, 5).pipe(
  exhaustMap(n => of(n * 10).pipe(delay(500)))
).subscribe(console.log);
// Only logs: 10
// 2, 3, 4, 5 all arrive while inner 1 is active → dropped
// WHY: of() emits synchronously — all 5 arrive before delay(500) completes
```

## Common Pitfalls

### Anti-pattern: Using `exhaustMap` When Events Must Not Be Lost
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';

// ❌ WRONG USE CASE — exhaustMap for events that must all be processed
fromEvent(document, 'keydown').pipe(
  exhaustMap(event => saveKeypress(event)) // drops keystrokes while saving!
).subscribe();
// User types quickly → some keystrokes silently discarded

// ✅ CORRECT — use concatMap to queue all events
import { concatMap } from 'rxjs/operators';
fromEvent(document, 'keydown').pipe(
  concatMap(event => saveKeypress(event)) // queues — no keystroke lost
).subscribe();

// WHY: exhaustMap drops source emissions while busy.
// Use exhaustMap only when it's acceptable to ignore excess:
// submit buttons, login flows, polling ticks.
// Use concatMap when every event must be processed.
```

### Anti-pattern: Confusing with `switchMap`
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap, switchMap } from 'rxjs/operators';

// ❌ WRONG: using exhaustMap for search (latest result wanted)
const search$ = fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);

search$.pipe(
  exhaustMap(query => ajax.getJSON(`/api/search?q=${query}`))
).subscribe(showResults);
// User types "rxjs" → 'r', 'rx', 'rxj' all dropped (first query in flight)
// Shows results for 'r', not 'rxjs'!

// ✅ CORRECT — use switchMap for search (cancel stale, use latest)
search$.pipe(
  switchMap(query => ajax.getJSON(`/api/search?q=${query}`))
).subscribe(showResults);

// WHY: switchMap cancels the previous request on each new keystroke,
// ensuring the latest typed query wins. exhaustMap ignores new keystrokes
// while the first request is in flight.
```

## Related Operators

- **`mergeMap`**: Concurrent — starts new inners without cancelling or dropping
- **`concatMap`**: Sequential queue — queues new emissions, never drops
- **`switchMap`**: Latest-wins — cancels current inner, starts new one
- **`exhaustAll`**: Same semantics applied to `Observable<Observable<T>>` (no projection)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/exhaustMap](https://rxjs.dev/api/operators/exhaustMap)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key teaching points**:
1. Drops (not queues) source emissions while an inner is active — use only when dropping is acceptable
2. Primary use cases: submit buttons, auth flows, non-overlapping polling
3. Contrast with `switchMap` (cancel) and `concatMap` (queue) to understand which to choose
