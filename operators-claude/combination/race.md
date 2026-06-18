# race

## Identity

- **Name**: race
- **Category**: Combination Operators (Join Creation)
- **Type**: First-wins selector — subscribes to all sources, forwards emissions from whichever emits first, immediately unsubscribes from all others
- **Import**:
  ```typescript
  import { race } from 'rxjs';
  import { raceWith } from 'rxjs/operators'; // pipeable form
  ```
- **Signature**:
  ```typescript
  function race<T extends readonly unknown[]>(
    ...sources: [...ObservableInputTuple<T>]
  ): Observable<T[number]>

  function raceWith<T, A extends readonly unknown[]>(
    ...otherSources: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

**Concept**: `race` subscribes to all input Observables simultaneously. The first one to emit a `next` value "wins" — its emissions are forwarded to the output. All other Observables are immediately unsubscribed from.

**Winner determination**: First to emit `next()`. Completion and error before emission do NOT win — a source that completes or errors without emitting is removed from the race; the race continues with remaining sources.

**Key invariants**:
- All sources are subscribed to simultaneously (not sequentially)
- Only `next` emissions determine the winner — completions/errors before any `next` just eliminate that source
- Once a winner is chosen, the output mirrors the winner's entire Observable (including completion and errors)
- If all sources complete without emitting, the output completes without emitting

**`raceWith`** (pipeable form): treats the upstream Observable as the first participant.

## Marble Diagram

```
Source A:  -------a1--a2--a3--|
Source B:  ---b1--b2-----------|
Source C:  ----------c1--------|

race(A, B, C):
           B emits b1 first → B wins
           A and C are unsubscribed immediately after b1

Result:    ---b1--b2-----------|   (mirrors B entirely)

race with timeout pattern:
ajax$:     --------data|
timeout$:  ------#  (errors after 5s — but errors before next don't win)

Actually:
ajax$:     --------data|
timeout$:  -----timeout-value-or-error

If timeout$ emits before ajax$:
Result:    -----timeout|  (timeout wins, ajax is cancelled)
```

## Type System Integration

```typescript
import { race, of, timer } from 'rxjs';
import { map } from 'rxjs/operators';

// Output type is union of all source types
const result$ = race(
  of(1, 2, 3),       // Observable<number>
  of('a', 'b', 'c')  // Observable<string>
);
// result$: Observable<number | string>

// Timeout race — both same type
import { ajax } from 'rxjs/ajax';
const withTimeout$ = race(
  ajax.getJSON<User>('/api/user'),
  timer(5000).pipe(map(() => { throw new Error('timeout'); }))
);
// Observable<User>  (throwError absorbed into the error channel)
```

## Examples

### Basic Usage
```typescript
import { race, of, timer } from 'rxjs';
import { delay } from 'rxjs/operators';

const fast$   = of('fast').pipe(delay(100));
const medium$ = of('medium').pipe(delay(200));
const slow$   = of('slow').pipe(delay(500));

race(fast$, medium$, slow$).subscribe(console.log); // 'fast'
// medium$ and slow$ are unsubscribed after fast$ emits
```

### Common Pattern — Request Timeout
```typescript
import { race, timer, throwError } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { switchMap } from 'rxjs/operators';

function withTimeout<T>(source$: Observable<T>, ms: number): Observable<T> {
  return race(
    source$,
    timer(ms).pipe(
      switchMap(() => throwError(() => new Error(`Timed out after ${ms}ms`)))
    )
  );
}

withTimeout(ajax.getJSON<User>('/api/user'), 5000).subscribe({
  next:  user => renderUser(user),
  error: err  => showError(err.message)
});

// Note: the built-in timeout() operator is cleaner for this specific case:
// ajax.getJSON('/api/user').pipe(timeout(5000))
// Use race when you need custom fallback logic beyond just erroring
```

### Common Pattern — Primary With Fallback
```typescript
import { race } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { catchError, of } from 'rxjs';

// Try primary CDN, fall back to secondary if secondary responds first
const primary$   = ajax.getJSON('/cdn-primary/resource');
const secondary$ = ajax.getJSON('/cdn-secondary/resource');

race(primary$, secondary$).subscribe(data => render(data));
// Whichever CDN responds first wins; other request is cancelled
```

### Common Pattern — `raceWith` in a Pipe
```typescript
import { fromEvent, timer } from 'rxjs';
import { raceWith, map } from 'rxjs/operators';

// User action vs automatic timeout
fromEvent(document, 'click').pipe(
  raceWith(
    timer(10_000).pipe(map(() => 'auto-proceed'))
  )
).subscribe(result => {
  // Either the user clicked (MouseEvent) or timer fired ('auto-proceed')
  if (result === 'auto-proceed') proceedAutomatically();
  else handleClick(result as MouseEvent);
});
```

### Edge Case — Error Before Emission Does Not Win
```typescript
import { race, throwError, of } from 'rxjs';
import { delay } from 'rxjs/operators';

// throwError emits an error synchronously — does it win the race?
race(
  throwError(() => new Error('fast error')), // errors immediately
  of('value').pipe(delay(100))               // emits after 100ms
).subscribe({
  next:  v => console.log('winner:', v),
  error: e => console.log('error:', e.message)
});
// Output: error: fast error
// The error from throwError propagates — it IS the "first event"
// NOTE: An error IS treated as a race-winning event — it propagates immediately
// (unlike completion, which just eliminates the source from the race)
```

## Common Pitfalls

### Anti-pattern: Using `race` When `combineLatest` or `forkJoin` Is Needed
```typescript
import { race, combineLatest, forkJoin } from 'rxjs';

// ❌ WRONG — race discards all but the fastest; you get only one result
race(
  fetchUserPreferences(),
  fetchUserProfile(),
  fetchUserPermissions()
).subscribe(result => setup(result)); // gets only whichever API responded first!

// ✅ CORRECT — forkJoin for "all must complete"; combineLatest for "live updates"
forkJoin({
  preferences: fetchUserPreferences(),
  profile:     fetchUserProfile(),
  permissions: fetchUserPermissions()
}).subscribe(({ preferences, profile, permissions }) => setup({ preferences, profile, permissions }));

// WHY: race discards all non-winning sources after the first emission.
// Use race only when you genuinely want the fastest source and the others
// are redundant (CDN fallback, timeout guards, equivalent endpoints).
// Use forkJoin when you need ALL results.
```

### Anti-pattern: Reusing a `WebSocketSubject` in `race`
```typescript
// ❌ PROBLEM — race unsubscribes the losing WebSocket subjects,
// closing connections that may be needed elsewhere
const ws1$ = webSocket('wss://server1.com');
const ws2$ = webSocket('wss://server2.com');

race(ws1$, ws2$).subscribe(handleMessage);
// ws2$ is unsubscribed (closed) after ws1$ emits first
// If ws2$ was also subscribed elsewhere, it's now closed!

// ✅ CORRECT — only race disposable sources, or use asObservable() copies
race(ws1$.asObservable(), ws2$.asObservable())
  .subscribe(handleMessage);
// The underlying WebSocket connections remain open; only the subscriptions race
```

## Related Operators

- **`forkJoin`**: Waits for ALL sources to complete — use when you need every result
- **`combineLatest`**: Combines ALL sources reactively — no winner/loser concept
- **`merge`**: Forwards all emissions from all sources — no cancellation
- **`timeout`**: Built-in timeout operator — cleaner for simple "error after N ms" patterns
- **`iif`**: Conditional source selection at subscription time (not race-based)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/race](https://rxjs.dev/api/index/function/race)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 3/5
**Key teaching points**:
1. First to emit `next` wins — completion before any `next` just eliminates that source; errors propagate immediately
2. Winning source's entire lifecycle (including completion/error) is forwarded
3. Use for CDN fallback, redundant endpoints, timeout races — not as a substitute for `forkJoin`
