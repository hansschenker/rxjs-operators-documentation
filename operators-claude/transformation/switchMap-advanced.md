# switchMap — Advanced Patterns

For `switchMap` fundamentals, see the core [switchMap](./switchMap) doc. This page covers advanced patterns, race conditions, and when NOT to use `switchMap`.

---

## What `switchMap` Guarantees (and Doesn't)

`switchMap` **cancels the previous inner Observable** when a new outer emission arrives. This means:

- ✅ Only the latest request is active
- ✅ No stale responses from earlier requests
- ❌ Previous inner work is **lost** — not queued, not completed
- ❌ Side effects in cancelled Observables may or may not have run

---

## Pattern 1: Type-ahead Search (The Classic)

```typescript
import { switchMap, debounceTime, distinctUntilChanged, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

searchInput$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query =>
    query.length < 2
      ? EMPTY                             // don't search on very short input
      : this.api.search(query).pipe(
          catchError(() => EMPTY)         // don't kill stream on failed search
        )
  )
).subscribe(renderResults);
```

The key: `catchError(() => EMPTY)` **inside** the `switchMap` projection. If it were outside, any single search error would kill the entire search stream.

---

## Pattern 2: Route-Driven Data Loading

```typescript
import { ActivatedRoute } from '@angular/router';
import { switchMap, catchError, of } from 'rxjs';

@Component({ ... })
export class DetailComponent {
  item$ = this.route.paramMap.pipe(
    map(p => p.get('id')!),
    distinctUntilChanged(),              // only reload on actual ID change
    switchMap(id =>
      this.api.getItem(id).pipe(
        catchError(err =>
          err.status === 404
            ? of(null)                   // show empty state for 404
            : throwError(() => err)      // rethrow unexpected errors
        )
      )
    )
  );
}
```

---

## Pattern 3: Form Submit — When `switchMap` Is WRONG

`switchMap` cancels previous submissions — a user clicking Submit twice would cancel the first payment.

```typescript
// ❌ WRONG for form submit — cancels first request if button clicked twice
submitBtn$.pipe(
  switchMap(() => this.api.submitOrder(this.form.value))
).subscribe(handleSuccess);
// If user double-clicks: first order request cancelled, second proceeds.
// Payment may or may not have processed on the server.

// ✅ exhaustMap — ignore subsequent clicks while submission is in flight:
submitBtn$.pipe(
  exhaustMap(() => this.api.submitOrder(this.form.value).pipe(
    catchError(err => {
      this.showError(err);
      return EMPTY;
    })
  ))
).subscribe(handleSuccess);

// ✅ concatMap — queue clicks (rarely correct for forms):
submitBtn$.pipe(
  concatMap(() => this.api.submitOrder(this.form.value))
).subscribe(handleSuccess);
```

**Rule**: `switchMap` is safe when cancelling is semantically correct. Reads: yes. Writes: almost never.

---

## Pattern 4: Cancellable Long-Running Operations

```typescript
import { switchMap, takeUntil } from 'rxjs/operators';
import { Subject } from 'rxjs';

const cancel$ = new Subject<void>();

startBtn$.pipe(
  switchMap(() =>
    longOperation$.pipe(
      takeUntil(cancel$)              // cancel$ also works inside
    )
  )
).subscribe({
  next:     result   => renderResult(result),
  complete: ()       => hideSpinner()
});

// Also cancels on new startBtn$ emission (switchMap behavior)
cancelBtn$.subscribe(() => cancel$.next());
```

---

## Pattern 5: Preventing Stale Data (The Race Condition `switchMap` Solves)

```typescript
// ❌ WITHOUT switchMap — stale data race:
mergeMap(id => this.api.getUser(id))
// Sequence: request A (slow), request B (fast)
// B resolves first: UI shows B
// A resolves: UI shows A (stale!) ← race condition

// ✅ WITH switchMap — always latest:
switchMap(id => this.api.getUser(id))
// Request B cancels A: only B can resolve
// No stale data possible
```

---

## Pattern 6: The `switchMap` Race Condition (When IT Causes Problems)

`switchMap` creates its own race condition for **side effects**:

```typescript
// ❌ RACE CONDITION IN WRITES — switchMap on mutations
userIdChanges$.pipe(
  switchMap(id => this.api.savePreferences(id, this.prefs))
  // If ID changes mid-save: first save is cancelled
  // BUT: the HTTP request may already be in-flight on the server
  // Server processes BOTH saves; client only knows about the second
)

// The server sees:
// 1. PATCH /users/1/prefs → in-flight when cancelled
// 2. PATCH /users/2/prefs → completes
// Result: user 1's prefs may have been partially written

// ✅ Use concatMap for writes that must complete:
userIdChanges$.pipe(
  concatMap(id => this.api.savePreferences(id, this.prefs))
  // Queues saves; each completes before the next starts
)
```

---

## Pattern 7: Window/Tab Visibility-Driven Data Refresh

```typescript
import { fromEvent, merge, of } from 'rxjs';
import { switchMap, filter, startWith } from 'rxjs/operators';

const visible$ = fromEvent(document, 'visibilitychange').pipe(
  map(() => document.visibilityState === 'visible'),
  startWith(true)
);

// Reload data when tab becomes visible; cancel if hidden again
visible$.pipe(
  filter(Boolean),
  switchMap(() =>
    // Poll while visible, cancel (and stop polling) when hidden
    interval(30_000).pipe(
      startWith(0),
      switchMap(() => this.api.getData())
    )
  )
).subscribe(renderData);
```

---

## Pattern 8: Loading State with `switchMap`

Track loading state across cancellations:

```typescript
import { BehaviorSubject } from 'rxjs';
import { switchMap, tap, finalize } from 'rxjs/operators';

const loading$ = new BehaviorSubject(false);

trigger$.pipe(
  tap(() => loading$.next(true)),
  switchMap(value =>
    this.api.fetch(value).pipe(
      finalize(() => loading$.next(false)) // clears on complete OR cancel
    )
  )
).subscribe(render);

// finalize() runs when the inner Observable unsubscribes (including via switchMap cancel)
// This ensures loading clears even when a new trigger cancels the previous request
```

---

## `switchMap` vs Other Flattening — Decision Table

| Situation | Use |
|---|---|
| Search / autocomplete | `switchMap` |
| Route param change → load data | `switchMap` |
| Form submit (non-idempotent) | `exhaustMap` |
| Must process every item in order | `concatMap` |
| Independent parallel requests | `mergeMap` |
| State accumulation | `mergeScan` / `switchScan` |

---

## Common Pitfalls

### `catchError` Outside `switchMap` Kills the Stream

```typescript
// ❌ ONE ERROR KILLS ALL FUTURE SEARCHES
searchInput$.pipe(
  switchMap(q => this.api.search(q)),
  catchError(() => EMPTY)  // stream ends after first error!
).subscribe(render);

// ✅ Catch INSIDE the switchMap projection:
searchInput$.pipe(
  switchMap(q =>
    this.api.search(q).pipe(
      catchError(() => EMPTY) // only this inner Observable ends
    )
  )
).subscribe(render);
// WHY: catchError replaces the Observable it's applied to. Outside switchMap,
// it replaces the entire outer stream. Inside, it only replaces the one inner Observable.
```

### Assuming Cancellation Means the Server Request Was Cancelled

```typescript
// switchMap unsubscribes from the Observable returned by Angular HttpClient.
// HttpClient cancels the underlying XMLHttpRequest on unsubscribe.
// BUT: fetch()-based APIs (fromFetch) also cancel via AbortController.
// HOWEVER: custom Observables wrapping setTimeout/Promise may NOT cancel.

// ✅ Always verify your Observable actually cancels on unsubscribe:
function fetchItem(id: string): Observable<Item> {
  return new Observable(subscriber => {
    const controller = new AbortController();
    fetch(`/api/items/${id}`, { signal: controller.signal })
      .then(r => r.json())
      .then(data => { subscriber.next(data); subscriber.complete(); })
      .catch(err => { if (!controller.signal.aborted) subscriber.error(err); });
    return () => controller.abort(); // ← cancellation hook
  });
}
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**The single most important rule**: `switchMap` is for reads; `exhaustMap` is for writes. When in doubt, ask "is cancelling the previous operation semantically correct?"
