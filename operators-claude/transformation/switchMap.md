# switchMap

## Identity
- **Name**: switchMap
- **Category**: Transformation Operators
- **Type**: Higher-order transformation operator (flattening strategy — cancelling)
- **Import**:
  ```typescript
  import { switchMap } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { switchMap } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // Most common form
  function switchMap<T, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O
  ): OperatorFunction<T, ObservedValueOf<O>>

  // With deprecated resultSelector (RxJS 6 — avoid)
  function switchMap<T, R, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O,
    resultSelector?: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R
  ): OperatorFunction<T, ObservedValueOf<O> | R>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable emitting trigger values

**Output**: `Observable<ObservedValueOf<O>>` — an Observable emitting values from the most recent inner Observable only

**Transformation**: For each source emission, `switchMap` applies the projection function to create a new inner Observable, **immediately unsubscribes from any previously active inner Observable**, and subscribes to the new one. Only one inner Observable is active at any time — the most recently created one.

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let project: T → Observable<R> be the projection function
Let O_i = project(v_i) be the inner Observable for each v_i

At time t:
  Output = emissions from O_j where j = max{ i : v_i emitted before t }

When v_k arrives:
  1. Unsubscribe from O_(k-1)  ← cancellation
  2. Subscribe to O_k           ← new inner
  3. Forward all emissions from O_k until v_(k+1) arrives
```

**Invariants**:
- **Single active subscription**: At most one inner Observable is subscribed at any time
- **Automatic cancellation**: Previous inner Observable is unsubscribed immediately when a new source value arrives
- **Latest-wins semantics**: Only the most recent inner Observable can produce output
- **Memory bounded**: O(1) — at most one inner subscription maintained
- **Immediate cancellation**: Unsubscription of the previous inner is synchronous
- **No queuing**: Source emissions trigger immediate cancellation and re-subscription; nothing is buffered

## Marble Diagram

```
Source:    --a--------b--c------|
               |       |  |
               v       v  v
           project(x) = Observable emitting x1, x2, x3 over time
               |       |  |
Inner a:       --a1--a2--a3...   (cancelled when b arrives ✂️)
Inner b:               --b1--b2... (cancelled when c arrives ✂️)
Inner c:                  --c1--c2--c3|
               switchMap(project)
Result:    -----a1--a2--b1--c1--c2--c3|

Legend:
  - : time unit (10ms)
  a,b,c : source values
  a1,a2 : emissions from inner Observable for 'a'
  ✂️ : cancellation point
  | : completion
```

**Cancellation in practice — HTTP requests**:
```
Source:    --1-----------2------|
               |          |
               v          v
           project(n) = HTTP request (~500ms)
               |          |
Inner 1:       ----response1    (cancelled before completing ✂️)
Inner 2:                  ----response2|
               switchMap(project)
Result:    -------------------response2|

response1 never reaches the subscriber.
```

**Key observation**: `switchMap` answers "what does the user want *right now*?" — every new source emission declares the previous answer obsolete.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily on output subscription
- Creates a new inner subscription for each source emission
- Cancels (unsubscribes from) the previous inner Observable immediately and synchronously
- Only one inner Observable is active at any time

**Completion semantics**:
- Waits for both the source AND the currently active inner Observable to complete
- Source completion does NOT cancel the active inner Observable — it lets it finish
- Result completes when:
  1. Source has completed AND
  2. The final inner Observable has completed
- Empty source (completes without emitting) causes immediate output completion

**Error handling**:
- Source errors propagate immediately, cancelling any active inner subscription
- Errors from the current inner Observable propagate immediately to the output
- Previously cancelled inner Observables cannot emit errors (they are unsubscribed)
- No built-in error recovery; use `catchError` inside the inner pipeline

**Backpressure**:
- No buffering — previous inner Observables are cancelled rather than queued
- Fast sources naturally rate-limit themselves through cancellation
- Memory usage is O(1) — only the current inner subscription is held

**Hot vs. Cold**:
- Works identically with hot and cold sources
- With hot inner Observables, cancellation via `unsubscribe` stops listening but does not stop the underlying producer — use `finalize` inside the inner pipeline to clean up shared resources
- With cold inner Observables (e.g. HTTP), cancellation typically aborts the underlying request (framework-dependent)

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source Observable emission type
 *   O extends ObservableInput<any> - Inner Observable type (Observable, Promise, Array, etc.)
 *   R - Result type, inferred as ObservedValueOf<O>
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<ObservedValueOf<O>>
 *   - Extracts the emission type from the inner ObservableInput
 *   - Flattens the nested Observable structure
 *
 * Type Narrowing:
 *   - R is inferred from the project function's return type
 *   - Supports Promises, Arrays, Iterables (auto-converted to Observables)
 *   - Union types are preserved through flattening
 *   - Cancellation is type-transparent (does not affect T or R)
 *
 * Type Safety:
 *   - Compile-time verification that project accepts T and returns ObservableInput
 *   - No Observable<Observable<T>> leaks — output is always flat
 */

import { fromEvent, of } from 'rxjs';
import { switchMap, debounceTime, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface SearchResult { id: number; title: string; }

const searchInput = document.getElementById('search') as HTMLInputElement;

// T = Event, R = SearchResult[] — inferred from ajax.getJSON return type
const results$ = fromEvent(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),
  switchMap(query => ajax.getJSON<SearchResult[]>(`/api/search?q=${query}`))
);
// results$: Observable<SearchResult[]>

// Promise auto-conversion: T = number, R = { name: string; email: string }
async function fetchUser(id: number) {
  const r = await fetch(`/api/users/${id}`);
  return r.json() as Promise<{ name: string; email: string }>;
}

of(1, 2, 3).pipe(
  switchMap(id => fetchUser(id))
).subscribe(user => {
  console.log(user.name);  // type-safe: string
  console.log(user.email); // type-safe: string
});

// Union type preservation
const button$ = fromEvent(document.getElementById('btn')!, 'click');

const mixed$ = button$.pipe(
  switchMap(() =>
    Math.random() > 0.5 ? of('string result') : of(42)
  )
);
// mixed$: Observable<string | number>

mixed$.subscribe(value => {
  if (typeof value === 'string') console.log(value.toUpperCase());
  else                           console.log(value.toFixed(2));
});
```

## Examples

### Basic Usage — Search with Automatic Cancellation
```typescript
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const searchBox = document.getElementById('search') as HTMLInputElement;

const searchResults$ = fromEvent(searchBox, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query => ajax.getJSON(`/api/search?q=${query}`))
);

searchResults$.subscribe(results => displayResults(results));

// User types "rxjs" quickly:
// 'r'    → request 1 starts
// 'rx'   → request 1 CANCELLED, request 2 starts
// 'rxj'  → request 2 CANCELLED, request 3 starts
// 'rxjs' → request 3 CANCELLED, request 4 starts
// 300ms silence → request 4 completes, results displayed
// Only one result set reaches the subscriber.
```

### Common Pattern — Route Navigation
```typescript
import { Subject } from 'rxjs';
import { switchMap, finalize } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface Route    { path: string; params: Record<string, string>; }
interface PageData { title: string; content: string; }

const router$ = new Subject<Route>();

const pageData$ = router$.pipe(
  switchMap(route =>
    ajax.getJSON<PageData>(`/api/pages${route.path}`).pipe(
      finalize(() => console.log(`Load complete/cancelled: ${route.path}`))
    )
  )
);

pageData$.subscribe(data => renderPage(data));

router$.next({ path: '/home', params: {} });

setTimeout(() => {
  router$.next({ path: '/about', params: {} });
  // /home load is cancelled; only /about renders
}, 100);
```

### Common Pattern — Form Auto-save
```typescript
import { fromEvent, merge } from 'rxjs';
import { debounceTime, switchMap, map, distinctUntilChanged, tap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const titleInput   = document.getElementById('title')   as HTMLInputElement;
const contentInput = document.getElementById('content') as HTMLTextAreaElement;

const formChange$ = merge(
  fromEvent(titleInput,   'input'),
  fromEvent(contentInput, 'input')
);

const autoSave$ = formChange$.pipe(
  debounceTime(1000),
  map(() => ({ title: titleInput.value, content: contentInput.value })),
  distinctUntilChanged((a, b) => a.title === b.title && a.content === b.content),
  tap(() => showStatus('Saving…')),
  switchMap(formData =>
    ajax.post('/api/save', formData).pipe(
      map(() => ({ ...formData, savedAt: new Date() })),
      finalize(() => console.log('save complete/cancelled'))
    )
  )
);

autoSave$.subscribe({
  next:  r   => showStatus(`Saved at ${r.savedAt.toLocaleTimeString()}`),
  error: err => showStatus(`Error: ${err.message}`),
});

// User types → wait 1s → save starts
// User types again mid-save → previous save cancelled, new save starts after 1s
```

### Edge Cases — Completion, Empty Inner, Synchronous Sources
```typescript
import { of, throwError, EMPTY } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

// Edge case 1: source completes while inner Observable is active — inner finishes
of(1).pipe(
  switchMap(() => of(10, 20, 30).pipe(delay(100)))
).subscribe({ next: v => console.log(v), complete: () => console.log('done') });
// Output: 10, 20, 30, done  (inner runs to completion)

// Edge case 2: empty inner Observable
of(1, 2, 3).pipe(
  switchMap(n => n === 2 ? EMPTY : of(n * 10))
).subscribe(v => console.log(v));
// Output: 10, 30  (n=2 produces nothing; not an error)

// Edge case 3: synchronous sources — all inner emissions complete before cancellation
of(1, 2, 3).pipe(
  switchMap(n => of(n * 10, n * 20))
).subscribe(v => console.log(v));
// Output: 10, 20, 20, 40, 30, 60
// All three synchronous inner Observables complete before any cancellation occurs.
// Cancellation only takes effect between asynchronous ticks.

// Edge case 4: error in inner Observable
of(1, 2, 3).pipe(
  switchMap(n =>
    n === 2
      ? throwError(() => new Error('inner error'))
      : of(n * 10).pipe(delay(50))
  )
).subscribe({ next: v => console.log(v), error: e => console.log('error:', e.message) });
// Output:
// 10
// error: inner error  (n=3 never runs)
```

### Advanced Pattern — Cancellable Request with Retry
```typescript
import { fromEvent } from 'rxjs';
import { switchMap, retry, catchError, map, tap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';
import { of } from 'rxjs';

type LoadResult =
  | { status: 'success'; data: unknown[] }
  | { status: 'error';   message: string };

const refresh$ = fromEvent(document.getElementById('refresh')!, 'click');

const data$ = refresh$.pipe(
  tap(() => showSpinner(true)),
  switchMap(() =>
    ajax.getJSON<unknown[]>('/api/tasks').pipe(
      retry({ count: 3, delay: 1000 }),
      map((data): LoadResult => ({ status: 'success', data })),
      catchError((err): ReturnType<typeof of<LoadResult>> =>
        of({ status: 'error', message: err.message })
      ),
      finalize(() => showSpinner(false))
    )
  )
);

data$.subscribe(result => {
  if (result.status === 'success') renderTasks(result.data);
  else                             showError(result.message);
});

// Click refresh mid-load → previous request (including its retries) is cancelled.
// Only the latest click's result ever renders.
```

## Common Pitfalls

### Anti-pattern: Using `switchMap` When All Results Are Needed
```typescript
import { of } from 'rxjs';
import { switchMap, mergeMap, delay } from 'rxjs/operators';

// ❌ INCORRECT — processing a collection but only getting the last result
of(1, 2, 3, 4, 5).pipe(
  switchMap(n => of(`Result ${n}`).pipe(delay(100)))
).subscribe(v => console.log(v));
// Output: Result 5
// Items 1–4 are cancelled before their inner Observables complete

// ✅ CORRECT — use mergeMap when every result is needed
of(1, 2, 3, 4, 5).pipe(
  mergeMap(n => of(`Result ${n}`).pipe(delay(100)))
).subscribe(v => console.log(v));
// Output: Result 1, Result 2, Result 3, Result 4, Result 5

// WHY: switchMap cancels previous inner Observables on each source emission.
// When source is a finite collection (from(), of()), every emission but the
// last is immediately cancelled. Use switchMap only for user-driven triggers
// where only the latest action matters; use mergeMap for collections.
```

### Anti-pattern: Missing Cleanup with `finalize`
```typescript
import { fromEvent, interval } from 'rxjs';
import { switchMap, take, map, finalize } from 'rxjs/operators';

let openConnections = 0;

// ❌ INCORRECT — no cleanup when inner Observable is cancelled
fromEvent(document, 'click').pipe(
  switchMap(() => {
    openConnections++;
    return interval(1000).pipe(take(10), map(n => `tick ${n}`));
    // openConnections never decremented on cancellation
  })
).subscribe(console.log);
// After 5 clicks: openConnections = 5, but only 1 is active

// ✅ CORRECT — use finalize to clean up on cancellation OR completion
fromEvent(document, 'click').pipe(
  switchMap(() => {
    openConnections++;
    return interval(1000).pipe(
      take(10),
      map(n => `tick ${n}`),
      finalize(() => openConnections--) // runs on cancel AND complete
    );
  })
).subscribe(console.log);

// WHY: Cancellation via unsubscribe does not trigger error/complete callbacks.
// finalize() is the only operator that fires on all three terminal events
// (complete, error, unsubscribe), making it the correct place for cleanup.
```

### Anti-pattern: Synchronous Inner Observables and Unexpected Emissions
```typescript
import { of } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

// ❌ SURPRISING — developer expects only the last inner Observable's values
of(1, 2, 3).pipe(
  switchMap(n => of(n * 10, n * 20))
).subscribe(v => console.log(v));
// Expected: 30, 60  (only from n=3)
// Actual:   10, 20, 20, 40, 30, 60  (all three complete synchronously)

// ✅ CORRECT — add async step so cancellation can take effect between ticks
of(1, 2, 3).pipe(
  switchMap(n => of(n * 10, n * 20).pipe(delay(0)))
).subscribe(v => console.log(v));
// Output: 30, 60  (only from n=3)

// WHY: JavaScript is single-threaded. Synchronous Observables complete
// entirely within the current tick before the next source emission is processed.
// switchMap cancellation happens between ticks, so synchronous inner Observables
// always run to completion. In practice, most real-world inner Observables
// (HTTP, timers) are async, so this is rarely a surprise in production code.
```

### Anti-pattern: Using `switchMap` for Sequential Dependent Operations
```typescript
import { from } from 'rxjs';
import { switchMap, concatMap, delay } from 'rxjs/operators';

const steps = [
  { id: 1, name: 'Initialize' },
  { id: 2, name: 'Process'    },
  { id: 3, name: 'Finalize'   },
];

// ❌ INCORRECT — steps 1 and 2 are cancelled before completing
from(steps).pipe(
  switchMap(step => of(step).pipe(delay(100)))
).subscribe(s => console.log('Done:', s.name));
// Output: Done: Finalize

// ✅ CORRECT — use concatMap for sequential, ordered operations
from(steps).pipe(
  concatMap(step => of(step).pipe(delay(100)))
).subscribe(s => console.log('Done:', s.name));
// Output: Done: Initialize, Done: Process, Done: Finalize

// WHY: switchMap is for "only the latest matters" — user-triggered actions.
// For multi-step workflows where each step must complete before the next,
// use concatMap (sequential) or mergeMap (parallel, all results needed).
```

### Performance: Excessive Cancellations on High-Frequency Sources
**When this matters**:
High-frequency sources (mousemove, scroll) paired with expensive inner Observable setup (WebSocket connection, heavy DOM operation) cancel and restart too rapidly.

**What to do**:
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, throttleTime, switchMap, auditTime } from 'rxjs/operators';
import { asyncScheduler } from 'rxjs';

// Rate-limit BEFORE switchMap to reduce cancellation churn
fromEvent(window, 'scroll').pipe(
  auditTime(100),                    // at most one trigger per 100ms
  switchMap(() => loadVisibleItems())
).subscribe(render);

fromEvent(window, 'mousemove').pipe(
  throttleTime(100, asyncScheduler, { leading: true, trailing: true }),
  switchMap(e => computeHover(e))
).subscribe(updateTooltip);
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`mergeMap`**: Concurrent — does NOT cancel previous inner Observables; all run simultaneously. Use when every result matters (parallel HTTP calls, processing collections).
- **`concatMap`**: Sequential — queues emissions, processes one at a time in order. Use when order matters (step-by-step workflows, dependent operations).
- **`exhaustMap`**: Ignoring — discards new source emissions while an inner Observable is active. Use to prevent duplicates (form submit buttons, save actions).
- **`switchAll`**: Flattens `Observable<Observable<T>>` with cancellation semantics, without a projection function.

**Complementary Operators**:
- **`debounceTime`**: Rate-limit source before `switchMap` — the canonical search pattern: `debounceTime(300), switchMap(search)`
- **`distinctUntilChanged`**: Skip unchanged queries — prevents unnecessary cancellation and re-subscription
- **`catchError`**: Handle inner errors without terminating the outer stream: `switchMap(x => work(x).pipe(catchError(...)))`
- **`retry`**: Retry failed inner Observables: `switchMap(x => work(x).pipe(retry(3)))`
- **`finalize`**: Clean up resources on cancellation OR completion: `switchMap(x => work(x).pipe(finalize(cleanup)))`

**Alternatives by Use Case**:

| Use Case | Instead of `switchMap` | Use This | Why |
|----------|------------------------|----------|-----|
| All results needed | `from(items).pipe(switchMap(...))` | `mergeMap(...)` | No cancellation, all complete |
| Sequential steps | `switchMap(x => step(x))` | `concatMap(x => step(x))` | Ordered, no cancellation |
| Prevent duplicates | `clicks$.pipe(switchMap(save))` | `exhaustMap(save)` | Ignore while active |
| Search (latest only) | — | `switchMap` ✓ | Correct choice |
| Route navigation | — | `switchMap` ✓ | Correct choice |
| Real-time data polling | — | `switchMap` ✓ | Cancel old, start new |

**Flattening operator comparison**:

| Operator | Concurrent | Cancels previous | Order preserved | Use when |
|----------|------------|------------------|-----------------|----------|
| `switchMap` | 1 (latest) | Yes | No | Only latest result matters |
| `mergeMap` | Unlimited | No | No | All results needed, parallel |
| `concatMap` | 1 (queued) | No | Yes | Order matters, sequential |
| `exhaustMap` | 1 (first) | No | N/A | Prevent duplicate operations |

**Migration from deprecated `resultSelector`**:
```typescript
// RxJS 6 — deprecated resultSelector form
source$.pipe(
  switchMap(outer => inner$, (outer, inner) => ({ outer, inner }))
)

// RxJS 7+ — use map inside the inner pipeline
source$.pipe(
  switchMap(outer => inner$.pipe(map(inner => ({ outer, inner }))))
)
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/switchMap](https://rxjs.dev/api/operators/switchMap)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/switchMap.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/switchMap.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Cancelling Composition Strategy (Latest-Only Flattening)
- **Cognitive Load**: 4/5 — The cancellation mechanic is intuitive once understood, but the synchronous inner Observable edge case and the distinction from other flattening operators require deliberate study
- **Usage Frequency**: 5/5 — Essential for every user-driven async operation; in virtually every production UI
- **Composability**: 5/5 — Pairs naturally with `debounceTime`, `distinctUntilChanged`, `catchError`, `retry`, `finalize`

**Problem Domain**:
User-driven asynchronous operations where only the most recent action's result is relevant and previous in-flight operations represent wasted work or potential race conditions. Classic examples: search-as-you-type, route navigation, real-time data subscriptions, live preview generation.

**When to Teach**:
After `mergeMap` — `switchMap` is most clearly understood by contrast: "like `mergeMap`, but cancels the previous inner Observable instead of keeping it." Teach alongside `debounceTime` using the canonical search pipeline.

- **Prerequisites**: `map`, `filter`, `mergeMap`, understanding of Observable subscription and cancellation
- **Teaches**: Automatic cancellation, race condition prevention, the "latest wins" pattern
- **Leads to**: `exhaustMap` (the complementary opposite — ignore new while active), `concatMap` (sequential), cleanup with `finalize`
- **Common with**: `debounceTime`, `distinctUntilChanged`, `catchError`, `finalize`

**Common Misconceptions**:
1. **"switchMap is always better than mergeMap"** — no; use `mergeMap` when all results are needed
2. **"Cancellation is wasteful"** — it prevents race conditions and resource waste
3. **"Cancelled operations throw errors"** — no; they silently unsubscribe; use `finalize` to detect cancellation
4. **"switchMap prevents all race conditions"** — mostly true for async inner Observables, but synchronous ones complete before cancellation takes effect
5. **"switchMap preserves order"** — it does not; order is irrelevant since only the latest inner Observable produces output
