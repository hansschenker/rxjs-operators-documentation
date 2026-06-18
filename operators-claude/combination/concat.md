# concat

## Identity
- **Name**: concat
- **Category**: Combination Operators (Join Creation)
- **Type**: Sequential stream combinator — subscribes to each source in order, only starting the next after the previous completes
- **Import**:
  ```typescript
  import { concat } from 'rxjs';              // creation function
  import { concatWith } from 'rxjs/operators'; // pipeable form (RxJS 7+)
  ```
- **Signature**:
  ```typescript
  // Creation function
  function concat<T>(
    ...sources: Array<ObservableInput<T>>
  ): Observable<T>

  // Pipeable form
  function concatWith<T, A extends readonly unknown[]>(
    ...otherSources: [...{ [K in keyof A]: ObservableInput<A[K]> }]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

**Input**: Two or more `ObservableInput<T>` sources

**Output**: `Observable<T>` — emissions from all sources in sequence, one after another

**Transformation**: Subscribes to the first source. Forwards all of its emissions. When it completes, subscribes to the second source and forwards all of its emissions. Continues until all sources have been subscribed and completed, then completes the output. If any source errors, propagates immediately and does not continue to subsequent sources.

**Mathematical representation**:
```
concat(S₁, S₂, ..., Sₙ) =
  emissions(S₁)  ++ emissions(S₂)  ++ ... ++ emissions(Sₙ)
  where ++ means "followed by, in order"

Timeline:
  0 → S₁ subscribes
  S₁ completes → S₂ subscribes
  S₂ completes → S₃ subscribes
  ...
  Sₙ completes → output completes
```

**Invariants**:
- **Sequential subscriptions**: Only one source is active at any time — no concurrency
- **Order guaranteed**: Emissions from S₁ always precede emissions from S₂
- **Completion-gated**: S₂ is not subscribed until S₁ completes — if S₁ never completes, S₂ never starts
- **Error short-circuit**: Any source error terminates the chain; subsequent sources are never subscribed

## Marble Diagram

```
S1:     --a--b--c--|
S2:     --d--e--f--|  (not subscribed until S1 completes)
S3:     --g--------|
        concat(S1, S2, S3)
Result: --a--b--c----d--e--f----g--|

S2 begins after S1's completion marker.
S3 begins after S2's completion marker.
Total output length = sum of all source lengths.
```

**Contrast with `merge`**:
```
merge(S1, S2):   subscribes to both immediately, interleaves
concat(S1, S2):  subscribes to S1, waits for completion, then subscribes S2

S1: --a-----c--|
S2: ---b--d----|

merge:  --a-b---c-d----|
concat: --a-----c-----b--d----|  (S2 starts at S1's |)
```

**Endless source blocks concat**:
```
S1: --a--b--c--...  (never completes, e.g. interval)
S2: --d--e--|

concat(S1, S2): --a--b--c--...  (S2 never starts)
```

**Key observation**: `concat` is a sequential queue — use it when operations must happen in order and each step should not start until the previous finishes.

## Behavioral Characteristics

**Subscription**:
- S₁ subscribed when concat's output is subscribed
- Each subsequent source subscribed only after the previous completes
- Only one active subscription at any time

**Completion semantics**:
- Output completes when the last source completes
- If any source never completes, subsequent sources and output completion are deferred forever

**Error handling**:
- First error from any source propagates immediately
- Subsequent sources (not yet subscribed) are never started

**Backpressure**:
- None — values forwarded synchronously as they arrive from the currently-active source

**Hot vs. Cold**:
- Cold sources start fresh when subscribed; concat's sequential model works naturally with cold sources
- Hot sources: the hot source is subscribed at its turn in the sequence — all emissions before that point are missed
- Typically used with cold sources (HTTP requests, timers, computed sequences)

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type; for homogeneous sources Observable<T>
 *   For heterogeneous: Observable<T1 | T2 | ...>
 *
 * concatWith returns OperatorFunction<T, T | A[number]>
 * where A is the tuple of other-source types.
 */

import { concat, of, timer } from 'rxjs';
import { concatWith, map } from 'rxjs/operators';

// Homogeneous — Observable<number>
const numbers$: Observable<number> = concat(
  of(1, 2, 3),
  of(4, 5, 6)
);

// Heterogeneous — Observable<string | number>
const mixed$ = concat(
  of('start'),
  of(1, 2, 3),
  of('end')
); // TypeScript infers Observable<string | number>

// Pipeable form
const result$ = of(1, 2, 3).pipe(
  concatWith(of(4, 5, 6))
); // Observable<number>

// Practical: loading sequence with typed phases
type Phase = 'init' | 'data' | 'ready';
const bootSequence$: Observable<Phase> = concat(
  of('init' as Phase),
  initializeApp().pipe(map(() => 'data' as Phase)),
  of('ready' as Phase)
);
```

## Examples

### Basic Usage — Sequential Values and Completion
```typescript
import { concat, of, timer } from 'rxjs';
import { map } from 'rxjs/operators';

// Sequential emission
concat(
  of(1, 2, 3),
  of(4, 5, 6),
  of(7, 8, 9)
).subscribe(console.log);
// Output: 1, 2, 3, 4, 5, 6, 7, 8, 9  (in guaranteed order)

// With delays — respects completion before moving on
concat(
  timer(1000).pipe(map(() => 'first')),
  timer(500).pipe(map(() => 'second')),
  of('third')
).subscribe(console.log);
// Output after 1000ms: 'first'
// Output after 1500ms: 'second'  (1000 + 500)
// Output after 1500ms: 'third'   (synchronous after second)
```

### Common Pattern — Sequential HTTP Operations
```typescript
import { concat } from 'rxjs';
import { map, tap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Upload file, then update metadata, then notify — each waits for previous
concat(
  ajax({ url: '/api/upload', method: 'POST', body: fileData }).pipe(
    tap(response => console.log('uploaded:', response.response.url))
  ),
  ajax({ url: '/api/metadata', method: 'POST', body: metaData }),
  ajax({ url: '/api/notify',   method: 'POST', body: { event: 'upload.complete' } })
).subscribe({
  next:     res => console.log('step complete:', res.status),
  error:    err => rollback(err),  // any step fails → chain stops
  complete: () => console.log('all steps done'),
});
```

### Common Pattern — Loading Sequence / Boot Phases
```typescript
import { concat, of, defer } from 'rxjs';
import { tap, ignoreElements } from 'rxjs/operators';

// Bootstrap sequence: config → auth → data → render
const bootSequence$ = concat(
  defer(() => loadConfig()).pipe(
    tap(config => applyConfig(config)),
    ignoreElements()
  ),
  defer(() => authenticate()).pipe(
    tap(user => setCurrentUser(user)),
    ignoreElements()
  ),
  defer(() => fetchInitialData()).pipe(
    tap(data => seedStore(data)),
    ignoreElements()
  ),
  of('ready') // final signal
);

bootSequence$.subscribe({
  next:     status => status === 'ready' && renderApp(),
  error:    err => showBootError(err),
  complete: () => console.log('boot complete'),
});
```

### Common Pattern — Prepend/Append Static Values
```typescript
import { concat, of, EMPTY } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Equivalent to startWith and endWith, but with full Observables
const users$ = concat(
  of({ id: -1, name: 'All Users' }), // prepend synthetic "all" option
  ajax.getJSON<User[]>('/api/users').pipe(
    mergeMap(users => from(users))    // flatten array to individual users
  )
);

// Add a loading sentinel + real data
const withLoadingState$ = concat(
  of({ type: 'loading' } as const),
  ajax.getJSON<Data>('/api/data').pipe(map(data => ({ type: 'data' as const, data })))
);
```

### Common Pattern — `concat` with `defer` for Lazy Evaluation
```typescript
import { concat, defer } from 'rxjs';

// defer() creates a new Observable on each subscription — critical for side effects
// Without defer, the side-effectful function runs at concat() creation time (eager)

// ❌ EAGER — createUser() called immediately when concat() is called, before S1 completes
concat(
  validateForm(formData),
  createUser(formData),  // ← called now, even before validate finishes
  sendWelcomeEmail()
);

// ✅ LAZY — defer wraps creation in a factory; each step created only when subscribed
concat(
  defer(() => validateForm(formData)),
  defer(() => createUser(formData)),   // called only after validateForm completes
  defer(() => sendWelcomeEmail())
);
```

### Edge Cases — One Source Never Completes, Empty Sources
```typescript
import { concat, interval, of, EMPTY } from 'rxjs';
import { take } from 'rxjs/operators';

// Edge case 1: first source never completes → second never starts
concat(
  interval(100),   // never completes without take()
  of('never seen')
).subscribe(console.log);
// 0, 1, 2, 3, ... — 'never seen' never emitted

// Fix: make all sources finite
concat(
  interval(100).pipe(take(3)),
  of('now seen')
).subscribe(console.log);
// 0, 1, 2, now seen

// Edge case 2: empty sources — skipped, next source starts
concat(EMPTY, EMPTY, of(1, 2, 3)).subscribe(console.log);
// Output: 1, 2, 3  (EMPTY completes instantly)

// Edge case 3: single source — passthrough
concat(of(1, 2, 3)).subscribe(console.log);
// Output: 1, 2, 3
```

## Common Pitfalls

### Anti-pattern: Using `concat` with Never-Completing Sources
```typescript
import { concat, interval, timer } from 'rxjs';

// ❌ BROKEN — interval never completes; subsequent sources never start
concat(
  interval(1000),   // emits 0, 1, 2, ... forever
  timer(5000).pipe(map(() => 'timeout'))
).subscribe(console.log);
// timer never fires. interval is in position 1 and never releases.

// ✅ CORRECT — use take() to make the source finite
concat(
  interval(1000).pipe(take(5)),  // completes after 5 values
  timer(5000).pipe(map(() => 'done'))
).subscribe(console.log);
// 0, 1, 2, 3, 4 over 5 seconds, then 'done' 5 seconds later

// ✅ ALSO CORRECT — use merge for concurrent + infinite sources
merge(
  interval(1000),
  fromEvent(document, 'click')
).subscribe(console.log);
// Both streams active simultaneously — neither needs to complete for the other to run

// WHY: concat is sequential — each source must complete before the next starts.
// Infinite sources (interval, fromEvent, BehaviorSubject) never release the chain.
// Use concat only with sources you know will complete; use merge for concurrent infinite streams.
```

### Anti-pattern: Confusing `concat` and `merge` for Parallel Requests
```typescript
import { concat, merge } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// ❌ SLOW — concat: requests fire one at a time (sequential)
// Total time = time(req1) + time(req2) + time(req3)
concat(
  ajax.getJSON('/api/users'),
  ajax.getJSON('/api/products'),
  ajax.getJSON('/api/orders')
).subscribe(console.log);
// Fires /users, waits for response, then fires /products, waits, then /orders

// ✅ FAST — forkJoin: all requests fire simultaneously (parallel)
forkJoin({
  users:    ajax.getJSON('/api/users'),
  products: ajax.getJSON('/api/products'),
  orders:   ajax.getJSON('/api/orders'),
}).subscribe(({ users, products, orders }) => {
  // All three arrived; results correlated
});

// ✅ ALSO VALID — merge: parallel, results arrive as ready
merge(
  ajax.getJSON('/api/users'),
  ajax.getJSON('/api/products'),
  ajax.getJSON('/api/orders')
).subscribe(console.log); // results in arrival order — no correlation

// WHY: Use concat only when request N depends on request N-1's result, or
// when you need guaranteed sequential side effects. For independent parallel
// requests, forkJoin (correlated results) or merge (streaming results) are faster.
```

### Anti-pattern: Forgetting `defer` for Lazy Side Effects
```typescript
import { concat, defer } from 'rxjs';

let step = 0;
function nextStep(): Observable<number> {
  step++;
  return of(step);
}

// ❌ EAGER — all nextStep() calls happen at concat() creation time
concat(
  nextStep(), // step = 1 immediately
  nextStep(), // step = 2 immediately
  nextStep()  // step = 3 immediately
).subscribe(console.log);
// Output: 1, 2, 3  (values captured eagerly — side effects already ran)
// This works here, but breaks if nextStep() depends on previous step's result.

// ✅ CORRECT — defer makes each factory call lazy (runs at subscription time)
concat(
  defer(() => nextStep()), // step++ only when this source is subscribed
  defer(() => nextStep()),
  defer(() => nextStep())
).subscribe(console.log);
// Output: 1, 2, 3  — same here, but sequential and lazy

// ✅ ESSENTIAL with side effects that must use previous result
function createUser(data: UserData): Observable<User> { /* HTTP */ }
function sendEmail(user: User): Observable<void> { /* HTTP */ }

// ❌ BROKEN — sendEmail(user) called before createUser() has a result
// `user` is undefined at concat() creation time
let user: User;
concat(
  createUser(data).pipe(tap(u => user = u)),
  sendEmail(user) // user is undefined here!
);

// ✅ CORRECT — use switchMap (or defer) for dependent sequential async
createUser(data).pipe(
  switchMap(user => sendEmail(user))
).subscribe();

// WHY: concat's sources are evaluated (but not subscribed) when concat() is called.
// Side effects in source factories run eagerly. Use defer() to wrap factories that
// should run lazily, or use switchMap/concatMap for result-dependent chaining.
```

## Related Operators

**Same Category (Combination)**:
- **`merge`**: Concurrent counterpart — subscribes to all sources simultaneously; use when order doesn't matter and sources are independent
- **`forkJoin`**: Parallel + final-values — waits for all to complete, emits last values; use for independent parallel requests
- **`race`**: First-wins — completes with whichever source emits first
- **`zip`**: Pairs by index — combines Nth emission from each source

**Higher-Order Variants**:
- **`concatAll`**: Flattens an `Observable<Observable<T>>` by subscribing to inner Observables sequentially
- **`concatMap`**: `map + concatAll` — maps each source value to an inner Observable and concatenates them in order

**Alternatives by Use Case**:

| Use Case | Instead of `concat` | Use This | Why |
|----------|---------------------|----------|-----|
| Parallel independent requests | `concat` | `forkJoin` / `merge` | No need to wait — run simultaneously |
| Sequential with result chaining | `concat + defer` | `switchMap` / `concatMap` | Better for dependent async chains |
| First emission only | `concat` | `race` | race selects fastest, ignores the rest |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/concat](https://rxjs.dev/api/index/function/concat)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/concat.html](http://reactivex.io/documentation/operators/concat.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/concat.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/concat.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Sequential Completion Queue
- **Cognitive Load**: 2/5 — Conceptually simple; the "never-completing source blocks the chain" and "defer for lazy evaluation" rules are the key subtleties
- **Usage Frequency**: 4/5 — Essential for sequential workflows; less frequent than merge/forkJoin for HTTP but critical for ordered bootstrapping
- **Composability**: 4/5 — Clean API; use defer for laziness; concatWith keeps pipe chains fluent

**Teaching Sequence**:
- **Prerequisites**: `merge`, Observable completion semantics
- **Teaches**: Sequential subscription, completion-gated chaining, the concat vs. merge vs. forkJoin decision matrix
- **Common with**: `defer`, `switchMap`, `of`, `timer`, `ajax`
