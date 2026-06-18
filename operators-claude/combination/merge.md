# merge

## Identity
- **Name**: merge
- **Category**: Combination Operators (Join Creation)
- **Type**: Concurrent stream combinator — subscribes to all sources simultaneously and forwards each emission as it arrives
- **Import**:
  ```typescript
  import { merge } from 'rxjs';            // creation function
  import { mergeWith } from 'rxjs/operators'; // pipeable form (RxJS 7+)
  ```
- **Signature**:
  ```typescript
  // Creation function (standalone)
  function merge<T>(
    ...observables: Array<ObservableInput<T>>
  ): Observable<T>

  // With concurrent limit
  function merge<T>(
    ...observablesAndConcurrent: [...Array<ObservableInput<T>>, number]
  ): Observable<T>

  // Pipeable form (RxJS 7+)
  function mergeWith<T, A extends readonly unknown[]>(
    ...otherSources: [...{ [K in keyof A]: ObservableInput<A[K]> }]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

**Input**: Two or more `ObservableInput<T>` sources

**Output**: `Observable<T>` — emits each value from any source the moment it arrives, in arrival order

**Transformation**: Subscribes to all sources simultaneously. Every emission from every source is forwarded to the output stream immediately, in the order it arrives (interleaved by time). The output completes only when ALL sources have completed. If any source errors, the error propagates immediately and all other sources are unsubscribed.

**Mathematical representation**:
```
merge(S₁, S₂, ..., Sₙ) =
  { (value, time) : ∃ i, (value, time) ∈ Sᵢ }  sorted by time

Output completes when: ∀ i, Sᵢ has completed
Output errors when:   ∃ i, Sᵢ errors
```

**Invariants**:
- **Concurrent subscriptions**: All sources subscribed simultaneously, not sequentially
- **Arrival-order forwarding**: Values are emitted in the order they arrive, regardless of which source produced them
- **Completion gate**: Output completes when the last source completes (not the first)
- **Error short-circuit**: First error from any source terminates the whole merged stream

## Marble Diagram

```
S1:     --a-----c-----e--|
S2:     -----b-----d-----|
        merge(S1, S2)
Result: --a--b--c--d--e--|

Values interleaved by arrival time.
Output completes when both S1 and S2 complete.
```

**Pipeable `mergeWith`**:
```
Source: --1-----3-----5--|
Other:  -----2-----4-----|
        pipe(mergeWith(Other))
Result: --1--2--3--4--5--|
```

**Error propagation**:
```
S1:     --a--b--c--d--|
S2:     ----------#
        merge(S1, S2)
Result: --a--b--c--#

Error from S2 terminates S1's subscription immediately.
d is never emitted.
```

**Key observation**: `merge` is the concurrent counterpart to `concat`. Where `concat` subscribes to sources one at a time in sequence, `merge` subscribes to all sources simultaneously. Use `merge` when sources are independent and you want them all active at once.

## Behavioral Characteristics

**Subscription**:
- All sources subscribed simultaneously when `merge`'s output is subscribed
- Each source runs at full speed — no throttling between sources

**Completion semantics**:
- Output completes when the last source completes
- Sources that complete early do not trigger output completion
- An infinite source (like `interval`) prevents the merge from ever completing

**Error handling**:
- First error from any source propagates immediately
- All other source subscriptions are unsubscribed on error

**Concurrent limit** (optional `concurrent` parameter):
- `merge(s1, s2, s3, 2)` — subscribes to at most 2 sources at a time; queues s3 until one of the first two completes
- Useful for controlling parallelism when merging many sources

**Backpressure**:
- None — values forwarded synchronously as they arrive; no buffering
- Memory: O(number of concurrent sources)

**Hot vs. Cold**:
- Transparent; works with any combination of hot and cold sources
- With two cold sources: both start fresh and run concurrently
- With hot sources: joins live emissions from wherever each hot source currently is

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The union of all source value types
 *
 * merge() with homogeneous sources: Observable<T>
 * merge() with heterogeneous sources: Observable<T1 | T2 | ...>
 *
 * mergeWith() output: Observable<T | A[number]>
 *   where T = source type, A = tuple of other-source types
 */

import { merge, fromEvent, interval } from 'rxjs';
import { mergeWith, map } from 'rxjs/operators';

// Homogeneous — Observable<number>
const numbers$: Observable<number> = merge(
  interval(500),
  interval(1000)
);

// Heterogeneous — Observable<MouseEvent | KeyboardEvent>
const userInteraction$ = merge(
  fromEvent<MouseEvent>(document, 'click'),
  fromEvent<KeyboardEvent>(document, 'keydown')
);

// Pipeable form — type preserved as union
const events$ = fromEvent(document, 'click').pipe(
  mergeWith(
    fromEvent(document, 'keydown'),
    fromEvent(document, 'touchstart')
  )
); // Observable<Event | Event | Event> → Observable<Event>
```

## Examples

### Basic Usage — Merging Independent Event Streams
```typescript
import { merge, fromEvent } from 'rxjs';
import { map } from 'rxjs/operators';

// Treat multiple user input events as a single stream
const userActivity$ = merge(
  fromEvent(document, 'click').pipe(map(() => 'click')),
  fromEvent(document, 'keydown').pipe(map(() => 'keydown')),
  fromEvent(document, 'touchstart').pipe(map(() => 'touch')),
  fromEvent(document, 'mousemove').pipe(map(() => 'mousemove'))
);

userActivity$.subscribe(eventType => console.log('user active:', eventType));
// Fires on any interaction, regardless of type
```

### Common Pattern — Merging Multiple Data Sources
```typescript
import { merge } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { webSocket } from 'rxjs/webSocket';
import { ajax } from 'rxjs/ajax';

interface Update { type: string; payload: unknown; }

// Pull initial data + push live updates into one stream
const liveData$ = merge(
  ajax.getJSON<Update[]>('/api/initial-state').pipe(
    map(items => items as Update[]),
    catchError(() => EMPTY)
  ),
  webSocket<Update>('wss://api.example.com/updates').pipe(
    map(msg => [msg]),
    catchError(() => EMPTY)
  )
);

liveData$.subscribe(updates => processUpdates(updates));
// REST data arrives first, then WebSocket updates flow in live
```

### Common Pattern — Action Stream Composition (Redux-style)
```typescript
import { merge, Subject } from 'rxjs';
import { scan, startWith, shareReplay } from 'rxjs/operators';

// Multiple action sources merged into one stream
const userActions$   = new Subject<Action>();
const systemActions$ = new Subject<Action>();
const networkActions$= new Subject<Action>();

// Single unified action stream
const actions$ = merge(userActions$, systemActions$, networkActions$);

const state$ = actions$.pipe(
  scan(reducer, initialState),
  startWith(initialState),
  shareReplay(1)
);

// Dispatch from any source — all flow through the same reducer
userActions$.next({ type: 'USER_CLICKED_BUY' });
networkActions$.next({ type: 'PRICE_UPDATED', price: 9.99 });
```

### Common Pattern — Timeout Race with `merge` and `takeUntil`
```typescript
import { merge, timer, EMPTY } from 'rxjs';
import { switchMap, takeUntil, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Merge request result with a timeout; whichever comes first wins
// (This pattern is usually better expressed with race() or timeout(),
//  but shows merge's flexibility)

function withTimeout<T>(source$: Observable<T>, ms: number): Observable<T> {
  return merge(
    source$,
    timer(ms).pipe(switchMap(() => { throw new Error(`Timeout after ${ms}ms`); }))
  ).pipe(
    // take(1) would stop the merge on first emission
  );
}
```

### Common Pattern — Isolated Error Handling per Source
```typescript
import { merge, EMPTY, interval } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

// Multiple data feeds — one failing should not kill the others
function resilientFeed(url: string): Observable<DataItem> {
  return ajax.getJSON<DataItem[]>(url).pipe(
    catchError(err => {
      console.warn(`Feed ${url} failed:`, err);
      return EMPTY; // this feed drops out silently
    }),
    mergeMap(items => from(items)) // flatten array to individual items
  );
}

const allData$ = merge(
  resilientFeed('/api/feed/a'),
  resilientFeed('/api/feed/b'),
  resilientFeed('/api/feed/c')
);

allData$.subscribe(item => process(item));
// If feed/b fails: feed/a and feed/c continue; feed/b silently drops out
```

### Edge Cases — Empty Sources, Single Source, Concurrent Limit
```typescript
import { merge, EMPTY, of, interval } from 'rxjs';
import { take } from 'rxjs/operators';

// Edge case 1: empty sources — completes immediately
merge(EMPTY, EMPTY).subscribe({ complete: () => console.log('done') });
// Output: done

// Edge case 2: single source — passthrough
merge(of(1, 2, 3)).subscribe(console.log);
// Output: 1, 2, 3

// Edge case 3: concurrent limit
// merge(s1, s2, s3, s4, s5, 2) — subscribes to s1, s2 first
// When s1 completes, subscribes to s3. When s2 completes, subscribes to s4. Etc.
merge(
  interval(100).pipe(take(3), map(n => `A${n}`)),
  interval(200).pipe(take(3), map(n => `B${n}`)),
  interval(150).pipe(take(3), map(n => `C${n}`)),
  2  // at most 2 concurrent subscriptions
).subscribe(console.log);
// C stream waits until either A or B completes before being subscribed

// Edge case 4: one infinite source — output never completes
merge(
  of(1, 2, 3),
  interval(1000) // never completes
).subscribe(console.log);
// 1, 2, 3, 0, 1, 2, ... (interval runs forever)
```

## Common Pitfalls

### Anti-pattern: Using `merge` When Sequential Order Is Required
```typescript
import { merge, concat, of } from 'rxjs';

// ❌ WRONG — merge does not guarantee ordering
// With synchronous sources, emissions interleave non-deterministically
merge(
  of(1, 2, 3),
  of(4, 5, 6)
).subscribe(console.log);
// Output: 1, 4, 2, 5, 3, 6  (synchronous interleaving — alternating)
// Actually with synchronous of(), this IS deterministic (source 1 fully completes first
// since of() is synchronous), but with async sources ordering is by arrival time.

// ❌ GENUINELY WRONG for sequential workflows
merge(
  uploadFile(file),          // step 1
  notifyServer(file.name)    // step 2 — must run AFTER upload completes!
).subscribe(console.log);
// Both start simultaneously — notification fires before upload finishes

// ✅ CORRECT — use concat for sequential ordering
concat(
  uploadFile(file),
  notifyServer(file.name)
).subscribe(console.log);
// uploadFile completes first, then notifyServer starts

// WHY: merge subscribes to all sources simultaneously. Use concat when you need
// step 2 to start only after step 1 completes; merge when steps are independent.
```

### Anti-pattern: Forgetting that One Infinite Source Prevents Completion
```typescript
import { merge, of, interval } from 'rxjs';
import { take, reduce } from 'rxjs/operators';

// ❌ BROKEN — reduce never emits because interval never completes
merge(
  of(1, 2, 3),
  interval(100).pipe(take(5)) // completes after 5, but it's async
).pipe(
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log);
// The reduce waits for the merged stream to complete.
// take(5) on interval means it WILL complete, but later.
// If interval had no take(): reduce would never fire.

// ❌ INFINITE SOURCE — reduce never fires
merge(
  of(1, 2, 3),
  interval(100) // no take() — never completes
).pipe(
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log);
// No output ever. reduce waits for source completion.

// ✅ CORRECT — use scan instead of reduce when source may not complete
merge(
  of(1, 2, 3),
  interval(100)
).pipe(
  scan((acc, v) => acc + v, 0)
).subscribe(console.log); // emits running total as it grows

// WHY: merge completes only when ALL sources complete. Operators like reduce,
// toArray, or last that wait for completion will hang if any source is infinite.
// Replace terminal aggregators with streaming alternatives (scan, shareReplay)
// when merging infinite streams.
```

### Anti-pattern: Using `merge` Instead of `mergeWith` in Pipe Chains
```typescript
import { merge, interval } from 'rxjs';
import { mergeWith, map } from 'rxjs/operators';

const source$ = interval(1000).pipe(map(n => `A${n}`));
const other$  = interval(1500).pipe(map(n => `B${n}`));

// ❌ VERBOSE — merge outside pipe breaks the fluent chain
merge(
  source$.pipe(/* transformations */),
  other$
).subscribe(console.log);

// ✅ CLEANER — mergeWith inside pipe maintains the fluent operator chain
source$.pipe(
  map(v => v.toUpperCase()),
  mergeWith(other$)
).subscribe(console.log);

// WHY: mergeWith (added in RxJS 7) is the pipeable form of merge.
// It's equivalent to merge(source$, other$) but keeps all transformations
// in a single pipe() chain, improving readability and tree-shakability.
// For new code, prefer mergeWith inside pipe() over merge() outside.
```

## Related Operators

**Same Category (Combination)**:
- **`concat`**: Sequential combination — subscribes to sources one at a time after each completes; use when order matters
- **`forkJoin`**: Waits for ALL sources to complete, emits only their last values — use for one-shot parallel requests
- **`race`**: Takes only the first source to emit, unsubscribes the rest — use for timeout patterns
- **`combineLatest`**: Emits on every source emission after all have emitted once — use for reactive derived state
- **`zip`**: Pairs emissions by index — emits after each source has emitted N times

**Higher-Order Variants**:
- **`mergeAll`**: Flattens an `Observable<Observable<T>>` by subscribing to inner Observables concurrently — `mergeMap` is `map + mergeAll`
- **`mergeMap`**: Maps each source value to an inner Observable and merges the results — the higher-order version of merge

**Alternatives by Use Case**:

| Use Case | Instead of `merge` | Use This | Why |
|----------|--------------------|----------|-----|
| Sequential streams | `merge` | `concat` | concat waits for each to complete |
| Parallel + only final values | `merge + last()` | `forkJoin` | forkJoin is purpose-built for this |
| Live reactive combination | `merge` | `combineLatest` | combineLatest correlates latest values |
| Pipeable in chain | `merge(source$, other$)` outside pipe | `source$.pipe(mergeWith(other$))` | Keeps chain fluent |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/merge](https://rxjs.dev/api/index/function/merge)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/merge.html](http://reactivex.io/documentation/operators/merge.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/merge.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/merge.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Concurrent Stream Union (Simultaneous Subscription Fan-In)
- **Cognitive Load**: 2/5 — Conceptually simple; the main subtlety is sequential vs. concurrent and the completion gate
- **Usage Frequency**: 4/5 — Ubiquitous for event stream merging, action streams, and multi-source data aggregation
- **Composability**: 5/5 — Works with any ObservableInput; composes cleanly via mergeWith in pipes

**Teaching Sequence**:
- **Prerequisites**: `concat` (sequential ordering), `Observable` subscription lifecycle
- **Teaches**: Concurrent subscriptions, arrival-order forwarding, the merge vs. concat distinction
- **Leads to**: `mergeMap` (higher-order merge), `combineLatest` (value correlation), `race` (first-wins)
- **Common with**: `catchError`, `EMPTY`, `fromEvent`, `Subject`, `mergeWith`
