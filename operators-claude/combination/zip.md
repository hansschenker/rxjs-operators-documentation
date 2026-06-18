# zip

## Identity

- **Name**: zip
- **Category**: Combination Operators (Join Creation)
- **Type**: Indexed pairing — combines values from multiple Observables by position (index), emitting a tuple when all sources have emitted their Nth value
- **Import**:
  ```typescript
  import { zip } from 'rxjs';
  import { zipWith } from 'rxjs/operators'; // pipeable form
  ```
- **Signature**:
  ```typescript
  // Creation form
  function zip<A extends readonly unknown[]>(
    ...sources: [...ObservableInputTuple<A>]
  ): Observable<A>

  // Pipeable form
  function zipWith<T, A extends readonly unknown[]>(
    ...otherInputs: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, Cons<T, A>>
  ```

## Functional Specification

**Concept**: `zip` waits for all sources to emit their Nth value before emitting `[source1[N], source2[N], source3[N]]`. Values are paired by index, not by time.

**Pairing rules**:
- Emission N only fires when ALL sources have contributed their Nth value
- If one source is slow, faster sources' values queue internally until all have contributed
- Completes when the FIRST source completes (no more pairs possible for that index)
- Source errors propagate immediately

**Contrast with `combineLatest`**:

| | `zip` | `combineLatest` |
|---|---|---|
| Pairing | By index (1st with 1st, 2nd with 2nd) | By latest value at time of emission |
| Fires when | All sources emitted index N | Any source emits (all have emitted ≥1) |
| Subsequent emissions | Waits for each source to produce a new value | Fires on any new value from any source |
| Use case | Pair request/response, pair ordered datasets | Combine live state |

**Mathematical representation**:
```
zip([a1,a2,...], [b1,b2,...], [c1,c2,...])
  → [(a1,b1,c1), (a2,b2,c2), ...]
```

## Marble Diagram

```
Source A:  --1-----2-----3----|
Source B:  ---10--20--30----|
Source C:  ------100----200--300--|

zip(A, B, C):
Result:    ------[1,10,100]------[2,20,200]--|

           Explanation:
           index 0: A emits 1, B emits 10, C emits 100 → [1,10,100]
           index 1: A emits 2, B emits 20, C emits 200 → [2,20,200]
           index 2: A would emit 3, but B completes → zip completes

Source A:  --1--2--3--|            (faster source)
Source B:  -----10-----20-----30--|  (slower source)

zip(A, B):
Result:    -----[1,10]-----[2,20]-----[3,30]--|

           A's 1,2,3 are buffered until B catches up.
           Completes when A completes after last pairing.
```

## Behavioral Characteristics

**Buffering**: Faster sources buffer their values internally waiting for slower sources. For unbounded sources with very different speeds, this can grow the buffer indefinitely — a potential memory concern.

**Completion**: Completes when ANY source completes and that index can no longer be satisfied.

**Error handling**: Any source error is immediately forwarded; other sources are unsubscribed.

**Hot/Cold**: Works with both. Cold sources are individually subscribed to.

## Type System Integration

```typescript
import { zip, of, interval } from 'rxjs';
import { take } from 'rxjs/operators';

// Tuple types inferred for up to 9 sources
const result$ = zip(
  of('a', 'b', 'c'),          // Observable<string>
  of(1, 2, 3),                 // Observable<number>
  of(true, false, true)        // Observable<boolean>
);
// result$: Observable<[string, number, boolean]>

result$.subscribe(([letter, num, flag]) => {
  console.log(letter, num, flag);
  // 'a' 1 true
  // 'b' 2 false
  // 'c' 3 true
});

// zipWith pipeable form
of('a', 'b', 'c').pipe(
  zipWith(of(1, 2, 3))
).subscribe(([letter, num]: [string, number]) => {
  console.log(letter, num);
});
```

## Examples

### Basic Usage — Pair Two Ordered Datasets
```typescript
import { zip, of } from 'rxjs';

const names$ = of('Alice', 'Bob', 'Carol');
const scores$ = of(95, 87, 92);

zip(names$, scores$).subscribe(([name, score]) => {
  console.log(`${name}: ${score}`);
});
// Output:
// Alice: 95
// Bob: 87
// Carol: 92
```

### Common Pattern — Pair HTTP Requests by Index
```typescript
import { zip, from } from 'rxjs';
import { map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const userIds = [1, 2, 3];

// Pair each user ID with their fetched profile — index-safe
zip(
  from(userIds),
  from(userIds.map(id => ajax.getJSON(`/api/users/${id}`)))
).pipe(
  map(([id, profile]) => ({ id, ...profile }))
).subscribe(console.log);
```

### Common Pattern — Synchronized Animations
```typescript
import { zip, interval } from 'rxjs';
import { take, map } from 'rxjs/operators';

// Run two animations in lockstep — one can't advance without the other
const frameA$ = interval(16).pipe(take(60), map(i => `frameA-${i}`));
const frameB$ = interval(32).pipe(take(60), map(i => `frameB-${i}`));

zip(frameA$, frameB$).subscribe(([a, b]) => {
  renderFrame(a, b); // frameB$ is slower — frameA$ values queue until frameB$ catches up
});
// Emits 60 pairs; frameA advances at 16ms, frameB at 32ms — zip syncs them
```

### Edge Case — Different Length Sources
```typescript
import { zip, of } from 'rxjs';

const long$  = of(1, 2, 3, 4, 5);
const short$ = of('a', 'b', 'c');

zip(long$, short$).subscribe({
  next: ([n, s]) => console.log(n, s),
  complete: () => console.log('done')
});
// Output:
// 1 a
// 2 b
// 3 c
// done   ← completes when short$ runs out; 4 and 5 are never emitted
```

## Common Pitfalls

### Anti-pattern: `zip` with Infinite Sources of Different Rates
```typescript
import { zip, interval } from 'rxjs';

// ❌ MEMORY LEAK — fast$ runs 10x faster than slow$
// fast$ values accumulate in zip's buffer indefinitely
const fast$ = interval(10);
const slow$ = interval(100);

zip(fast$, slow$).subscribe(console.log);
// After 10 seconds: fast$ has emitted ~1000 values, slow$ ~100.
// zip is buffering ~900 fast$ values waiting for slow$ to catch up.

// ✅ CORRECT — use combineLatest when "latest pair" is what you need
import { combineLatest } from 'rxjs';
combineLatest([fast$, slow$]).subscribe(console.log); // always latest pair, no buffer

// WHY: zip pairs by index. If sources emit at different rates, the faster
// source's unconsumed values accumulate in memory. Use zip only when the
// sources emit at similar rates, or when positional pairing is specifically needed.
```

### Anti-pattern: Using `zip` When `combineLatest` Is Intended
```typescript
import { zip, combineLatest, BehaviorSubject } from 'rxjs';

const user$ = new BehaviorSubject<User>(currentUser);
const settings$ = new BehaviorSubject<Settings>(defaultSettings);

// ❌ WRONG — zip pairs by index; it waits for BOTH to emit a NEW value
// On initialization: user$ emits 1 value, settings$ emits 1 value → one pair emitted.
// On user update: zip waits for settings$ to ALSO emit a new value before firing.
// Setting changes also require a new user emission. They get out of sync.
zip(user$, settings$).subscribe(([user, settings]) => render(user, settings));

// ✅ CORRECT — combineLatest for combining live state
combineLatest([user$, settings$]).subscribe(([user, settings]) => render(user, settings));
// Fires whenever EITHER emits; always uses the latest from each.

// WHY: zip is for indexed pairing of ordered datasets. combineLatest is for
// combining reactive state. Use zip for "pair item N of A with item N of B";
// use combineLatest for "whenever anything changes, combine the latest values."
```

## Related Operators

- **`combineLatest`**: Combines by latest value at time of emission, not by index — better for live state
- **`forkJoin`**: Combines by last value on completion — `zip` of the last values; use for parallel one-shots
- **`withLatestFrom`**: Combines on trigger, using the latest value — asymmetric (one source drives timing)
- **`merge`**: Combines streams with no pairing — just forwards all values as they arrive
- **`concat`**: Sequential not parallel — no pairing

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/zip](https://rxjs.dev/api/index/function/zip)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 3/5 | **Composability**: 3/5
**Key teaching point**: zip pairs by INDEX, not by time. The combineLatest confusion (index vs latest) and buffer accumulation for different-rate sources are the two critical distinctions.
