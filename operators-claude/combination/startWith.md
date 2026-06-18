# startWith

## Identity
- **Name**: startWith
- **Category**: Combination Operators
- **Type**: Initial value prepender — emits one or more static values synchronously before forwarding source emissions
- **Import**:
  ```typescript
  import { startWith } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function startWith<T, A extends readonly unknown[]>(
    ...values: A
  ): OperatorFunction<T, T | ValueFromArray<A>>
  ```

## Functional Specification

**Input**: `Observable<T>` — the source Observable

**Output**: `Observable<T | A[number]>` — an Observable that first emits each provided value in order, then forwards all source emissions

**Transformation**: Equivalent to `concat(of(...values), source$)`. The prepended values are emitted synchronously at subscription time, before the source is subscribed to. If the source is cold, it begins only after all `startWith` values have been emitted.

**Mathematical representation**:
```
startWith(v₁, v₂, ..., vₙ)(source)
  = concat(of(v₁, v₂, ..., vₙ), source)

Output sequence: v₁, v₂, ..., vₙ, s₁, s₂, ..., sₘ
  where s₁..sₘ are source emissions
```

**Invariants**:
- **Synchronous initial emission**: All `startWith` values emit synchronously during subscription, before any async source emissions
- **Multiple values allowed**: `startWith(a, b, c)` emits a, b, c in order
- **Source unchanged**: Source emissions are forwarded verbatim after the initial values
- **Completion from source**: The output completes when the source completes (not after the initial values)
- **Error from source**: Source errors propagate normally; `startWith` values have already been emitted before any async error can occur

## Marble Diagram

```
Source:   -----1-----2-----3--|
          startWith(0)
Result:   0----1-----2-----3--|

'0' emitted synchronously at subscription time (t=0).
Source then begins emitting.
```

**Multiple initial values**:
```
Source:   ----a----|
          startWith('x', 'y', 'z')
Result:   xyz-a----|

x, y, z emitted synchronously, then source begins.
```

**With scan — seeding the state stream**:
```
Actions:  ----inc----inc-----|
          scan(reducer, state0)
          → (emits only after first action)

Actions:  ----inc----inc-----|
          scan(reducer, state0), startWith(state0)
          → state0----state1----state2-----|
            ↑
            Emitted immediately on subscribe — no waiting for first action
```

**Key observation**: `startWith(initialValue)` after `scan(reducer, seed)` creates a stream that behaves like `BehaviorSubject` — subscribers always receive a current value immediately on subscription.

## Behavioral Characteristics

**Subscription**:
- All `startWith` values are emitted synchronously in the subscriber's call stack before `subscribe()` returns
- Source subscription begins immediately after the last `startWith` value is emitted

**Completion semantics**:
- Output completes when source completes
- If source never completes, output never completes (despite having emitted initial values)

**Error handling**:
- `startWith` values are synchronous — they cannot error
- If source errors, the error propagates normally after initial values have been emitted

**Backpressure**:
- None — initial values are synchronous; no buffering required

**Hot vs. Cold**:
- With cold source: initial values emit, then source starts fresh
- With hot source: initial values emit synchronously, then live emissions from wherever the hot source currently is; no buffering of missed hot values

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T  - Source Observable value type
 *   A  - Tuple type of startWith arguments (readonly unknown[])
 *
 * Output Type: Observable<T | ValueFromArray<A>>
 *   If A = [number] and T = number → Observable<number>  (same type — no widening)
 *   If A = [null]   and T = User   → Observable<User | null>  (union)
 *
 * Single-value startWith of the same type does not widen:
 *   source$: Observable<number>
 *   source$.pipe(startWith(0)) → Observable<number>  (not Observable<number | number>)
 */

import { Subject } from 'rxjs';
import { scan, startWith, map } from 'rxjs/operators';

// Same type — no union widening
const counter$ = new Subject<number>();
const count$: Observable<number> = counter$.pipe(
  scan((acc, _) => acc + 1, 0),
  startWith(0) // type: Observable<number>
);

// Different type — union
interface UserProfile { id: number; name: string; }
const profile$ = fetchProfile().pipe(
  startWith(null) // Observable<UserProfile | null>
);

profile$.subscribe((p: UserProfile | null) => {
  if (p === null) showLoadingSpinner();
  else            showProfile(p);
});

// Multiple values with mixed types
const stream$ = source$.pipe(
  startWith('loading', 'initializing') // startWith values: string; T = Data
  // output: Observable<Data | string>
);
```

## Examples

### Basic Usage — Seeding a Counter or State
```typescript
import { Subject } from 'rxjs';
import { scan, startWith } from 'rxjs/operators';

const increment$ = new Subject<void>();

const count$ = increment$.pipe(
  scan(n => n + 1, 0),
  startWith(0)  // emit 0 before any increment arrives
);

count$.subscribe(n => console.log('count:', n));
// Output immediately: count: 0  (before any button clicks)

increment$.next(); // → count: 1
increment$.next(); // → count: 2
```

### Common Pattern — Loading State with Null Sentinel
```typescript
import { startWith, catchError } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';
import { of } from 'rxjs';

interface User { id: number; name: string; }

// null = loading; User = loaded; 'error' = failed
const user$ = ajax.getJSON<User>('/api/me').pipe(
  catchError(() => of('error' as const)),
  startWith(null)  // emit null immediately while HTTP is in flight
);

user$.subscribe(state => {
  if (state === null)      showSpinner();
  else if (state === 'error') showErrorBanner();
  else                     renderProfile(state);
});
// On subscribe: showSpinner() fires immediately.
// When HTTP resolves: renderProfile() fires.
// On HTTP error: showErrorBanner() fires.
```

### Common Pattern — combineLatest Compatibility
```typescript
import { combineLatest, Subject } from 'rxjs';
import { scan, startWith, map, distinctUntilChanged } from 'rxjs/operators';

// combineLatest requires every source to have emitted at least once.
// Without startWith, the combined stream never starts until both act.

const priceActions$ = new Subject<number>();
const qtyActions$   = new Subject<number>();

const orderTotal$ = combineLatest([
  priceActions$.pipe(scan((_, p) => p),   startWith(0)),
  qtyActions$.pipe(  scan((_, q) => q),   startWith(1)),
]).pipe(
  map(([price, qty]) => price * qty),
  distinctUntilChanged()
);

orderTotal$.subscribe(t => console.log('total:', t));
// Output immediately: total: 0  (0 * 1 = 0 from startWith values)

priceActions$.next(9.99);  // → total: 9.99
qtyActions$.next(3);       // → total: 29.97
```

### Common Pattern — Reactive Store with Initial State
```typescript
import { Subject } from 'rxjs';
import { scan, startWith, shareReplay, map, distinctUntilChanged } from 'rxjs/operators';

interface AppState { theme: 'light' | 'dark'; language: string; }
type SettingsAction =
  | { type: 'SET_THEME';    theme: 'light' | 'dark' }
  | { type: 'SET_LANGUAGE'; language: string };

const initial: AppState = { theme: 'light', language: 'en' };

function settingsReducer(state: AppState, action: SettingsAction): AppState {
  switch (action.type) {
    case 'SET_THEME':    return { ...state, theme: action.theme };
    case 'SET_LANGUAGE': return { ...state, language: action.language };
  }
}

const actions$ = new Subject<SettingsAction>();

// The canonical reactive store pattern:
const settings$ = actions$.pipe(
  scan(settingsReducer, initial),
  startWith(initial),    // emit current state immediately on subscribe
  shareReplay(1)         // cache for late subscribers
);

// Selectors
const theme$    = settings$.pipe(map(s => s.theme),    distinctUntilChanged());
const language$ = settings$.pipe(map(s => s.language), distinctUntilChanged());

theme$.subscribe(t => applyTheme(t));   // fires immediately: 'light'
language$.subscribe(l => setLocale(l)); // fires immediately: 'en'

actions$.next({ type: 'SET_THEME', theme: 'dark' }); // → applyTheme('dark')
```

### Edge Cases — Synchronous Ordering, Empty Source, Error After Values
```typescript
import { EMPTY, throwError, of } from 'rxjs';
import { startWith } from 'rxjs/operators';

// Edge case 1: synchronous ordering guarantee
let order: string[] = [];
of('source').pipe(
  startWith('first', 'second')
).subscribe(v => order.push(v));
console.log(order); // ['first', 'second', 'source']

// Edge case 2: empty source — startWith values still emitted
EMPTY.pipe(
  startWith('x', 'y')
).subscribe({ next: v => console.log(v), complete: () => console.log('done') });
// Output: x, y, done

// Edge case 3: source errors — startWith values emitted before error
throwError(() => new Error('fail')).pipe(
  startWith('initial')
).subscribe({ next: v => console.log(v), error: e => console.log('error:', e.message) });
// Output: initial, error: fail

// Edge case 4: startWith with no values — no-op
of(1, 2, 3).pipe(startWith()).subscribe(console.log);
// Output: 1, 2, 3  (identical to source)
```

## Common Pitfalls

### Anti-pattern: Using `startWith` Instead of `BehaviorSubject` for Push-Based State
```typescript
import { Subject, BehaviorSubject } from 'rxjs';
import { startWith, scan } from 'rxjs/operators';

// ❌ WRONG TOOL — trying to push new values via startWith
const state$ = new Subject<string>().pipe(
  startWith('initial')
);
// You cannot push new "initial" values — startWith is a one-time prepend at subscribe time.
// Calling state$.next(...) doesn't exist because state$ is an Observable, not a Subject.

// ✅ CORRECT — BehaviorSubject for push-based current-value stores
const state$ = new BehaviorSubject<string>('initial');
state$.next('updated'); // push new value to all subscribers

// ✅ ALSO CORRECT — scan + startWith for event-derived state (immutable, no direct push)
const actions$ = new Subject<string>();
const derived$ = actions$.pipe(
  scan((_, v) => v, 'initial'),
  startWith('initial')
);
// Push via actions$.next(), not derived$ directly

// WHY: startWith is a static prepend — it emits its values once at subscribe time.
// BehaviorSubject allows imperative pushes to a live value. Use startWith for
// seeding derived streams; use BehaviorSubject when external code needs to
// imperatively set the current value.
```

### Anti-pattern: Putting `startWith` Before `scan`
```typescript
import { Subject } from 'rxjs';
import { scan, startWith } from 'rxjs/operators';

const actions$ = new Subject<number>();

// ❌ INCORRECT — startWith before scan; the initial value is fed INTO the accumulator
actions$.pipe(
  startWith(0),         // emits 0 as if it were an action
  scan((acc, v) => acc + v, 0)
  // scan receives 0 as first source value: acc=0+0=0, then actions as usual
  // This is NOT a bug here (coincidentally correct for sum), but the intent is wrong
).subscribe(console.log);

// ❌ BREAKS with non-additive reducers — the initial value corrupts reducer logic
actions$.pipe(
  startWith({ type: 'INIT' }), // a fake action — may not match any reducer case
  scan(reducer, initialState)
).subscribe(console.log);
// Reducer receives { type: 'INIT' } — may fall through switch, return wrong state

// ✅ CORRECT — startWith AFTER scan, to emit the seed value as the first output
actions$.pipe(
  scan(reducer, initialState),
  startWith(initialState)  // emits initialState to subscribers immediately
).subscribe(console.log);

// WHY: startWith before scan injects values into the scan accumulator as if
// they were source actions. startWith after scan seeds the *output stream*
// with the initial state — subscribers see the current state immediately,
// without any synthetic action being processed by the reducer.
```

### Anti-pattern: `startWith` on a Hot Source Without Understanding Timing
```typescript
import { fromEvent } from 'rxjs';
import { startWith, map } from 'rxjs/operators';

const resize$ = fromEvent(window, 'resize').pipe(
  map(() => { width: window.innerWidth, height: window.innerHeight }),
  startWith({ width: window.innerWidth, height: window.innerHeight })
);

// This is actually CORRECT and a good pattern:
// The initial window dimensions are emitted synchronously on subscribe,
// then live resize events follow.

// ❌ INCORRECT expectation — thinking startWith buffers missed hot events
const lateSubscriber$ = resize$.pipe(/* some delay */);
// startWith emits the initial value at subscribe time, NOT missed hot events.
// Events fired between creation and subscription are still lost.
// Use shareReplay(1) for late-subscriber replay of hot sources.

// WHY: startWith provides a static initial value. It does not buffer or replay
// historical hot emissions. For late-subscriber caching of the most recent hot
// value, use shareReplay({ bufferSize: 1, refCount: true }).
```

## Related Operators

**Same Category (Combination)**:
- **`endWith`**: Appends static values after the source completes — the symmetric counterpart of `startWith`
- **`concat`**: Concatenates full Observables in sequence — `startWith(v)` is shorthand for `concat(of(v), source$)`
- **`defaultIfEmpty`**: Emits a fallback value only if the source completes without emitting — different semantic than providing an initial value

**Complementary Operators**:
- **`scan`**: The primary partner — `scan + startWith(initialState)` is the canonical reactive state pattern
- **`shareReplay(1)`**: Caches the latest emitted value; with `startWith`, ensures all subscribers (including late ones) see the current state
- **`combineLatest`**: Requires all inputs to have emitted; `startWith` satisfies this requirement without waiting for the first action

**Alternatives by Use Case**:

| Use Case | Instead of `startWith` | Use This | Why |
|----------|------------------------|----------|-----|
| Push-based current value | `startWith` on a Subject | `BehaviorSubject` | BehaviorSubject allows `.next()` for updates |
| Replay missed hot emissions | `startWith(lastKnown)` | `shareReplay(1)` | Automatic live replay, not static |
| Fallback for empty source | `startWith(fallback)` | `defaultIfEmpty(fallback)` | `defaultIfEmpty` only fires if source is empty |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/startWith](https://rxjs.dev/api/operators/startWith)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/startwith.html](http://reactivex.io/documentation/operators/startwith.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/startWith.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/startWith.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Synchronous Stream Seeding
- **Cognitive Load**: 2/5 — Conceptually simple; the main subtlety is placement relative to `scan` and the distinction from `BehaviorSubject`
- **Usage Frequency**: 5/5 — Near-universal in reactive state patterns; required for `combineLatest` compatibility
- **Composability**: 5/5 — Slots naturally after `scan` and before `shareReplay`; completes the reactive store triple

**Teaching Sequence**:
- **Prerequisites**: `scan`, `combineLatest`, `Subject`
- **Teaches**: Synchronous emission, stream seeding, the scan+startWith+shareReplay pattern
- **Common with**: `scan`, `shareReplay`, `combineLatest`, `BehaviorSubject`, `distinctUntilChanged`
