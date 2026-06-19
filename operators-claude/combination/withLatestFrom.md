# withLatestFrom

## Identity
- **Name**: withLatestFrom
- **Category**: Combination Operators
- **Type**: Trigger-driven snapshot — when source emits, samples the latest value from secondary Observables
- **Import**:
  ```typescript
  import { withLatestFrom } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // Single secondary source
  function withLatestFrom<T, A>(
    input: ObservableInput<A>
  ): OperatorFunction<T, [T, A]>

  // Multiple secondary sources
  function withLatestFrom<T, A extends readonly unknown[]>(
    ...inputs: [...{ [K in keyof A]: ObservableInput<A[K]> }]
  ): OperatorFunction<T, [T, ...A]>

  // With result selector
  function withLatestFrom<T, A extends readonly unknown[], R>(
    ...inputs: [...{ [K in keyof A]: ObservableInput<A[K]> }, (...values: [T, ...A]) => R]
  ): OperatorFunction<T, R>
  ```

## Functional Specification

**Input**: `Observable<T>` (trigger / primary source) + one or more secondary `ObservableInput<A>` sources

**Output**: `Observable<[T, A]>` or `Observable<R>` — emits a tuple of `[triggerValue, ...latestSecondaryValues]` each time the trigger source emits, provided all secondary sources have already emitted at least once

**Transformation**:
The operator subscribes to all secondary sources immediately to track their latest values. When the trigger source emits value `v`:
1. Each secondary source's latest value is read
2. If all have emitted at least once: a tuple `[v, latest₁, latest₂, ...]` is forwarded
3. If any has not yet emitted: the trigger emission is **silently dropped**

The secondary sources only provide their latest value — they do not drive emissions. Only the primary trigger source drives output.

**Mathematical representation**:
```
Let T = trigger Observable, S₁, S₂, ..., Sₙ = secondary Observables
Let latestᵢ = most recently emitted value from Sᵢ (undefined until first emission)

On trigger emission v:
  if ∀ i, latestᵢ is defined:  emit [v, latest₁, ..., latestₙ]
  else:                         drop v silently
```

**Invariants**:
- **Trigger-driven only**: Secondary sources never cause output emissions — only trigger does
- **Silent drop**: If any secondary has not yet emitted, the trigger emission is lost (no error, no buffering)
- **Latest-value semantics**: Intermediate secondary values emitted between trigger firings are discarded — only the most recent matters
- **Secondary subscriptions live for source lifetime**: Secondary sources are subscribed when `withLatestFrom` is subscribed and unsubscribed when it completes/errors

## Marble Diagram

```
Trigger:   --a-------b-------c--|
Secondary: -----1------2--------|
           withLatestFrom(secondary$)
Result:    ----------[b,1]--[c,2]-|

Trigger fires 'a': secondary hasn't emitted yet → DROPPED
Trigger fires 'b': secondary has emitted 1 → emit [b, 1]
Trigger fires 'c': secondary has emitted 2 → emit [c, 2]
```

**Multiple secondaries**:
```
Trigger:   ----a----b----|
State1:    --x------------|
State2:    ---y-----------|
           withLatestFrom(state1$, state2$)
Result:    ----[a,x,y]--[b,x,y]-|

Both secondaries emitted before 'a' → both trigger emissions pass.
'x' and 'y' are the latest values when both triggers fire.
```

**Contrast with `combineLatest`**:
```
combineLatest emits when ANY source emits (after all have emitted once):
Trigger:   --a-------b--|
State:     -----1-2-----|
combineLatest → [a,1], [a,2], [b,2]  (state changes drive output too)

withLatestFrom emits only when trigger emits:
Trigger:   --a-------b--|
State:     -----1-2-----|
withLatestFrom → [b,1], [b,2]? No — only [b, latestState]
→ actually: 'a' dropped (state not yet emitted), 'b' → [b, 2]  (2 is latest)
```

**Key observation**: `withLatestFrom` answers "what is the current state when this event fires?" — not "whenever state or event changes, recalculate." Use it when the secondary data is contextual, not reactive.

## Behavioral Characteristics

**Subscription**:
- Secondary sources subscribed immediately when `withLatestFrom` output is subscribed
- Primary source subscribed at the same time
- All subscriptions maintained for the duration of the primary source's lifetime

**Completion semantics**:
- Output completes when the primary trigger source completes
- Secondary source completion does not trigger output completion (their last value remains cached)

**Error handling**:
- Trigger error propagates immediately
- Secondary source error propagates immediately (even if trigger hasn't fired)

**Backpressure**:
- None — each trigger emission is handled synchronously; O(n secondaries) memory for latest values

**Hot vs. Cold**:
- Secondary sources are typically hot (state stores, BehaviorSubjects, shared streams)
- With cold secondaries: they begin on the first subscription but may not emit before the first trigger — causing silent drops
- This is the most common source of bugs with `withLatestFrom`

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Trigger/source value type
 *   A - Tuple of secondary value types
 *   R - Result type (when result selector is provided)
 *
 * Output: Observable<[T, ...A]> without selector
 *         Observable<R> with selector
 *
 * TypeScript infers tuple positions precisely from source types.
 */

import { withLatestFrom } from 'rxjs/operators';
import { Subject, BehaviorSubject } from 'rxjs';

interface User    { id: number; name: string; }
interface Cart    { items: CartItem[]; total: number; }
interface Config  { currency: string; tax: number; }

const clicks$  = new Subject<MouseEvent>();
const user$    = new BehaviorSubject<User>({ id: 1, name: 'Alice' });
const cart$    = new BehaviorSubject<Cart>({ items: [], total: 0 });
const config$  = new Subject<Config>();

// Two secondaries — tuple type inferred
clicks$.pipe(
  withLatestFrom(user$, cart$)
  // output: Observable<[MouseEvent, User, Cart]>
).subscribe(([event, user, cart]) => {
  // TypeScript knows exact types at each position
  console.log(user.name, cart.total);
});

// With result selector — maps tuple to custom type
interface CheckoutContext { userId: number; cartTotal: number; }
clicks$.pipe(
  withLatestFrom(
    user$,
    cart$,
    (event, user, cart): CheckoutContext => ({
      userId:    user.id,
      cartTotal: cart.total,
    })
  )
  // output: Observable<CheckoutContext>
).subscribe((ctx: CheckoutContext) => startCheckout(ctx));

// BehaviorSubject always has a current value → no silent drops
// Subject may not have emitted → potential silent drops on early triggers
```

## Examples

### Basic Usage — Combining Event with Current State
```typescript
import { fromEvent, BehaviorSubject } from 'rxjs';
import { withLatestFrom, map } from 'rxjs/operators';

const isLoggedIn$ = new BehaviorSubject<boolean>(false);
const loginButton = document.getElementById('login-btn')!;

fromEvent(loginButton, 'click').pipe(
  withLatestFrom(isLoggedIn$),
  map(([_, isLoggedIn]) => isLoggedIn)
).subscribe(loggedIn => {
  if (loggedIn) navigateToDashboard();
  else          showLoginModal();
});

// BehaviorSubject(false) has already emitted — no silent drops.
// Click always sees current auth state.
```

### Common Pattern — Checkout with Context Snapshot
```typescript
import { Subject, BehaviorSubject } from 'rxjs';
import { withLatestFrom } from 'rxjs/operators';

interface AppState { user: User; cart: Cart; currency: string; }

const checkoutClicks$ = new Subject<void>();
const state$          = new BehaviorSubject<AppState>(initialState);

checkoutClicks$.pipe(
  withLatestFrom(state$),
  // withLatestFrom(user$, cart$, config$)  — also valid for granular sources
).subscribe(([_, state]) => {
  const { user, cart, currency } = state;
  processCheckout({ userId: user.id, items: cart.items, currency });
});

// Clicking "checkout" snapshots the current full state at that instant.
// State changes between clicks are irrelevant — only the state at click time matters.
```

### Common Pattern — Action + State Enrichment (Redux-style)
```typescript
import { Subject, BehaviorSubject } from 'rxjs';
import { withLatestFrom, filter, map } from 'rxjs/operators';

interface ProductState { items: Product[]; loading: boolean; }

const addToCart$ = new Subject<number>(); // productId
const productState$ = new BehaviorSubject<ProductState>({ items: [], loading: false });

addToCart$.pipe(
  withLatestFrom(productState$),
  filter(([_, state]) => !state.loading),
  map(([productId, state]) => {
    const product = state.items.find(p => p.id === productId);
    if (!product) throw new Error(`Product ${productId} not found`);
    return product;
  })
).subscribe(product => cartStore.addItem(product));

// The action stream (addToCart$) drives the pipeline.
// productState$ provides context — a snapshot at each action's arrival.
// filter guards against acting during loading state.
```

### Common Pattern — Form Submission with Current Values
```typescript
import { fromEvent, combineLatest, Subject } from 'rxjs';
import { withLatestFrom, debounceTime, map, distinctUntilChanged } from 'rxjs/operators';

const nameInput  = document.getElementById('name')  as HTMLInputElement;
const emailInput = document.getElementById('email') as HTMLInputElement;
const submitBtn  = document.getElementById('submit') as HTMLButtonElement;

const name$  = fromEvent(nameInput,  'input').pipe(map(e => (e.target as HTMLInputElement).value), distinctUntilChanged());
const email$ = fromEvent(emailInput, 'input').pipe(map(e => (e.target as HTMLInputElement).value), distinctUntilChanged());

// On submit, snapshot current field values — don't re-emit on every keystroke
fromEvent(submitBtn, 'click').pipe(
  withLatestFrom(name$, email$)
).subscribe(([_, name, email]) => {
  submitForm({ name, email });
});

// name$ and email$ may not have emitted if user never typed in the fields.
// Handle this: provide initial values via startWith or use BehaviorSubject for form state.
fromEvent(submitBtn, 'click').pipe(
  withLatestFrom(
    name$.pipe(startWith('')),
    email$.pipe(startWith(''))
  )
).subscribe(([_, name, email]) => submitForm({ name, email }));
```

### Edge Cases — Secondary Not Yet Emitted, Secondary Completes, Error
```typescript
import { Subject, timer, EMPTY } from 'rxjs';
import { withLatestFrom, take } from 'rxjs/operators';

const trigger$   = new Subject<string>();
const secondary$ = new Subject<number>();

// Edge case 1: trigger fires before secondary has emitted → silent drop
trigger$.pipe(
  withLatestFrom(secondary$)
).subscribe(v => console.log(v));

trigger$.next('early'); // DROPPED — secondary hasn't emitted yet
secondary$.next(1);
trigger$.next('after'); // [after, 1] — secondary has now emitted

// Edge case 2: secondary completes — last value cached; operator continues
trigger$.pipe(
  withLatestFrom(secondary$.pipe(take(1)))
).subscribe(v => console.log(v));
secondary$.next(42); // secondary completes after first value
trigger$.next('a');  // [a, 42]
trigger$.next('b');  // [b, 42]  — 42 stays cached even after secondary completed

// Edge case 3: secondary errors before trigger fires → error propagates
const brokenSecondary$ = new Subject<number>();
trigger$.pipe(
  withLatestFrom(brokenSecondary$)
).subscribe({ next: v => console.log(v), error: e => console.log('error:', e.message) });
brokenSecondary$.error(new Error('secondary failed')); // → error: secondary failed
// Even though trigger hasn't fired, the secondary error propagates immediately.
```

## Common Pitfalls

### Anti-pattern: Secondary Source That May Not Emit Before First Trigger
```typescript
import { fromEvent } from 'rxjs';
import { withLatestFrom, ajax } from 'rxjs/operators';

const saveBtn = document.getElementById('save')!;

// ❌ SILENT DROPS — ajax hasn't responded yet when button is clicked early
fromEvent(saveBtn, 'click').pipe(
  withLatestFrom(ajax.getJSON('/api/config')) // cold — starts on subscription
).subscribe(([_, config]) => saveWithConfig(config));

// If user clicks before the HTTP response arrives → click is silently dropped
// No error, no warning — the click just disappears.

// ✅ CORRECT option 1: use shareReplay to ensure config is cached
const config$ = ajax.getJSON('/api/config').pipe(shareReplay(1));
fromEvent(saveBtn, 'click').pipe(
  withLatestFrom(config$)
).subscribe(([_, config]) => saveWithConfig(config));

// ✅ CORRECT option 2: withLatestFrom + startWith for a safe default
fromEvent(saveBtn, 'click').pipe(
  withLatestFrom(ajax.getJSON('/api/config').pipe(startWith(defaultConfig)))
).subscribe(([_, config]) => saveWithConfig(config));

// ✅ CORRECT option 3: if config MUST be loaded first, block until available
combineLatest([
  fromEvent(saveBtn, 'click'),
  config$
]).pipe(take(1)).subscribe(([_, config]) => saveWithConfig(config));
// But this fires on config$ emission too — see combineLatest pitfalls

// WHY: withLatestFrom subscribes to secondary sources immediately but only
// snapshots their latest value when the trigger fires. A cold secondary that
// hasn't completed its async work will silently discard early trigger emissions.
// Use BehaviorSubject, shareReplay, or startWith to guarantee a value exists.
```

### Anti-pattern: Using `withLatestFrom` When `combineLatest` Is Needed
```typescript
import { combineLatest, BehaviorSubject, Subject } from 'rxjs';
import { withLatestFrom, map } from 'rxjs/operators';

const searchTerm$ = new BehaviorSubject<string>('');
const filters$    = new BehaviorSubject<Filters>({ category: 'all', sort: 'date' });

// ❌ WRONG — withLatestFrom: only searchTerm$ drives re-execution
// Changing filters$ does NOT trigger a new search
searchTerm$.pipe(
  withLatestFrom(filters$),
  map(([term, filters]) => buildQuery(term, filters))
).subscribe(query => fetchResults(query));

// filters$.next({ category: 'books' }) → NO new search! (filters$ is secondary)

// ✅ CORRECT — combineLatest: either change triggers re-execution
combineLatest([searchTerm$, filters$]).pipe(
  map(([term, filters]) => buildQuery(term, filters)),
  distinctUntilChanged()
).subscribe(query => fetchResults(query));

// Now: changing either searchTerm$ or filters$ triggers a new search.

// ✅ ALSO VALID — withLatestFrom when only one source should drive:
// "Re-run search only when user explicitly submits, but include current filters"
const submitSearch$ = new Subject<void>();
submitSearch$.pipe(
  withLatestFrom(searchTerm$, filters$),
  map(([_, term, filters]) => buildQuery(term, filters))
).subscribe(query => fetchResults(query));
// Here searchTerm$/filters$ are contextual — explicit submit drives execution.

// WHY: withLatestFrom is "trigger + context snapshot" — secondary changes don't matter.
// combineLatest is "reactive recalculation" — any input change triggers output.
// Pick based on whether secondary-only changes should produce new output.
```

## Related Operators

**Same Category (Combination)**:
- **`combineLatest`**: Reactive — emits when ANY source emits (after all have emitted); use when all sources should drive output
- **`zipWith`**: Paired by index — emits when all sources have emitted their Nth value; use for strict positional pairing
- **`merge`**: Forwards each emission from any source without pairing; use for fan-in without correlation

**Complementary Operators**:
- **`startWith`**: Ensures secondary sources have emitted before the first trigger (prevents silent drops)
- **`shareReplay(1)`**: Guarantees a secondary has a cached value for instant delivery to `withLatestFrom`
- **`filter`**: Commonly applied after `withLatestFrom` to guard based on the secondary state

**Alternatives by Use Case**:

| Use Case | Instead of `withLatestFrom` | Use This | Why |
|----------|-----------------------------|----------|-----|
| Reactive recalculation | `withLatestFrom` | `combineLatest` | combineLatest emits when any input changes |
| Both sources should drive output | `withLatestFrom` | `combineLatest` | withLatestFrom only reacts to trigger |
| Pair by index | `withLatestFrom` | `zip` | zip pairs by emission count, not latest |
| Secondary may not have emitted | bare `withLatestFrom` | `withLatestFrom(source$.pipe(startWith(default)))` | Prevents silent drops |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/withLatestFrom](https://rxjs.dev/api/operators/withLatestFrom)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/withLatestFrom.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/withLatestFrom.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Trigger-Driven Context Snapshot
- **Cognitive Load**: 3/5 — The silent-drop behavior when secondary hasn't emitted is the main footgun; the trigger-vs-secondary asymmetry requires careful teaching
- **Usage Frequency**: 5/5 — Essential for action + state patterns in any reactive app
- **Composability**: 4/5 — Clean API; requires BehaviorSubject or startWith guards on cold secondaries

**Teaching Sequence**:
- **Prerequisites**: `combineLatest`, `BehaviorSubject`, `Subject`
- **Teaches**: Asymmetric combination, snapshot semantics, trigger-driven vs. reactive patterns
- **Common with**: `BehaviorSubject`, `shareReplay`, `startWith`, `filter`, `map`
