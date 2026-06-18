# takeUntil

## Identity
- **Name**: takeUntil
- **Category**: Filtering Operators
- **Type**: Completion trigger — unsubscribes source when a notifier emits
- **Import**:
  ```typescript
  import { takeUntil } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function takeUntil<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable that may run indefinitely

**Output**: `Observable<T>` — an Observable that mirrors the source until the notifier emits its first value

**Transformation**: Subscribes to both the source and the notifier. All source emissions are forwarded downstream. On the notifier's first emission (regardless of value), the source is unsubscribed and the output Observable completes normally. If the source completes before the notifier emits, the output completes normally. If the source errors, the error propagates regardless of notifier state.

**Mathematical representation**:
```
Let S be the source Observable producing values v₁, v₂, ..., vₙ at times t₁, t₂, ..., tₙ
Let N be the notifier Observable emitting its first value at time tₙₒₜᵢfʏ

Output = { vᵢ : tᵢ < tₙₒₜᵢfʏ }  ++ complete()

If tₙₒₜᵢfʏ never arrives and S completes at tₛₜₒₚ:
  Output = { v₁, v₂, ..., vₙ }  ++ complete()
```

**Invariants**:
- **Notifier value is irrelevant**: Only the *timing* of the first emission matters — its value is ignored
- **Clean completion**: Output always completes normally on notifier emission; it does **not** error
- **Unsubscription is synchronous**: Source and notifier are unsubscribed immediately after the notifier fires
- **Late notifier subscription**: The notifier is subscribed when `takeUntil` itself is subscribed, not when created
- **Emission on same tick**: If source and notifier emit synchronously in the same tick, the emission that arrives first wins — this depends on subscription order and is generally not guaranteed

## Marble Diagram

```
Source:    --a--b--c--d--e--|
Notifier:  ----------n------|
           takeUntil(notifier)
Result:    --a--b--c--|

Legend:
  - : time unit (10ms)
  a,b,c,d,e : emitted values
  n : notifier emits (value ignored)
  | : completion
  Source emission c passes (before notifier fires).
  Notifier fires at tick 10; output completes immediately.
  Values d, e never forwarded.
```

**Source completes before notifier**:
```
Source:    --a--b--|
Notifier:  ----------n--|
           takeUntil(notifier)
Result:    --a--b--|

Source completes normally; notifier subscription is cancelled.
```

**Notifier completes without emitting**:
```
Source:    --a--b--c--|
Notifier:  --------------|
           takeUntil(notifier)
Result:    --a--b--c--|

If notifier completes without emitting, source is not terminated.
Output completes only when source does.
```

**Key observation**: `takeUntil` is a declarative unsubscription mechanism — it converts imperative cleanup (`subscription.unsubscribe()`) into a reactive signal.

## Behavioral Characteristics

**Subscription**:
- Subscribes to both source and notifier lazily when the output is subscribed
- Maintains one subscription to each
- Notifier subscription is always cancelled when output completes or errors

**Completion semantics**:
- Source completion → output completes (notifier subscription released)
- Notifier emits → output completes (source subscription released)
- Notifier completes without emitting → output continues; notifier subscription is already gone
- Notifier errors → error propagates downstream (source subscription released)

**Error handling**:
- Source error propagates immediately; notifier subscription released
- Notifier error propagates immediately; source subscription released

**Backpressure**:
- None — synchronous pass-through until the notifier fires
- Internally holds two subscriptions simultaneously: O(1) state

**Hot vs. Cold**:
- Common pattern: use a `Subject` as notifier to trigger completion imperatively from component lifecycle
- Notifier is typically a hot Observable (event stream, component destroy signal)

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source and output type (MonoTypeOperatorFunction<T>)
 *   The notifier is typed as ObservableInput<any> — its value type is irrelevant
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * Type Safety:
 *   - T is fully preserved; takeUntil never touches value types
 *   - The notifier's value type does not need to match T
 *   - Accepts any ObservableInput (Observable, Promise, array, etc.)
 */

import { interval, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

// Common pattern: Subject as a component-lifetime notifier
class DataService {
  private destroy$ = new Subject<void>();

  start() {
    interval(1000).pipe(
      takeUntil(this.destroy$) // T = number, notifier emits void — mismatch is fine
    ).subscribe(n => console.log(n));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

// Notifier can be any ObservableInput: Promise, array, etc.
import { timer, fromEvent } from 'rxjs';

// Stop after 5 seconds
interval(500).pipe(
  takeUntil(timer(5000))
).subscribe(console.log);

// Stop on first button click
interval(500).pipe(
  takeUntil(fromEvent(document.getElementById('stop')!, 'click'))
).subscribe(console.log);
```

## Examples

### Basic Usage — Stop on a Timer
```typescript
import { interval, timer } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

// Emit every 100ms, stop after 500ms
interval(100).pipe(
  takeUntil(timer(500))
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done'),
});
// Output: 0, 1, 2, 3  (4 values in ~400ms), done
```

### Common Pattern — Angular Component Lifecycle (the canonical use)
```typescript
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subject, interval } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({ selector: 'app-timer', template: '{{ count }}' })
export class TimerComponent implements OnInit, OnDestroy {
  count = 0;
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(1000).pipe(
      takeUntil(this.destroy$)
    ).subscribe(n => this.count = n);
  }

  ngOnDestroy() {
    this.destroy$.next();   // triggers takeUntil completion
    this.destroy$.complete(); // convention: complete the subject too
  }
}

// When ngOnDestroy fires, interval subscription is cleaned up automatically.
// No manual subscription tracking needed.
```

### Common Pattern — Race Condition Prevention
```typescript
import { fromEvent, merge, of } from 'rxjs';
import { switchMap, takeUntil, delay } from 'rxjs/operators';

const button = document.getElementById('submit') as HTMLButtonElement;
const cancel = document.getElementById('cancel') as HTMLButtonElement;

fromEvent(button, 'click').pipe(
  switchMap(() => {
    const cancel$ = fromEvent(cancel, 'click');

    return submitRequest().pipe(
      takeUntil(cancel$) // cancel in-flight request on cancel button click
    );
  })
).subscribe({ next: handleSuccess, error: handleError });
```

### Common Pattern — Feature Flag Window
```typescript
import { Subject, interval } from 'rxjs';
import { takeUntil, filter, distinctUntilChanged } from 'rxjs/operators';

class FeatureService {
  private featureDisabled$ = new Subject<void>();

  whenFeatureEnabled$<T>(source$: Observable<T>): Observable<T> {
    return source$.pipe(
      takeUntil(this.featureDisabled$)
    );
  }

  disableFeature() {
    this.featureDisabled$.next();
  }
}
```

### Edge Cases — Synchronous Notifier, Empty Source, Notifier Completes
```typescript
import { of, EMPTY, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

// Edge case 1: synchronous notifier fires immediately → no values forwarded
of(1, 2, 3).pipe(
  takeUntil(of('stop')) // of() emits synchronously on subscribe
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete'),
});
// Output: complete  (notifier fires before source gets to emit)

// Edge case 2: empty source
EMPTY.pipe(
  takeUntil(new Subject())
).subscribe({ complete: () => console.log('complete') });
// Output: complete  (source already done, notifier never needed)

// Edge case 3: notifier completes without emitting → source runs to natural end
const neverEmits$ = new Subject<void>();
of(1, 2, 3).pipe(
  takeUntil(neverEmits$)
).subscribe(console.log);
neverEmits$.complete(); // completes subject without emitting
// Output: 1, 2, 3  (source completes normally)

// Edge case 4: notifier emits after source completes → no effect
const late$ = new Subject<void>();
of(1, 2, 3).pipe(
  takeUntil(late$)
).subscribe({ complete: () => console.log('done') });
// Output: done
late$.next(); // no-op, subscription already cleaned up
```

## Common Pitfalls

### Anti-pattern: Forgetting to Complete the `destroy$` Subject
```typescript
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

class MyComponent {
  private destroy$ = new Subject<void>();

  ngOnDestroy() {
    // ❌ INCOMPLETE — Subject is not completed, stays open in memory
    this.destroy$.next();

    // ✅ CORRECT — Always complete after next() to release Subject's internal resources
    this.destroy$.next();
    this.destroy$.complete();
  }
}

// WHY: Calling next() triggers the takeUntil completion (correct) but the
// Subject itself remains open and holds references. Calling complete() releases
// the Subject's internal subscriber list. While rarely catastrophic, it is a
// mild memory leak and violates Observable contract hygiene.
```

### Anti-pattern: Placing `takeUntil` Before Other Filtering Operators
```typescript
import { interval, Subject } from 'rxjs';
import { takeUntil, filter, map } from 'rxjs/operators';

const destroy$ = new Subject<void>();

// ❌ INCORRECT — takeUntil in the middle of the chain
// Operators after takeUntil may hold their own subscriptions
interval(100).pipe(
  filter(n => n % 2 === 0),
  takeUntil(destroy$),
  map(n => n * 2), // this operator is downstream of takeUntil — fine
).subscribe(console.log);

// ❌ PROBLEMATIC — takeUntil before an operator that creates inner subscriptions
// The inner subscriptions may outlive the outer stream
interval(100).pipe(
  takeUntil(destroy$),
  // switchMap's inner subscription is managed by switchMap, not takeUntil
  // This is usually fine, but the intent is easier to see if takeUntil is last
).subscribe(console.log);

// ✅ CORRECT — put takeUntil last in the chain
// All prior operators are cleaned up when takeUntil completes the outer stream
interval(100).pipe(
  filter(n => n % 2 === 0),
  map(n => n * 2),
  takeUntil(destroy$),  // <-- always last
).subscribe(console.log);

// WHY: takeUntil completes the Observable it is applied to. Operators further
// downstream still execute their teardown on completion. Putting takeUntil last
// ensures the entire chain tears down cleanly when destroy$ fires.
```

### Anti-pattern: Using `take(1)` on the Notifier Inside `takeUntil`
```typescript
import { Subject } from 'rxjs';
import { takeUntil, take } from 'rxjs/operators';

const destroy$ = new Subject<void>();

// ❌ REDUNDANT — take(1) on the notifier is unnecessary
interval(100).pipe(
  takeUntil(destroy$.pipe(take(1))) // takeUntil already only reacts to the first emission
).subscribe(console.log);

// ✅ CORRECT — takeUntil already stops at the first notifier emission
interval(100).pipe(
  takeUntil(destroy$)
).subscribe(console.log);

// WHY: takeUntil internally subscribes to the notifier and unsubscribes it after
// the first emission. The take(1) is not wrong, but it adds visual noise while
// duplicating behavior that takeUntil provides for free.
```

### Anti-pattern: Sharing a `destroy$` Subject Across Multiple Components
```typescript
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

// ❌ DANGEROUS — shared destroy$ subject
const sharedDestroy$ = new Subject<void>(); // module-level singleton

class ComponentA {
  ngOnInit() {
    interval(100).pipe(takeUntil(sharedDestroy$)).subscribe(console.log);
  }
  ngOnDestroy() {
    sharedDestroy$.next(); // ← destroys ComponentB's subscriptions too!
  }
}

class ComponentB {
  ngOnInit() {
    interval(200).pipe(takeUntil(sharedDestroy$)).subscribe(console.log);
  }
  // ComponentB's subscriptions are killed when ComponentA is destroyed
}

// ✅ CORRECT — each component owns its own destroy$ instance
class SafeComponentA {
  private destroy$ = new Subject<void>(); // instance property, not shared
  ngOnInit()    { interval(100).pipe(takeUntil(this.destroy$)).subscribe(console.log); }
  ngOnDestroy() { this.destroy$.next(); this.destroy$.complete(); }
}

// WHY: A Subject emits to all current subscribers. A shared destroy$ fires
// to every subscriber in every component, causing premature unsubscription
// in components that are still alive. Always make destroy$ an instance property.
```

## Related Operators

**Same Category (Filtering — Count/Condition)**:
- **`take(n)`**: Completes after exactly N emissions from source — use when count is known; unlike `takeUntil`, the termination condition is numeric not temporal
- **`takeWhile(predicate)`**: Completes when source emits a value that fails the predicate — use when termination depends on *source values* rather than an external signal
- **`first(predicate?)`**: Completes after the first emission matching an optional predicate — shorthand for `takeWhile` on a single value
- **`skip(n)` / `skipUntil(notifier)`**: Symmetric counterparts — skip the beginning of a stream instead of the end

**Lifecycle Management Alternatives**:
- **`takeWhile(_, true)`**: `takeWhile` with `inclusive: true` — includes the failing value before completing; `takeUntil` does not include the notifier value (it's discarded)
- **Angular `AsyncPipe`**: Handles subscription lifecycle automatically for template bindings — prefer over manual `takeUntil` patterns for simple template cases

**Commonly Composed With**:
- **`Subject`**: The standard notifier for component lifetime management
- **`switchMap`**: Use `takeUntil` last in chains containing `switchMap` to ensure inner subscription cleanup
- **`timer` / `interval`**: Convert time-based termination to a notifier Observable

**Alternatives by Use Case**:

| Use Case | Instead of `takeUntil` | Use This | Why |
|----------|------------------------|----------|-----|
| Stop after N values | `takeUntil(timer)` | `take(N)` | Clearer intent when count is fixed |
| Stop when value condition fails | `takeUntil(conditionChange$)` | `takeWhile(pred)` | Condition tied to source values |
| Angular template binding | `takeUntil(destroy$)` in subscribe | `AsyncPipe` | Lifecycle managed automatically |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/takeUntil](https://rxjs.dev/api/operators/takeUntil)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/takeuntil.html](http://reactivex.io/documentation/operators/takeuntil.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/takeUntil.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/takeUntil.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Declarative Lifecycle Management (Reactive Unsubscription)
- **Cognitive Load**: 3/5 — The pattern is simple but the synchronous-notifier edge case and "put it last" convention trip beginners
- **Usage Frequency**: 5/5 — The dominant pattern for managing Observable lifetimes in Angular and similar frameworks
- **Composability**: 4/5 — Works with any Observable as notifier; positioning rule (always last) requires discipline

**Problem Domain**:
Managing the lifetime of long-lived or infinite Observables (intervals, WebSocket streams, state subscriptions) by tying them to an external lifecycle signal rather than managing subscriptions imperatively.

**When to Teach**:
Teach immediately after `take` and `takeWhile`. The trio forms a complete picture of stream termination: by count, by value, by external signal.

- **Prerequisites**: `Subject`, `interval`, `filter`
- **Teaches**: Declarative lifecycle management, the destroy-Subject pattern, reactive teardown
- **Leads to**: Angular component patterns, `shareReplay`, reactive state management
- **Common with**: `Subject`, `switchMap`, `interval`, `timer`, `fromEvent`

**Common Misconceptions**:
1. **"takeUntil errors when notifier emits"** — it completes normally; errors only propagate if the notifier *errors*
2. **"It waits for all in-flight operations"** — it unsubscribes immediately; use `finalize` for cleanup side effects
3. **"Notifier value matters"** — only timing matters; the emitted value is discarded
4. **"I need take(1) on the notifier"** — already built in
