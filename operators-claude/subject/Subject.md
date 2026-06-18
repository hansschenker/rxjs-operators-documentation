# Subject

## Identity
- **Name**: Subject
- **Category**: Subject / Notification
- **Type**: Multicast bridge — simultaneously an Observable (subscribe) and an Observer (next/error/complete)
- **Import**:
  ```typescript
  import { Subject } from 'rxjs';
  ```
- **Signature** (class):
  ```typescript
  class Subject<T> extends Observable<T> implements Observer<T> {
    next(value: T): void       // push a value to all current subscribers
    error(err: any): void      // push an error to all current subscribers; closes subject
    complete(): void           // push completion to all current subscribers; closes subject
    subscribe(...): Subscription
    pipe(...operators): Observable<T>
    asObservable(): Observable<T>
    observed: boolean          // true if there are active subscribers
    isStopped: boolean         // true after complete() or error()
    closed: boolean            // true after complete() or error()
  }
  ```

## Functional Specification

**Concept**: A `Subject` is both an `Observable` (you can subscribe to it) and an `Observer` (you can call `next`, `error`, `complete` on it). It is the fundamental multicast primitive in RxJS — multiple subscribers share the same execution, and values pushed via `next()` are broadcast to all current subscribers simultaneously.

**How it differs from a regular Observable**:
- Regular Observable: each subscription creates an independent execution (cold)
- Subject: one shared execution; all subscribers see the same values at the same time (hot)

**No stored value**: Unlike `BehaviorSubject`, a `Subject` has no "current value". New subscribers receive only values pushed via `next()` after they subscribe.

**Mathematical representation**:
```
Subject maintains a list of active subscribers: [s₁, s₂, ..., sₙ]

On next(v):      ∀ sᵢ: sᵢ.next(v)  — broadcast to all
On error(e):     ∀ sᵢ: sᵢ.error(e); subject.isStopped = true
On complete():   ∀ sᵢ: sᵢ.complete(); subject.isStopped = true

On new subscribe after complete/error: immediately receives complete/error
On new subscribe otherwise: added to list; receives future next() calls only
```

**Invariants**:
- **Hot**: All subscribers share one execution; new subscribers miss past values
- **No replay**: New subscribers receive nothing from before their subscription
- **Closed after complete/error**: `next()` after close is a silent no-op
- **Error replays to late subscribers**: After `error()`, new subscribers immediately receive that same error

## Marble Diagram

```
Subject — two subscribers at different times:

                  s.next('a')  s.next('b')  s.next('c')
                      |            |            |
Timeline:    ---------a------------b------------c------

Sub A subscribes at t=0:  ---------a------------b------------c------
Sub B subscribes at t=2 (after 'a'):  -----------b------------c------
                                       ↑ 'a' was missed — no replay

Subject.next() broadcasts to all CURRENT subscribers at the moment of emission.
```

**Contrast with `BehaviorSubject`**:
```
Subject:           new subscriber sees nothing until next next() call
BehaviorSubject:   new subscriber immediately receives current value
```

**Key observation**: `Subject` is the imperative "push" gateway into a reactive pipeline. Use it at system boundaries where events come from outside RxJS (user actions dispatched from UI, responses from non-reactive APIs, test harnesses). Internally, keep state declarative.

## Behavioral Characteristics

**Subscription**:
- New subscribers are added to the internal list immediately; no "current value" is replayed
- Subscription is synchronous — subscriber is in the list before `subscribe()` returns

**Completion semantics**:
- After `complete()`: new subscribers immediately receive `complete()`
- Subject becomes permanently closed — no further values

**Error semantics**:
- After `error(e)`: all current subscribers receive the error; subject closes
- New subscribers after `error()` immediately receive the same error
- This means errors "replay" to late subscribers — unlike values

**Backpressure**:
- None — synchronous delivery to all subscribers; O(n subscribers) per `next()` call

**Thread safety**:
- JavaScript is single-threaded; concurrent `next()` calls don't exist in browser/Node.js runtimes

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type of values the Subject emits
 *
 * Subject<T> extends Observable<T> implements Observer<T>
 *
 * .next(v: T)         — requires T; TypeScript enforces the type
 * .asObservable()     → Observable<T>  — read-only facade
 * .pipe(...)          → Observable<T>  — creates a derived Observable (does NOT modify the Subject)
 *
 * IMPORTANT: .pipe() on a Subject returns a plain Observable, not a Subject.
 * The source Subject remains unmodified. You cannot .next() on the piped result.
 */

import { Subject } from 'rxjs';
import { filter, map } from 'rxjs/operators';

const events$ = new Subject<{ type: string; payload: unknown }>();

// Derived Observables from the Subject
const clicks$  = events$.pipe(filter(e => e.type === 'CLICK'));
const keydowns$ = events$.pipe(filter(e => e.type === 'KEYDOWN'), map(e => e.payload as string));

// Type safety: next() enforces T
events$.next({ type: 'CLICK', payload: null }); // ✅
events$.next('wrong');                           // ❌ TypeScript error

// asObservable() hides the Subject interface
class EventBus {
  private bus$ = new Subject<AppEvent>();
  readonly events$ = this.bus$.asObservable(); // consumers can't call .next()

  emit(event: AppEvent): void { this.bus$.next(event); }
}
```

## Examples

### Basic Usage — Event Bus
```typescript
import { Subject } from 'rxjs';
import { filter } from 'rxjs/operators';

interface AppEvent {
  type: 'USER_LOGIN' | 'USER_LOGOUT' | 'ITEM_ADDED' | 'ITEM_REMOVED';
  payload?: unknown;
}

const eventBus$ = new Subject<AppEvent>();

// Subscribe to all events
eventBus$.subscribe(e => console.log('all events:', e.type));

// Subscribe to filtered events
eventBus$.pipe(
  filter(e => e.type === 'USER_LOGIN')
).subscribe(e => console.log('login event'));

// Dispatch events
eventBus$.next({ type: 'USER_LOGIN', payload: { userId: 1 } });
// Output: all events: USER_LOGIN, login event

eventBus$.next({ type: 'ITEM_ADDED', payload: { id: 42 } });
// Output: all events: ITEM_ADDED  (login filter doesn't fire)
```

### Common Pattern — `destroy$` for Lifecycle Management
```typescript
import { Subject, interval } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { Component, OnInit, OnDestroy } from '@angular/core';

@Component({ selector: 'app-live', template: '{{ count }}' })
export class LiveComponent implements OnInit, OnDestroy {
  count = 0;
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(1000).pipe(
      takeUntil(this.destroy$) // Subject used purely as a trigger
    ).subscribe(n => this.count = n);
  }

  ngOnDestroy() {
    this.destroy$.next();   // trigger takeUntil
    this.destroy$.complete(); // release Subject's resources
  }
}
// Subject<void> is the canonical choice for destroy$ — the void type communicates
// that only the timing matters, not the emitted value.
```

### Common Pattern — Manual Trigger / Bridge from Imperative to Reactive
```typescript
import { Subject } from 'rxjs';
import { switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

// Bridge: button click (imperative DOM event) → reactive pipeline
const refreshTrigger$ = new Subject<void>();

refreshTrigger$.pipe(
  switchMap(() =>
    ajax.getJSON<Data>('/api/data').pipe(
      catchError(() => of(null))
    )
  )
).subscribe(data => data ? renderData(data) : showError());

// Wire up the button
document.getElementById('refresh')!.addEventListener(
  'click',
  () => refreshTrigger$.next()
);

// Can also be triggered programmatically
function triggerRefresh(): void { refreshTrigger$.next(); }
```

### Common Pattern — Action Dispatcher (Redux-style)
```typescript
import { Subject } from 'rxjs';
import { scan, startWith, shareReplay } from 'rxjs/operators';

type Action =
  | { type: 'INCREMENT' }
  | { type: 'DECREMENT' }
  | { type: 'RESET' };

interface CounterState { count: number; }

function counterReducer(state: CounterState, action: Action): CounterState {
  switch (action.type) {
    case 'INCREMENT': return { count: state.count + 1 };
    case 'DECREMENT': return { count: state.count - 1 };
    case 'RESET':     return { count: 0 };
  }
}

// Subject is the action dispatcher
const dispatch$ = new Subject<Action>();

// State is derived reactively
const state$ = dispatch$.pipe(
  scan(counterReducer, { count: 0 }),
  startWith({ count: 0 }),
  shareReplay(1)
);

state$.subscribe(s => console.log('count:', s.count));

dispatch$.next({ type: 'INCREMENT' }); // count: 1
dispatch$.next({ type: 'INCREMENT' }); // count: 2
dispatch$.next({ type: 'DECREMENT' }); // count: 1
dispatch$.next({ type: 'RESET' });     // count: 0
```

### Edge Cases — Late Subscriber, After Complete, Error Replay
```typescript
import { Subject } from 'rxjs';

// Edge case 1: late subscriber misses past values
const s$ = new Subject<number>();
s$.next(1);
s$.next(2);
s$.subscribe(v => console.log('late:', v)); // nothing yet — 1 and 2 were missed
s$.next(3); // late: 3

// Edge case 2: complete() — late subscribers get immediate complete
const s2$ = new Subject<number>();
s2$.next(42);
s2$.complete();
s2$.subscribe({
  next:     v => console.log(v),     // nothing — no replay
  complete: () => console.log('done') // done — immediately
});

// Edge case 3: next() after complete() is silently ignored
s2$.next(99); // no-op — subject is closed
console.log(s2$.isStopped); // true

// Edge case 4: error replays to late subscribers (dangerous)
const s3$ = new Subject<number>();
s3$.error(new Error('boom'));
s3$.subscribe({ error: e => console.log('late error:', e.message) });
// late error: boom — error replayed even to post-error subscribers
```

## Common Pitfalls

### Anti-pattern: Using Subject as a Mutable State Container
```typescript
import { Subject, BehaviorSubject } from 'rxjs';

// ❌ WRONG TOOL — Subject for current state; late subscribers miss the value
class UserStore {
  private user$ = new Subject<User | null>();

  setUser(user: User | null) { this.user$.next(user); }

  // Any subscriber that arrives AFTER setUser() was called sees nothing!
  get currentUser$() { return this.user$.asObservable(); }
}

const store = new UserStore();
store.setUser({ id: 1, name: 'Alice', role: 'user' });

// Component subscribes after setUser — gets nothing
store.currentUser$.subscribe(user => console.log(user)); // no output

// ✅ CORRECT — BehaviorSubject for "current value" state
class SafeUserStore {
  private user$ = new BehaviorSubject<User | null>(null);
  readonly currentUser$ = this.user$.asObservable();
  get currentUser(): User | null { return this.user$.value; }

  setUser(user: User | null) { this.user$.next(user); }
}

const safe = new SafeUserStore();
safe.setUser({ id: 1, name: 'Alice', role: 'user' });

safe.currentUser$.subscribe(user => console.log(user?.name)); // Alice — gets current value immediately

// WHY: Subject has no "current value." Subscribers receive only future emissions.
// For state that components need immediately on subscribe, use BehaviorSubject.
// Use Subject for events/actions where "current state" is not meaningful.
```

### Anti-pattern: Calling `complete()` or `error()` Without Considering Reuse
```typescript
import { Subject } from 'rxjs';

const trigger$ = new Subject<void>();

// ❌ DANGEROUS — completing trigger$ breaks all future subscribers
function onModalClose() {
  trigger$.next();
  trigger$.complete(); // ← closes the Subject permanently!
}

// Later, user opens modal again:
trigger$.subscribe(() => handleTrigger()); // does nothing — Subject is closed
trigger$.next(); // silently ignored

// ✅ CORRECT — only complete when the Subject's lifetime truly ends
// For reusable triggers, never complete:
class RefreshService {
  private refresh$ = new Subject<void>();
  readonly onRefresh$ = this.refresh$.asObservable();

  requestRefresh(): void { this.refresh$.next(); }
  // complete() only called when the service is destroyed
  destroy(): void { this.refresh$.complete(); }
}

// WHY: complete() closes the Subject forever. Future next() calls are no-ops.
// Future subscribers immediately complete without receiving any value.
// Only call complete() when the Subject's purpose has permanently ended
// (service destroyed, test completed, application shut down).
```

### Anti-pattern: Exposing the Subject Directly
```typescript
import { Subject } from 'rxjs';

// ❌ BREAKS ENCAPSULATION — every consumer can call next/error/complete
class NotificationService {
  notifications$ = new Subject<string>(); // public — anyone can push!
}

const ns = new NotificationService();
ns.notifications$.next('genuine notification');
ns.notifications$.error(new Error('consumer caused error')); // breaks all subscribers!
ns.notifications$.next('ignored — subject closed');

// ✅ CORRECT — private Subject, public Observable
class SafeNotificationService {
  private _notifications$ = new Subject<string>();
  readonly notifications$ = this._notifications$.asObservable();

  notify(message: string): void { this._notifications$.next(message); }
  destroy(): void { this._notifications$.complete(); }
}

// WHY: Subject is both Observable and Observer. Exposing it publicly gives every
// consumer write access — they can push arbitrary values, errors, or completions.
// Always make the Subject private and expose only the read-only Observable face
// via asObservable() or a getter that returns this.subject$.asObservable().
```

## Related Types

**Subject Family**:
- **`BehaviorSubject<T>(initialValue)`**: Stores and replays current value to new subscribers — use for state that has a meaningful "current" value
- **`ReplaySubject<T>(bufferSize)`**: Replays the last `n` emissions to new subscribers — use when late subscribers need history
- **`AsyncSubject<T>`**: Emits only the final value on completion — use for "result of an operation" that multiple consumers wait for

**Commonly Used With**:
- **`takeUntil`**: `destroy$ = new Subject<void>()` is the canonical pattern for component lifecycle management
- **`scan + startWith`**: Subject as dispatcher; scan derives state; startWith seeds initial value
- **`asObservable()`**: Always call this when exposing a Subject publicly

**Decision — Which Subject?**:

| Need | Subject type | Why |
|------|-------------|-----|
| Event bus, triggers, actions | `Subject` | No stored value; events are momentary |
| Current mutable state | `BehaviorSubject` | Stores current value; new subscribers get it immediately |
| Replay N past events | `ReplaySubject(N)` | History buffer for late subscribers |
| Single async result | `AsyncSubject` | Emit once on completion, like a Promise |
| Lifecycle teardown | `Subject<void>` | destroy$ pattern — value type communicates intent |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/class/Subject](https://rxjs.dev/api/index/class/Subject)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/subject.html](http://reactivex.io/documentation/subject.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/Subject.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/Subject.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Imperative-to-Reactive Bridge / Multicast Hot Source
- **Cognitive Load**: 2/5 — Conceptually simple; the "no replay" and "error replays but values don't" asymmetry are the key teaching points
- **Usage Frequency**: 5/5 — Foundational; used in destroy$ patterns, action dispatchers, event buses, and test harnesses everywhere
- **Composability**: 5/5 — The primary gateway from imperative code into reactive pipelines

**Teaching Sequence**:
- **Prerequisites**: Observable subscription, Observer interface
- **Teaches**: Hot Observables, multicast semantics, the Observable-Observer duality, encapsulation with asObservable()
- **Leads to**: `BehaviorSubject` (add current value), `ReplaySubject` (add history), `takeUntil` (lifecycle pattern)
- **Common with**: `takeUntil`, `scan`, `startWith`, `shareReplay`, `asObservable()`
