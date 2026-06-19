# BehaviorSubject

## Identity
- **Name**: BehaviorSubject
- **Category**: Subject / Notification
- **Type**: Stateful multicast Observable — always holds a current value and replays it to each new subscriber
- **Import**:
  ```typescript
  import { BehaviorSubject } from 'rxjs';
  ```
- **Signature** (class):
  ```typescript
  class BehaviorSubject<T> extends Subject<T> {
    constructor(initialValue: T)

    get value(): T                    // synchronous current value access
    getValue(): T                     // synchronous current value access (throws if closed)
    next(value: T): void              // push new value to all subscribers
    complete(): void                  // complete the subject
    error(err: any): void             // error the subject
    subscribe(...): Subscription      // subscribe for updates
    pipe(...operators): Observable<T> // transform without altering the Subject
    asObservable(): Observable<T>     // expose read-only Observable face
  }
  ```

## Functional Specification

**Concept**: A `Subject` that **always holds a current value**. Any new subscriber immediately receives the current value on subscription (synchronously), then receives future `next()` calls as they arrive.

**How it differs from `Subject`**:
- `Subject`: new subscribers receive only future emissions (nothing on subscribe if no value has been emitted yet)
- `BehaviorSubject`: new subscribers always receive the current value immediately on subscribe

**How it differs from `ReplaySubject(1)`**:
- `ReplaySubject(1)`: replays the last emitted value; if nothing has been emitted yet, new subscribers wait
- `BehaviorSubject(initialValue)`: always has a value; even before any `next()`, new subscribers get `initialValue`

**Mathematical representation**:
```
BehaviorSubject(v₀) holds a "current value" slot initialized to v₀.

On next(vₙ):    current = vₙ; emit vₙ to all current subscribers
On subscribe:   emit current synchronously; then emit all future next() calls
On complete():  emit complete to all subscribers; subject is closed
On error(e):    emit error to all subscribers; subject is closed

value getter:   returns current synchronously (no subscription needed)
```

**Invariants**:
- **Always has a value**: `getValue()` / `.value` is always available and synchronous
- **New subscribers get current value immediately**: guaranteed synchronous emission on subscription
- **Single active value**: only the most recent `next()` value is held; no history
- **Mutable reference**: the stored value is the exact object passed to `next()` — mutation is invisible
- **Closed after complete/error**: calling `next()` after `complete()` or `error()` is silently ignored

## Marble Diagram

```
BehaviorSubject(0) — initial value: 0

                         bs.next(1)    bs.next(2)    bs.next(3)
                            |             |             |
Timeline:         0---------1-------------2-------------3---

Sub A subscribes at t=0:  0---------1-------------2-------------3---
                          ↑ receives initial value 0 immediately

Sub B subscribes at t=2 (after next(1), before next(2)):
                                    1-------------2-------------3---
                                    ↑ receives latest value (1) immediately

Sub C subscribes at t=4 (after next(3)):
                                                              3---
                                                              ↑ receives latest value (3) immediately
```

**Contrast with `Subject`**:
```
Subject (no initial value):
Sub A at t=0: ---------1-------------2------
Sub B at t=2:           -------------2------  (missed 1, no initial replay)

BehaviorSubject(0):
Sub A at t=0: 0--------1-------------2------
Sub B at t=2:           1------------2------  (gets 1 = current value at subscribe time)
```

**Key observation**: `BehaviorSubject` is the RxJS equivalent of a reactive variable — it stores one value that any code can read synchronously (`getValue()`) and any number of subscribers can react to asynchronously.

## Behavioral Characteristics

**Subscription**:
- New subscriber receives the current value **synchronously** before `subscribe()` returns
- Subsequent `next()` calls are delivered synchronously to all subscribers

**Completion semantics**:
- After `complete()`: new subscribers receive the current value then immediately complete (no future values)
- `next()` after `complete()` is a no-op

**Error semantics**:
- After `error(e)`: new subscribers receive the error immediately
- Storing the error state means every future subscriber also errors — be careful with long-lived BehaviorSubjects

**Backpressure**:
- None — synchronous delivery; O(1) state (current value + subscriber list)

**Hot vs. Cold**:
- Hot by nature: always active, shares state across all subscribers
- Each subscriber sees the same current value and future updates

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type of the stored value
 *
 * BehaviorSubject<T> extends Subject<T> extends Observable<T>
 *
 * .value and getValue(): T  — synchronous, always available
 * .next(value: T): void    — push a new value
 * .asObservable(): Observable<T>  — exposes only the Observable interface
 *
 * asObservable() is used in services to prevent consumers from calling .next()
 * on the Subject directly — encapsulates the write capability.
 */

import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

// Strongly typed — T = User | null
interface User { id: number; name: string; role: 'admin' | 'user'; }
const currentUser$ = new BehaviorSubject<User | null>(null);

// .value is typed as User | null
const user: User | null = currentUser$.value;

// Derived Observable — typed as Observable<boolean>
const isAdmin$: Observable<boolean> = currentUser$.pipe(
  map(u => u?.role === 'admin' ?? false),
  distinctUntilChanged()
);

// Type-safe encapsulation in a service:
class AuthService {
  private _user$ = new BehaviorSubject<User | null>(null);
  readonly user$ = this._user$.asObservable(); // Observable<User | null> — no .next() exposed

  login(user: User): void  { this._user$.next(user); }
  logout(): void           { this._user$.next(null); }
  get currentUser(): User | null { return this._user$.value; }
}
```

## Examples

### Basic Usage — Reactive Variable
```typescript
import { BehaviorSubject } from 'rxjs';

const count$ = new BehaviorSubject<number>(0);

// Any subscriber gets current value immediately
count$.subscribe(n => console.log('A:', n)); // A: 0 (immediately)

count$.next(1); // A: 1
count$.next(2); // A: 2

// Late subscriber gets current value
count$.subscribe(n => console.log('B:', n)); // B: 2 (immediately — current value)

count$.next(3); // A: 3, B: 3

// Synchronous read
console.log('current:', count$.value); // current: 3
```

### Common Pattern — Auth State Service
```typescript
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface User { id: number; name: string; role: 'admin' | 'user'; }

class AuthService {
  private _user$ = new BehaviorSubject<User | null>(null);

  // Public read-only Observable — consumers cannot call .next()
  readonly user$       = this._user$.asObservable();
  readonly isLoggedIn$ = this.user$.pipe(map(u => u !== null), distinctUntilChanged());
  readonly isAdmin$    = this.user$.pipe(map(u => u?.role === 'admin' ?? false), distinctUntilChanged());

  // Synchronous getter for non-reactive code
  get currentUser(): User | null { return this._user$.value; }

  login(user: User): void { this._user$.next(user); }
  logout(): void          { this._user$.next(null); }
}

const auth = new AuthService();

// Any component subscribes and immediately gets current state
auth.isLoggedIn$.subscribe(loggedIn => updateNavBar(loggedIn));
auth.user$.subscribe(user => user ? showWelcome(user.name) : showLoginPrompt());

auth.login({ id: 1, name: 'Alice', role: 'admin' });
// → updateNavBar(true), showWelcome('Alice')
```

### Common Pattern — Feature Store (Mini Redux)
```typescript
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface CartState { items: CartItem[]; total: number; }

class CartStore {
  private state$ = new BehaviorSubject<CartState>({ items: [], total: 0 });

  readonly items$ = this.state$.pipe(map(s => s.items), distinctUntilChanged());
  readonly total$ = this.state$.pipe(map(s => s.total), distinctUntilChanged());
  readonly count$ = this.state$.pipe(map(s => s.items.length), distinctUntilChanged());

  addItem(item: CartItem): void {
    const { items } = this.state$.value;
    const newItems = [...items, item];
    this.state$.next({
      items: newItems,
      total: newItems.reduce((t, i) => t + i.price * i.qty, 0)
    });
  }

  removeItem(id: number): void {
    const newItems = this.state$.value.items.filter(i => i.id !== id);
    this.state$.next({
      items: newItems,
      total: newItems.reduce((t, i) => t + i.price * i.qty, 0)
    });
  }

  get snapshot(): CartState { return this.state$.value; }
}

// Usage
const cart = new CartStore();
cart.total$.subscribe(t => renderTotal(t)); // fires immediately: 0
cart.addItem({ id: 1, name: 'Widget', price: 9.99, qty: 2 });
// → renderTotal(19.98)
```

### Common Pattern — Form Field Binding
```typescript
import { BehaviorSubject, combineLatest } from 'rxjs';
import { map, distinctUntilChanged, debounceTime } from 'rxjs/operators';

const name$  = new BehaviorSubject<string>('');
const email$ = new BehaviorSubject<string>('');

// Two-way bind to inputs
nameInput.addEventListener('input',  e => name$.next((e.target as HTMLInputElement).value));
emailInput.addEventListener('input', e => email$.next((e.target as HTMLInputElement).value));

// Reactive form validation
const formValid$ = combineLatest([name$, email$]).pipe(
  map(([name, email]) => name.length >= 2 && email.includes('@')),
  distinctUntilChanged()
);

formValid$.subscribe(valid => (submitBtn.disabled = !valid));
// Fires immediately (both are empty → invalid → button disabled)
// Updates on every keystroke
```

### Edge Cases — Complete After next, Error Replay, getValue After Close
```typescript
import { BehaviorSubject } from 'rxjs';

// Edge case 1: complete() preserves current value for new subscribers, then completes
const bs = new BehaviorSubject<number>(0);
bs.next(42);
bs.complete();
bs.subscribe({
  next:     v => console.log('value:', v),   // value: 42
  complete: () => console.log('complete')    // complete
});

// Edge case 2: next() after complete() is silently ignored
bs.next(99); // no-op — subject is closed
bs.subscribe({ next: v => console.log(v) }); // still receives 42, then complete

// Edge case 3: error replays to new subscribers (dangerous!)
const errSubject = new BehaviorSubject<number>(0);
errSubject.error(new Error('broken'));
errSubject.subscribe({ error: e => console.log('new sub error:', e.message) });
// → new sub error: broken   (error replayed to every new subscriber forever)

// Fix: use refCount: true with shareReplay, or catch errors before they reach the Subject

// Edge case 4: getValue() after closed — returns last value (does not throw)
const bs2 = new BehaviorSubject<string>('hello');
bs2.complete();
console.log(bs2.getValue()); // 'hello'  — still accessible synchronously
```

## Common Pitfalls

### Anti-pattern: Exposing the Subject Directly (Missing Encapsulation)
```typescript
import { BehaviorSubject } from 'rxjs';

// ❌ BREAKS ENCAPSULATION — service exposes the writable Subject publicly
class UserService {
  user$ = new BehaviorSubject<User | null>(null); // public Subject!
}

const userService = new UserService();
// Any consumer can call .next() or .error() directly:
userService.user$.next({ id: 999, name: 'Hacker', role: 'admin' }); // external mutation!
userService.user$.error(new Error('goodbye')); // breaks all subscribers!

// ✅ CORRECT — expose only the Observable face
class SafeUserService {
  private _user$ = new BehaviorSubject<User | null>(null); // private
  readonly user$ = this._user$.asObservable(); // read-only Observable

  updateUser(user: User): void { this._user$.next(user); } // controlled mutation
}

// WHY: BehaviorSubject extends Subject extends Observable — it has .next(), .error(),
// .complete(). Exposing it directly gives every consumer write access.
// asObservable() returns a plain Observable with no Subject methods — encapsulates
// the write capability behind controlled service methods.
```

### Anti-pattern: Mutating the Stored Object
```typescript
import { BehaviorSubject } from 'rxjs';

interface Config { debug: boolean; timeout: number; }

const config$ = new BehaviorSubject<Config>({ debug: false, timeout: 3000 });

// ❌ DANGEROUS — mutating the object held inside the BehaviorSubject
const cfg = config$.getValue();
cfg.debug = true;    // mutates the stored object in-place
config$.next(cfg);   // emits the same reference

// Subscribers using distinctUntilChanged() will NOT be notified (same reference!)
// Components using Angular's OnPush change detection will NOT re-render
// Any code holding a reference to the old value now sees mutated data

// ✅ CORRECT — always spread to produce a new reference
config$.next({ ...config$.getValue(), debug: true });
// New object reference → distinctUntilChanged detects change
// → OnPush components re-render

// WHY: BehaviorSubject stores a reference. Mutating the stored object changes
// the value without going through next() — change detection mechanisms that
// rely on reference equality (===) will miss the update. Always produce a
// new value with spread or Object.assign.
```

### Anti-pattern: Using BehaviorSubject When `scan + startWith` Is Better
```typescript
import { BehaviorSubject, Subject } from 'rxjs';
import { scan, startWith, shareReplay } from 'rxjs/operators';

// ❌ IMPERATIVE — manually managing accumulated state in BehaviorSubject
class ItemService {
  private items$ = new BehaviorSubject<Item[]>([]);

  addItem(item: Item): void {
    this.items$.next([...this.items$.getValue(), item]);
  }

  removeItem(id: number): void {
    this.items$.next(this.items$.getValue().filter(i => i.id !== id));
  }
}

// ✅ DECLARATIVE — event-sourced via scan + startWith
class ReactiveItemService {
  private actions$ = new Subject<ItemAction>();
  readonly items$ = this.actions$.pipe(
    scan(itemsReducer, [] as Item[]),
    startWith([] as Item[]),
    shareReplay(1)
  );

  addItem(item: Item): void    { this.actions$.next({ type: 'ADD', item }); }
  removeItem(id: number): void { this.actions$.next({ type: 'REMOVE', id }); }
}

// WHY: BehaviorSubject stores current state and requires reading it (getValue())
// to derive the next state — imperative pull. scan + startWith is purely
// declarative: actions are inputs, state is a derivation. The declarative approach
// is easier to test, replay, and compose with other operators (distinctUntilChanged,
// map, combineLatest) without fighting the Subject's mutable reference.
// Use BehaviorSubject when external code needs to imperatively get current value;
// use scan+startWith for accumulated state that derives entirely from events.
```

## Related Operators / Types

**Subject Family**:
- **`Subject`**: No stored value — new subscribers receive only future emissions; use when there is no meaningful "current" state
- **`ReplaySubject(n)`**: Replays last `n` emissions to new subscribers — use when subscribers need more than the most recent value (e.g., an event history)
- **`AsyncSubject`**: Emits only the last value before completion — use when subscribers only care about the final result (like `forkJoin` for push-based sources)

**Patterns that replace or complement BehaviorSubject**:
- **`scan + startWith + shareReplay(1)`**: Event-sourced reactive store — prefer over BehaviorSubject for state derived from events
- **`withLatestFrom`**: Read a BehaviorSubject's current value when a trigger fires — snapshot semantics
- **`combineLatest`**: Combine multiple BehaviorSubjects reactively — any change in any subject re-emits

**Alternatives by Use Case**:

| Use Case | Instead of `BehaviorSubject` | Use | Why |
|----------|------------------------------|-----|-----|
| Event bus (no current state) | `BehaviorSubject` | `Subject` | No default value needed |
| Replay last N values | `BehaviorSubject` | `ReplaySubject(N)` | BehaviorSubject only replays 1 |
| State derived from actions | `BehaviorSubject.next()` | `scan + startWith + shareReplay` | Declarative, composable, testable |
| Cache HTTP result | `BehaviorSubject` | `shareReplay(1)` | shareReplay manages the subscription |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/class/BehaviorSubject](https://rxjs.dev/api/index/class/BehaviorSubject)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/subject.html](http://reactivex.io/documentation/subject.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/BehaviorSubject.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/BehaviorSubject.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Stateful Reactive Variable (Current-Value Subject)
- **Cognitive Load**: 3/5 — The encapsulation anti-pattern and mutation footgun require explicit teaching; the core concept is intuitive
- **Usage Frequency**: 5/5 — The most-used Subject type; foundational to Angular services and any push-based reactive state
- **Composability**: 4/5 — Excellent as a source in combineLatest/withLatestFrom; the asObservable() pattern is essential for encapsulation

**Teaching Sequence**:
- **Prerequisites**: `Subject`, Observable subscription model, `of`/`startWith`
- **Teaches**: Stateful subjects, synchronous current-value access, encapsulation with asObservable, BehaviorSubject vs scan+startWith tradeoff
- **Leads to**: `ReplaySubject`, `combineLatest`, NgRx/Redux-style stores, reactive service design
- **Common with**: `combineLatest`, `withLatestFrom`, `map`, `distinctUntilChanged`, `asObservable()`
