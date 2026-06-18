# Subject Decision Guide

Subjects are simultaneously Observables and Observers — they bridge imperative code (calling `.next()`) with reactive streams. Choosing the right Subject variant determines what late subscribers receive.

---

## The Four Subject Variants

| Subject | Buffer | Late subscriber receives | Requires seed |
|---|---|---|---|
| `Subject` | None | Nothing (only future emissions) | No |
| `BehaviorSubject(seed)` | Last value | Current value immediately | Yes |
| `ReplaySubject(n)` | Last N values | Last N values replayed | No |
| `AsyncSubject` | Last value | Last value only on complete | No |

---

## Decision Guide

```
Does the subscriber need historical values?
├── No → Subject                (plain pub/sub, no memory)
│
└── Yes → How many?
           ├── Just the latest one (always has a value) → BehaviorSubject(initialValue)
           │
           ├── Latest N values → ReplaySubject(N)
           │
           └── Only the final value → AsyncSubject
```

---

## `Subject` — Plain Multicast

```typescript
import { Subject } from 'rxjs';

const events$ = new Subject<string>();

events$.subscribe(v => console.log('A:', v));
events$.next('hello');  // A: hello
events$.next('world');  // A: world

// Late subscriber misses prior emissions:
events$.subscribe(v => console.log('B:', v));
events$.next('late');   // A: late, B: late (B only sees this and future)
```

**Use when**: Broadcasting events where late subscribers starting fresh is correct — UI events, message buses, command streams.

---

## `BehaviorSubject` — Always Has a Value

```typescript
import { BehaviorSubject } from 'rxjs';

const count$ = new BehaviorSubject(0); // seed required

count$.subscribe(v => console.log('A:', v)); // immediately logs: A: 0
count$.next(1); // A: 1
count$.next(2); // A: 2

// Late subscriber gets CURRENT value immediately:
count$.subscribe(v => console.log('B:', v)); // immediately logs: B: 2
count$.next(3); // A: 3, B: 3

// Access current value imperatively:
console.log(count$.value); // 3
```

**Use when**: State that always has a meaningful current value — user authentication state, current route, feature flags, form values.

**Key distinction from `Subject`**: `BehaviorSubject` always has a value. If you call `.value` before any emission, you get the seed.

---

## `ReplaySubject(n)` — Cache Last N

```typescript
import { ReplaySubject } from 'rxjs';

const log$ = new ReplaySubject<string>(3); // buffer last 3

log$.next('first');
log$.next('second');
log$.next('third');
log$.next('fourth'); // 'first' drops out of buffer

// Late subscriber gets last 3:
log$.subscribe(v => console.log(v));
// Immediately: second, third, fourth
// Then future emissions as they arrive

// ReplaySubject(1) without a seed — similar to BehaviorSubject but:
// - No required seed value
// - Does not emit to late subscribers until first emission
const noSeed$ = new ReplaySubject<number>(1);
noSeed$.subscribe(console.log); // nothing yet
noSeed$.next(42); // logs: 42
```

**Use when**: Late subscribers need recent history — log viewers, undo stacks, WebSocket reconnection replay, component initialization after data has loaded.

**`ReplaySubject(1)` vs `BehaviorSubject`**:
- `BehaviorSubject`: requires a seed; exposes `.value`; late subscribers always get something
- `ReplaySubject(1)`: no seed required; no `.value` property; late subscribers get nothing until first emission

---

## `AsyncSubject` — Final Value on Complete

```typescript
import { AsyncSubject } from 'rxjs';

const result$ = new AsyncSubject<number>();

result$.subscribe(v => console.log('A:', v)); // nothing yet
result$.next(1); // buffered
result$.next(2); // replaces buffer
result$.next(3); // replaces buffer

result$.complete(); // NOW emits 3 to all subscribers
// A: 3

// Late subscriber after complete gets the final value:
result$.subscribe(v => console.log('B:', v)); // immediately: B: 3
```

**Use when**: You want Promise-like semantics — one final result, delivered to all subscribers (current and future) when the operation completes. Rarely used directly; `forkJoin` and `lastValueFrom` cover most cases.

---

## Subjects vs Observables — When to Use Each

```
Do you need to push values imperatively (call .next() from outside)?
├── Yes → Subject family (which variant depends on buffering needs above)
│
└── No → Use Observable creation operators:
          - Known values         → of(), from()
          - Time-based           → interval(), timer()
          - Events               → fromEvent()
          - Lazy/conditional     → defer(), iif()
          - Async operation      → ajax(), fromFetch()
```

**Avoid Subject as a workaround for Observable creation.** The most common misuse is wrapping an existing Observable in a Subject unnecessarily:

```typescript
// ❌ ANTI-PATTERN — wrapping Observable in Subject
const subject = new Subject<User>();
ajax.getJSON<User>('/api/user').subscribe(user => subject.next(user));
const user$ = subject.asObservable();

// ✅ CORRECT — use the Observable directly
const user$ = ajax.getJSON<User>('/api/user').pipe(shareReplay(1));
```

---

## Common Pitfalls

### Completing a Subject Closes It Permanently
```typescript
const subject = new BehaviorSubject(0);
subject.next(1);
subject.complete(); // closed

subject.next(2);    // silently ignored — no error, no emission
subject.subscribe(v => console.log(v)); // immediately logs 1 (last value before complete)

// WHY: A completed Subject will never emit again. Late subscribers get
// the completion (and final value for BehaviorSubject/AsyncSubject).
// If you need a resettable stream, create a new Subject instance.
```

### Using `Subject.value` Outside Angular/State Patterns
```typescript
import { BehaviorSubject } from 'rxjs';

const state$ = new BehaviorSubject({ count: 0 });

// ❌ FRAGILE — reading .value then deriving new state is a race condition
const current = state$.value;
state$.next({ count: current.count + 1 }); // fine in single-threaded JS, but:
// if two places read .value before either calls .next(), one update is lost

// ✅ CORRECT — use scan for derived state
import { scan } from 'rxjs/operators';
const increment$ = new Subject<void>();
const count$ = increment$.pipe(scan(count => count + 1, 0));
// count$ always reflects current count without .value reads
```

---

## Quick Reference

```typescript
// Pub/sub (no history)
const bus$ = new Subject<Event>();

// Current value (always initialized)
const auth$ = new BehaviorSubject<AuthState>({ loggedIn: false });

// Recent history for late subscribers
const recent$ = new ReplaySubject<Message>(10);

// One-shot result (Promise analogue)
const result$ = new AsyncSubject<Result>();
```
