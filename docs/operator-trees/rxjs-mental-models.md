# RxJS Mental Models

Conceptual frameworks for understanding Observables, operators, and reactive programming. Start here if operators feel abstract.

---

## Mental Model 1: The Observable as a Lazy Function

A cold Observable is just a function that hasn't been called yet:

```typescript
// A plain function — runs when called:
function getUsers() {
  return fetch('/api/users').then(r => r.json());
}
getUsers(); // runs now

// An Observable — runs when subscribed:
const users$ = this.http.get('/api/users');
users$.subscribe(render); // runs now
// users$ itself is just a description — no work done until subscribe()
```

**Key insight**: `const stream$ = someObservable()` does nothing. `.subscribe()` is the "call" that starts execution. This is why unsubscribing cancels the work.

---

## Mental Model 2: Operators as Conveyor Belt Transforms

Think of a pipeline as a factory conveyor belt:

```
Raw material → [wash] → [cut] → [package] → output
        source$ → [map] → [filter] → [map] → subscriber
```

Each operator:
- Receives values from the upstream belt
- Does something to each value
- Passes results to the downstream belt

No value moves backward. Each operator is independent and composable.

```typescript
// Read as: "for each user, keep active ones, transform to view model"
users$
  .pipe(
    filter(u => u.active),           // keep
    map(u => ({ name: u.name, id: u.id })) // transform
  )
  .subscribe(render);
```

---

## Mental Model 3: Time as a Dimension

Unlike arrays (values in space), Observables emit values **over time**:

```
Array:      [1, 2, 3, 4, 5]      — all values at once, in memory
Observable: --1--2--3--4--5--|   — values arrive over time

filter on Array:      [2, 4]                  — instant
filter on Observable: ----2----4---------     — values arrive as they pass the filter
```

This time dimension is what makes Observables powerful for:
- HTTP responses (arrive later)
- User events (arrive when user acts)
- WebSocket messages (arrive continuously)
- Timers (arrive periodically)

---

## Mental Model 4: Hot vs Cold = Shared vs Independent

```
Cold Observable = movie on DVD
  Each viewer (subscriber) gets their own copy from the beginning.
  Subscriber A starts at 0:00. Subscriber B subscribes later — also starts at 0:00.

Hot Observable = live TV broadcast
  One stream, all viewers see the same frames at the same time.
  Subscriber B joins late and misses what already aired.
```

```typescript
// Cold — each subscriber gets independent execution:
const cold$ = this.http.get('/api/data');
cold$.subscribe(A); // HTTP request 1
cold$.subscribe(B); // HTTP request 2 (separate!)

// Hot — one execution, many subscribers:
const hot$ = cold$.pipe(shareReplay(1));
hot$.subscribe(A); // HTTP request — only one
hot$.subscribe(B); // gets cached result from same request
```

---

## Mental Model 5: flattening Operators = Nested Loops

Higher-order Observables (Observables of Observables) are like nested loops. The flattening strategy determines how the "loop" runs:

```typescript
// concatMap = sequential for-loop (wait for each to finish):
// for (const id of ids) { await fetch(id); }
ids$.pipe(concatMap(id => fetch$(id)))

// mergeMap = parallel for-loop (all at once):
// ids.forEach(id => fetch(id)); // all concurrent
ids$.pipe(mergeMap(id => fetch$(id)))

// switchMap = "latest only" (cancel previous iteration):
// Start fetch, but if a new id arrives, cancel the current fetch
ids$.pipe(switchMap(id => fetch$(id)))

// exhaustMap = "busy? skip" (ignore new until current finishes):
// If a fetch is in progress, ignore new ids until it completes
ids$.pipe(exhaustMap(id => fetch$(id)))
```

---

## Mental Model 6: `scan` as a Running Accumulator

`scan` is like a bank account balance — every transaction updates it:

```
Transaction: +100  -50  +200  -30
Balance:      100   50   250  220   (running total after each)

scan((balance, transaction) => balance + transaction, 0)
```

```typescript
// Every click increments count:
clicks$.pipe(
  scan(count => count + 1, 0)
).subscribe(renderCounter);

// Every event modifies state:
actions$.pipe(
  scan((state, action) => reducer(state, action), initialState)
).subscribe(renderUI);
```

---

## Mental Model 7: `combineLatest` as a Spreadsheet

`combineLatest` works like spreadsheet cells — when any input changes, the formula recalculates:

```
Cell A: [name input]    = "Alice"
Cell B: [prefix select] = "Dr."
Cell C: [formula]       = B + " " + A   → "Dr. Alice"

When A changes to "Bob": C automatically → "Dr. Bob"
When B changes to "Mr.": C automatically → "Mr. Bob"
```

```typescript
combineLatest({
  name:   nameInput$,   // Cell A
  prefix: prefixSelect$ // Cell B
}).pipe(
  map(({ prefix, name }) => `${prefix} ${name}`) // Cell C formula
).subscribe(updateDisplay);
// Recalculates whenever either input changes
```

---

## Mental Model 8: Error Propagation as a Short-Circuit

An error in an Observable is like a thrown exception in a loop — it stops everything unless caught:

```
Normal:    --1--2--ERROR              // stops here
Caught:    --1--2--catch(fallback)--| // recovers

// catchError is try/catch for Observables:
source$.pipe(
  catchError(err => of(FALLBACK))   // catch and continue
)
```

**Critical**: After an error, the stream terminates (unless `retry` re-subscribes or `catchError` returns a new Observable). You cannot "resume" a stream after an uncaught error.

---

## Mental Model 9: Subjects as EventEmitters

A Subject is a bridge between imperative code (push values in) and reactive code (observe values):

```
Imperative code         Reactive code
─────────────           ─────────────
button.click()   ──→   subject$.next()  ──→  pipeline  ──→  UI update
api.response()   ──→   subject$.next()  ──→  pipeline  ──→  store update
```

```typescript
// Bridge: DOM event → Subject → Observable pipeline:
const clicks$ = new Subject<MouseEvent>();
button.addEventListener('click', e => clicks$.next(e));

clicks$.pipe(
  throttleTime(500),
  switchMap(() => api.submit())
).subscribe(handleResult);
```

---

## Mental Model 10: `takeUntil` as a Lifetime Boundary

`takeUntil` gives a stream a lifetime — it exists as long as the component/page/session exists:

```
Component lifetime:    [============================]
Stream lifetime:       [============================]
                       ↑ subscribe (ngOnInit)       ↑ unsubscribe (ngOnDestroy)

takeUntil(destroy$):   --1--2--3--4--[destroy!]
                                      (stream ends here)
```

```typescript
// The stream lives as long as the component:
interval(1000).pipe(
  takeUntil(this.destroy$) // boundary = component lifetime
).subscribe(updateClock);

ngOnDestroy() { this.destroy$.next(); } // end the lifetime
```

---

## The Five Questions to Ask About Any Observable

When you encounter an unfamiliar Observable or stream, ask:

1. **When does it start?** (subscribe, component init, immediately?)
2. **How does it end?** (complete, error, takeUntil, or never?)
3. **Is it hot or cold?** (shared or independent per subscriber?)
4. **What are its error semantics?** (retries? fallback? propagates?)
5. **Who owns unsubscription?** (async pipe? takeUntil? manual?)

Answering these five questions describes a stream completely.

---

## Common Conceptual Errors

### "The Observable runs when I create it"
No — cold Observables run when subscribed. `const x$ = of(1, 2, 3)` does nothing.

### "`map` can do async operations"
No — `map` is synchronous. Returning a Promise from `map` gives `Observable<Promise<T>>`. Use `switchMap`/`mergeMap` for async.

### "I can restart a stream after an error by calling `.next()` again"
No — after an unhandled error, the stream is dead. Use `retry` to re-subscribe, or `catchError` to recover.

### "`combineLatest` gives me the latest from all sources at any time"
Partially — it waits for ALL sources to emit at least once. Add `startWith` to sources that might not emit immediately.

### "Unsubscribing prevents the source from producing values"
Only for cold Observables. Hot Observables (WebSocket, DOM events, Subjects) continue producing regardless of subscribers.
