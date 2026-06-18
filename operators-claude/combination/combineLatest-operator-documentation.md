# combineLatest

## Identity

- **Name**: combineLatest
- **Category**: Combination Operators
- **Type**: Reactive combiner — emits a new array whenever any source emits, combining each source's latest value
- **Import**:
  ```typescript
  import { combineLatest } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // Array form (recommended — typed tuple)
  function combineLatest<T extends readonly unknown[]>(
    sources: readonly [...ObservableInputTuple<T>]
  ): Observable<T>

  // Dictionary form
  function combineLatest<T extends Record<string, ObservableInput<any>>>(
    sources: T
  ): Observable<{ [K in keyof T]: ObservedValueOf<T[K]> }>
  ```

## Functional Specification

`combineLatest` subscribes to all sources simultaneously. Once **every** source has emitted at least one value, it emits a combined array of the latest value from each source. After that, it re-emits whenever any source emits.

**Invariants**:
- Does not emit until ALL sources have emitted at least once ("all seeded")
- Emits synchronously if all sources are seeded at subscription time
- Completes when ALL sources complete
- Errors immediately if ANY source errors

**Dictionary form** (preferred for named streams):
```typescript
combineLatest({ price: price$, quantity: quantity$ })
// emits: { price: number, quantity: number }
```

## Marble Diagram

```
A:    --1-----------3--------5--|
B:    ------2-----4----------|
C:    ----------3------------|

combineLatest([A, B, C]):
Wait: all three must emit once before first output

First output when C emits 3: [1, 2, 3]  (latest from each)
A emits 3:                    [3, 2, 3]
B emits 4:                    [3, 4, 3]
A emits 5:                    [5, 4, 3]
B completes → still active (A and C still open)
C completes → still active (A still open)
A completes → ALL complete → outer completes

Result: ----[1,2,3]--[3,2,3]-[3,4,3]-[5,4,3]--|
```

## Behavioral Characteristics

**Startup**: Waits for all sources to have emitted at least once before emitting anything. If one source never emits (EMPTY, NEVER), the combination never starts.

**Completion**: Completes when ALL sources complete. A slow source keeps the combination alive.

**Error**: First error from any source propagates immediately, cancelling all other subscriptions.

**Synchronous sources**: If all sources are seeded synchronously (e.g., `BehaviorSubject`s), the first emission happens synchronously at subscription time.

## Type System Integration

```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';

// Array form — typed tuple output
const a$ = new BehaviorSubject(1);
const b$ = new BehaviorSubject('hello');
const c$ = new BehaviorSubject(true);

combineLatest([a$, b$, c$]).subscribe(([n, s, b]) => {
  // TypeScript knows: n: number, s: string, b: boolean
  console.log(n, s, b);
});

// Dictionary form — named output (preferred for clarity)
combineLatest({ price: price$, quantity: quantity$ }).subscribe(({ price, quantity }) => {
  console.log(`Total: ${price * quantity}`);
});
```

## Examples

### Basic Usage — Form Validation
```typescript
import { combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';

// All fields update independently; validate whenever any changes
combineLatest({
  email:    emailField$,
  password: passwordField$,
  confirm:  confirmField$
}).pipe(
  map(({ email, password, confirm }) => ({
    valid:  isValidEmail(email) && password.length >= 8 && password === confirm,
    email,
    password,
    confirm
  }))
).subscribe(({ valid }) => submitBtn.disabled = !valid);
```

### Common Pattern — Dependent UI State
```typescript
import { combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';

// Dashboard driven by three independent data streams
combineLatest({
  user:        userService.currentUser$,
  permissions: authService.permissions$,
  theme:       settingsService.theme$
}).pipe(
  map(({ user, permissions, theme }) => ({
    showAdminPanel: permissions.includes('admin'),
    displayName:    user.name,
    darkMode:       theme === 'dark'
  }))
).subscribe(state => renderHeader(state));
```

### Common Pattern — Combine with Initial Value via `startWith`
```typescript
import { combineLatest, fromEvent } from 'rxjs';
import { startWith, map } from 'rxjs/operators';

// fromEvent never has a "current value" — seed it with startWith
const width$  = fromEvent(window, 'resize').pipe(map(() => window.innerWidth),  startWith(window.innerWidth));
const height$ = fromEvent(window, 'resize').pipe(map(() => window.innerHeight), startWith(window.innerHeight));

combineLatest([width$, height$]).subscribe(([w, h]) => {
  console.log(`Viewport: ${w}×${h}`);
});
// Logs immediately on subscription, then on every resize
```

### Edge Case — One Source That Never Emits (EMPTY)
```typescript
import { combineLatest, EMPTY, of } from 'rxjs';

combineLatest([of(1, 2, 3), EMPTY]).subscribe({
  next:     v        => console.log(v),       // never called
  complete: ()       => console.log('done')   // called immediately
});
// EMPTY completes without emitting → combination never seeded → just completes
```

## Common Pitfalls

### Anti-pattern: Using `combineLatest` When You Need `forkJoin`
```typescript
import { combineLatest, forkJoin } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// ❌ WRONG — combineLatest for one-shot HTTP requests
combineLatest([
  ajax.getJSON('/api/user'),
  ajax.getJSON('/api/settings')
]).subscribe(([user, settings]) => bootstrap(user, settings));
// This works but emits once per response (2 emissions possible), not just once

// ✅ CORRECT — forkJoin for "all complete, one result"
forkJoin([
  ajax.getJSON('/api/user'),
  ajax.getJSON('/api/settings')
]).subscribe(([user, settings]) => bootstrap(user, settings));
// Emits exactly once when both complete

// WHY: forkJoin waits for all sources to complete and emits one combined
// result. combineLatest emits on every source change — correct for live
// streams, unnecessary complexity for finite HTTP calls.
```

### Anti-pattern: Forgetting That All Sources Must Emit Before First Output
```typescript
import { combineLatest, Subject } from 'rxjs';

// ❌ SURPRISE — Subject with no initial value silently blocks the combination
const a$ = new Subject<number>();
const b$ = new Subject<number>();

combineLatest([a$, b$]).subscribe(console.log);

a$.next(1); // nothing logged — b$ hasn't emitted yet
a$.next(2); // nothing logged — b$ hasn't emitted yet

// Only after b$ emits does the combination start:
b$.next(10); // logs [2, 10]

// ✅ CORRECT — use BehaviorSubject for sources with a known initial value
import { BehaviorSubject } from 'rxjs';
const a$ = new BehaviorSubject(0);
const b$ = new BehaviorSubject(0);

combineLatest([a$, b$]).subscribe(console.log);
// Logs [0, 0] immediately on subscription

// WHY: combineLatest won't emit until ALL sources have emitted at least once.
// Seed plain Subjects with startWith() or switch to BehaviorSubject.
```

## Related Operators

- **`forkJoin`**: "All complete, one result" — for finite sources like HTTP requests
- **`zip`**: Combines by index (pairs first-to-first, second-to-second) — not latest
- **`withLatestFrom`**: Pipeable — "use latest from B whenever A emits"
- **`combineLatestAll`**: Higher-order version for dynamic sets of inner Observables

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/combineLatest](https://rxjs.dev/api/index/function/combineLatest)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key teaching points**:
1. Does not emit until ALL sources have emitted at least once — seed slow sources with `startWith` or use `BehaviorSubject`
2. Dictionary form (`{ a: a$, b: b$ }`) is clearer than array form for named streams
3. Use `forkJoin` for one-shot parallel requests; use `combineLatest` for reactive live state
