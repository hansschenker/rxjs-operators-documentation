# of

## Identity
- **Name**: of
- **Category**: Creation Operators
- **Type**: Synchronous static emitter — emits each argument in order then completes
- **Import**:
  ```typescript
  import { of } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function of<T>(...args: T[]): Observable<T>

  // Overloads for heterogeneous arguments (TypeScript infers union):
  function of<A, B>(a: A, b: B): Observable<A | B>
  function of<A, B, C>(a: A, b: B, c: C): Observable<A | B | C>
  // ...and so on
  ```

## Functional Specification

**Input**: Zero or more values of any type

**Output**: `Observable<T>` — emits each argument synchronously, in order, then completes

**Transformation**: Creates a cold Observable that, upon subscription, synchronously pushes each argument to the subscriber as a `next` notification, then immediately emits `complete`. All of this happens in the subscriber's call stack — `subscribe()` does not return until all values have been emitted.

**Mathematical representation**:
```
of(v₁, v₂, ..., vₙ) → v₁, v₂, ..., vₙ, complete

All emissions occur synchronously within the subscribe() call.
of() (no arguments) → complete  (no emissions)
```

**Invariants**:
- **Synchronous**: All values and completion happen before `subscribe()` returns
- **Cold**: Each subscriber triggers its own independent synchronous emission
- **Arguments emitted as-is**: `of([1,2,3])` emits the array as one value; use `from([1,2,3])` to iterate
- **No errors possible**: `of` cannot error — its values are known at creation time

## Marble Diagram

```
of(1, 2, 3):  (1)(2)(3)|

Parens = same synchronous tick.
All three values and completion occur in a single call stack frame.
```

**Contrast with `from`**:
```
of([1, 2, 3]):   ([1,2,3])|   — emits the array as one value
from([1, 2, 3]): (1)(2)(3)|   — iterates and emits each element
of(1, 2, 3):     (1)(2)(3)|   — spread form, same as from([1,2,3])
```

**`of()` with no arguments**:
```
of():  |   — completes immediately, no emissions
       (equivalent to EMPTY)
```

**Key observation**: `of` is the simplest possible Observable — a synchronous sequence of known values. It is used extensively in tests, fallback values (`catchError(() => of(defaultValue))`), and anywhere you need to inject a static value into a reactive pipeline.

## Behavioral Characteristics

**Subscription**:
- All values and completion emitted synchronously, before `subscribe()` returns
- Creating the Observable (`of(...)`) does nothing — cold; work happens only on subscription

**Completion semantics**:
- Always completes — `of` is inherently finite
- Completion is the last action in the subscribe call stack

**Error handling**:
- `of` never errors — values are predetermined
- If a downstream operator throws while processing an `of` emission, the error propagates as usual

**Backpressure**:
- None — synchronous; all values emitted before any async code can run

## Type System Integration

```typescript
/**
 * of infers T from the arguments.
 *
 * Homogeneous: of(1, 2, 3) → Observable<number>
 * Heterogeneous: of(1, 'a', true) → Observable<number | string | boolean>
 * Single value: of(42) → Observable<number>
 * No args: of() → Observable<never>  (completes with no type)
 *
 * Key distinction — of vs from with arrays:
 *   of([1,2,3])   → Observable<number[]>   (one emission: the array)
 *   from([1,2,3]) → Observable<number>     (three emissions: each element)
 *   of(1, 2, 3)   → Observable<number>     (three emissions: spread)
 */

import { of } from 'rxjs';

const n$: Observable<number>              = of(42);
const s$: Observable<string>              = of('hello');
const arr$: Observable<number[]>          = of([1, 2, 3]);  // ONE array emission
const mixed$: Observable<number | string> = of(1, 'two');
```

## Examples

### Basic Usage — Static Values, Fallbacks, Tests
```typescript
import { of } from 'rxjs';
import { map } from 'rxjs/operators';

// Static sequence
of(1, 2, 3).subscribe(console.log);
// Output: 1, 2, 3

// Single value — common as a fallback
of('default').subscribe(console.log);
// Output: default

// Synchronous ordering proof
let log: string[] = [];
of(1, 2, 3).subscribe(v => log.push(`value:${v}`));
log.push('after subscribe');
console.log(log);
// ['value:1', 'value:2', 'value:3', 'after subscribe']
// All values emitted before 'after subscribe' is pushed
```

### Common Pattern — Fallback Value in `catchError`
```typescript
import { of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface UserProfile { id: number; name: string; }

// Return a safe default when the HTTP request fails
ajax.getJSON<UserProfile>('/api/me').pipe(
  catchError(() => of<UserProfile | null>(null))
).subscribe(user => {
  if (user) showProfile(user);
  else      showGuestBanner();
});
```

### Common Pattern — `startWith` Equivalent and Loading States
```typescript
import { of, concat } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

type State = { status: 'loading' } | { status: 'ready'; data: Data };

// concat(of(loading), realStream) is equivalent to startWith(loading)
const page$ = concat(
  of<State>({ status: 'loading' }),
  ajax.getJSON<Data>('/api/data').pipe(
    switchMap(data => of<State>({ status: 'ready', data }))
  )
);

page$.subscribe(state => renderPage(state));
// Immediately renders loading spinner; updates when data arrives
```

### Common Pattern — In Tests: Mocking Observables
```typescript
import { of } from 'rxjs';
import { TestBed } from '@angular/core/testing';

// Mock a service that returns an Observable
const mockUserService = {
  getUser: (id: number) => of({ id, name: 'Mock User', role: 'user' as const })
};

// of is synchronous — tests run predictably without async helpers
it('should display user name', () => {
  spyOn(userService, 'getUser').and.returnValue(of(testUser));
  component.ngOnInit();
  // No need for fakeAsync/tick — of() is synchronous
  expect(component.userName).toBe(testUser.name);
});
```

### Common Pattern — Sequence of Observables with `concat`
```typescript
import { of, concat } from 'rxjs';
import { map } from 'rxjs/operators';

// Boot sequence with status messages
concat(
  of('Initializing...'),
  initApp().pipe(map(() => 'App initialized')),
  of('Loading user...'),
  loadUser().pipe(map(u => `Welcome, ${u.name}`)),
  of('Ready')
).subscribe(msg => console.log(msg));
```

### Edge Cases — No Arguments, Null/Undefined, Object Reference
```typescript
import { of } from 'rxjs';

// of() — completes with no emissions (≡ EMPTY)
of().subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done')
});
// Output: done

// of(null), of(undefined) — null/undefined ARE valid values
of(null).subscribe(v => console.log(v === null ? 'null' : v));
// Output: null

of(undefined).subscribe(v => console.log(v === undefined ? 'undef' : v));
// Output: undef

// Object reference — emits the reference, not a copy
const obj = { x: 1 };
of(obj).subscribe(v => {
  console.log(v === obj); // true — same reference
  v.x = 99; // mutation visible everywhere (standard JS reference semantics)
});
```

## Common Pitfalls

### Anti-pattern: `of(array)` vs `from(array)` Confusion
```typescript
import { of, from } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

const ids = [1, 2, 3];

// ❌ WRONG — of([1,2,3]) emits the array as a single value
of(ids).pipe(
  mergeMap(id => fetchUser(id)) // id is number[] — type error!
).subscribe(console.log);

// ✅ CORRECT — from([1,2,3]) emits each element individually
from(ids).pipe(
  mergeMap(id => fetchUser(id)) // id is number — correct
).subscribe(console.log);

// ✅ ALSO CORRECT — spread into of()
of(...ids).pipe(
  mergeMap(id => fetchUser(id))
).subscribe(console.log);

// WHY: of([arr]) treats the array as a single argument and emits it as one value.
// from([arr]) iterates the array and emits each element.
// of(...arr) spreads elements as separate arguments — same as from([arr]).
// When you want to process array elements individually, use from() or spread.
```

### Anti-pattern: Using `of` for Async Operations
```typescript
import { of, from } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// ❌ WRONG — of() wraps a Promise without resolving it
of(fetch('/api/data')).subscribe(v => {
  console.log(v); // v is a Promise object, not the data!
});

// ✅ CORRECT — from() converts Promise to Observable
from(fetch('/api/data').then(r => r.json())).subscribe(data => {
  console.log(data); // actual response data
});

// ✅ ALSO CORRECT in a pipe — switchMap accepts ObservableInput
source$.pipe(
  switchMap(() => fetch('/api/data').then(r => r.json()))
).subscribe(console.log);

// WHY: of() emits its arguments as-is — a Promise is emitted as a Promise object.
// from() understands Promises and subscribes to their resolution.
// Use of() for resolved values; from() / switchMap return for async operations.
```

## Related Operators

**Same Category (Creation)**:
- **`from(input)`**: Converts arrays, Promises, iterables, async iterables — use when the input is not a static set of known values
- **`interval(n)`**: Time-based repeating source
- **`timer(n)`**: One-shot delayed source
- **`EMPTY`**: Completes immediately with no emissions — equivalent to `of()`
- **`NEVER`**: Never emits, never completes — useful in tests and race patterns
- **`throwError(fn)`**: Creates an Observable that immediately errors

**Commonly Used With**:
- **`catchError`**: `catchError(() => of(fallback))` is the canonical error recovery fallback
- **`concat`**: Prepend/append static values around dynamic streams
- **`startWith`**: `startWith(v)` is internally implemented as `concat(of(v), source)`
- **`switchMap` / `mergeMap`**: Return `of(computed)` to convert synchronous values to Observables inside pipe operators

**Decision — `of` vs `from` vs `EMPTY`**:

| Input | Operator | Why |
|-------|----------|-----|
| Static known values | `of(a, b, c)` | Synchronous, explicit, no wrapping |
| Array to iterate | `from([a, b, c])` | Iterates elements |
| Promise | `from(promise)` | Resolves async |
| No value, just complete | `EMPTY` | Semantic constant, clearer than `of()` |
| Conditional value | `condition ? of(v) : EMPTY` | Optional emission pattern |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/of](https://rxjs.dev/api/index/function/of)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/just.html](http://reactivex.io/documentation/operators/just.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/observable/of.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/observable/of.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Synchronous Static Source
- **Cognitive Load**: 1/5 — The simplest operator; the only subtlety is of(arr) vs from(arr)
- **Usage Frequency**: 5/5 — Present in virtually every RxJS file; the universal fallback/default/test value
- **Composability**: 5/5 — Universal source; works everywhere an Observable is expected

**Teaching Sequence**:
- **Prerequisites**: None — ideal as the very first creation operator
- **Teaches**: Cold Observables, synchronous emission, the Observable contract (next→complete)
- **Common with**: `catchError`, `concat`, `startWith`, `switchMap`, `from`
