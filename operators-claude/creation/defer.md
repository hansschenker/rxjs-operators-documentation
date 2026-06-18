# defer

## Identity
- **Name**: defer
- **Category**: Creation Operators
- **Type**: Lazy Observable factory — creates a new Observable for each subscriber by calling a factory function at subscription time
- **Import**:
  ```typescript
  import { defer } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function defer<R extends ObservableInput<any>>(
    observableFactory: () => R
  ): Observable<ObservedValueOf<R>>
  ```

## Functional Specification

**Input**: `observableFactory` — a zero-argument function that returns an `ObservableInput<T>`

**Output**: `Observable<T>` — a cold Observable that calls the factory on each subscription

**Transformation**: At creation time, `defer` does nothing. On each subscription, `defer` calls `observableFactory()` and immediately subscribes to the returned Observable. The subscriber sees the returned Observable's emissions as if they were subscribed to it directly.

**Key difference from direct creation**:
```typescript
// EAGER — ajax call starts NOW, at creation time
const request$ = ajax.getJSON('/api/data'); // HTTP request fires immediately

// LAZY — ajax call starts LATER, when subscribe() is called
const request$ = defer(() => ajax.getJSON('/api/data')); // HTTP request defers
```

**Invariants**:
- **Factory called per subscription**: N subscriptions → N factory calls → N independent Observables
- **Truly cold**: Even for Observables that are normally warm/hot, `defer` guarantees a fresh factory call each time
- **Factory can return any ObservableInput**: Observable, Promise, array, iterable
- **Factory errors propagate**: If the factory throws, the error is delivered as an Observable error

## Marble Diagram

```
defer(() => of(Math.random())):

Sub A subscribes: factory called → of(0.42) → emits 0.42|
Sub B subscribes: factory called → of(0.87) → emits 0.87|

Each subscription gets its own independent value from a fresh factory call.

Compare to: const r = of(Math.random())  — evaluated ONCE at creation
Sub A: 0.42|
Sub B: 0.42|  (same value — factory was called once, at creation)
```

**Lazy condition-based Observable**:
```
defer(() => isLoggedIn() ? adminApi$() : publicApi$())

Each subscriber evaluates the condition at subscription time.
If auth state changes between subscriptions, each gets the right Observable.
```

## Behavioral Characteristics

**Subscription**: Factory is called synchronously at subscription time; the returned Observable is subscribed to immediately after.

**Completion/Error**: Mirrors whatever the factory-returned Observable does. Factory throwing → Observable error.

**Hot vs Cold**: `defer` always produces a cold outer Observable. The inner Observable can be anything — `defer` calls the factory freshly per subscription.

## Type System Integration

```typescript
/**
 * R extends ObservableInput<any>
 * Output: Observable<ObservedValueOf<R>>
 *
 * defer(() => ajax.getJSON<User>('/api/me'))  → Observable<User>
 * defer(() => Promise.resolve(42))            → Observable<number>
 * defer(() => [1, 2, 3])                      → Observable<number>
 */

import { defer } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Type inferred correctly
const user$: Observable<User> = defer(() => ajax.getJSON<User>('/api/me'));
```

## Examples

### Basic Usage — Lazy Evaluation
```typescript
import { defer, of } from 'rxjs';

// Eager: Math.random() called once at creation
const eager$ = of(Math.random());
eager$.subscribe(v => console.log('A:', v)); // A: 0.42
eager$.subscribe(v => console.log('B:', v)); // B: 0.42 — same value!

// Lazy: Math.random() called on each subscription
const lazy$ = defer(() => of(Math.random()));
lazy$.subscribe(v => console.log('A:', v)); // A: 0.42
lazy$.subscribe(v => console.log('B:', v)); // B: 0.87 — fresh value each time
```

### Common Pattern — Lazy HTTP Requests
```typescript
import { defer } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Define the request lazily — no HTTP fired until subscribe()
function getUser(id: number): Observable<User> {
  return defer(() => ajax.getJSON<User>(`/api/users/${id}`));
}

// HTTP only fires here:
getUser(1).subscribe(console.log);
// Can safely pass getUser(1) around without triggering the request
```

### Common Pattern — Conditional Observable Selection
```typescript
import { defer } from 'rxjs';

// Each subscriber evaluates the condition fresh at subscription time
const data$ = defer(() =>
  authService.isAdmin()
    ? ajax.getJSON<AdminData>('/api/admin/data')
    : ajax.getJSON<UserData>('/api/user/data')
);

// If auth state changes between subscriptions, each gets the right data
data$.subscribe(renderData);
```

### Common Pattern — `concat` with Side Effects (from concat doc)
```typescript
import { defer, concat } from 'rxjs';

// Without defer: all factories called eagerly at concat() creation
// With defer: each factory called only when that source's turn comes
concat(
  defer(() => validateForm(formData)),    // called when concat starts
  defer(() => createUser(formData)),      // called only after validate completes
  defer(() => sendWelcomeEmail(userId))   // called only after createUser completes
).subscribe();
```

### Common Pattern — Retry with Fresh State
```typescript
import { defer } from 'rxjs';
import { retry } from 'rxjs/operators';

// Without defer: token captured once at creation — stale on retry
const request$ = ajax({
  url: '/api/data',
  headers: { Authorization: `Bearer ${getToken()}` } // token captured NOW
}).pipe(retry(3));

// With defer: getToken() called fresh on each retry attempt
const request$ = defer(() => ajax({
  url: '/api/data',
  headers: { Authorization: `Bearer ${getToken()}` } // token re-evaluated each retry
})).pipe(retry(3));
```

## Common Pitfalls

### Anti-pattern: Unnecessary `defer` Wrapping
```typescript
import { defer } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// ❌ REDUNDANT — ajax.getJSON() is already lazy (cold); defer adds nothing
const users$ = defer(() => ajax.getJSON('/api/users'));

// ✅ CORRECT — cold Observables are already lazy
const users$ = ajax.getJSON('/api/users');

// WHY: defer is only needed when the Observable creation itself has side effects
// that must be deferred, or when a value (like a token or condition) must be
// captured at subscription time rather than creation time.
// Cold Observables (ajax, interval, of, from) are already lazy — no defer needed.
```

### Anti-pattern: `defer` for Observables That Should Share a Single Execution
```typescript
import { defer, shareReplay } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// ❌ WRONG — defer makes every subscriber fire a new HTTP request
const config$ = defer(() => ajax.getJSON('/api/config'));
config$.subscribe(renderHeader);  // HTTP request 1
config$.subscribe(renderFooter);  // HTTP request 2 — duplicate!

// ✅ CORRECT — shareReplay for shared, cached execution
const config$ = ajax.getJSON('/api/config').pipe(shareReplay(1));
config$.subscribe(renderHeader);  // HTTP request fires once
config$.subscribe(renderFooter);  // uses cached result

// WHY: defer guarantees a fresh factory call per subscription — the opposite
// of sharing. For shared execution with caching, use shareReplay.
// Use defer for cases where each subscriber SHOULD get independent state.
```

## Related Operators

**Same Category (Creation)**:
- **`of`**: Eager synchronous emission — use when values are known at creation time
- **`from`**: Eager conversion from array/Promise/iterable — use for already-available inputs
- **`timer` / `interval`**: Already lazy (cold) — no defer needed
- **`new Observable(subscriber => {...})`**: Full custom Observable — use when defer's factory pattern isn't enough

**Complementary Operators**:
- **`concat`**: `defer` inside `concat` ensures each step's factory runs at the right time
- **`retry`**: `defer` ensures fresh values (tokens, timestamps) are captured on each retry
- **`shareReplay`**: The opposite of `defer` — share one execution vs. create one per subscriber

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/defer](https://rxjs.dev/api/index/function/defer)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/defer.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/defer.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: Lazy Observable Factory
**Cognitive Load**: 3/5 — The eager vs. lazy distinction requires careful teaching; the contrast with shareReplay is the critical anti-pattern
**Usage Frequency**: 4/5 — Essential for conditional Observable selection, retry with fresh state, and sequential side-effect chains with concat
**Common with**: `concat`, `retry`, `shareReplay` (contrast), `ajax`
