# iif

## Identity

- **Name**: iif
- **Category**: Creation Operators (Conditional)
- **Type**: Lazy conditional Observable selection — picks between two Observables based on a condition evaluated at subscription time
- **Import**:
  ```typescript
  import { iif } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function iif<T = never, F = never>(
    condition: () => boolean,
    trueResult: ObservableInput<T>,
    falseResult: ObservableInput<F>
  ): Observable<T | F>
  ```

## Functional Specification

**Concept**: `iif(condition, true$, false$)` is shorthand for `defer(() => condition() ? true$ : false$)`. The condition is evaluated lazily at each subscription. The winning Observable's emissions are forwarded to the subscriber.

**Key properties**:
- Condition is evaluated **at subscription time**, not at `iif()` call time — lazy like `defer`
- Both `trueResult` and `falseResult` are passed eagerly (they exist before the condition runs), but only one is subscribed to
- If `falseResult` is omitted, it defaults to `EMPTY`
- Re-evaluates condition on each new subscription — different subscribers can take different paths

**`iif` vs `defer(() => condition ? a$ : b$)`**:
- They are functionally equivalent
- `iif` is more readable for simple boolean conditions
- `defer` is more flexible for complex logic or when the chosen Observable needs to be freshly created

## Marble Diagram

```
iif(() => user.isAdmin, adminData$, publicData$):

Subscription at t=0, user.isAdmin = true:
Result mirrors adminData$:   --a--b--c--|

Subscription at t=1, user.isAdmin = false:
Result mirrors publicData$:  --x--y--|

Condition re-evaluated per subscription:
Sub A (isAdmin=true):   --adminA--adminB--|
Sub B (isAdmin=false):  --public1--public2--|

iif(() => true, of(1), of(2)):   1|
iif(() => false, of(1), of(2)):  2|
iif(() => false, of(1)):         |  (EMPTY when false and no falseResult)
```

## Type System Integration

```typescript
import { iif, of, EMPTY } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Output type is T | F
const result$ = iif(
  () => Math.random() > 0.5,
  of('heads'),    // T = string
  of('tails')     // F = string
);
// result$: Observable<string>

// With different types — union
const data$ = iif(
  () => isLoggedIn(),
  fetchUserData(),    // Observable<UserData>
  of(GUEST_DATA)      // Observable<GuestData>
);
// data$: Observable<UserData | GuestData>

// With EMPTY as false branch
const admin$ = iif(
  () => user.isAdmin,
  fetchAdminPanel()   // Observable<AdminPanel>
);
// admin$: Observable<AdminPanel>  (EMPTY when not admin — nothing emitted)
```

## Examples

### Basic Usage
```typescript
import { iif, of, EMPTY } from 'rxjs';

// Simple boolean gate
const greeting$ = iif(
  () => localStorage.getItem('name') !== null,
  of(`Hello, ${localStorage.getItem('name')}!`),
  of('Hello, stranger!')
);

greeting$.subscribe(console.log); // evaluates condition NOW (at subscribe time)

// With EMPTY as default
const adminContent$ = iif(
  () => currentUser.role === 'admin',
  fetchAdminData()
  // no falseResult: non-admins get EMPTY (nothing emitted, stream completes)
);
```

### Common Pattern — Conditional API Call
```typescript
import { iif, of } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Choose endpoint based on runtime condition
userAction$.pipe(
  switchMap(action =>
    iif(
      () => action.useCache,
      of(cache.get(action.key)),
      ajax.getJSON(`/api/${action.key}`)
    )
  )
).subscribe(handleResult);
```

### Common Pattern — Feature Flag Gating
```typescript
import { iif, NEVER } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// Feature flag evaluated fresh on each subscription
function featureStream$<T>(flag: string, stream$: Observable<T>): Observable<T> {
  return iif(
    () => featureFlags.isEnabled(flag),
    stream$,
    NEVER  // feature disabled → silent, never-completing placeholder
  );
}

featureStream$('new-dashboard', dashboardData$).subscribe(render);
```

### Common Pattern — Retry With Condition
```typescript
import { iif, throwError, timer } from 'rxjs';
import { retry, mergeMap } from 'rxjs/operators';

let retryCount = 0;

apiCall$.pipe(
  retry({
    count: 5,
    delay: (error, attempt) =>
      iif(
        () => error.status === 429,      // rate limited
        timer(5000),                      // wait 5s for rate limit errors
        timer(Math.pow(2, attempt) * 500) // exponential backoff otherwise
      )
  })
).subscribe(handleResult);
```

## Common Pitfalls

### Anti-pattern: Eagerly Evaluating the Condition Outside `iif`
```typescript
import { iif, of } from 'rxjs';

// ❌ WRONG — condition evaluated at iif() call time (eager), not subscription time
const isAdmin = user.isAdmin; // captured NOW
const data$ = iif(
  () => isAdmin,        // closure captures stale value
  of(adminData),
  of(publicData)
);

// If user.isAdmin changes between iif() and subscribe(), data$ uses the old value
user.isAdmin = true;    // changed after iif() was called
data$.subscribe(console.log); // still uses the old isAdmin = false!

// ✅ CORRECT — read the condition inside the function so it's fresh each time
const data$ = iif(
  () => user.isAdmin,   // read fresh at subscription time
  of(adminData),
  of(publicData)
);

user.isAdmin = true;
data$.subscribe(console.log); // now reads current user.isAdmin = true ✓

// WHY: The condition function is called at subscription time. If you capture
// a value outside the function, you lose the lazy evaluation benefit.
// Always read dynamic state INSIDE the condition function.
```

### Anti-pattern: Using `iif` for Complex Branching (Use `defer`)
```typescript
import { iif, of } from 'rxjs';

// ❌ HARD TO READ — iif with complex multi-condition logic
const result$ = iif(
  () => user.role === 'admin',
  iif(
    () => user.department === 'finance',
    financeAdminData$,
    regularAdminData$
  ),
  publicData$
);

// ✅ CLEARER — defer with a switch/if chain
import { defer } from 'rxjs';
const result$ = defer(() => {
  if (user.role === 'admin' && user.department === 'finance') return financeAdminData$;
  if (user.role === 'admin') return regularAdminData$;
  return publicData$;
});

// WHY: iif is for a single boolean branch (true/false). For multi-condition
// logic, nested iif calls become hard to read. Use defer with a plain
// if/switch statement for cleaner conditional Observable selection.
```

## Related Operators

- **`defer(factory)`**: The generalization of `iif` — any lazy Observable creation; use for complex conditions
- **`EMPTY`**: The implicit `falseResult` when omitted from `iif`
- **`NEVER`**: Use as `falseResult` when you want permanent silence (not completion) for the false branch
- **`filter`**: For filtering values within a stream (not for choosing between streams)
- **`switchMap`**: Often paired with `iif` inside a pipe to switch between sources on each emission

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/iif](https://rxjs.dev/api/index/function/iif)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching point**: `iif` is `defer(() => condition() ? a$ : b$)` — the condition is lazy (evaluated at subscription time, not at call time). Always read dynamic state INSIDE the condition function, never capture it outside.
**Teaching sequence**: After `defer` — iif is a specialized, readable shorthand for the most common defer pattern.
