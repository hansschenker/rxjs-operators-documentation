# filter — Advanced Patterns

For `filter` fundamentals see the core [filter](./filter) doc. This page covers TypeScript type predicates, complex predicates, composable filter operators, and performance patterns.

---

## The Type Predicate Form — The Most Important `filter` Pattern

Without a type predicate, `filter` does not narrow the TypeScript type:

```typescript
import { filter } from 'rxjs/operators';

const obs$: Observable<string | null> = /* ... */;

// ❌ No type narrowing — result is still Observable<string | null>:
obs$.pipe(filter(x => x !== null)).subscribe(x => x.toUpperCase()); // ERROR!

// ✅ Type predicate narrows to Observable<string>:
obs$.pipe(
  filter((x): x is string => x !== null)
).subscribe(x => x.toUpperCase()); // x is string ✓
```

The syntax `(x): x is T` is a **type predicate** — it tells TypeScript that when the function returns `true`, the type is narrowed to `T`.

---

## Pattern 1: Filtering Null / Undefined

```typescript
import { filter } from 'rxjs/operators';

// The canonical non-null filter:
function filterNil<T>() {
  return filter((x: T | null | undefined): x is T => x != null);
}

// Usage:
const maybeUser$: Observable<User | null> = /* ... */;
const user$: Observable<User> = maybeUser$.pipe(filterNil());

// Inline form:
source$.pipe(
  filter((x): x is NonNullable<typeof x> => x != null)
)
```

---

## Pattern 2: Discriminated Union Filtering

```typescript
import { filter } from 'rxjs/operators';

type Action =
  | { type: 'USER_LOADED'; user: User }
  | { type: 'USER_ERROR'; error: string }
  | { type: 'USER_RESET' };

const actions$: Observable<Action> = /* ... */;

// Filter to a specific action type — fully typed:
const userLoaded$ = actions$.pipe(
  filter((a): a is Extract<Action, { type: 'USER_LOADED' }> => a.type === 'USER_LOADED')
);
// userLoaded$ is Observable<{ type: 'USER_LOADED'; user: User }>
// .user is available without further narrowing

// Reusable ofType operator (see TypeScript+RxJS guide):
function ofType<T extends { type: string }, K extends T['type']>(
  ...types: K[]
): OperatorFunction<T, Extract<T, { type: K }>> {
  return filter((a): a is Extract<T, { type: K }> => types.includes(a.type as K));
}

actions$.pipe(ofType('USER_LOADED')).subscribe(a => renderUser(a.user));
```

---

## Pattern 3: Filtering by Instance Type

```typescript
import { filter } from 'rxjs/operators';

class HttpError  extends Error { constructor(public status: number, msg: string) { super(msg); } }
class AuthError  extends Error {}
class ParseError extends Error {}

type AppError = HttpError | AuthError | ParseError;

const errors$: Observable<AppError> = /* ... */;

// Filter to specific error type:
const httpErrors$ = errors$.pipe(
  filter((e): e is HttpError => e instanceof HttpError)
);
// httpErrors$.status is available — HttpError specific property
```

---

## Pattern 4: Composite Predicates

Build readable complex conditions from small functions:

```typescript
import { filter } from 'rxjs/operators';

// Small, named predicate functions:
const isActive       = (u: User) => u.active;
const isAdmin        = (u: User) => u.role === 'admin';
const hasRecentLogin = (u: User) => Date.now() - u.lastLogin.getTime() < 7 * 86_400_000;
const isVerified     = (u: User) => u.emailVerified;

// Compose:
const and = <T>(...predicates: ((x: T) => boolean)[]) =>
  (x: T) => predicates.every(p => p(x));

const or = <T>(...predicates: ((x: T) => boolean)[]) =>
  (x: T) => predicates.some(p => p(x));

users$.pipe(
  filter(and(isActive, isVerified, or(isAdmin, hasRecentLogin)))
).subscribe(renderUser);
```

---

## Pattern 5: Filter with Side Effect (Rejected Value Logging)

```typescript
import { filter, tap, partition } from 'rxjs/operators';

// Log rejected values using partition:
const [valid$, invalid$] = partition(
  items$,
  item => item.value > 0 && item.category !== 'archived'
);

invalid$.subscribe(item => logger.warn('Filtered item:', item));
valid$.subscribe(processItem);

// Or with tap before filter:
items$.pipe(
  tap(item => {
    if (item.value <= 0) logger.debug(`Filtered: value=${item.value}`);
  }),
  filter(item => item.value > 0)
).subscribe(processItem);
```

---

## Pattern 6: Dynamic Predicate

Change the filter condition at runtime:

```typescript
import { BehaviorSubject, combineLatest } from 'rxjs';
import { switchMap, filter } from 'rxjs/operators';

const showInactive$ = new BehaviorSubject(false);

// Refilter when the toggle changes:
const visibleUsers$ = combineLatest({
  users:       allUsers$,
  showInactive: showInactive$
}).pipe(
  map(({ users, showInactive }) =>
    showInactive ? users : users.filter(u => u.active)
  )
);

// Toggle:
toggleButton.addEventListener('click', () => {
  showInactive$.next(!showInactive$.getValue());
});
```

---

## Pattern 7: `filter` as Circuit Breaker

Skip events when the system is in a bad state:

```typescript
import { filter, withLatestFrom } from 'rxjs/operators';

const systemReady$: Observable<boolean> = /* ... */;

// Only process events when system is ready:
userActions$.pipe(
  withLatestFrom(systemReady$),
  filter(([, ready]) => ready),     // gate on system state
  map(([action]) => action)         // unwrap to just the action
).subscribe(processAction);
```

---

## `filter` vs `first` vs `find` vs `take(1)`

```typescript
// filter — keeps ALL matching values (stream continues):
source$.pipe(filter(x => x > 5))          // emits every value > 5

// first(predicate) — keeps FIRST matching value, then completes:
source$.pipe(first(x => x > 5))           // emits first value > 5, then completes

// find — same as first(), different naming:
source$.pipe(find(x => x > 5))            // emits first match (or undefined on complete)

// take(1) after filter — equivalent to first():
source$.pipe(filter(x => x > 5), take(1)) // first match, then complete
```

---

## Common Pitfalls

### Boolean Coercion vs Explicit Null Check

```typescript
// ❌ Boolean coercion loses type narrowing AND has subtle falsy bugs:
source$.pipe(
  filter(Boolean) // filters 0, '', false, null, undefined — may not be intended!
)

// ✅ Explicit predicate with type narrowing:
source$.pipe(
  filter((x): x is NonNullable<typeof x> => x != null) // only null/undefined removed
)

// ✅ Or if you truly want all falsy removed:
source$.pipe(
  filter((x): x is Exclude<typeof x, null | undefined | false | 0 | ''> => Boolean(x))
)
```

### `filter` Does Not Short-Circuit Upstream

```typescript
// ❌ Misconception: filter stops the source from producing values
// Reality: source produces ALL values; filter just drops them before delivery

expensiveSource$.pipe(
  filter(x => x.type === 'important') // source still runs for ALL values
)

// ✅ If source production is expensive, filter at source if possible:
this.api.getItems({ type: 'important' }) // filter in the query
// Or use partition to handle both paths efficiently
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key insight**: `filter` is one of the most common operators, but its superpower is **TypeScript type narrowing via type predicates**. The pattern `filter((x): x is T => condition)` is the idiomatic way to narrow union types in Observable pipelines. Always use it when filtering null/undefined or discriminated union members.
