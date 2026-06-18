# TypeScript Type Guards with RxJS

Narrowing Observable types at compile time — type predicates in `filter`, discriminated unions, type-safe event streams, and eliminating `as` casts from your pipelines.

---

## The Problem: `filter` Doesn't Narrow Types

```typescript
import { filter } from 'rxjs/operators';

const mixed$: Observable<string | null> = source$;

// ❌ filter() alone doesn't narrow — TypeScript still sees string | null:
mixed$.pipe(
  filter(v => v !== null)
).subscribe(v => {
  console.log(v.toUpperCase()); // Error: v is still string | null
});

// ✅ Use a type predicate (type guard) in filter:
mixed$.pipe(
  filter((v): v is string => v !== null)
).subscribe(v => {
  console.log(v.toUpperCase()); // ✓ v is string
});
```

The `(v): v is T` syntax is a **type predicate** — it tells TypeScript: "if this function returns true, then `v` is of type `T`."

---

## Pattern 1: Filtering Out `null` and `undefined`

The single most common type guard in RxJS pipelines:

```typescript
import { filter, map } from 'rxjs/operators';

// Reusable type-guard operators:
function filterNull<T>(): OperatorFunction<T | null, T> {
  return filter((v): v is T => v !== null);
}

function filterNullish<T>(): OperatorFunction<T | null | undefined, T> {
  return filter((v): v is T => v != null);
}

function filterDefined<T>(): OperatorFunction<T | undefined, T> {
  return filter((v): v is T => v !== undefined);
}

// Usage:
store.select(selectActiveUser).pipe(
  filterNullish(),         // Observable<User | null> → Observable<User>
  map(user => user.name),  // TypeScript knows user is User
  takeUntilDestroyed()
).subscribe(name => setTitle(name));

// Without the helper (verbose but equivalent):
store.select(selectActiveUser).pipe(
  filter((user): user is User => user !== null && user !== undefined),
  map(user => user.name)
).subscribe(setTitle);
```

---

## Pattern 2: Discriminated Union Narrowing

Discriminated unions are the backbone of typed event streams — narrow to a specific variant using `filter` with a type predicate:

```typescript
type AppEvent =
  | { type: 'USER_LOGIN';   userId: string; sessionId: string }
  | { type: 'USER_LOGOUT';  userId: string }
  | { type: 'ORDER_PLACED'; orderId: string; amount: number }
  | { type: 'ERROR';        code: number;   message: string };

// Type-safe event selector:
function ofType<T extends AppEvent, K extends T['type']>(
  ...types: K[]
): OperatorFunction<T, Extract<T, { type: K }>> {
  return filter(
    (event): event is Extract<T, { type: K }> => types.includes(event.type as K)
  );
}

const events$: Observable<AppEvent> = eventBus$;

// Narrowed — TypeScript knows the shape inside subscribe:
events$.pipe(ofType('USER_LOGIN')).subscribe(e => {
  console.log(e.userId, e.sessionId); // ✓ both properties exist
});

events$.pipe(ofType('ORDER_PLACED')).subscribe(e => {
  console.log(e.orderId, e.amount); // ✓ TypeScript knows this is ORDER_PLACED
});

events$.pipe(ofType('USER_LOGIN', 'USER_LOGOUT')).subscribe(e => {
  console.log(e.userId); // ✓ userId exists on both variants
  // e.sessionId would be a type error — it only exists on USER_LOGIN
});

// NgRx-style ofType for Actions:
import { ofType } from '@ngrx/effects';
actions$.pipe(ofType(OrderActions.loadOrdersSuccess)).subscribe(action => {
  console.log(action.orders); // ✓ typed as Order[]
});
```

---

## Pattern 3: Class Instance Guards

Narrow to a specific class when a stream emits a union of types:

```typescript
import { filter } from 'rxjs/operators';

class NetworkError extends Error { constructor(public statusCode: number, msg: string) { super(msg); } }
class ValidationError extends Error { constructor(public fields: string[], msg: string) { super(msg); } }
class TimeoutError extends Error {}

type AppError = NetworkError | ValidationError | TimeoutError;

const errors$: Observable<AppError> = errorStream$;

// instanceof type guard:
errors$.pipe(
  filter((e): e is NetworkError => e instanceof NetworkError)
).subscribe(e => {
  if (e.statusCode === 401) redirectToLogin();
  if (e.statusCode >= 500) showServerErrorBanner();
});

errors$.pipe(
  filter((e): e is ValidationError => e instanceof ValidationError)
).subscribe(e => {
  e.fields.forEach(field => markFieldInvalid(field));
});

// Reusable class guard operator:
function instanceOf<T>(cls: new (...args: any[]) => T): OperatorFunction<unknown, T> {
  return filter((v): v is T => v instanceof cls);
}

errors$.pipe(instanceOf(NetworkError)).subscribe(handleNetwork);
errors$.pipe(instanceOf(ValidationError)).subscribe(handleValidation);
```

---

## Pattern 4: Structural Type Guards

Guard based on the shape of an object when you don't have a discriminant property:

```typescript
interface SuccessResponse<T> { data: T;    error: null }
interface ErrorResponse      { data: null; error: string }
type ApiResponse<T> = SuccessResponse<T> | ErrorResponse;

// Structural type guard:
function isSuccess<T>(r: ApiResponse<T>): r is SuccessResponse<T> {
  return r.error === null && r.data !== null;
}

function isError<T>(r: ApiResponse<T>): r is ErrorResponse {
  return r.error !== null;
}

apiResponse$.pipe(
  filter(isSuccess)
).subscribe(r => {
  render(r.data); // ✓ r.data is T, not null
});

apiResponse$.pipe(
  filter(isError)
).subscribe(r => {
  showError(r.error); // ✓ r.error is string, not null
});

// Partition into two streams — each narrowed:
const [success$, error$] = partition(
  apiResponse$,
  (r): r is SuccessResponse<User> => isSuccess(r)
);

success$.subscribe(r => renderUser(r.data));
error$.subscribe(r => showError(r.error));
```

---

## Pattern 5: Array and Tuple Guards

Narrow typed arrays in stream emissions:

```typescript
import { filter, map } from 'rxjs/operators';

// Non-empty array guard:
function isNonEmpty<T>(arr: T[]): arr is [T, ...T[]] {
  return arr.length > 0;
}

searchResults$.pipe(
  filter(isNonEmpty),
  map(results => results[0]) // ✓ first element definitely exists
).subscribe(topResult => renderTopResult(topResult));

// Tuple guard — [key, value] pair from Map.entries():
function isStringEntry(entry: [unknown, unknown]): entry is [string, string] {
  return typeof entry[0] === 'string' && typeof entry[1] === 'string';
}

from(configMap.entries()).pipe(
  filter(isStringEntry),
  map(([key, value]) => ({ key, value })) // ✓ both typed as string
).subscribe(({ key, value }) => applyConfig(key, value));

// Guard for tuples from combineLatest:
type UserAndOrders = [User | null, Order[] | null];

combineLatest([user$, orders$]).pipe(
  filter((pair): pair is [User, Order[]] =>
    pair[0] !== null && pair[1] !== null
  )
).subscribe(([user, orders]) => {
  // ✓ both non-null here
  renderUserOrders(user, orders);
});
```

---

## Pattern 6: Generic `assertDefined` and `assertInstanceOf` Utilities

Build a small type-guard utility library for your project:

```typescript
import { OperatorFunction, filter } from 'rxjs';

// Assert non-null/undefined and narrow:
export function assertDefined<T>(
  message?: string
): OperatorFunction<T | null | undefined, T> {
  return source$ => source$.pipe(
    map(v => {
      if (v == null) throw new Error(message ?? 'Expected defined value, got null/undefined');
      return v;
    })
  );
}

// Filter (not assert) — silent on null:
export function isDefined<T>(): OperatorFunction<T | null | undefined, T> {
  return filter((v): v is T => v != null);
}

// Type-narrowing cast (use sparingly — you're asserting, not proving):
export function asType<T>(): OperatorFunction<unknown, T> {
  return map(v => v as T);
}

// Discriminant selector:
export function selectType<
  T extends { type: string },
  K extends T['type']
>(type: K): OperatorFunction<T, Extract<T, { type: K }>> {
  return filter((e): e is Extract<T, { type: K }> => e.type === type);
}

// Usage:
activeUser$.pipe(
  assertDefined('Active user must be set before this stream is created')
).subscribe(user => {
  // ✓ TypeScript knows user is User, not User | null
  // Runtime error thrown immediately if null — not a silent undefined access
  renderProfile(user);
});
```

---

## Pattern 7: Runtime Schema Validation as Type Guards

Integrate schema validation (Zod, Valibot) as Observable type guards:

```typescript
import { z } from 'zod';
import { filter, map, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

const UserSchema = z.object({
  id:    z.string().uuid(),
  email: z.string().email(),
  role:  z.enum(['admin', 'user', 'guest'])
});

type ValidatedUser = z.infer<typeof UserSchema>;

// Validate and narrow in one operator:
function zodFilter<T>(schema: z.ZodType<T>): OperatorFunction<unknown, T> {
  return source$ => source$.pipe(
    map(v => {
      const result = schema.safeParse(v);
      return result.success ? result.data : null;
    }),
    filter((v): v is T => v !== null)
  );
}

// Log invalid but continue stream:
function zodFilterWithLog<T>(
  schema: z.ZodType<T>,
  onInvalid?: (err: z.ZodError) => void
): OperatorFunction<unknown, T> {
  return source$ => source$.pipe(
    map(v => ({ result: schema.safeParse(v), raw: v })),
    tap(({ result, raw }) => {
      if (!result.success) onInvalid?.(result.error);
    }),
    filter(({ result }): result is { success: true; data: T } => result.success),
    map(({ result }) => result.data)
  );
}

// Usage — type-safe WebSocket messages:
webSocketMessages$.pipe(
  zodFilterWithLog(
    UserSchema,
    err => console.warn('Invalid user message:', err.format())
  )
).subscribe((user: ValidatedUser) => {
  // ✓ user is ValidatedUser — runtime-verified AND compile-time-typed
  console.log(user.role); // TypeScript knows this is 'admin' | 'user' | 'guest'
});
```

---

## Common Pitfalls

### Type Predicate That Lies

```typescript
// ❌ Incorrect predicate — says it's T but doesn't check thoroughly:
function isUser(v: unknown): v is User {
  return typeof v === 'object' && v !== null; // only checks "object-ish"
  // Doesn't verify id, name, email properties exist
}

stream$.pipe(filter(isUser)).subscribe(user => {
  user.email.toLowerCase(); // Runtime error if email is missing!
});

// ✅ Verify the shape you claim:
function isUser(v: unknown): v is User {
  return (
    typeof v === 'object' && v !== null &&
    typeof (v as any).id    === 'string' &&
    typeof (v as any).name  === 'string' &&
    typeof (v as any).email === 'string'
  );
}
// Or use a schema validator (Zod pattern above) for automatic sync
```

### Using `as` Instead of Type Guards

```typescript
// ❌ Casting with `as` — bypasses type safety, runtime errors possible:
stream$.pipe(
  map(v => v as User) // TypeScript trusts you — may crash if v isn't User
).subscribe(user => user.email.toLowerCase());

// ✅ Use type guards to narrow at the point of the check:
stream$.pipe(
  filter((v): v is User => isUser(v)) // verified before use
).subscribe(user => user.email.toLowerCase());
```

---

**Key insight**: Type guards transform `filter()` from a runtime-only operation into a compile-time narrowing step. The pattern `filter((v): v is T => condition)` is idiomatic TypeScript-RxJS — write a reusable `ofType`, `isDefined`, and `instanceOf` utility early in any project and the `as` cast count in your codebase will drop dramatically. When possible, couple runtime guards with schema validation (Zod/Valibot) so the type system reflects what you've actually verified, not what you've assumed.
