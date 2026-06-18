# map — Advanced Patterns

For `map` fundamentals see the core [map](./map) doc. This page covers type narrowing, computed property patterns, discriminated unions, and composing `map` with TypeScript generics.

---

## `map` as the Primary Type Transformer

Every value that flows through a pipeline should have a meaningful type at each stage. `map` is the operator that transforms both the value AND the TypeScript type:

```typescript
import { map } from 'rxjs/operators';

// Each map call shifts the type:
http.get<ApiResponse>('/api/users')   // Observable<ApiResponse>
  .pipe(
    map(res => res.data),             // Observable<User[]>
    map(users => users.filter(u => u.active)), // Observable<User[]>
    map(users => users.map(u => ({    // Observable<UserViewModel[]>
      id:          u.id,
      displayName: `${u.firstName} ${u.lastName}`,
      initials:    `${u.firstName[0]}${u.lastName[0]}`
    })))
  )
  .subscribe(vms => renderList(vms));
```

---

## Pattern 1: Narrowing Union Types

`map` with explicit return type narrows discriminated unions:

```typescript
import { map } from 'rxjs/operators';

type ApiResult<T> =
  | { status: 'ok';    data: T }
  | { status: 'error'; message: string };

// Without narrowing — still ApiResult<User>:
apiResult$.pipe(map(r => r))

// ✅ Narrow to just the data on success (combined with filter):
apiResult$.pipe(
  filter((r): r is { status: 'ok'; data: User } => r.status === 'ok'),
  map(r => r.data)  // Observable<User> — error case already filtered out
)

// ✅ Map to a normalized shape:
apiResult$.pipe(
  map(r => r.status === 'ok'
    ? { ok: true  as const, value: r.data }
    : { ok: false as const, error: r.message }
  )
)
```

---

## Pattern 2: View Model Transformation

Transform domain models to view models inside `map`:

```typescript
import { map } from 'rxjs/operators';

interface User { id: string; firstName: string; lastName: string; role: string; lastLogin: Date }

interface UserViewModel {
  id:          string;
  name:        string;
  initials:    string;
  role:        string;
  lastSeenAgo: string;
  isAdmin:     boolean;
}

function toUserViewModel(user: User): UserViewModel {
  const now = Date.now();
  const diff = now - user.lastLogin.getTime();
  const hours = Math.floor(diff / 3_600_000);

  return {
    id:          user.id,
    name:        `${user.firstName} ${user.lastName}`,
    initials:    `${user.firstName[0]}${user.lastName[0]}`.toUpperCase(),
    role:        user.role,
    lastSeenAgo: hours < 1 ? 'Just now' : hours < 24 ? `${hours}h ago` : `${Math.floor(hours / 24)}d ago`,
    isAdmin:     user.role === 'admin' || user.role === 'superadmin'
  };
}

users$.pipe(
  map(users => users.map(toUserViewModel)) // named function — testable independently
).subscribe(renderUserList);
```

---

## Pattern 3: Normalizing API Responses

Map inconsistent API shapes to a consistent internal model:

```typescript
import { map } from 'rxjs/operators';

// API v1 shape:
interface ApiV1User { user_id: string; full_name: string; email_address: string }

// API v2 shape:
interface ApiV2User { id: string; name: { first: string; last: string }; email: string }

// Internal model:
interface User { id: string; name: string; email: string }

function normalizeV1(u: ApiV1User): User {
  return { id: u.user_id, name: u.full_name, email: u.email_address };
}

function normalizeV2(u: ApiV2User): User {
  return { id: u.id, name: `${u.name.first} ${u.name.last}`, email: u.email };
}

// Both streams produce the same internal type:
const fromV1$ = v1Api.getUsers().pipe(map(res => res.users.map(normalizeV1)));
const fromV2$ = v2Api.getUsers().pipe(map(res => res.data.map(normalizeV2)));

merge(fromV1$, fromV2$).subscribe(renderUsers); // both typed as User[]
```

---

## Pattern 4: Computed Properties (Derived Fields)

Add computed fields to objects without mutation:

```typescript
import { map } from 'rxjs/operators';

interface Order { id: string; items: OrderItem[]; taxRate: number; discount: number }

orders$.pipe(
  map(orders => orders.map(order => {
    const subtotal  = order.items.reduce((s, i) => s + i.price * i.qty, 0);
    const discounted = subtotal * (1 - order.discount);
    const tax       = discounted * order.taxRate;
    return {
      ...order,          // preserve original fields
      subtotal,          // add computed fields
      discountAmount: subtotal - discounted,
      tax,
      total: discounted + tax
    };
  }))
).subscribe(renderOrders);
```

---

## Pattern 5: Conditional Transformation

Transform differently based on value content:

```typescript
import { map } from 'rxjs/operators';

type Event =
  | { type: 'click';   x: number; y: number }
  | { type: 'key';     code: string }
  | { type: 'resize';  width: number; height: number };

events$.pipe(
  map(event => {
    switch (event.type) {
      case 'click':  return { action: 'interact', position: { x: event.x, y: event.y } };
      case 'key':    return { action: 'input',    key: event.code };
      case 'resize': return { action: 'layout',   dimensions: { w: event.width, h: event.height } };
    }
  })
).subscribe(analytics.track);
```

---

## Pattern 6: Extracting Nested Values Safely

```typescript
import { map } from 'rxjs/operators';

interface DeepResponse { data: { user: { profile: { avatar: string } | null } | null } | null }

// Safe extraction with nullish coalescing:
response$.pipe(
  map(res => res.data?.user?.profile?.avatar ?? '/default-avatar.png')
).subscribe(setAvatarSrc);

// Or extract to a structured type:
response$.pipe(
  map(res => ({
    avatar:   res.data?.user?.profile?.avatar ?? '/default-avatar.png',
    hasProfile: res.data?.user?.profile != null
  }))
).subscribe(({ avatar, hasProfile }) => {
  setAvatarSrc(avatar);
  if (!hasProfile) showProfilePrompt();
});
```

---

## Pattern 7: `map` for Index-Aware Transformation

`map` receives `(value, index)` — use the index for position-aware transforms:

```typescript
import { map } from 'rxjs/operators';

// Add position metadata:
itemStream$.pipe(
  map((item, index) => ({
    ...item,
    position: index + 1,
    isFirst:  index === 0,
    isEven:   index % 2 === 0
  }))
).subscribe(renderItem);
```

---

## `map` vs `switchMap` — The Common Confusion

```typescript
// map — synchronous 1:1 transformation, stays Observable<T>:
users$.pipe(
  map(user => user.name)          // Observable<string> — no new subscription
)

// switchMap — returns a new Observable per value (for async operations):
userId$.pipe(
  switchMap(id => this.http.get<User>(`/api/users/${id}`)) // Observable<User>
)

// ❌ INCORRECT: returning Observable from map creates Observable<Observable<T>>:
userId$.pipe(
  map(id => this.http.get<User>(`/api/users/${id}`)) // Observable<Observable<User>>!
)
// WHY: map wraps its return value. switchMap/mergeMap/concatMap flatten it.
```

---

## Common Pitfalls

### Side Effects in `map`

```typescript
// ❌ Side effects in map — breaks referential transparency:
source$.pipe(
  map(item => {
    this.logger.log(item);   // side effect!
    this.counter++;          // side effect!
    return transform(item);
  })
)

// ✅ Side effects in tap, transformation in map:
source$.pipe(
  tap(item => this.logger.log(item)),
  tap(() => this.counter++),
  map(item => transform(item))
)
```

### Returning `undefined` from `map`

```typescript
// ❌ Forgetting return — all values become undefined:
source$.pipe(
  map(item => {
    const result = transform(item);
    // forgot: return result;
  }) // Observable<undefined>!
)

// ✅ Always return from map:
source$.pipe(
  map(item => transform(item)) // arrow function with implicit return
)
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key insight**: `map` is both a value transformer and a TypeScript type transformer. Every call shifts the Observable's type parameter. Named transformation functions (`toUserViewModel`, `normalizeV1`) keep pipelines readable and make transformation logic independently testable.
