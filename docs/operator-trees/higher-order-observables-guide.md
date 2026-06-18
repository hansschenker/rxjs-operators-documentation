# Higher-Order Observables Guide

A higher-order Observable is an Observable that emits Observables. Understanding them — and the four flattening strategies — is the most important conceptual leap in RxJS.

---

## What Is a Higher-Order Observable?

A normal Observable emits values: `Observable<number>`, `Observable<User>`, `Observable<string>`.

A higher-order Observable emits Observables: `Observable<Observable<User>>`.

```typescript
// Normal Observable:
const users$: Observable<User> = this.http.get<User[]>('/api/users');

// Higher-order Observable — emits Observables, not Users:
const userIds$: Observable<number>             = of(1, 2, 3);
const usersHOO$: Observable<Observable<User>>  = userIds$.pipe(
  map(id => this.http.get<User>(`/api/users/${id}`))  // map returns Observable
);
// usersHOO$ emits three Observables — you must flatten to get Users
```

Without flattening, `usersHOO$` is useless — you'd subscribe to Observables inside subscribe.

---

## The Four Flattening Operators

Flattening = subscribing to inner Observables and re-emitting their values in the outer stream.

```
Input stream:    --A--------B--------C--|

Inner A:         a1--a2--a3--|
Inner B:                b1--b2--|
Inner C:                        c1--|
```

Each strategy answers: **what do you do when a new inner Observable arrives?**

---

### `mergeMap` — Run All Concurrently (No Order Guarantee)

Subscribe to every inner Observable immediately. All run in parallel.

```
Input:   --A--------B--------C--|
Inner A: a1--a2--a3--|
Inner B:         b1--b2--|
Inner C:                 c1--|

Result:  --a1-a2-b1-a3-b2-c1--|
         (order depends on timing)
```

```typescript
import { mergeMap } from 'rxjs/operators';

// All requests fire immediately — fastest response wins order:
userIds$.pipe(
  mergeMap(id => this.http.get<User>(`/api/users/${id}`))
).subscribe(renderUser);
```

**Use when**: Order doesn't matter, maximize throughput. File uploads, parallel data enrichment.

---

### `concatMap` — Run Sequentially (FIFO Queue)

Queue inner Observables. Start next only when current completes.

```
Input:   --A--------B--------C--|
Inner A: a1--a2--a3--|
Inner B:              b1--b2--|     (waits for A to finish)
Inner C:                      c1--|  (waits for B to finish)

Result:  --a1-a2-a3-b1-b2-c1--|
         (order guaranteed)
```

```typescript
import { concatMap } from 'rxjs/operators';

// Requests execute in order, one at a time:
userIds$.pipe(
  concatMap(id => this.http.get<User>(`/api/users/${id}`))
).subscribe(renderUser);
```

**Use when**: Order matters. Save operations, audit logs, sequential animations.

---

### `switchMap` — Cancel Previous, Use Latest

Subscribe to new inner Observable, **cancel** (unsubscribe) the previous one.

```
Input:   --A--------B--------C--|
Inner A: a1--a2--×  (cancelled when B arrives)
Inner B:         b1--×       (cancelled when C arrives)
Inner C:                 c1--|

Result:  --a1-a2-b1-c1--|
         (only latest survives)
```

```typescript
import { switchMap } from 'rxjs/operators';

// Each new search cancels the previous request:
searchInput$.pipe(
  debounceTime(300),
  switchMap(query => this.api.search(query))
).subscribe(renderResults);
```

**Use when**: Only the latest matters. Search, autocomplete, route data loading, live filters.

---

### `exhaustMap` — Ignore New While Busy

When an inner Observable is running, **ignore** new outer values until it completes.

```
Input:   --A--------B--------C--|
                    B ignored (A still running)
                             C subscribed (A done)
Inner A: a1--a2--a3--|
Inner C:              c1--|

Result:  --a1-a2-a3-c1--|
         (B dropped entirely)
```

```typescript
import { exhaustMap } from 'rxjs/operators';

// Second click ignored while first submission is in-flight:
submitBtn$.pipe(
  exhaustMap(() => this.api.submitForm(formData))
).subscribe(handleSuccess);
```

**Use when**: Prevent duplicate submissions. Login button, form submit, expensive triggers.

---

## Decision Table

| Strategy | New arrives while busy | Guarantees order? | Cancel previous? | Use case |
|---|---|---|---|---|
| `mergeMap` | Runs concurrently | No | No | Parallel, throughput |
| `concatMap` | Queues (waits) | Yes | No | Sequential, ordered |
| `switchMap` | Cancels old, starts new | No | Yes | Latest-only (search) |
| `exhaustMap` | Dropped/ignored | N/A | No | Prevent duplicates |

---

## The "Which One?" Decision Tree

```
Does order matter?
├── Yes → concatMap (queue)
└── No →
    Is only the latest result relevant?
    ├── Yes → switchMap (cancel)
    └── No →
        Should duplicate triggers be ignored?
        ├── Yes → exhaustMap (drop)
        └── No  → mergeMap (parallel)
```

Quick heuristic by operation type:
- **HTTP GET (search, autocomplete)** → `switchMap`
- **HTTP POST (save, submit)** → `exhaustMap` (no duplicates) or `concatMap` (queue)
- **Multiple independent GETs** → `mergeMap`
- **Sequential writes (audit, animation)** → `concatMap`

---

## Creating Higher-Order Observables

Three ways an HOO appears in practice:

### 1. `map` returning an Observable

```typescript
// ❌ This is the bug: map returns Observable<Observable<User>>
ids$.pipe(
  map(id => this.http.get<User>(`/api/users/${id}`))
  // subscribe gets Observables, not Users
)

// ✅ Replace map with a flattening operator:
ids$.pipe(
  mergeMap(id => this.http.get<User>(`/api/users/${id}`))
)
```

### 2. `of(observable$)` — wrapping in `of`

```typescript
// Produces Observable<Observable<T>>:
const wrapped$: Observable<Observable<T>> = of(innerStream$);

// Flatten with mergeAll, concatAll, etc.:
wrapped$.pipe(mergeAll()).subscribe(render);
```

### 3. `Subject` that emits Observables

```typescript
const requests$ = new Subject<Observable<Response>>();

// Somewhere:
requests$.next(this.api.getData());

// Flatten all emitted Observables:
requests$.pipe(mergeAll()).subscribe(handleResponse);
```

---

## The `*All` Operators

When you already have an `Observable<Observable<T>>`, the `*All` operators flatten it:

```typescript
import { mergeAll, concatAll, switchAll, exhaustAll } from 'rxjs/operators';

// These are equivalent:
source$.pipe(mergeMap(fn))     ===   source$.pipe(map(fn), mergeAll())
source$.pipe(concatMap(fn))    ===   source$.pipe(map(fn), concatAll())
source$.pipe(switchMap(fn))    ===   source$.pipe(map(fn), switchAll())
source$.pipe(exhaustMap(fn))   ===   source$.pipe(map(fn), exhaustAll())
```

The `*Map` form is almost always cleaner. Use `*All` when the inner Observable is produced by external code.

---

## Concurrency Limit with `mergeMap`

`mergeMap` accepts an optional second argument: max concurrent subscriptions:

```typescript
// At most 3 concurrent requests at a time:
largeIdList$.pipe(
  mergeMap(id => this.api.fetch(id), 3)
).subscribe(render);

// Equivalent longhand:
largeIdList$.pipe(
  mergeMap(id => this.api.fetch(id), 3)
)
// IDs 4, 5, 6, ... queue until one of the 3 in-flight completes
```

Use this to avoid overwhelming an API or saturating a connection pool.

---

## Nested Higher-Order Observables

Sometimes the inner Observable itself needs flattening:

```typescript
// Load all users, then for each user load their permissions:
this.api.getUsers().pipe(
  switchMap(users =>
    // forkJoin runs concurrently for all users:
    forkJoin(
      users.map(user =>
        this.api.getPermissions(user.id).pipe(
          map(perms => ({ ...user, permissions: perms }))
        )
      )
    )
  )
).subscribe(usersWithPerms => renderTable(usersWithPerms));
```

---

## The #1 Mistake: `subscribe` Inside `subscribe`

This is the sign that you need a flattening operator:

```typescript
// ❌ Classic nested subscribe — creates subscription leak, hard to cancel:
outer$.subscribe(value => {
  inner$(value).subscribe(result => {
    render(result); // no unsubscription, no error handling
  });
});

// ✅ Use a flattening operator:
outer$.pipe(
  switchMap(value => inner$(value))
).subscribe(render);
// Automatically cancels inner subscriptions, handles errors, composable
```

---

## Common Pitfalls

### Choosing `mergeMap` by Default

```typescript
// ❌ Using mergeMap everywhere — misses important semantics:
searchInput$.pipe(mergeMap(q => this.api.search(q)))
// Problem: slow request from 3 keystrokes ago can overwrite fast recent result

// ✅ Use the semantically correct operator:
searchInput$.pipe(switchMap(q => this.api.search(q))) // cancel stale requests
```

### `switchMap` for Write Operations

```typescript
// ❌ switchMap for form submission — cancels in-flight save!
saveBtn$.pipe(
  switchMap(() => this.api.save(data))
)
// If user clicks twice quickly, first save is cancelled — data may not be saved!

// ✅ exhaustMap prevents duplicate submissions:
saveBtn$.pipe(
  exhaustMap(() => this.api.save(data))
)
```

### Memory Leak from `mergeMap` on Infinite Inner Streams

```typescript
// ❌ mergeMap never unsubscribes inner Observable:
triggers$.pipe(
  mergeMap(() => interval(1000)) // never completes — leaks accumulate!
)

// ✅ Limit inner lifetime:
triggers$.pipe(
  mergeMap(() => interval(1000).pipe(take(10)))  // complete after 10 values
)
// Or use switchMap if only latest matters:
triggers$.pipe(
  switchMap(() => interval(1000)) // previous interval cancelled on new trigger
)
```
