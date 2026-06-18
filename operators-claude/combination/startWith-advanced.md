# startWith — Advanced Patterns

For `startWith` fundamentals see the core [startWith](./startWith) doc. This page covers initialization patterns, `scan` + `startWith` for stateful streams, loading states, and multi-value seeding.

---

## The Core Roles of `startWith`

`startWith` serves three distinct purposes in practice:

1. **Seed `combineLatest`** — prevent blocking when a source may not emit immediately
2. **Set initial state for `scan`** — equivalent to the seed value but observable
3. **Provide synchronous defaults** — give late subscribers something to render immediately

---

## Pattern 1: Seeding `combineLatest` (Most Common Use)

```typescript
import { combineLatest } from 'rxjs';
import { startWith } from 'rxjs/operators';

// Without startWith — vm$ blocks until BOTH sources emit:
combineLatest({
  user:    this.userService.currentUser$,    // emits async
  filter:  this.filterControl.valueChanges,  // only emits on change — never initially!
}).subscribe(render); // never fires until user types something

// ✅ With startWith — vm$ emits immediately when user$ emits:
combineLatest({
  user:   this.userService.currentUser$,
  filter: this.filterControl.valueChanges.pipe(startWith(''))  // default empty
}).subscribe(render); // fires as soon as user$ emits
```

---

## Pattern 2: `startWith` + `scan` = Stateful Stream

`startWith` provides the initial seed; `scan` accumulates changes on top of it:

```typescript
import { merge, Subject } from 'rxjs';
import { scan, startWith, map } from 'rxjs/operators';

interface TodoState {
  items: Todo[];
  filter: 'all' | 'active' | 'done';
  loading: boolean;
}

const INITIAL_STATE: TodoState = { items: [], filter: 'all', loading: true };

// Actions:
const add$    = new Subject<Todo>();
const remove$ = new Subject<string>();
const filter$ = new Subject<'all' | 'active' | 'done'>();
const loaded$ = new Subject<Todo[]>();

// State stream:
const state$ = merge(
  add$.pipe(   map(todo => (s: TodoState) => ({ ...s, items: [...s.items, todo] }))),
  remove$.pipe(map(id   => (s: TodoState) => ({ ...s, items: s.items.filter(i => i.id !== id) }))),
  filter$.pipe(map(f    => (s: TodoState) => ({ ...s, filter: f }))),
  loaded$.pipe(map(items => (s: TodoState) => ({ ...s, items, loading: false })))
).pipe(
  startWith(INITIAL_STATE),                   // seed with initial state
  scan((state, reducer) =>
    typeof reducer === 'function'
      ? reducer(state)
      : reducer,                              // startWith value passes through
    INITIAL_STATE
  )
);

state$.subscribe(renderTodos);
```

---

## Pattern 3: Loading State Initialization

```typescript
import { startWith, map, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

type LoadState<T> =
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string };

// Pattern: start loading, then transition to success or error:
const users$ = this.api.getUsers().pipe(
  map((data): LoadState<User[]>    => ({ status: 'success', data })),
  catchError((err): Observable<LoadState<User[]>> =>
    of({ status: 'error', message: err.message })
  ),
  startWith<LoadState<User[]>>({ status: 'loading' })
);

// Template reacts to all three states without any imperative code:
// @switch ((users$ | async)?.status) {
//   @case ('loading') { <spinner /> }
//   @case ('success') { <user-list [users]="..." /> }
//   @case ('error')   { <error-msg /> }
// }
```

---

## Pattern 4: Multi-Value Seeding

`startWith` accepts multiple values — they're emitted synchronously before the source:

```typescript
import { startWith } from 'rxjs/operators';
import { interval } from 'rxjs';

// Emit 0, 0, 0 synchronously, then start interval:
interval(1000).pipe(
  startWith(0, 0, 0) // three synchronous emissions: 0, 0, 0, then 0, 1, 2...
).subscribe(console.log);

// Practical use: pre-populate a chart with empty data points:
this.sensorData$.pipe(
  startWith(null, null, null, null, null) // show 5 empty points before data arrives
).subscribe(renderChart);
```

---

## Pattern 5: Async Initialization with `startWith`

Combine synchronous defaults with async data:

```typescript
import { startWith, switchMap, shareReplay } from 'rxjs/operators';

// Show cached data immediately, refresh in background:
readonly products$ = this.cache.getProducts().pipe(
  startWith([] as Product[]), // show empty list immediately (avoids spinner flash)
  switchMap(cached =>
    this.api.getProducts().pipe(
      startWith(cached) // while fetching, show cached
    )
  ),
  shareReplay(1)
);
```

---

## Pattern 6: Route Parameter with Default

```typescript
import { ActivatedRoute } from '@angular/router';
import { startWith, map, distinctUntilChanged } from 'rxjs/operators';

@Component({ ... })
export class ListComponent {
  readonly page$ = this.route.queryParams.pipe(
    map(params => Number(params['page']) || 1),
    startWith(1),              // default to page 1 before route emits
    distinctUntilChanged()
  );

  readonly items$ = this.page$.pipe(
    switchMap(page => this.api.getPage(page))
  );
}
```

---

## `startWith` vs `BehaviorSubject` — When to Use Each

```typescript
// startWith — for values that are known at pipe construction time:
source$.pipe(startWith(DEFAULT_VALUE))

// BehaviorSubject — when you need to:
// 1. Update the initial value imperatively
// 2. Read the current value synchronously (.getValue())
// 3. Have the initial value as a separate concern from the stream
const subject$ = new BehaviorSubject<string>('default');
subject$.next('updated'); // can update anytime
subject$.getValue();       // synchronous read
```

---

## `startWith` vs `defaultIfEmpty`

```typescript
// startWith — always emits the seed value FIRST, regardless of source:
of(1, 2, 3).pipe(startWith(0)).subscribe(console.log);
// 0, 1, 2, 3 — seed always prepended

// defaultIfEmpty — only emits if source completes without emitting:
EMPTY.pipe(defaultIfEmpty('fallback')).subscribe(console.log);
// 'fallback' — source was empty so default is used

of(1, 2, 3).pipe(defaultIfEmpty('fallback')).subscribe(console.log);
// 1, 2, 3 — source had values, no default needed
```

---

## Common Pitfalls

### `startWith` After `map` Loses Type Safety

```typescript
// ❌ TypeScript infers wrong type — startWith null widens the Observable type:
this.api.getUsers().pipe(
  map(users => users.filter(u => u.active)),
  startWith(null) // Observable<User[] | null> — often unintentional
)

// ✅ Explicit type annotation:
this.api.getUsers().pipe(
  map(users => users.filter(u => u.active)),
  startWith([] as User[]) // Observable<User[]>  ← keeps intended type
)
```

### Using `startWith` Instead of `BehaviorSubject` for Mutable State

```typescript
// ❌ startWith can't update its seed — the "initial value" is frozen
const count$ = of(1).pipe(startWith(0));
// To change the starting value, you'd need to recreate the whole stream

// ✅ BehaviorSubject for state that changes:
const count$ = new BehaviorSubject(0);
count$.next(1); // update anytime
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Primary use cases**: Seed `combineLatest` sources so they don't block, provide loading state before async data arrives, initialize `scan` state reactively. The `startWith` + `scan` + reducer pattern is a complete mini state management solution.
