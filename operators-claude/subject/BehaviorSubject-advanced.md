# BehaviorSubject — Advanced Patterns

For `BehaviorSubject` fundamentals see the core [BehaviorSubject](./BehaviorSubject) doc. This page covers state management patterns, derived state, slices, and `BehaviorSubject` vs signals vs `scan`.

---

## `BehaviorSubject` as a State Container

`BehaviorSubject` is the simplest possible state container — it holds a current value and emits it to new subscribers:

```typescript
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface AppState {
  user:     User | null;
  loading:  boolean;
  error:    string | null;
  items:    Item[];
}

const initialState: AppState = { user: null, loading: false, error: null, items: [] };

class StateStore {
  private state$ = new BehaviorSubject<AppState>(initialState);

  // Read:
  readonly snapshot = () => this.state$.getValue();
  readonly stream$  = this.state$.asObservable();

  // Write:
  patch(partial: Partial<AppState>): void {
    this.state$.next({ ...this.snapshot(), ...partial });
  }

  // Update with function (safe for derived values):
  update(fn: (state: AppState) => AppState): void {
    this.state$.next(fn(this.snapshot()));
  }
}
```

---

## Pattern 1: Derived State (Selectors)

```typescript
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

class UserStore {
  private state$ = new BehaviorSubject<UserState>(initialState);

  // Slice — select a single property:
  readonly user$    = this.select(s => s.user);
  readonly loading$ = this.select(s => s.loading);
  readonly error$   = this.select(s => s.error);

  // Derived — computed from multiple properties:
  readonly isLoggedIn$ = this.select(s => s.user !== null);
  readonly displayName$ = this.select(s =>
    s.user ? `${s.user.firstName} ${s.user.lastName}` : 'Guest'
  );
  readonly canEdit$ = this.select(s =>
    s.user?.role === 'admin' || s.user?.role === 'editor'
  );

  // select() helper — re-emits only when projected value changes:
  private select<T>(projector: (s: UserState) => T): Observable<T> {
    return this.state$.pipe(
      map(projector),
      distinctUntilChanged()
    );
  }
}
```

---

## Pattern 2: Immutable Array Operations

```typescript
class TodoStore {
  private state$ = new BehaviorSubject<Todo[]>([]);
  readonly todos$ = this.state$.asObservable();

  add(todo: Omit<Todo, 'id'>): void {
    this.state$.next([
      ...this.state$.getValue(),
      { ...todo, id: crypto.randomUUID() }
    ]);
  }

  remove(id: string): void {
    this.state$.next(
      this.state$.getValue().filter(t => t.id !== id)
    );
  }

  toggle(id: string): void {
    this.state$.next(
      this.state$.getValue().map(t =>
        t.id === id ? { ...t, done: !t.done } : t
      )
    );
  }

  update(id: string, changes: Partial<Todo>): void {
    this.state$.next(
      this.state$.getValue().map(t =>
        t.id === id ? { ...t, ...changes } : t
      )
    );
  }

  // Derived:
  readonly pending$  = this.todos$.pipe(map(ts => ts.filter(t => !t.done)));
  readonly done$     = this.todos$.pipe(map(ts => ts.filter(t => t.done)));
  readonly count$    = this.todos$.pipe(map(ts => ts.length));
}
```

---

## Pattern 3: Async State Machine

Model loading/error/success states:

```typescript
type AsyncState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error';   error: string };

class AsyncStore<T> {
  private state$ = new BehaviorSubject<AsyncState<T>>({ status: 'idle' });

  readonly state = this.state$.asObservable();
  readonly data$ = this.state$.pipe(
    filter((s): s is Extract<AsyncState<T>, { status: 'success' }> =>
      s.status === 'success'
    ),
    map(s => s.data)
  );
  readonly loading$ = this.state$.pipe(map(s => s.status === 'loading'));
  readonly error$   = this.state$.pipe(
    map(s => s.status === 'error' ? s.error : null)
  );

  load(fetch$: Observable<T>): void {
    this.state$.next({ status: 'loading' });
    fetch$.subscribe({
      next:  data  => this.state$.next({ status: 'success', data }),
      error: err   => this.state$.next({ status: 'error', error: err.message })
    });
  }

  reset(): void { this.state$.next({ status: 'idle' }); }
}
```

---

## Pattern 4: Multi-Slice Store (Mini NgRx)

```typescript
interface RootState {
  auth:    AuthState;
  cart:    CartState;
  catalog: CatalogState;
}

class RootStore {
  private state$ = new BehaviorSubject<RootState>(initialRootState);

  // Typed slice selectors:
  readonly auth$    = this.slice('auth');
  readonly cart$    = this.slice('cart');
  readonly catalog$ = this.slice('catalog');

  private slice<K extends keyof RootState>(key: K): Observable<RootState[K]> {
    return this.state$.pipe(
      map(s => s[key]),
      distinctUntilChanged()
    );
  }

  // Typed slice updates:
  updateAuth(fn: (s: AuthState) => AuthState): void {
    this.state$.next({
      ...this.state$.getValue(),
      auth: fn(this.state$.getValue().auth)
    });
  }

  updateCart(fn: (s: CartState) => CartState): void {
    this.state$.next({
      ...this.state$.getValue(),
      cart: fn(this.state$.getValue().cart)
    });
  }
}
```

---

## Pattern 5: Combining Multiple BehaviorSubjects

```typescript
import { combineLatest } from 'rxjs';

class SearchStore {
  readonly query$    = new BehaviorSubject('');
  readonly filters$  = new BehaviorSubject<Filter[]>([]);
  readonly sortBy$   = new BehaviorSubject<SortOption>('relevance');
  readonly page$     = new BehaviorSubject(1);

  // Combined view — recalculates when any input changes:
  readonly searchParams$ = combineLatest({
    query:   this.query$,
    filters: this.filters$,
    sortBy:  this.sortBy$,
    page:    this.page$
  });

  // Reset page when search criteria change:
  constructor() {
    merge(this.query$, this.filters$, this.sortBy$).pipe(
      skip(1),       // skip initial values
      distinctUntilChanged()
    ).subscribe(() => this.page$.next(1));
  }
}
```

---

## Pattern 6: Undo History

```typescript
class UndoableStore<T> {
  private history: T[] = [];
  private state$  = new BehaviorSubject<T>(initialState as T);

  readonly current$ = this.state$.asObservable();
  readonly canUndo$ = this.state$.pipe(map(() => this.history.length > 0));

  set(newState: T): void {
    this.history.push(this.state$.getValue()); // save before changing
    this.state$.next(newState);
  }

  undo(): void {
    const previous = this.history.pop();
    if (previous !== undefined) this.state$.next(previous);
  }

  clearHistory(): void { this.history = []; }
}
```

---

## `BehaviorSubject` vs `scan` vs Angular Signals

```typescript
// BehaviorSubject — imperative push, synchronous current value:
const count$ = new BehaviorSubject(0);
count$.next(count$.getValue() + 1); // increment
count$.getValue();                  // sync read: 1

// scan — event-driven state derived from stream:
const count$ = actions$.pipe(
  scan((count, _) => count + 1, 0)
);
// Can't set count directly — must push an action into actions$
// No sync .getValue() — async only

// Angular Signal — synchronous, fine-grained reactivity:
const count = signal(0);
count.set(1);
count.update(n => n + 1);
count();  // sync read
// Best for component-local state in Angular 17+
```

**Decision rule**:
- Component-local, synchronous: → Signal (Angular 17+)
- Service-level shared state, needs `.getValue()`: → `BehaviorSubject`
- State derived from event stream: → `scan`
- Complex state tree with actions: → NgRx/NGXS or scan-based reducer

---

## Common Pitfalls

### Mutating the Value In-Place

```typescript
// ❌ Direct mutation — subscribers won't re-emit:
const items$ = new BehaviorSubject<Item[]>([]);
items$.getValue().push(newItem);  // mutates array in place — no emission!

// ✅ Always replace with a new value:
items$.next([...items$.getValue(), newItem]); // new array → new emission
```

### Using `getValue()` Instead of `subscribe` for Reactive Derivations

```typescript
// ❌ getValue() in reactive context — not reactive — stale if state changes:
const isAdmin = userStore.state$.getValue().user?.role === 'admin';
// isAdmin is computed once and never updates

// ✅ Use pipe + map for reactive derivations:
const isAdmin$ = userStore.state$.pipe(
  map(s => s.user?.role === 'admin'),
  distinctUntilChanged()
);
```

### Not Exposing `asObservable()`

```typescript
// ❌ Exposing BehaviorSubject directly — callers can call .next():
@Injectable()
class Store {
  readonly state$ = new BehaviorSubject(initial); // anyone can state$.next(...)!
}

// ✅ Expose as read-only Observable:
@Injectable()
class Store {
  private _state$ = new BehaviorSubject(initial);
  readonly state$ = this._state$.asObservable(); // read-only
  update(fn: (s: State) => State) { this._state$.next(fn(this._state$.getValue())); }
}
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Core rule**: Always expose `asObservable()` externally, keep the `BehaviorSubject` private. Use `map` + `distinctUntilChanged()` for derived state (selectors). Never mutate in-place — always `.next(newValue)`. For synchronous reads in templates/handlers, `getValue()` is fine; for reactive derivations, use the Observable form.
