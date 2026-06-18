# State Management with RxJS

Patterns for managing application state using RxJS primitives — without a full store library.

---

## The Core Pattern: `BehaviorSubject` as State Store

```typescript
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface AppState {
  user:    User | null;
  items:   Item[];
  loading: boolean;
  error:   string | null;
}

const initialState: AppState = {
  user: null, items: [], loading: false, error: null
};

class Store {
  private state$ = new BehaviorSubject<AppState>(initialState);

  // Read current state:
  get snapshot(): AppState { return this.state$.value; }

  // Subscribe to full state:
  select(): Observable<AppState>;
  // Subscribe to a slice (with auto-dedup):
  select<K extends keyof AppState>(key: K): Observable<AppState[K]>;
  select<K extends keyof AppState>(key?: K): Observable<any> {
    if (!key) return this.state$.asObservable();
    return this.state$.pipe(
      map(s => s[key]),
      distinctUntilChanged()
    );
  }

  // Update state (immutably):
  patch(partial: Partial<AppState>): void {
    this.state$.next({ ...this.state$.value, ...partial });
  }
}
```

---

## Scan-Based Reducer Pattern

`scan` is the RxJS equivalent of Redux `reduce` — accumulate state from actions.

```typescript
import { Subject, merge, scan, shareReplay } from 'rxjs';

// Actions:
const setUser$     = new Subject<User>();
const setItems$    = new Subject<Item[]>();
const setLoading$  = new Subject<boolean>();
const clearError$  = new Subject<void>();

// Reducer — maps actions to state transitions:
const state$ = merge(
  setUser$.pipe(map(user      => (s: AppState) => ({ ...s, user }))),
  setItems$.pipe(map(items    => (s: AppState) => ({ ...s, items }))),
  setLoading$.pipe(map(loading=> (s: AppState) => ({ ...s, loading }))),
  clearError$.pipe(map(()     => (s: AppState) => ({ ...s, error: null })))
).pipe(
  scan((state, reducer) => reducer(state), initialState),
  shareReplay(1) // all subscribers share state, late subscribers get current
);

// Usage:
state$.subscribe(state => renderApp(state));
setUser$.next(currentUser);   // triggers state update
setLoading$.next(true);
```

---

## Derived State (Computed Selectors)

```typescript
const state$ = /* ... store$ from above ... */;

// Derived streams — computed from state, update automatically:
const user$       = state$.pipe(map(s => s.user),  distinctUntilChanged());
const items$      = state$.pipe(map(s => s.items), distinctUntilChanged());
const itemCount$  = items$.pipe(map(items => items.length));
const hasItems$   = items$.pipe(map(items => items.length > 0));
const isAdmin$    = user$.pipe(map(user => user?.role === 'admin'));

// Combine multiple slices:
const dashboardVm$ = combineLatest({
  user:      user$,
  items:     items$,
  itemCount: itemCount$,
  isAdmin:   isAdmin$
});
```

---

## Optimistic Updates Pattern

Apply UI update immediately, roll back on API failure.

```typescript
class ItemStore {
  private items$ = new BehaviorSubject<Item[]>([]);
  readonly items = this.items$.asObservable();

  add(newItem: Item): void {
    const previous = this.items$.value;

    // 1. Optimistic update — instant UI feedback:
    this.items$.next([...previous, { ...newItem, pending: true }]);

    // 2. Persist to server:
    this.api.createItem(newItem).subscribe({
      next: savedItem => {
        // Replace optimistic with confirmed item:
        this.items$.next(
          this.items$.value.map(item =>
            item.id === newItem.id ? savedItem : item
          )
        );
      },
      error: () => {
        // Rollback on failure:
        this.items$.next(previous);
        this.notify.error('Failed to save item');
      }
    });
  }

  remove(id: string): void {
    const previous = this.items$.value;
    this.items$.next(previous.filter(i => i.id !== id)); // optimistic

    this.api.deleteItem(id).subscribe({
      error: () => this.items$.next(previous) // rollback
    });
  }
}
```

---

## Async Action Pattern (Loading + Error States)

```typescript
type AsyncState<T> =
  | { phase: 'idle' }
  | { phase: 'loading' }
  | { phase: 'success'; data: T }
  | { phase: 'error';   message: string };

class UserStore {
  private state$ = new BehaviorSubject<AsyncState<User>>({ phase: 'idle' });

  readonly user$    = this.state$.pipe(
    map(s => s.phase === 'success' ? s.data : null)
  );
  readonly loading$ = this.state$.pipe(map(s => s.phase === 'loading'));
  readonly error$   = this.state$.pipe(
    map(s => s.phase === 'error' ? s.message : null)
  );

  load(id: string): void {
    this.state$.next({ phase: 'loading' });
    this.api.getUser(id).subscribe({
      next:  data => this.state$.next({ phase: 'success', data }),
      error: err  => this.state$.next({ phase: 'error', message: err.message })
    });
  }
}
```

---

## Entity Collection Pattern

```typescript
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface EntityState<T extends { id: string }> {
  ids:      string[];
  entities: Record<string, T>;
  loading:  boolean;
}

class EntityStore<T extends { id: string }> {
  private state$ = new BehaviorSubject<EntityState<T>>({
    ids: [], entities: {}, loading: false
  });

  readonly all$    = this.state$.pipe(
    map(s => s.ids.map(id => s.entities[id]))
  );
  readonly count$  = this.state$.pipe(map(s => s.ids.length));

  selectById(id: string): Observable<T | undefined> {
    return this.state$.pipe(
      map(s => s.entities[id]),
      distinctUntilChanged()
    );
  }

  upsertMany(items: T[]): void {
    const current = this.state$.value;
    const newEntities = items.reduce((acc, item) => {
      acc[item.id] = item;
      return acc;
    }, { ...current.entities });
    const newIds = Array.from(new Set([...current.ids, ...items.map(i => i.id)]));
    this.state$.next({ ...current, ids: newIds, entities: newEntities });
  }

  removeOne(id: string): void {
    const current = this.state$.value;
    const { [id]: _removed, ...remaining } = current.entities;
    this.state$.next({
      ...current,
      ids:      current.ids.filter(i => i !== id),
      entities: remaining
    });
  }
}
```

---

## Undo/Redo with History

```typescript
import { scan, map, Subject, merge } from 'rxjs';

interface HistoryState<T> {
  past:    T[];
  present: T;
  future:  T[];
}

const action$  = new Subject<T>();
const undo$    = new Subject<void>();
const redo$    = new Subject<void>();

const history$ = merge(
  action$.pipe(map(next  => (h: HistoryState<T>) => ({
    past:    [...h.past, h.present],
    present: next,
    future:  []
  }))),
  undo$.pipe(map(()      => (h: HistoryState<T>) => h.past.length === 0 ? h : ({
    past:    h.past.slice(0, -1),
    present: h.past[h.past.length - 1],
    future:  [h.present, ...h.future]
  }))),
  redo$.pipe(map(()      => (h: HistoryState<T>) => h.future.length === 0 ? h : ({
    past:    [...h.past, h.present],
    present: h.future[0],
    future:  h.future.slice(1)
  })))
).pipe(
  scan((history, reducer) => reducer(history), {
    past: [], present: initialState, future: []
  })
);

const canUndo$ = history$.pipe(map(h => h.past.length > 0));
const canRedo$ = history$.pipe(map(h => h.future.length > 0));
const state$   = history$.pipe(map(h => h.present));
```

---

## When to Use RxJS State vs NgRx/Redux

| Use RxJS directly | Use NgRx/Redux |
|---|---|
| Single service / feature scope | App-wide shared state |
| Simple CRUD with optimistic UI | Complex action flows / effects |
| No devtools requirement | Need time-travel debugging |
| Team familiar with RxJS | Large team, need conventions |
| < 10 state slices | Many interconnected state slices |

---

## Key Principles

1. **Immutable updates** — always spread (`{ ...state, key: newValue }`), never mutate in-place
2. **`distinctUntilChanged` on selectors** — prevent downstream re-renders when value hasn't changed
3. **`shareReplay(1)` on state** — late subscribers get current state immediately
4. **`asObservable()`** — expose read-only Observables from services; keep `Subject` private
5. **Derive, don't duplicate** — computed values (`itemCount$`, `hasItems$`) should be derived from source state, not stored separately
