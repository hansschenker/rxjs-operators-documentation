# Build a Reactive Store from Scratch

A mini-Redux implementation using `scan`, `BehaviorSubject`, and selectors — demonstrating that the core of state management is just RxJS.

---

## The Core Idea

Redux's three principles map directly to RxJS primitives:

| Redux concept | RxJS primitive |
|---|---|
| Store (holds state) | `BehaviorSubject<State>` |
| Dispatch (send action) | `subject.next(action)` |
| Reducer (state → state) | `scan((state, action) => newState)` |
| Selector (derive state) | `pipe(map(selectSlice), distinctUntilChanged())` |
| Effect (async action) | `Observable` that dispatches more actions |

---

## Step 1: Types

```typescript
// Action — discriminated union for type safety
type Action =
  | { type: 'INCREMENT'; payload?: number }
  | { type: 'DECREMENT'; payload?: number }
  | { type: 'RESET' }
  | { type: 'SET_USER'; payload: User }
  | { type: 'CLEAR_USER' }
  | { type: 'SET_LOADING'; payload: boolean };

// State shape
interface AppState {
  counter: number;
  user: User | null;
  loading: boolean;
}

const INITIAL_STATE: AppState = {
  counter: 0,
  user: null,
  loading: false,
};
```

---

## Step 2: Reducer

```typescript
function rootReducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'INCREMENT':
      return { ...state, counter: state.counter + (action.payload ?? 1) };
    case 'DECREMENT':
      return { ...state, counter: state.counter - (action.payload ?? 1) };
    case 'RESET':
      return { ...state, counter: 0 };
    case 'SET_USER':
      return { ...state, user: action.payload };
    case 'CLEAR_USER':
      return { ...state, user: null };
    case 'SET_LOADING':
      return { ...state, loading: action.payload };
    default:
      return state;
  }
}
```

---

## Step 3: The Store

```typescript
import { BehaviorSubject, Observable, Subject } from 'rxjs';
import { scan, startWith, distinctUntilChanged, map, shareReplay } from 'rxjs/operators';

class Store<S, A> {
  private actions$ = new Subject<A>();
  private state$: Observable<S>;
  private stateSnapshot: S;

  constructor(reducer: (state: S, action: A) => S, initialState: S) {
    this.stateSnapshot = initialState;

    this.state$ = this.actions$.pipe(
      startWith(null as unknown as A),
      scan((state, action) => {
        if (action === null) return state; // skip the startWith null
        const next = reducer(state, action);
        this.stateSnapshot = next;
        return next;
      }, initialState),
      shareReplay(1), // late subscribers get current state
    );

    // Keep state hot — always running
    this.state$.subscribe();
  }

  dispatch(action: A): void {
    this.actions$.next(action);
  }

  select<R>(selector: (state: S) => R): Observable<R> {
    return this.state$.pipe(
      map(selector),
      distinctUntilChanged(), // only emit when selected slice changes
    );
  }

  getSnapshot(): S {
    return this.stateSnapshot;
  }

  get stream$(): Observable<S> {
    return this.state$;
  }
}

// Instantiate
const store = new Store<AppState, Action>(rootReducer, INITIAL_STATE);
```

---

## Step 4: Selectors

Selectors are pure functions that derive values from state. Compose them for memoized slices.

```typescript
// Primitive selectors
const selectCounter   = (s: AppState) => s.counter;
const selectUser      = (s: AppState) => s.user;
const selectLoading   = (s: AppState) => s.loading;

// Derived selectors (composed)
const selectIsLoggedIn  = (s: AppState) => s.user !== null;
const selectDisplayName = (s: AppState) => s.user?.name ?? 'Guest';
const selectUserInitials = (s: AppState) => {
  const name = s.user?.name ?? '';
  return name.split(' ').map(n => n[0]).join('').toUpperCase();
};

// Usage — each emits only when its slice changes
const counter$ = store.select(selectCounter);
const user$    = store.select(selectUser);
const isLoggedIn$ = store.select(selectIsLoggedIn);

counter$.subscribe(count => console.log('counter:', count));
isLoggedIn$.subscribe(loggedIn => updateNavbar(loggedIn));
```

---

## Step 5: Effects

Effects handle async operations — they listen to actions, perform side effects, and dispatch result actions.

```typescript
import { Observable, merge } from 'rxjs';
import { filter, switchMap, map, catchError, startWith } from 'rxjs/operators';

type ActionStream = Observable<Action>;

// Effect: load user on LOGIN action
function loadUserEffect(actions$: Observable<Action>): ActionStream {
  return actions$.pipe(
    filter((a): a is { type: 'SET_LOADING'; payload: boolean } =>
      a.type === 'SET_LOADING' && a.payload === true
    ),
    // treat SET_LOADING:true as a trigger to fetch user
  );
}

// More realistic: dedicated LOGIN action
type ExtendedAction = Action | { type: 'LOGIN'; payload: { username: string; password: string } };

function authEffect(
  actions$: Observable<ExtendedAction>,
  authService: AuthService,
): Observable<Action> {
  return actions$.pipe(
    filter((a): a is { type: 'LOGIN'; payload: { username: string; password: string } } =>
      a.type === 'LOGIN'
    ),
    switchMap(({ payload }) =>
      authService.login(payload.username, payload.password).pipe(
        switchMap(user => [
          { type: 'SET_USER' as const, payload: user },
          { type: 'SET_LOADING' as const, payload: false },
        ]),
        startWith({ type: 'SET_LOADING' as const, payload: true }),
        catchError(err => [
          { type: 'SET_LOADING' as const, payload: false },
          { type: 'CLEAR_USER' as const },
        ]),
      )
    ),
  );
}
```

---

## Step 6: Wiring Effects into the Store

```typescript
class StoreWithEffects<S, A extends { type: string }> extends Store<S, A> {
  private effectActions$ = new Subject<A>();

  constructor(reducer: (s: S, a: A) => S, initialState: S) {
    super(reducer, initialState);
    // Merge effect-dispatched actions into the main action stream
    this.effectActions$.subscribe(action => this.dispatch(action));
  }

  addEffect(
    effectFn: (actions$: Observable<A>, state$: Observable<S>) => Observable<A>
  ): void {
    effectFn(this.actions$, this.stream$).subscribe(action =>
      this.effectActions$.next(action)
    );
  }
}

// Usage
const store = new StoreWithEffects<AppState, Action>(rootReducer, INITIAL_STATE);

store.addEffect((actions$) => authEffect(actions$, authService));
store.addEffect((actions$, state$) => {
  // Effect that reads current state
  return actions$.pipe(
    filter(a => a.type === 'RESET'),
    withLatestFrom(state$),
    switchMap(([_, state]) => {
      console.log('Reset from state:', state);
      return EMPTY;
    }),
  );
});
```

---

## Step 7: Angular Integration

```typescript
import { Injectable } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';

@Injectable({ providedIn: 'root' })
export class AppStore extends StoreWithEffects<AppState, Action> {
  constructor(authService: AuthService) {
    super(rootReducer, INITIAL_STATE);
    this.addEffect(actions$ => authEffect(actions$, authService));
  }

  // Convenience signal accessors for zoneless Angular
  readonly counter  = toSignal(this.select(selectCounter),   { initialValue: 0 });
  readonly user     = toSignal(this.select(selectUser),      { initialValue: null });
  readonly loading  = toSignal(this.select(selectLoading),   { initialValue: false });
  readonly isLoggedIn = toSignal(this.select(selectIsLoggedIn), { initialValue: false });
}

// Component usage
@Component({
  template: `
    <p>Count: {{ store.counter() }}</p>
    <p>User: {{ store.user()?.name ?? 'Guest' }}</p>
    <button (click)="store.dispatch({ type: 'INCREMENT' })">+</button>
  `,
})
export class CounterComponent {
  store = inject(AppStore);
}
```

---

## Devtools Integration

Add time-travel debugging by recording action/state history.

```typescript
class DevStore<S, A extends { type: string }> extends StoreWithEffects<S, A> {
  readonly history: Array<{ action: A; state: S }> = [];

  constructor(reducer: (s: S, a: A) => S, initialState: S) {
    // Wrap reducer to record history
    const recordingReducer = (state: S, action: A) => {
      const next = reducer(state, action);
      this.history.push({ action, state: next });
      return next;
    };
    super(recordingReducer, initialState);
  }

  // Time-travel: replay history up to index
  replayTo(index: number): void {
    const target = this.history[index];
    if (target) {
      // Emit state directly (bypasses reducer)
      this.dispatch({ type: '__REPLAY__', payload: target.state } as unknown as A);
    }
  }
}
```

---

## When to Build vs Use a Library

| Scenario | Recommendation |
|---|---|
| Learning / small app | Build from scratch — understand the primitives |
| Medium app, no framework | This pattern — ~100 lines, no dependencies |
| Angular app | NgRx or ComponentStore — devtools, ecosystem |
| React app | Redux Toolkit or Zustand — better ecosystem |
| Complex async workflows | NgRx Effects or Redux-Observable — mature patterns |

---

## Related Guides

- **[NgRx Effects Patterns](./ngrx-effects-patterns.md)** — production-grade effects
- **[NgRx ComponentStore](./rxjs-ngrx-component-store.md)** — local component state
- **[State Management Patterns](./state-management-patterns.md)** — broader state overview
- **[BehaviorSubject](../operators-claude/subject/BehaviorSubject.md)** — the core primitive
