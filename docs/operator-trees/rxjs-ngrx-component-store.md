# NgRx ComponentStore Patterns with RxJS

NgRx ComponentStore is a lightweight, component-scoped state management solution built entirely on RxJS. This guide covers its core patterns, advanced compositions, and integration with the broader NgRx ecosystem.

---

## What ComponentStore Is (and Isn't)

ComponentStore manages **local component state** — state that lives and dies with a component tree, doesn't belong in the global store, but is too complex for simple component properties.

```typescript
import { ComponentStore } from '@ngrx/component-store';
import { Injectable } from '@angular/core';

interface MovieState {
  movies:  Movie[];
  loading: boolean;
  error:   string | null;
  filter:  string;
}

const initialState: MovieState = {
  movies:  [],
  loading: false,
  error:   null,
  filter:  ''
};

@Injectable()
class MovieStore extends ComponentStore<MovieState> {
  constructor() {
    super(initialState);
  }
}
```

ComponentStore exposes three primitives:
- **`select()`** — derive Observable slices of state
- **`updater()`** — synchronous state transitions
- **`effect()`** — async side-effects that can also update state

---

## Pattern 1: Selectors — Derived State

```typescript
@Injectable()
class MovieStore extends ComponentStore<MovieState> {
  // Basic selector:
  readonly movies$ = this.select(state => state.movies);
  readonly loading$ = this.select(state => state.loading);
  readonly filter$ = this.select(state => state.filter);

  // Composed selector (memoized — only re-emits if inputs change):
  readonly filteredMovies$ = this.select(
    this.movies$,
    this.filter$,
    (movies, filter) =>
      filter
        ? movies.filter(m => m.title.toLowerCase().includes(filter.toLowerCase()))
        : movies
  );

  // Selector with projector for multiple inputs:
  readonly viewModel$ = this.select(
    this.filteredMovies$,
    this.loading$,
    this.select(state => state.error),
    (movies, loading, error) => ({ movies, loading, error })
  );

  // Debounced selector for expensive derivations:
  readonly expensiveStats$ = this.select(
    this.movies$,
    movies => computeStats(movies),
    { debounce: true } // defer until microtask queue is empty
  );
}
```

---

## Pattern 2: Updaters — Synchronous State Mutations

```typescript
@Injectable()
class MovieStore extends ComponentStore<MovieState> {
  // Simple updater:
  readonly setLoading = this.updater((state, loading: boolean) => ({
    ...state,
    loading
  }));

  // Updater with complex logic:
  readonly addMovie = this.updater((state, movie: Movie) => ({
    ...state,
    movies: [...state.movies, movie]
  }));

  readonly removeMovie = this.updater((state, movieId: string) => ({
    ...state,
    movies: state.movies.filter(m => m.id !== movieId)
  }));

  readonly setFilter = this.updater((state, filter: string) => ({
    ...state,
    filter
  }));

  // Updater that patches (partial update pattern):
  readonly patchMovie = this.updater(
    (state, patch: { id: string; changes: Partial<Movie> }) => ({
      ...state,
      movies: state.movies.map(m =>
        m.id === patch.id ? { ...m, ...patch.changes } : m
      )
    })
  );

  // Updater can be called directly or piped:
  readonly setMovies = this.updater((state, movies: Movie[]) => ({
    ...state,
    movies,
    loading: false,
    error: null
  }));
}
```

---

## Pattern 3: Effects — Async Side-Effects

Effects observe an Observable trigger (or imperative call), perform async work, and optionally update state:

```typescript
@Injectable()
class MovieStore extends ComponentStore<MovieState> {
  private movieService = inject(MovieService);

  // Load movies effect:
  readonly loadMovies = this.effect<void>(
    trigger$ => trigger$.pipe(
      tap(() => this.setLoading(true)),
      switchMap(() =>
        this.movieService.getMovies().pipe(
          tapResponse({
            next:  movies => this.setMovies(movies),
            error: err    => this.setError(err.message)
          })
        )
      )
    )
  );

  // Effect with parameter:
  readonly loadMovieById = this.effect<string>(
    id$ => id$.pipe(
      tap(() => this.setLoading(true)),
      switchMap(id =>
        this.movieService.getMovie(id).pipe(
          tapResponse({
            next:  movie => this.addMovie(movie),
            error: err   => this.setError(err.message)
          })
        )
      )
    )
  );

  // Effect with Observable input (connects directly to a stream):
  readonly syncFilterToUrl = this.effect<string>(
    filter$ => filter$.pipe(
      debounceTime(300),
      tap(filter => this.router.navigate([], { queryParams: { filter } }))
    )
  );

  readonly setError = this.updater((state, error: string | null) => ({
    ...state,
    error,
    loading: false
  }));
}
```

---

## Pattern 4: `tapResponse` — Safe Effect Error Handling

`tapResponse` from `@ngrx/operators` is the standard way to handle async results in effects — it prevents unhandled errors from killing the effect:

```typescript
import { tapResponse } from '@ngrx/operators';

// ❌ Without tapResponse — unhandled error kills the effect permanently:
readonly loadMovies = this.effect<void>(
  trigger$ => trigger$.pipe(
    switchMap(() =>
      this.api.getMovies().pipe(
        tap(movies => this.setMovies(movies)),
        catchError(err => {
          this.setError(err.message);
          return EMPTY;
        })
      )
    )
  )
);

// ✅ tapResponse — handles next/error without killing the outer effect:
readonly loadMovies = this.effect<void>(
  trigger$ => trigger$.pipe(
    switchMap(() =>
      this.api.getMovies().pipe(
        tapResponse({
          next:     movies => this.setMovies(movies),
          error:    err    => this.setError(err.message),
          finalize: ()     => this.setLoading(false)
        })
      )
    )
  )
);

// tapResponse is equivalent to:
source$.pipe(
  tap({ next: onNext, error: onError, finalize: onFinalize }),
  catchError(() => EMPTY) // prevents error propagation to outer operator
)
```

---

## Pattern 5: Connecting ComponentStore to Template

```typescript
@Component({
  providers: [MovieStore], // component-scoped — destroyed with component
  template: `
    <ng-container *ngIf="vm$ | async as vm">
      <app-loading *ngIf="vm.loading" />
      <app-error   *ngIf="vm.error" [message]="vm.error" />
      <app-movie-list
        *ngIf="!vm.loading && !vm.error"
        [movies]="vm.movies"
        (remove)="store.removeMovie($event)"
      />
    </ng-container>

    <input [value]="(filter$ | async) ?? ''"
           (input)="store.setFilter($event.target.value)" />
  `
})
export class MovieListComponent implements OnInit {
  store = inject(MovieStore);

  vm$     = this.store.viewModel$;
  filter$ = this.store.filter$;

  ngOnInit() {
    this.store.loadMovies(); // trigger the effect
  }
}
```

---

## Pattern 6: Composing with RxJS Operators

ComponentStore selectors are plain Observables — compose freely:

```typescript
@Injectable()
class SearchStore extends ComponentStore<SearchState> {
  private searchService = inject(SearchService);

  // Selector piped through RxJS operators:
  readonly query$ = this.select(state => state.query);

  // Derived stream using standard RxJS:
  readonly results$ = this.query$.pipe(
    debounceTime(300),
    filter(q => q.length >= 2),
    switchMap(q =>
      this.searchService.search(q).pipe(
        catchError(() => of([]))
      )
    ),
    shareReplay(1)
  );

  readonly resultCount$ = this.results$.pipe(
    map(results => results.length),
    startWith(0),
    distinctUntilChanged()
  );

  // Pagination: current page × results per page:
  readonly paginatedResults$ = combineLatest([
    this.results$,
    this.select(state => state.page),
    this.select(state => state.pageSize)
  ]).pipe(
    map(([results, page, size]) =>
      results.slice(page * size, (page + 1) * size)
    )
  );

  // Effect that connects to an external Observable source:
  readonly connectLiveUpdates = this.effect<void>(
    _ => this.liveUpdateService.updates$.pipe(
      tap(update => this.applyUpdate(update))
    )
  );
}
```

---

## Pattern 7: Optimistic Updates in ComponentStore

```typescript
@Injectable()
class TodoStore extends ComponentStore<TodoState> {
  private api = inject(TodoApiService);

  readonly todos$  = this.select(s => s.todos);

  readonly toggleTodo = this.effect<string>(
    id$ => id$.pipe(
      exhaustMap(id => {
        const snapshot = this.get(s => s.todos);

        // Optimistic update:
        this.patchState(state => ({
          todos: state.todos.map(t =>
            t.id === id ? { ...t, done: !t.done } : t
          )
        }));

        return this.api.toggleTodo(id).pipe(
          tapResponse({
            next: updated => this.patchTodo({ id, changes: updated }),
            error: () => {
              // Rollback:
              this.patchState({ todos: snapshot });
              this.setError('Toggle failed — changes reverted');
            }
          })
        );
      })
    )
  );

  readonly patchTodo = this.updater(
    (state, patch: { id: string; changes: Partial<Todo> }) => ({
      ...state,
      todos: state.todos.map(t =>
        t.id === patch.id ? { ...t, ...patch.changes } : t
      )
    })
  );
}
```

---

## Pattern 8: ComponentStore vs NgRx Global Store — Decision Matrix

```
Use ComponentStore when:
  ✅ State is owned by one component subtree (destroyed with it)
  ✅ State doesn't need to be shared across unrelated parts of the app
  ✅ You want local state without boilerplate actions/reducers/effects
  ✅ Feature is self-contained: a data table, a form, a wizard

Use Global NgRx Store when:
  ✅ State is shared across sibling components that aren't ancestors
  ✅ State must survive navigation (a shopping cart, auth state)
  ✅ You need Redux DevTools time-travel debugging
  ✅ Multiple effects react to the same action
  ✅ State is central to the app's identity (user session, preferences)

ComponentStore + Global Store together:
  ✅ ComponentStore for local UI state (loading, pagination, selection)
  ✅ Global Store for shared domain entities (products, users, orders)
  ✅ ComponentStore effect dispatches to global store when needed
```

```typescript
// Hybrid: local UI state in ComponentStore, entities in global store:
@Injectable()
class ProductListStore extends ComponentStore<ProductListUiState> {
  private store = inject(Store);

  // Local UI state:
  readonly selectedIds$ = this.select(s => s.selectedIds);
  readonly sortBy$      = this.select(s => s.sortBy);

  // Global entity data:
  readonly products$ = this.store.select(selectAllProducts);

  // Composed view model — local + global:
  readonly viewModel$ = combineLatest([
    this.products$,
    this.selectedIds$,
    this.sortBy$
  ]).pipe(
    map(([products, selectedIds, sortBy]) => ({
      products: sortProducts(products, sortBy),
      selectedIds,
      sortBy
    }))
  );

  // Effect that dispatches to global store:
  readonly deleteSelected = this.effect<void>(
    _ => _.pipe(
      withLatestFrom(this.selectedIds$),
      tap(([, ids]) => {
        this.store.dispatch(ProductActions.deleteMany({ ids }));
        this.clearSelection();
      })
    )
  );

  readonly clearSelection = this.updater(state => ({
    ...state,
    selectedIds: []
  }));
}
```

---

## Common Pitfalls

### Providing ComponentStore at the Wrong Level

```typescript
// ❌ Provided in module — behaves like a singleton, not component-scoped:
@NgModule({
  providers: [MovieStore] // shared across the entire module
})

// ✅ Provide in the component — destroyed with the component:
@Component({
  providers: [MovieStore] // scoped to this component and its children
})
```

### Forgetting That `effect()` Doesn't Cancel In-Flight Work on Destroy

```typescript
// ❌ HTTP request may complete after component destroys:
readonly loadData = this.effect<void>(
  _ => _.pipe(
    switchMap(() => this.api.loadData().pipe(
      tap(data => this.setData(data)) // may run after store is destroyed
    ))
  )
);

// ✅ tapResponse handles this gracefully — it won't throw on destroyed store:
readonly loadData = this.effect<void>(
  _ => _.pipe(
    switchMap(() => this.api.loadData().pipe(
      tapResponse({
        next: data => this.setData(data), // no-op if store destroyed
        error: () => {}
      })
    ))
  )
);
```

### Overusing `patchState` for Complex Transitions

```typescript
// ❌ Multiple patchState calls — two state emissions, may cause flicker:
this.patchState({ loading: true });
this.patchState({ error: null });

// ✅ Single patchState for atomic transitions:
this.patchState({ loading: true, error: null });

// ✅ Or use an updater for reusable named transitions:
readonly startLoading = this.updater(state => ({
  ...state,
  loading: true,
  error: null
}));
```
