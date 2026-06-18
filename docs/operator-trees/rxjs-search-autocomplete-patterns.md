# Search & Autocomplete Patterns with RxJS

From simple typeahead to multi-source faceted search — the canonical RxJS use case, fully covered.

---

## Why RxJS Is Perfect for Search

Search involves:
- **Debouncing** — don't query on every keystroke
- **Cancellation** — discard stale requests when query changes
- **Deduplication** — don't re-query the same string
- **Loading state** — show spinner while waiting
- **Error recovery** — network blip shouldn't break the search box

RxJS handles all of these in a composable pipeline.

---

## Pattern 1: Basic Typeahead

```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, map } from 'rxjs/operators';

const searchInput = document.querySelector<HTMLInputElement>('#search')!;

fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),           // wait 300ms after typing stops
  distinctUntilChanged(),      // skip if same value as before
  switchMap(query =>
    query.length >= 2
      ? this.api.search(query) // only search 2+ chars
      : of([])                 // empty results for short queries
  )
).subscribe(renderResults);
```

---

## Pattern 2: Full Search State Machine

Complete state with loading, results, error, and empty state:

```typescript
import { combineLatest, Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, map, startWith, catchError } from 'rxjs/operators';

interface SearchState<T> {
  query:   string;
  loading: boolean;
  results: T[];
  error:   string | null;
  empty:   boolean;
}

@Injectable()
export class SearchService<T> {
  private query$ = new Subject<string>();

  private search$ = this.query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(query => {
      if (!query.trim()) return of({ loading: false, results: [] as T[], error: null });

      return this.api.search<T>(query).pipe(
        map(results => ({ loading: false, results, error: null })),
        catchError(err => of({ loading: false, results: [] as T[], error: err.message })),
        startWith({ loading: true, results: [] as T[], error: null })
      );
    }),
    startWith({ loading: false, results: [] as T[], error: null })
  );

  readonly state$: Observable<SearchState<T>> = combineLatest({
    query:  this.query$.pipe(startWith('')),
    search: this.search$
  }).pipe(
    map(({ query, search }) => ({
      query,
      ...search,
      empty: !search.loading && !search.error && search.results.length === 0 && query.length >= 2
    }))
  );

  search(query: string): void { this.query$.next(query); }
}
```

---

## Pattern 3: Minimum Query Length + Instant Clear

Different behavior for empty string vs short strings:

```typescript
import { switchMap, map, debounceTime, distinctUntilChanged } from 'rxjs/operators';

const MIN_LENGTH = 3;

query$.pipe(
  debounceTime(250),
  distinctUntilChanged(),
  switchMap(query => {
    if (query === '')            return of({ results: [], reason: 'cleared' });
    if (query.length < MIN_LENGTH) return of({ results: [], reason: 'too_short' });
    return this.api.search(query).pipe(
      map(results => ({ results, reason: 'search' }))
    );
  })
).subscribe(({ results, reason }) => {
  if (reason === 'cleared')  hideDropdown();
  if (reason === 'too_short') showHint(`Type ${MIN_LENGTH}+ characters`);
  if (reason === 'search')    renderDropdown(results);
});
```

---

## Pattern 4: Multi-Source Search (Federated Results)

Search across multiple backends simultaneously:

```typescript
import { combineLatest, forkJoin } from 'rxjs';
import { switchMap, map, catchError, of, startWith } from 'rxjs/operators';

interface FederatedResults {
  users:    User[];
  products: Product[];
  docs:     DocPage[];
}

query$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  filter(q => q.length >= 2),
  switchMap(query =>
    forkJoin({
      users:    this.userApi.search(query).pipe(catchError(() => of([]))),
      products: this.productApi.search(query).pipe(catchError(() => of([]))),
      docs:     this.docsApi.search(query).pipe(catchError(() => of([])))
    })
  )
).subscribe(renderFederatedResults);
```

---

## Pattern 5: Faceted Search with Multiple Filters

Multiple independent filters — any change triggers a new search:

```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { switchMap, debounceTime, map } from 'rxjs/operators';

@Injectable()
export class FacetedSearchService {
  readonly query$     = new BehaviorSubject('');
  readonly category$  = new BehaviorSubject<string | null>(null);
  readonly priceRange$ = new BehaviorSubject<[number, number]>([0, 1000]);
  readonly sortBy$    = new BehaviorSubject<'relevance' | 'price' | 'date'>('relevance');
  readonly page$      = new BehaviorSubject(1);

  readonly results$ = combineLatest({
    query:      this.query$,
    category:   this.category$,
    priceRange: this.priceRange$,
    sortBy:     this.sortBy$,
    page:       this.page$
  }).pipe(
    debounceTime(200),   // debounce rapid filter changes
    switchMap(params =>
      this.api.search(params).pipe(
        catchError(() => of({ results: [], total: 0 }))
      )
    ),
    shareReplay(1)
  );

  // Reset page when filters change (not when page changes):
  constructor() {
    merge(this.query$, this.category$, this.priceRange$, this.sortBy$).pipe(
      skip(1)  // skip initial values
    ).subscribe(() => this.page$.next(1));
  }
}
```

---

## Pattern 6: Autocomplete with Keyboard Navigation

```typescript
import { fromEvent, merge } from 'rxjs';
import { filter, map, scan, withLatestFrom } from 'rxjs/operators';

@Component({ ... })
export class AutocompleteComponent {
  private input$        = fromEvent<InputEvent>(this.inputEl, 'input');
  private keydown$      = fromEvent<KeyboardEvent>(this.inputEl, 'keydown');
  private arrowDown$    = this.keydown$.pipe(filter(e => e.key === 'ArrowDown'));
  private arrowUp$      = this.keydown$.pipe(filter(e => e.key === 'ArrowUp'));
  private enter$        = this.keydown$.pipe(filter(e => e.key === 'Enter'));
  private escape$       = this.keydown$.pipe(filter(e => e.key === 'Escape'));

  readonly suggestions$ = this.input$.pipe(
    map(e => (e.target as HTMLInputElement).value),
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(q => q.length >= 2 ? this.api.suggest(q) : of([])),
    shareReplay(1)
  );

  readonly activeIndex$ = merge(
    this.suggestions$.pipe(map(() => -1)),   // reset on new results
    this.arrowDown$.pipe(
      withLatestFrom(this.suggestions$),
      map(([, sugs]) => (idx: number) => Math.min(idx + 1, sugs.length - 1))
    ),
    this.arrowUp$.pipe(map(() => (idx: number) => Math.max(idx - 1, -1)))
  ).pipe(
    scan((idx, action) =>
      typeof action === 'function' ? action(idx) : action,
      -1
    )
  );

  readonly selected$ = this.enter$.pipe(
    withLatestFrom(this.suggestions$, this.activeIndex$),
    filter(([, , idx]) => idx >= 0),
    map(([, sugs, idx]) => sugs[idx])
  );
}
```

---

## Pattern 7: Search with History and Suggestions

Combine recent searches with live suggestions:

```typescript
@Injectable({ providedIn: 'root' })
export class SearchWithHistoryService {
  private history$ = new BehaviorSubject<string[]>(
    JSON.parse(localStorage.getItem('search-history') ?? '[]')
  );

  getSuggestions(query: string): Observable<Suggestion[]> {
    if (!query.trim()) {
      // No query — show recent searches:
      return this.history$.pipe(
        take(1),
        map(history =>
          history.slice(0, 5).map(q => ({ text: q, type: 'history' as const }))
        )
      );
    }

    return combineLatest({
      api:     this.api.suggest(query).pipe(
                 catchError(() => of([])),
                 map(s => s.map(t => ({ text: t, type: 'suggestion' as const })))
               ),
      history: this.history$.pipe(
                 take(1),
                 map(h =>
                   h.filter(q => q.toLowerCase().includes(query.toLowerCase()))
                    .slice(0, 3)
                    .map(q => ({ text: q, type: 'history' as const }))
                 )
               )
    }).pipe(
      map(({ api, history }) => [...history, ...api].slice(0, 8))
    );
  }

  saveSearch(query: string): void {
    const current = this.history$.getValue();
    const updated = [query, ...current.filter(q => q !== query)].slice(0, 20);
    this.history$.next(updated);
    localStorage.setItem('search-history', JSON.stringify(updated));
  }
}
```

---

## Pattern 8: Instant (Client-Side) Search

For smaller datasets, filter locally without API calls:

```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { map, debounceTime, distinctUntilChanged } from 'rxjs/operators';

@Component({ ... })
export class LocalSearchComponent {
  private allItems$  = this.api.getAll().pipe(shareReplay(1));
  private query$     = new BehaviorSubject('');
  private sortField$ = new BehaviorSubject<keyof Item>('name');

  readonly filtered$ = combineLatest({
    items: this.allItems$,
    query: this.query$.pipe(debounceTime(100), distinctUntilChanged()),
    sort:  this.sortField$
  }).pipe(
    map(({ items, query, sort }) => {
      const q = query.toLowerCase();
      return items
        .filter(item =>
          item.name.toLowerCase().includes(q) ||
          item.description.toLowerCase().includes(q)
        )
        .sort((a, b) => String(a[sort]).localeCompare(String(b[sort])));
    })
  );

  setQuery(q: string)   { this.query$.next(q); }
  sortBy(f: keyof Item) { this.sortField$.next(f); }
}
```

---

## Decision Table: Which Search Pattern?

| Scenario | Pattern | Key operators |
|---|---|---|
| Simple typeahead | Basic + `switchMap` | `debounceTime`, `distinctUntilChanged`, `switchMap` |
| Need loading/error state | State machine | `startWith`, `catchError`, `combineLatest` |
| Multiple APIs | Federated | `forkJoin`, `catchError` per source |
| Multiple filter UI controls | Faceted | `combineLatest`, `BehaviorSubject` per filter |
| Keyboard navigation | Autocomplete UI | `scan`, `withLatestFrom`, `merge` |
| Small dataset | Client-side | `combineLatest`, `map` filter+sort |
| Show history | History + suggestions | `combineLatest`, `localStorage` |

---

## Common Pitfalls

### Using `mergeMap` Instead of `switchMap`

```typescript
// ❌ mergeMap — all requests run, stale responses can overwrite fresh ones:
query$.pipe(
  debounceTime(300),
  mergeMap(q => this.api.search(q)) // response from 2 keystrokes ago can arrive last
)

// ✅ switchMap — cancels previous request when new query arrives:
query$.pipe(
  debounceTime(300),
  switchMap(q => this.api.search(q)) // only the latest in-flight request counts
)
```

### Not Resetting Page on Filter Change

```typescript
// ❌ Page stays at 5 when user changes category:
combineLatest({ query: query$, category: category$, page: page$ }).pipe(
  switchMap(params => api.search(params))
)
// User is looking at page 5 of "Electronics", changes to "Books"
// Still shows page 5 of "Books" — almost certainly no results!

// ✅ Reset page whenever non-page params change:
merge(query$, category$).pipe(skip(1))
  .subscribe(() => page$.next(1));
```
