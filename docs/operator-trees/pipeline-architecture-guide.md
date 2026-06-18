# Pipeline Architecture Guide

How to structure, compose, and organize RxJS pipelines in production applications — reusable operators, layered pipelines, service architecture, and testability.

---

## The Three Layers of a Pipeline

Every production RxJS pipeline can be decomposed into three layers:

```
1. SOURCE LAYER     — where data comes from
   fromEvent, HTTP, WebSocket, Subject, BehaviorSubject

2. TRANSFORM LAYER  — what happens to data
   map, filter, mergeMap, scan, groupBy, combineLatest

3. SIDE-EFFECT LAYER — consequences
   tap, finalize, subscribe
```

Keeping these concerns separate makes pipelines testable and reusable:

```typescript
// ❌ Mixed concerns — untestable, hard to reuse
fromEvent(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),
  switchMap(q => this.http.get(`/api/search?q=${q}`)),
  tap(results => (document.getElementById('results')!.innerHTML = renderHtml(results)))
).subscribe();

// ✅ Separated concerns:
// Source layer:
const rawQuery$ = fromEvent<InputEvent>(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);

// Transform layer (testable — pure Observable → Observable):
function searchPipeline(query$: Observable<string>): Observable<SearchResult[]> {
  return query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    filter(q => q.length >= 2),
    switchMap(q => searchService.search(q)),
    catchError(() => of([]))
  );
}

// Side-effect layer (imperative):
searchPipeline(rawQuery$).subscribe(results => renderResults(results));
```

---

## Reusable Operator Factories

Extract common pipeline patterns into named, parameterizable operators:

```typescript
import { MonoTypeOperatorFunction, OperatorFunction, pipe } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, switchMap, catchError } from 'rxjs/operators';

// Named reusable pipeline:
function searchDebounce(
  opts: { minLength?: number; dueTime?: number } = {}
): MonoTypeOperatorFunction<string> {
  const { minLength = 2, dueTime = 300 } = opts;
  return pipe(
    debounceTime(dueTime),
    distinctUntilChanged(),
    filter(q => q.length >= minLength)
  );
}

function withFallback<T>(fallback: T): MonoTypeOperatorFunction<T> {
  return catchError(() => of(fallback));
}

function toSearchResults<T>(
  searchFn: (q: string) => Observable<T[]>
): OperatorFunction<string, T[]> {
  return pipe(
    switchMap(q => searchFn(q).pipe(withFallback<T[]>([])))
  );
}

// Composing named operators into a readable pipeline:
userInput$.pipe(
  searchDebounce({ minLength: 2, dueTime: 400 }),
  toSearchResults(productService.search.bind(productService))
).subscribe(renderResults);
```

---

## Layered Service Architecture

Structure RxJS in Angular services with clear public API:

```typescript
@Injectable({ providedIn: 'root' })
export class ProductStore {
  // Private state — only the store can modify
  private _state$ = new BehaviorSubject<ProductState>(INITIAL_STATE);

  // Public read-only projections — consumers observe but don't mutate
  readonly products$  = this._state$.pipe(map(s => s.products), distinctUntilChanged());
  readonly loading$   = this._state$.pipe(map(s => s.loading),  distinctUntilChanged());
  readonly error$     = this._state$.pipe(map(s => s.error),    distinctUntilChanged());
  readonly selected$  = this._state$.pipe(
    map(s => s.products.find(p => p.id === s.selectedId) ?? null),
    distinctUntilChanged((a, b) => a?.id === b?.id)
  );

  // Public actions — commands to the store
  load(): void {
    this._patch({ loading: true, error: null });
    this.api.getProducts().pipe(
      take(1),
      catchError(err => {
        this._patch({ loading: false, error: err.message });
        return EMPTY;
      })
    ).subscribe(products => this._patch({ products, loading: false }));
  }

  select(id: string): void { this._patch({ selectedId: id }); }

  private _patch(partial: Partial<ProductState>): void {
    this._state$.next({ ...this._state$.getValue(), ...partial });
  }
}
```

---

## Pipeline Composition Patterns

### Vertical Composition (sequential operators in one pipe)

```typescript
// All steps in one pipeline — clear sequence:
userInput$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  filter(q => q.length >= 2),
  switchMap(q => api.search(q)),
  catchError(() => of([]))
)
```

### Horizontal Composition (combining multiple pipelines)

```typescript
// Multiple independent pipelines combined:
combineLatest({
  query:    userInput$.pipe(debounceTime(300), distinctUntilChanged()),
  category: categorySelect$.pipe(startWith('all')),
  page:     pageChange$.pipe(startWith(1))
}).pipe(
  switchMap(params => api.search(params))
)
```

### Nested Composition (pipeline inside a pipeline)

```typescript
// Inner pipeline handles per-item logic; outer handles coordination:
ids$.pipe(
  mergeMap(id =>
    api.getItem(id).pipe(      // inner pipeline per id
      retry({ count: 2 }),
      catchError(() => of(null)),
      filter(Boolean),
      map(item => ({ ...item, loaded: true }))
    )
  )
)
```

---

## Testing Pipeline Architecture

Structured pipelines are easy to unit test:

```typescript
// The transform layer is pure Observable → Observable — test it in isolation:
describe('searchPipeline', () => {
  it('debounces and filters short queries', fakeAsync(() => {
    const results: string[][] = [];
    const input$ = new Subject<string>();
    const mockSearch = (q: string) => of([`result-for-${q}`]);

    input$.pipe(
      searchDebounce({ dueTime: 300, minLength: 2 }),
      toSearchResults(mockSearch)
    ).subscribe(r => results.push(r));

    input$.next('a');   // too short — filtered
    tick(400);
    expect(results.length).toBe(0);

    input$.next('hello');
    tick(400);
    expect(results).toEqual([['result-for-hello']]);
  }));
});
```

---

## Anti-Pattern: Logic in `subscribe`

```typescript
// ❌ Business logic in subscribe — untestable, unreusable
source$.subscribe(data => {
  const filtered = data.filter(x => x.active);
  const sorted   = filtered.sort((a, b) => a.name.localeCompare(b.name));
  const mapped   = sorted.map(x => ({ ...x, display: x.name.toUpperCase() }));
  render(mapped);
});

// ✅ Logic in the pipeline — testable, composable:
source$.pipe(
  map(data =>
    data
      .filter(x => x.active)
      .sort((a, b) => a.name.localeCompare(b.name))
      .map(x => ({ ...x, display: x.name.toUpperCase() }))
  )
).subscribe(render);
```

---

## Anti-Pattern: Nested `subscribe`

```typescript
// ❌ Nested subscribe — creates subscription leaks, hard to manage:
userIdStream$.subscribe(userId => {
  this.api.getUser(userId).subscribe(user => {    // inner sub leaks!
    this.api.getOrders(user.id).subscribe(orders => { // deeper leak!
      render(user, orders);
    });
  });
});

// ✅ switchMap for sequential dependent fetches:
userIdStream$.pipe(
  switchMap(userId => this.api.getUser(userId)),
  switchMap(user   =>
    this.api.getOrders(user.id).pipe(
      map(orders => ({ user, orders }))
    )
  )
).subscribe(({ user, orders }) => render(user, orders));
```

---

## Anti-Pattern: Side Effects in `map`

```typescript
// ❌ Side effects in map — unexpected execution timing, breaks referential transparency:
source$.pipe(
  map(item => {
    this.store.dispatch(itemLoaded(item)); // side effect!
    return transform(item);
  })
)

// ✅ Side effects in tap:
source$.pipe(
  tap(item => this.store.dispatch(itemLoaded(item))), // explicit side effect
  map(item => transform(item))                        // pure transformation
)
```

---

## Pipeline Documentation Convention

Self-document complex pipelines with labeled `tap`:

```typescript
const checkoutFlow$ = cartItems$.pipe(
  // Step 1: Validate cart
  tap(items => this.analytics.track('checkout-started', { count: items.length })),
  switchMap(items => this.validator.validate(items)),

  // Step 2: Apply promotions
  switchMap(validated =>
    this.promos.apply(validated).pipe(
      tap(result => this.analytics.track('promo-applied', result.discount))
    )
  ),

  // Step 3: Finalize order
  switchMap(priced =>
    this.orderService.create(priced).pipe(
      retry({ count: 2, delay: 1000 }),
      catchError(err => this.handleOrderError(err))
    )
  ),

  // Side effects on success:
  tap(order => this.router.navigate(['/confirmation', order.id])),
  finalize(() => this.loading$.next(false))
);
```

---

## Observable Lifecycle Checklist

For any new pipeline, answer these questions:

```
□ SOURCE: Is this hot or cold? Does it complete or run forever?
□ ERROR: Is catchError inside or outside flattening operators?
□ COMPLETION: When does the pipeline complete? Is that right?
□ TEARDOWN: Is takeUntil (or takeUntilDestroyed) in place for long-lived streams?
□ SHARING: Is shareReplay(1) used for streams consumed by multiple subscribers?
□ SIDE EFFECTS: Are they in tap/finalize, not in map/filter?
□ TESTING: Can the transform layer be tested independently of the source?
```
