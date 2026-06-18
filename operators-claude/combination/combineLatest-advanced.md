# combineLatest — Advanced Patterns

For `combineLatest` fundamentals see the core [combineLatest](./combineLatest-operator-documentation) doc. This page covers the view-model pattern, derived state, performance tuning with `shareReplay`, and the comparison with `withLatestFrom`.

---

## The View-Model Pattern

`combineLatest` is the canonical tool for building a reactive view-model — combining multiple state streams into a single object the template subscribes to:

```typescript
import { combineLatest } from 'rxjs';
import { map, shareReplay } from 'rxjs/operators';

@Component({ ... })
export class DashboardComponent {
  // Individual state streams:
  private users$   = this.userService.users$;
  private filter$  = this.filterControl.valueChanges.pipe(startWith(''));
  private sort$    = this.sortControl.valueChanges.pipe(startWith('name'));
  private loading$ = this.userService.loading$;

  // Single view-model combining all state:
  readonly vm$ = combineLatest({
    users:   this.users$,
    filter:  this.filter$,
    sort:    this.sort$,
    loading: this.loading$
  }).pipe(
    map(({ users, filter, sort, loading }) => ({
      loading,
      items: users
        .filter(u => u.name.toLowerCase().includes(filter.toLowerCase()))
        .sort((a, b) => a[sort].localeCompare(b[sort])),
      count: users.length
    })),
    shareReplay(1) // share derived computation between template bindings
  );
}
```

```html
<!-- One async pipe — one subscription: -->
@if (vm$ | async; as vm) {
  <spinner *ngIf="vm.loading" />
  <p>{{ vm.count }} users ({{ vm.items.length }} shown)</p>
  <user-list [users]="vm.items" />
}
```

---

## Pattern 1: Dependent Derived State

Build computed values that depend on multiple state sources:

```typescript
import { combineLatest, of } from 'rxjs';
import { map, distinctUntilChanged, shareReplay } from 'rxjs/operators';

// Raw state:
const cartItems$   = this.cart.items$;           // Observable<CartItem[]>
const discountCode$= this.promo.activeCode$;     // Observable<string | null>
const taxRate$     = this.location.taxRate$;     // Observable<number>

// Derived state:
const subtotal$ = cartItems$.pipe(
  map(items => items.reduce((sum, i) => sum + i.price * i.qty, 0))
);

const discount$ = combineLatest({ subtotal: subtotal$, code: discountCode$ }).pipe(
  map(({ subtotal, code }) => code ? subtotal * 0.1 : 0)
);

const total$ = combineLatest({ subtotal: subtotal$, discount: discount$, rate: taxRate$ }).pipe(
  map(({ subtotal, discount, rate }) => {
    const taxable = subtotal - discount;
    return taxable + (taxable * rate);
  }),
  distinctUntilChanged() // only re-render when total actually changes
);
```

---

## Pattern 2: `combineLatest` with `startWith` for Optional Streams

`combineLatest` waits for ALL sources to emit before producing any value. Use `startWith` for optional inputs:

```typescript
import { combineLatest } from 'rxjs';
import { startWith } from 'rxjs/operators';

combineLatest({
  required:  this.required$,            // must emit first
  optional1: this.optional1$.pipe(startWith(null)),  // won't block
  optional2: this.optional2$.pipe(startWith([]))     // won't block
}).subscribe(({ required, optional1, optional2 }) => {
  // fires as soon as required$ emits, optional streams default to null/[]
});
```

---

## Pattern 3: Selective Re-computation

Avoid expensive re-computation when only one of many inputs changes:

```typescript
import { combineLatest } from 'rxjs';
import { map, distinctUntilChanged, shareReplay } from 'rxjs/operators';

// Expensive operation — only recompute when inputs meaningfully change:
const processedData$ = combineLatest({
  rawData:   this.data$,
  config:    this.config$,
  userPrefs: this.prefs$
}).pipe(
  distinctUntilChanged((prev, curr) =>
    prev.rawData === curr.rawData &&       // reference equality for arrays
    prev.config.algorithm === curr.config.algorithm && // structural equality for config
    prev.userPrefs.locale === curr.userPrefs.locale
  ),
  map(({ rawData, config, userPrefs }) =>
    expensiveTransform(rawData, config, userPrefs)
  ),
  shareReplay(1)
);
```

---

## Pattern 4: `combineLatest` for Route + Data

Combine route parameters with fetched data:

```typescript
import { combineLatest } from 'rxjs';
import { switchMap, map } from 'rxjs/operators';
import { ActivatedRoute } from '@angular/router';

@Component({ ... })
export class ItemDetailComponent {
  private route = inject(ActivatedRoute);

  readonly vm$ = combineLatest({
    id:   this.route.params.pipe(map(p => p['id'])),
    tab:  this.route.queryParams.pipe(map(p => p['tab'] ?? 'overview')),
  }).pipe(
    switchMap(({ id, tab }) =>
      combineLatest({
        item:     this.itemService.getItem(id),
        comments: tab === 'comments' ? this.commentService.getComments(id) : of([]),
        related:  tab === 'related'  ? this.itemService.getRelated(id)     : of([])
      })
    ),
    shareReplay(1)
  );
}
```

---

## Pattern 5: Multi-Store Selector (NgRx-like)

Combine selectors from multiple store slices:

```typescript
import { combineLatest } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

// Instead of NgRx's createSelector, compose Observables directly:
const canEditPost$ = combineLatest({
  user:     this.store.select(selectCurrentUser),
  post:     this.store.select(selectCurrentPost),
  settings: this.store.select(selectAppSettings)
}).pipe(
  map(({ user, post, settings }) =>
    user.isAdmin ||
    (post.authorId === user.id && settings.allowUserEdits)
  ),
  distinctUntilChanged() // only emit when boolean changes
);

// Use in template:
// [disabled]="!(canEditPost$ | async)"
```

---

## Pattern 6: Staggered Loading with `combineLatest`

Show data as it arrives, but use `combineLatest` for the final combined state:

```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { map, catchError, of, startWith } from 'rxjs/operators';

type LoadState<T> = { loading: boolean; data: T | null; error: string | null };

function loadState<T>(source$: Observable<T>): Observable<LoadState<T>> {
  return source$.pipe(
    map(data => ({ loading: false, data, error: null })),
    catchError(err => of({ loading: false, data: null, error: err.message })),
    startWith({ loading: true, data: null, error: null })
  );
}

readonly vm$ = combineLatest({
  user:     loadState(this.userService.getUser()),
  products: loadState(this.productService.getProducts()),
  promo:    loadState(this.promoService.getActive())
}).pipe(
  map(states => ({
    loading: Object.values(states).some(s => s.loading),
    errors:  Object.entries(states)
               .filter(([, s]) => s.error)
               .map(([key, s]) => `${key}: ${s.error}`),
    user:     states.user.data,
    products: states.products.data,
    promo:    states.promo.data
  }))
);
```

---

## `combineLatest` vs `withLatestFrom` — The Critical Distinction

```typescript
// combineLatest: emits when ANY source changes (both sources are "active")
combineLatest({ a: a$, b: b$ }).subscribe(({ a, b }) => {
  // fires when a$ changes OR when b$ changes
  // a change → b change both produce emissions
});

// withLatestFrom: emits ONLY when the primary source emits (b$ is "passive")
a$.pipe(withLatestFrom(b$)).subscribe(([a, b]) => {
  // fires ONLY when a$ emits
  // b$ changes are silently ignored — only its latest value is sampled
});
```

**Choose `combineLatest`** when both streams are equally "active" — any change should trigger recalculation.  
**Choose `withLatestFrom`** when one stream drives events and the other provides context.

---

## Common Pitfalls

### Initial Emission Blocked by Never-Emitting Source

```typescript
// ❌ vm$ never emits if optional$ never emits:
combineLatest({
  data:     this.data$,   // emits immediately
  optional: this.optional$ // might not emit for seconds, or ever
}).subscribe(vm => render(vm)); // blocked!

// ✅ Seed optional streams with startWith:
combineLatest({
  data:     this.data$,
  optional: this.optional$.pipe(startWith(null))
}).subscribe(vm => render(vm));
```

### Performance: combineLatest Emits on Every Source Change

```typescript
// ❌ If formGroup has 10 fields, each keystroke re-runs the entire pipe:
combineLatest(Object.values(this.form.controls).map(c => c.valueChanges)).pipe(
  map(values => computeExpensiveSummary(values))
).subscribe(render);

// ✅ Add debounceTime + shareReplay for expensive computation:
combineLatest(Object.values(this.form.controls).map(c => c.valueChanges)).pipe(
  debounceTime(50),                // batch rapid changes
  map(values => computeExpensiveSummary(values)),
  shareReplay(1)                   // cache result for multiple subscribers
).subscribe(render);
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key pattern**: `combineLatest({ ... }).pipe(map(vm => ({ ... })), shareReplay(1))` is the reactive view-model pattern — the most commonly used `combineLatest` shape in production Angular applications.
