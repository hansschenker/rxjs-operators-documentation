# RxJS ↔ Angular Signals: Deep Interop

Advanced patterns for bidirectional conversion, computed signal chains driven by Observables, effect-free reactive data flows, and hybrid architectures where Signals and RxJS coexist.

---

## The Two-World Model

```
RxJS World                      Signals World
─────────────────────           ──────────────────────
Observable<T>                   Signal<T>
Push-based, lazy                Push-based, eager (glitch-free)
Explicit subscription           Automatic dependency tracking
Rich operator library           Lightweight, synchronous
Async-first                     Sync-first (async via effect)
Error channel                   No error channel (handle in pipe)
Completion semantics            Always-live (no completion)

Boundary operators:
toSignal(obs$)      → Signal<T>
toObservable(sig)   → Observable<T>
```

---

## Pattern 1: One-Way RxJS → Signal

```typescript
import { Component, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';

@Component({ standalone: true })
class UserDashboardComponent {
  private readonly userService = inject(UserService);

  // toSignal — wire an Observable directly into template/computed:
  readonly user = toSignal(
    this.userService.currentUser$,
    { initialValue: null as User | null }
  );

  // toSignal with error boundary — errors become null, not exceptions:
  readonly safeUser = toSignal(
    this.userService.currentUser$.pipe(
      catchError(() => of(null))  // prevent toSignal from propagating errors
    ),
    { initialValue: null as User | null }
  );

  // toSignal({ requireSync: true }) — for BehaviorSubject that always has a value:
  readonly theme = toSignal(
    inject(ThemeService).theme$,  // BehaviorSubject<'light' | 'dark'>
    { requireSync: true }          // Signal<'light' | 'dark'> — no undefined
  );
}
```

---

## Pattern 2: One-Way Signal → RxJS

```typescript
import { signal, computed } from '@angular/core';
import { toObservable } from '@angular/core/rxjs-interop';
import { switchMap, debounceTime } from 'rxjs/operators';

@Component({ standalone: true })
class SearchComponent {
  // Signal-driven search:
  readonly query = signal('');

  // toObservable converts signal changes → Observable stream:
  readonly query$ = toObservable(this.query);

  // Now use full RxJS operator power:
  readonly results = toSignal(
    this.query$.pipe(
      debounceTime(300),
      distinctUntilChanged(),
      filter(q => q.length >= 2),
      switchMap(q => inject(SearchService).search$(q)),
      catchError(() => of([]))
    ),
    { initialValue: [] as SearchResult[] }
  );

  // toObservable also works with computed():
  readonly cartItemCount = computed(() => inject(CartStore).items().length);
  readonly cartCount$ = toObservable(this.cartItemCount);

  // Use cart count stream for side effects:
  constructor() {
    toObservable(this.cartItemCount).pipe(
      skip(1),  // skip initial emit
      takeUntilDestroyed()
    ).subscribe(count => {
      if (count > 0) inject(NotificationService).badge(count);
    });
  }
}
```

---

## Pattern 3: Computed Signal Chains Seeded by Observables

Combine multiple Observables into a single derived signal:

```typescript
import { computed, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { combineLatest, map } from 'rxjs';

@Component({ standalone: true })
class OrderSummaryComponent {
  private readonly orderService   = inject(OrderService);
  private readonly productService = inject(ProductService);
  private readonly priceService   = inject(PriceService);

  // Base signals from Observables:
  private readonly order    = toSignal(this.orderService.current$,  { initialValue: null });
  private readonly products = toSignal(this.productService.catalog$, { initialValue: [] });
  private readonly prices   = toSignal(this.priceService.current$,  { initialValue: new Map() });

  // Derived computed signals — no need for Observable combinators:
  readonly lineItems = computed(() => {
    const order = this.order();
    const catalog = this.products();
    if (!order) return [];

    return order.itemIds.map(id => ({
      product: catalog.find(p => p.id === id),
      quantity: order.quantities[id] ?? 0
    })).filter(item => item.product !== undefined);
  });

  readonly subtotal = computed(() =>
    this.lineItems().reduce((sum, item) => {
      const price = this.prices().get(item.product!.id) ?? item.product!.basePrice;
      return sum + price * item.quantity;
    }, 0)
  );

  readonly tax     = computed(() => this.subtotal() * 0.08);
  readonly total   = computed(() => this.subtotal() + this.tax());
  readonly isEmpty = computed(() => this.lineItems().length === 0);
}
```

---

## Pattern 4: Effect-Free Reactive Side Effects

Replace `effect()` (which has footguns) with `toObservable` + RxJS:

```typescript
import { effect, inject, DestroyRef } from '@angular/core';
import { toObservable } from '@angular/core/rxjs-interop';
import { tap, switchMap } from 'rxjs/operators';

@Component({ standalone: true })
class AnalyticsComponent {
  readonly selectedTab = signal<'overview' | 'details' | 'export'>('overview');

  // ❌ effect() — runs synchronously, complex cleanup, can cause infinite loops:
  constructor() {
    effect(() => {
      // This runs during change detection — tricky timing
      analyticsService.track('tab_change', { tab: this.selectedTab() });
    });
  }

  // ✅ toObservable + subscribe — explicit async side effect with clean lifecycle:
  constructor() {
    toObservable(this.selectedTab).pipe(
      skip(1),            // skip initial value — only track actual changes
      tap(tab => analyticsService.track('tab_change', { tab })),
      takeUntilDestroyed()
    ).subscribe();
  }

  // ✅ Complex side effects with RxJS operators:
  constructor() {
    toObservable(this.selectedTab).pipe(
      skip(1),
      distinctUntilChanged(),
      switchMap(tab => this.prefetchTabData$(tab)), // cancel previous prefetch on tab change
      takeUntilDestroyed()
    ).subscribe();
  }
}
```

---

## Pattern 5: Service-Layer Architecture — Signals on the Outside, RxJS on the Inside

```typescript
import { Injectable, signal, computed } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';

// Service uses RxJS internally for async coordination,
// exposes Signals externally for components to consume:
@Injectable({ providedIn: 'root' })
class UserStoreService {
  // Internal RxJS pipeline:
  private readonly userLoad$ = new Subject<string>();
  private readonly loadedUser$: Observable<User | null>;

  // External Signal API (what components see):
  readonly currentUser: Signal<User | null>;
  readonly isLoading:   Signal<boolean>;
  readonly hasError:    Signal<boolean>;

  constructor(private api: UserApiService) {
    const state$: Observable<UserState> = this.userLoad$.pipe(
      switchMap(id =>
        this.api.getUser$(id).pipe(
          map(user  => ({ user, loading: false, error: false } as UserState)),
          startWith(          { user: null, loading: true,  error: false } as UserState),
          catchError(()  => of({ user: null, loading: false, error: true  } as UserState))
        )
      ),
      shareReplay(1)
    );

    // Convert the RxJS state stream to Signals once, at the service level:
    const state = toSignal(state$, {
      initialValue: { user: null, loading: false, error: false } as UserState
    });

    this.currentUser = computed(() => state().user);
    this.isLoading   = computed(() => state().loading);
    this.hasError    = computed(() => state().error);
  }

  load(userId: string) {
    this.userLoad$.next(userId);
  }
}

// Component consumes the Signal API — no subscription management needed:
@Component({ standalone: true })
class UserCardComponent {
  private readonly store = inject(UserStoreService);

  readonly user      = this.store.currentUser;
  readonly isLoading = this.store.isLoading;
  readonly hasError  = this.store.hasError;

  ngOnInit() {
    this.store.load(this.userId());
  }
}
```

---

## Pattern 6: `toSignal` Error Handling

`toSignal` propagates errors from the Observable — always handle errors before the boundary:

```typescript
import { toSignal } from '@angular/core/rxjs-interop';
import { catchError, retry } from 'rxjs/operators';

// ❌ If http$ errors, toSignal rethrows in the component — unhandled error:
readonly data = toSignal(this.http.get('/api/data'));

// ✅ Handle errors before reaching toSignal:
readonly data = toSignal(
  this.http.get<Data>('/api/data').pipe(
    retry({ count: 2, delay: 1000 }),
    catchError(err => {
      this.errorHandler.handle(err);
      return of(null);   // fallback value
    })
  ),
  { initialValue: null as Data | null }
);

// ✅ Typed error state:
readonly dataState = toSignal(
  this.http.get<Data>('/api/data').pipe(
    map(data  => ({ data, error: null }) as DataState),
    catchError(err => of({ data: null, error: err.message }) as Observable<DataState>),
    startWith({ data: null, error: null } as DataState)
  ),
  { requireSync: true }  // startWith makes it always sync
);

readonly data  = computed(() => this.dataState().data);
readonly error = computed(() => this.dataState().error);
```

---

## When to Use Signals vs Observables

```
Signal ✓                              Observable ✓
────────────────────────────          ────────────────────────────
Component state (show/hide,           HTTP requests
  selectedTab, counter)
Template bindings                     WebSocket streams
Derived/computed values               Event streams (fromEvent)
Simple boolean flags                  Retry/error handling logic
Preferences (theme, locale)           Complex async coordination
Route params (from toSignal)          Cross-component messaging
                                      Database streams (Firebase)
                                      Polling
                                      Complex timing (debounce, throttle)

Hybrid (Signal surface, RxJS core):
- Store services that fetch async data
- Search/autocomplete (query signal → RxJS pipeline → results signal)
- Pagination (page signal → HTTP → items signal)
- Form submission (form values → RxJS retry/error → state signal)
```

---

**Key insight**: The practical rule is: **use Signals for state that lives in components and services, use Observables for async operations and event streams, and bridge at the boundary with `toSignal`/`toObservable`**. The signal layer should be thin at the top (component state, template bindings) and the RxJS layer should be thick at the bottom (data fetching, coordination, error handling). Service-layer pattern: internal `Observable<State>` with `shareReplay(1)`, external `Signal<State>` via `toSignal` — this gives components the ergonomics of Signals without losing the power of RxJS for async work.
