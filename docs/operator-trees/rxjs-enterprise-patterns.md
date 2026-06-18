# Enterprise RxJS Architecture Patterns

Large-scale RxJS architecture for enterprise Angular applications — domain isolation, façade services, feature-level state, cross-feature communication buses, and Observable-based API layers.

---

## The Core Problem at Enterprise Scale

In large codebases the naive RxJS approach breaks down in predictable ways:
- Components subscribe directly to services → tight coupling, impossible to test
- `BehaviorSubject` properties scattered across services → no clear ownership
- Features communicate via shared services → circular dependencies
- Error handling is inconsistent → some streams swallow errors, others crash the app
- No convention for subscription lifecycle → leaks accumulate over months

Enterprise patterns solve these problems with architectural conventions, not new operators.

---

## Pattern 1: The Façade Service Pattern

A façade encapsulates all RxJS complexity behind a clean component-facing API. Components interact only with the façade — never with stores, HTTP services, or domain logic directly:

```typescript
import { Injectable, inject } from '@angular/core';
import { Observable, combineLatest } from 'rxjs';
import { map, distinctUntilChanged, shareReplay } from 'rxjs/operators';

// The façade — the only thing a component imports:
@Injectable({ providedIn: 'root' })
export class OrderFacade {
  private store   = inject(OrderStore);
  private api     = inject(OrderApiService);
  private router  = inject(Router);

  // ─── Selectors (read-only Observable slices) ───────────────────────
  readonly orders$       = this.store.select(selectAllOrders);
  readonly activeOrder$  = this.store.select(selectActiveOrder);
  readonly isLoading$    = this.store.select(selectOrdersLoading);
  readonly error$        = this.store.select(selectOrdersError);

  // Composed view model — one Observable for the template:
  readonly ordersViewModel$ = combineLatest({
    orders:    this.orders$,
    active:    this.activeOrder$,
    loading:   this.isLoading$,
    error:     this.error$,
    canCreate: this.store.select(selectUserCanCreateOrders)
  }).pipe(
    distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
    shareReplay(1)
  );

  // ─── Commands (imperative actions) ─────────────────────────────────
  loadOrders(): void {
    this.store.dispatch(OrderActions.loadOrders());
  }

  createOrder(data: CreateOrderDto): void {
    this.store.dispatch(OrderActions.createOrder({ data }));
  }

  selectOrder(id: string): void {
    this.store.dispatch(OrderActions.selectOrder({ id }));
    this.router.navigate(['/orders', id]);
  }

  cancelOrder(id: string): Observable<void> {
    return this.api.cancelOrder(id).pipe(
      tap(() => this.store.dispatch(OrderActions.orderCancelled({ id }))),
      map(() => void 0)
    );
  }
}

// Component — zero knowledge of store, API, or router:
@Component({})
export class OrderListComponent {
  facade = inject(OrderFacade);
  vm$    = this.facade.ordersViewModel$;

  ngOnInit() { this.facade.loadOrders(); }
}
```

---

## Pattern 2: Domain Event Bus

Decouple features using a typed event bus — features publish events, other features react without knowing who published:

```typescript
import { Injectable } from '@angular/core';
import { Subject, Observable, filter } from 'rxjs';

// Discriminated union of all domain events:
type DomainEvent =
  | { type: 'ORDER_PLACED';     orderId: string; customerId: string }
  | { type: 'PAYMENT_RECEIVED'; orderId: string; amount: number }
  | { type: 'ITEM_SHIPPED';     orderId: string; trackingId: string }
  | { type: 'USER_LOGGED_IN';   userId: string;  sessionId: string }
  | { type: 'USER_LOGGED_OUT';  userId: string };

@Injectable({ providedIn: 'root' })
export class DomainEventBus {
  private bus$ = new Subject<DomainEvent>();

  publish(event: DomainEvent): void {
    this.bus$.next(event);
  }

  on<T extends DomainEvent['type']>(
    eventType: T
  ): Observable<Extract<DomainEvent, { type: T }>> {
    return this.bus$.pipe(
      filter((e): e is Extract<DomainEvent, { type: T }> => e.type === eventType)
    );
  }
}

// Publishing side (OrderFeature):
@Injectable({ providedIn: 'root' })
export class OrderService {
  private bus = inject(DomainEventBus);

  placeOrder(data: CreateOrderDto): Observable<Order> {
    return this.api.createOrder(data).pipe(
      tap(order => this.bus.publish({
        type:       'ORDER_PLACED',
        orderId:    order.id,
        customerId: order.customerId
      }))
    );
  }
}

// Reacting side (NotificationFeature — knows nothing about orders):
@Injectable({ providedIn: 'root' })
export class OrderNotificationEffect {
  private bus = inject(DomainEventBus);

  constructor() {
    this.bus.on('ORDER_PLACED').pipe(
      mergeMap(event =>
        this.notificationService.send({
          userId:  event.customerId,
          message: `Order ${event.orderId} confirmed!`
        })
      ),
      takeUntilDestroyed()
    ).subscribe();
  }
}

// Reacting side (InventoryFeature):
@Injectable({ providedIn: 'root' })
export class InventoryReservationEffect {
  private bus = inject(DomainEventBus);

  constructor() {
    this.bus.on('ORDER_PLACED').pipe(
      switchMap(event => this.inventoryService.reserve(event.orderId)),
      takeUntilDestroyed()
    ).subscribe();
  }
}
```

---

## Pattern 3: Observable API Layer

Wrap all HTTP interactions in a consistent, typed Observable-based layer:

```typescript
import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable, throwError, timer } from 'rxjs';
import { catchError, retry, map, timeout } from 'rxjs/operators';

interface ApiConfig {
  baseUrl:     string;
  timeoutMs:   number;
  maxRetries:  number;
}

@Injectable({ providedIn: 'root' })
export class ApiGateway {
  private http   = inject(HttpClient);
  private config = inject(API_CONFIG) as ApiConfig;

  get<T>(path: string, params?: Record<string, string>): Observable<T> {
    return this.http.get<T>(`${this.config.baseUrl}${path}`, {
      params: params ? new HttpParams({ fromObject: params }) : undefined
    }).pipe(
      timeout(this.config.timeoutMs),
      retry({
        count: this.config.maxRetries,
        delay: (err, n) => {
          // Only retry on server errors (5xx), not client errors (4xx):
          if (err.status >= 400 && err.status < 500) return throwError(() => err);
          return timer(1000 * Math.pow(2, n - 1));
        }
      }),
      catchError(err => throwError(() => this.normalizeError(err)))
    );
  }

  post<T, B = unknown>(path: string, body: B): Observable<T> {
    return this.http.post<T>(`${this.config.baseUrl}${path}`, body).pipe(
      timeout(this.config.timeoutMs),
      catchError(err => throwError(() => this.normalizeError(err)))
    );
  }

  private normalizeError(err: unknown): AppError {
    if (err instanceof HttpErrorResponse) {
      return {
        code:    err.status,
        message: err.error?.message ?? err.statusText,
        field:   err.error?.field
      };
    }
    if (err instanceof TimeoutError) {
      return { code: 408, message: 'Request timed out' };
    }
    return { code: 0, message: 'Unknown error' };
  }
}
```

---

## Pattern 4: Feature-Level Store with `ComponentStore`

For large features, use `ComponentStore` at the feature module level (not per-component):

```typescript
interface ShoppingCartState {
  items:      CartItem[];
  couponCode: string | null;
  loading:    boolean;
  error:      string | null;
}

@Injectable()
export class ShoppingCartStore extends ComponentStore<ShoppingCartState> {
  private api    = inject(CartApiService);
  private events = inject(DomainEventBus);

  // ─── Selectors ────────────────────────────────────────────────────
  readonly items$      = this.select(s => s.items);
  readonly itemCount$  = this.select(s => s.items.reduce((n, i) => n + i.qty, 0));
  readonly subtotal$   = this.select(s =>
    s.items.reduce((sum, i) => sum + i.price * i.qty, 0)
  );
  readonly discount$   = this.select(
    this.subtotal$,
    this.select(s => s.couponCode),
    (subtotal, code) => code ? subtotal * 0.1 : 0
  );
  readonly total$ = this.select(
    this.subtotal$, this.discount$,
    (sub, disc) => sub - disc
  );

  // ─── Updaters ──────────────────────────────────────────────────────
  readonly addItem = this.updater((state, item: CartItem) => ({
    ...state,
    items: [...state.items.filter(i => i.id !== item.id), item]
  }));

  readonly removeItem = this.updater((state, itemId: string) => ({
    ...state,
    items: state.items.filter(i => i.id !== itemId)
  }));

  readonly applyCoupon = this.updater((state, code: string) => ({
    ...state, couponCode: code
  }));

  // ─── Effects ───────────────────────────────────────────────────────
  readonly checkout = this.effect<void>(
    _ => _.pipe(
      withLatestFrom(this.select(s => s)),
      exhaustMap(([, state]) =>
        this.api.checkout({ items: state.items, couponCode: state.couponCode }).pipe(
          tapResponse({
            next: order => {
              this.patchState({ items: [], couponCode: null });
              this.events.publish({ type: 'ORDER_PLACED', orderId: order.id, customerId: order.customerId });
            },
            error: err => this.patchState({ error: err.message, loading: false })
          })
        )
      )
    )
  );
}

// Provide at feature module / route level:
export const CART_ROUTES: Routes = [{
  path: 'cart',
  providers: [ShoppingCartStore], // scoped to cart feature
  component: CartComponent
}];
```

---

## Pattern 5: Cross-Feature Communication via Shared Selectors

When features need to read each other's state without coupling:

```typescript
// selectors/cross-feature.selectors.ts — the only shared file between features
export const selectCheckoutEligibility = createSelector(
  selectCartTotal,           // from cart feature
  selectUserPaymentMethods,  // from user feature
  selectInventoryAvailable,  // from inventory feature
  (total, methods, available): CheckoutEligibility => ({
    canCheckout: total > 0 && methods.length > 0 && available,
    reason: !available ? 'Out of stock' :
            methods.length === 0 ? 'No payment method' :
            total === 0 ? 'Cart is empty' : null
  })
);

// Features read from cross-feature selectors, never import from each other:
@Injectable()
export class CheckoutFacade {
  readonly eligibility$ = this.store.select(selectCheckoutEligibility);
}
```

---

## Pattern 6: Global Error Boundary

Centralize unhandled Observable errors at the application level:

```typescript
import { ErrorHandler, Injectable, inject } from '@angular/core';
import { Subject, Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class RxJSErrorBoundary {
  private errors$ = new Subject<{ error: unknown; context: string }>();

  // All unhandled errors funnel here:
  readonly fatalErrors$ = this.errors$.pipe(
    filter(e => isFatal(e.error)),
    share()
  );

  readonly recoverableErrors$ = this.errors$.pipe(
    filter(e => !isFatal(e.error)),
    share()
  );

  report(error: unknown, context: string): void {
    this.errors$.next({ error, context });
  }

  // Operator that catches and reports without crashing the stream:
  catchAndReport<T>(context: string): MonoTypeOperatorFunction<T> {
    return source$ => source$.pipe(
      catchError(err => {
        this.report(err, context);
        return EMPTY; // stream continues (swallows error after reporting)
      })
    );
  }
}

// Global Angular ErrorHandler integration:
@Injectable()
export class GlobalErrorHandler implements ErrorHandler {
  private boundary = inject(RxJSErrorBoundary);

  handleError(error: unknown): void {
    this.boundary.report(error, 'GlobalErrorHandler');
    // Optionally re-throw for non-Observable errors:
    if (!(error instanceof RxJSUnhandledError)) throw error;
  }
}

// Usage in effects — never crash the effect:
@Injectable()
export class OrderEffects {
  private boundary = inject(RxJSErrorBoundary);

  loadOrders$ = createEffect(() =>
    this.actions$.pipe(
      ofType(OrderActions.loadOrders),
      switchMap(() =>
        this.api.getOrders().pipe(
          map(orders => OrderActions.loadOrdersSuccess({ orders })),
          catchError(err => {
            this.boundary.report(err, 'loadOrders$');
            return of(OrderActions.loadOrdersFailure({ error: err.message }));
          })
        )
      )
    )
  );
}
```

---

## Pattern 7: Lazy Feature Streams with Route-Level Providers

Angular 14+ standalone APIs enable per-route Observable scoping:

```typescript
// Feature streams that live only while the route is active:
export const ANALYTICS_ROUTES: Routes = [{
  path: 'analytics',
  providers: [
    AnalyticsFacade,       // façade with selectors/commands
    AnalyticsStore,        // ComponentStore for local state
    AnalyticsEffects,      // side-effects (auto-subscribe on inject)
  ],
  loadComponent: () => import('./analytics.component').then(m => m.AnalyticsComponent)
}];

// AnalyticsEffects auto-starts when injected:
@Injectable()
export class AnalyticsEffects {
  private store  = inject(AnalyticsStore);
  private events = inject(DomainEventBus);

  constructor() {
    // React to domain events while route is active:
    this.events.on('ORDER_PLACED').pipe(
      tap(e => this.store.recordConversion(e.orderId)),
      takeUntilDestroyed() // auto-cleanup when route is destroyed
    ).subscribe();
  }
}
```

---

## Architecture Decision Matrix

```
For state that...                       Use...
──────────────────────────────────────────────────────
Belongs to the whole app (auth, user)   NgRx Global Store
Belongs to one feature (cart, search)   NgRx ComponentStore (feature-provided)
Belongs to one component (pagination)   NgRx ComponentStore (component-provided)
Is derived from multiple features       Cross-feature selectors (shared file)
Requires inter-feature notification     Domain Event Bus
Needs HTTP retry/timeout/error norm.    Observable API Gateway
Needs to be tested in isolation         Inject observables; use Subjects as fakes
```

---

## Common Pitfalls

### Service Coupling Without Façade

```typescript
// ❌ Component knows about 4 different services — impossible to test or refactor:
@Component({})
class OrderComponent {
  constructor(
    private orderService: OrderService,
    private store: Store,
    private router: Router,
    private analytics: AnalyticsService
  ) {}
}

// ✅ Component knows only the façade:
@Component({})
class OrderComponent {
  facade = inject(OrderFacade);
  vm$    = this.facade.viewModel$;
}
```

### Circular Events (Event Bus Loops)

```typescript
// ❌ Feature A publishes event → Feature B reacts → publishes event → Feature A reacts...
// Easy to create infinite loops on the event bus

// ✅ Apply distinctUntilChanged or debounceTime on event bus reactions:
this.bus.on('CART_UPDATED').pipe(
  debounceTime(100), // prevent rapid re-triggering
  distinctUntilChanged((a, b) => a.total === b.total),
  switchMap(event => this.recalculateTax(event.total)),
  takeUntilDestroyed()
).subscribe();
```
