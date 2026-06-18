# RxJS with Angular Standalone APIs

Angular 14+ standalone components, directives, and pipes eliminate NgModule entirely. This guide covers how RxJS patterns change — and improve — in a standalone Angular application.

---

## What Changes with Standalone

```typescript
// OLD: NgModule-based — Observable subscriptions often tied to module lifecycle
@NgModule({
  declarations: [UserListComponent],
  imports: [CommonModule, HttpClientModule],
  providers: [UserService]
})
export class UserModule {}

// NEW: Standalone — each component declares its own imports, providers are scoped
@Component({
  standalone: true,
  selector: 'app-user-list',
  imports: [CommonModule, AsyncPipe],  // AsyncPipe imported directly
  template: `<li *ngFor="let user of users$ | async">{{ user.name }}</li>`
})
export class UserListComponent {
  readonly users$ = inject(UserService).getAll$();
}
```

**Key RxJS implications of standalone:**
- `inject()` works in constructor, field initializers, and factory functions — no constructor injection boilerplate
- `takeUntilDestroyed()` from `@angular/core/rxjs-interop` is the new unsubscribe pattern
- `toSignal()` / `toObservable()` bridge RxJS ↔ Angular Signals
- Route-scoped providers replace module-level singletons for stream ownership

---

## Pattern 1: Injecting and Subscribing Without Constructor

```typescript
import { Component, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { AsyncPipe } from '@angular/common';

@Component({
  standalone: true,
  selector: 'app-dashboard',
  imports: [AsyncPipe],
  template: `
    <div *ngFor="let item of items$ | async">{{ item.label }}</div>
    <p>Count: {{ count }}</p>
  `
})
export class DashboardComponent {
  private readonly dataService = inject(DataService);

  // Stream for template binding:
  readonly items$ = this.dataService.getItems$();

  // Imperative subscription — use takeUntilDestroyed() for cleanup:
  count = 0;
  constructor() {
    this.dataService.getCount$().pipe(
      takeUntilDestroyed()  // auto-unsubscribes when component destroys
    ).subscribe(count => this.count = count);
  }
}
```

`takeUntilDestroyed()` with no arguments uses the current injection context's `DestroyRef` — no need to pass it explicitly when called in constructor or field initializer.

---

## Pattern 2: `toSignal` — Consuming Observables as Signals

```typescript
import { Component, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';

@Component({
  standalone: true,
  selector: 'app-user-profile',
  template: `
    @if (user()) {
      <h2>{{ user()!.name }}</h2>
      <p>{{ user()!.email }}</p>
    } @else {
      <p>Loading...</p>
    }
  `
})
export class UserProfileComponent {
  private readonly userService = inject(UserService);

  // toSignal wraps the Observable — no AsyncPipe, no manual subscribe:
  readonly user = toSignal(
    this.userService.getCurrentUser$(),
    { initialValue: null }  // avoids undefined before first emission
  );

  // toSignal with error handling:
  readonly settings = toSignal(
    this.userService.getSettings$().pipe(
      catchError(() => of(DEFAULT_SETTINGS))
    ),
    { initialValue: DEFAULT_SETTINGS }
  );
}
```

**`toSignal` rules:**
- Must be called in an injection context (constructor, field init, or `runInInjectionContext`)
- Returns `Signal<T | undefined>` unless `initialValue` or `requireSync: true` is set
- Automatically unsubscribes when the component destroys — no cleanup needed

---

## Pattern 3: `toObservable` — Using Signals in RxJS Pipelines

```typescript
import { Component, signal, computed, inject } from '@angular/core';
import { toObservable } from '@angular/core/rxjs-interop';
import { switchMap, debounceTime } from 'rxjs/operators';

@Component({
  standalone: true,
  selector: 'app-search',
  template: `
    <input (input)="query.set($event.target.value)" [value]="query()" />
    <ul>
      @for (result of results(); track result.id) {
        <li>{{ result.title }}</li>
      }
    </ul>
  `
})
export class SearchComponent {
  private readonly searchService = inject(SearchService);

  // Signal drives the search:
  readonly query = signal('');

  // toObservable bridges signal → Observable for RxJS operators:
  readonly results = toSignal(
    toObservable(this.query).pipe(
      debounceTime(300),
      switchMap(q => q.length >= 2
        ? this.searchService.search$(q)
        : of([])
      )
    ),
    { initialValue: [] }
  );
}
```

`toObservable` emits on signal changes synchronously through Angular's change detection cycle. It uses `effect()` internally and must be called in an injection context.

---

## Pattern 4: Route-Scoped Service Providers

Standalone components allow `providers` at the component level, creating route-scoped singletons:

```typescript
import { Component, inject } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { switchMap } from 'rxjs/operators';

// Feature-level service scoped to this route tree:
@Injectable()
export class OrderFeatureService {
  private readonly orderId$ = inject(ActivatedRoute).paramMap.pipe(
    map(params => params.get('id')!),
    distinctUntilChanged()
  );

  readonly order$ = this.orderId$.pipe(
    switchMap(id => inject(OrderApiService).getOrder$(id)),
    shareReplay(1)
  );
}

// Route config with scoped provider:
export const ORDER_ROUTES: Routes = [{
  path: ':id',
  component: OrderDetailComponent,
  providers: [OrderFeatureService]  // scoped to this route and children
}];

// Child components share the same OrderFeatureService instance:
@Component({
  standalone: true,
  selector: 'app-order-detail',
  template: `<div>{{ (orderService.order$ | async)?.status }}</div>`
})
export class OrderDetailComponent {
  readonly orderService = inject(OrderFeatureService);
}
```

Route-scoped providers solve the "component store vs global store" problem: the service lives exactly as long as the route is active, and is automatically destroyed when navigating away.

---

## Pattern 5: HTTP with `provideHttpClient` and `httpResource`

```typescript
// main.ts — standalone bootstrap:
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { provideRouter } from '@angular/router';

bootstrapApplication(AppComponent, {
  providers: [
    provideHttpClient(
      withInterceptors([authInterceptor, retryInterceptor])
    ),
    provideRouter(APP_ROUTES)
  ]
});

// Component — using HttpClient directly with inject():
@Component({ standalone: true, imports: [AsyncPipe] })
export class ProductListComponent {
  private readonly http = inject(HttpClient);

  readonly products$ = this.http.get<Product[]>('/api/products').pipe(
    retry({ count: 3, delay: 1000 }),
    shareReplay(1)
  );
}

// Angular 19+: httpResource (experimental) — signal-based HTTP:
import { httpResource } from '@angular/core';

@Component({ standalone: true })
export class ProductDetailComponent {
  readonly productId = input.required<string>();

  // httpResource creates a Signal<ResourceStatus<Product>>:
  readonly productResource = httpResource<Product>(
    () => `/api/products/${this.productId()}`
  );

  // Access the signal values:
  // productResource.value()  → Product | undefined
  // productResource.status() → 'idle' | 'loading' | 'resolved' | 'error'
  // productResource.error()  → Error | undefined
}
```

---

## Pattern 6: RxJS in Functional Route Guards and Resolvers

```typescript
import { inject } from '@angular/core';
import { CanActivateFn, ResolveFn } from '@angular/router';
import { map, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

// Functional guard — returns Observable<boolean | UrlTree>:
export const authGuard: CanActivateFn = (route, state) => {
  const authService = inject(AuthService);
  const router      = inject(Router);

  return authService.isAuthenticated$().pipe(
    map(authenticated =>
      authenticated ? true : router.createUrlTree(['/login'])
    )
  );
};

// Functional resolver — pre-fetches data before component activates:
export const orderResolver: ResolveFn<Order> = (route) => {
  const orderId = route.paramMap.get('id')!;
  return inject(OrderApiService).getOrder$(orderId).pipe(
    catchError(() => {
      inject(Router).navigate(['/orders']);
      return EMPTY; // cancel navigation
    })
  );
};

// Route config:
export const ROUTES: Routes = [{
  path: 'orders/:id',
  component: OrderDetailComponent,
  canActivate: [authGuard],
  resolve: { order: orderResolver }
}];

// Component reads resolved data as a signal:
@Component({ standalone: true })
export class OrderDetailComponent {
  private readonly route = inject(ActivatedRoute);

  readonly order$ = this.route.data.pipe(
    map(data => data['order'] as Order)
  );

  // Or as a signal:
  readonly order = toSignal(this.order$);
}
```

---

## Pattern 7: Effect-Free Reactive Forms

```typescript
import { Component, inject } from '@angular/core';
import { NonNullableFormBuilder, ReactiveFormsModule } from '@angular/forms';
import { toSignal } from '@angular/core/rxjs-interop';
import { debounceTime, switchMap, distinctUntilChanged } from 'rxjs/operators';

@Component({
  standalone: true,
  imports: [ReactiveFormsModule, AsyncPipe],
  template: `
    <form [formGroup]="form" (ngSubmit)="submit()">
      <input formControlName="email" />
      <span>{{ emailStatus() }}</span>
      <input formControlName="username" />
      <span>{{ usernameAvailable() ? '✓ available' : '✗ taken' }}</span>
      <button type="submit" [disabled]="form.invalid">Register</button>
    </form>
  `
})
export class RegistrationFormComponent {
  private readonly fb         = inject(NonNullableFormBuilder);
  private readonly authService = inject(AuthService);

  readonly form = this.fb.group({
    email:    ['', [Validators.required, Validators.email]],
    username: ['', Validators.required]
  });

  // Status stream → signal:
  readonly emailStatus = toSignal(
    this.form.get('email')!.statusChanges.pipe(
      map(status => status === 'VALID' ? '✓' : '✗')
    ),
    { initialValue: '' }
  );

  // Async username availability check:
  readonly usernameAvailable = toSignal(
    this.form.get('username')!.valueChanges.pipe(
      debounceTime(400),
      distinctUntilChanged(),
      switchMap(username => username.length >= 3
        ? this.authService.checkUsername$(username)
        : of(false)
      )
    ),
    { initialValue: false }
  );

  submit() {
    if (this.form.valid) {
      this.authService.register$(this.form.getRawValue()).pipe(
        takeUntilDestroyed(this.destroyRef)
      ).subscribe({ next: () => this.router.navigate(['/dashboard']) });
    }
  }
}
```

---

## Common Pitfalls

### Calling `toSignal` / `takeUntilDestroyed` Outside Injection Context

```typescript
// ❌ Called in a method — not in injection context:
@Component({ standalone: true })
class BadComponent {
  ngOnInit() {
    const data = toSignal(this.service.data$()); // Error: must be in injection context
  }
}

// ✅ Call in constructor or field initializer:
@Component({ standalone: true })
class GoodComponent {
  readonly data = toSignal(inject(DataService).data$()); // field initializer — injection context

  constructor() {
    inject(OtherService).events$().pipe(
      takeUntilDestroyed() // constructor — injection context
    ).subscribe(this.handleEvent.bind(this));
  }
}

// ✅ Or use runInInjectionContext for deferred calls:
@Component({ standalone: true })
class FlexibleComponent {
  private readonly injector = inject(Injector);

  setupDynamicStream() {
    runInInjectionContext(this.injector, () => {
      const sig = toSignal(this.service.dynamic$()); // now valid
    });
  }
}
```

### `toSignal` Without `initialValue` Returns `T | undefined`

```typescript
// ❌ No initialValue — TypeScript type is Signal<User | undefined>:
readonly user = toSignal(this.userService.getUser$());
// Template: {{ user()?.name }} — optional chaining required everywhere

// ✅ Provide initialValue to match the expected type:
readonly user = toSignal(
  this.userService.getUser$(),
  { initialValue: null as User | null }
);
// Template: @if (user()) { {{ user()!.name }} }

// ✅ Or use requireSync if the Observable emits synchronously:
readonly theme = toSignal(
  this.themeService.theme$, // BehaviorSubject always has a value
  { requireSync: true }      // Signal<Theme> — no undefined
);
```

---

**Key insight**: The standalone API + RxJS interop operators (`toSignal`, `toObservable`, `takeUntilDestroyed`) form a clean boundary between the reactive core (RxJS) and the reactive UI (Angular Signals). Use RxJS for async coordination, data fetching, and complex event processing; use Signals for component state that drives templates. `toSignal` at the boundary replaces `AsyncPipe` for most cases, and `takeUntilDestroyed()` replaces `Subject`/`takeUntil` teardown boilerplate.
