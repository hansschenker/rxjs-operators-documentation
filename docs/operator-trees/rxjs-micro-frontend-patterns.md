# Micro-Frontend Patterns with RxJS

Cross-module event buses, shell-to-remote communication, shared state isolation, and lifecycle coordination across independently deployed frontends.

---

## The Micro-Frontend Communication Challenge

Micro-frontends (MFEs) need to communicate without tight coupling — they may be built with different frameworks, deployed independently, and loaded lazily. RxJS Subjects make ideal event buses because they:

- Decouple sender from receiver (Subject is the bridge)
- Support late subscribers via `ReplaySubject` / `BehaviorSubject`
- Compose cleanly across module boundaries
- Work framework-agnostically

---

## Pattern 1: Global Event Bus (Shell-Owned)

The shell application owns the bus; MFEs receive it via injection or `window`:

```typescript
// shell/src/event-bus.ts
import { Subject, Observable } from 'rxjs';
import { filter, map, share } from 'rxjs/operators';

type AppEvent =
  | { type: 'USER_LOGGED_IN';  user: User }
  | { type: 'USER_LOGGED_OUT' }
  | { type: 'NAVIGATION';      path: string }
  | { type: 'CART_UPDATED';    count: number }
  | { type: 'THEME_CHANGED';   theme: 'light' | 'dark' };

class GlobalEventBus {
  private bus$ = new Subject<AppEvent>();

  // Typed event emitter:
  emit(event: AppEvent): void {
    this.bus$.next(event);
  }

  // Typed event listener with auto-narrowing:
  on<T extends AppEvent['type']>(
    type: T
  ): Observable<Extract<AppEvent, { type: T }>> {
    return this.bus$.pipe(
      filter((e): e is Extract<AppEvent, { type: T }> => e.type === type),
      share()
    );
  }

  // Listen to multiple event types:
  onAny<T extends AppEvent['type']>(
    ...types: T[]
  ): Observable<Extract<AppEvent, { type: T }>> {
    return this.bus$.pipe(
      filter((e): e is Extract<AppEvent, { type: T }> =>
        types.includes(e.type as T)
      )
    );
  }
}

// Expose on window for cross-bundle access:
const eventBus = new GlobalEventBus();
(window as any).__mfe_event_bus = eventBus;
export { eventBus };
```

---

## Pattern 2: MFE Receiving Shell Events

Each MFE accesses the bus and subscribes to what it needs:

```typescript
// cart-mfe/src/shell-bridge.ts
import { Observable, EMPTY } from 'rxjs';

function getShellBus(): GlobalEventBus | null {
  return (window as any).__mfe_event_bus ?? null;
}

// Angular service wrapper:
@Injectable({ providedIn: 'root' })
export class ShellBridgeService implements OnDestroy {
  private destroy$ = new Subject<void>();

  readonly currentUser$ = this.listenFor('USER_LOGGED_IN').pipe(
    map(e => e.user),
    startWith(null as User | null),
    shareReplay(1)
  );

  readonly theme$ = this.listenFor('THEME_CHANGED').pipe(
    map(e => e.theme),
    startWith('light' as const),
    shareReplay(1)
  );

  listenFor<T extends AppEvent['type']>(type: T): Observable<Extract<AppEvent, { type: T }>> {
    const bus = getShellBus();
    if (!bus) return EMPTY;
    return bus.on(type).pipe(takeUntil(this.destroy$));
  }

  emit(event: AppEvent): void {
    getShellBus()?.emit(event);
  }

  ngOnDestroy(): void {
    this.destroy$.next();
  }
}

// Cart MFE component:
@Component({ ... })
export class CartComponent {
  readonly user$ = inject(ShellBridgeService).currentUser$;

  addToCart(item: CartItem): void {
    this.cartService.add(item).subscribe(count =>
      inject(ShellBridgeService).emit({ type: 'CART_UPDATED', count })
    );
  }
}
```

---

## Pattern 3: Shared State Store (BehaviorSubject-Based)

Shell exposes a reactive state store that all MFEs can read:

```typescript
// shell/src/shared-state.ts
import { BehaviorSubject } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface SharedState {
  user:    User | null;
  theme:   'light' | 'dark';
  locale:  string;
  features: Record<string, boolean>;
}

class SharedStateStore {
  private state$ = new BehaviorSubject<SharedState>({
    user:     null,
    theme:    'light',
    locale:   'en',
    features: {}
  });

  // Typed selector:
  select<K extends keyof SharedState>(key: K): Observable<SharedState[K]> {
    return this.state$.pipe(
      map(s => s[key]),
      distinctUntilChanged()
    );
  }

  // Shallow merge patch:
  patch(partial: Partial<SharedState>): void {
    this.state$.next({ ...this.state$.getValue(), ...partial });
  }

  snapshot(): SharedState {
    return this.state$.getValue();
  }
}

const sharedState = new SharedStateStore();
(window as any).__mfe_state = sharedState;
```

```typescript
// product-mfe/src/state.bridge.ts
const store = (window as any).__mfe_state as SharedStateStore;

// Read shared state:
store.select('user').pipe(takeUntilDestroyed()).subscribe(user => {
  if (!user) redirectToLogin();
});

store.select('features').pipe(
  map(f => f['new_checkout'] ?? false),
  distinctUntilChanged(),
  takeUntilDestroyed()
).subscribe(enabled => toggleNewCheckoutUI(enabled));
```

---

## Pattern 4: MFE Lifecycle Coordination

Coordinate mount/unmount with the shell to prevent memory leaks:

```typescript
// shell/src/mfe-loader.ts
import { Subject, ReplaySubject } from 'rxjs';
import { filter, map, takeUntil } from 'rxjs/operators';

interface MFELifecycle {
  name:   string;
  mount$: Observable<void>;
  unmount$: Observable<void>;
}

class MFERegistry {
  private lifecycles = new Map<string, MFELifecycle>();
  private events$    = new Subject<{ type: 'MOUNTED' | 'UNMOUNTED'; name: string }>();

  register(name: string): MFELifecycle {
    const mount$   = new Subject<void>();
    const unmount$ = new Subject<void>();
    const lifecycle: MFELifecycle = { name, mount$, unmount$ };
    this.lifecycles.set(name, lifecycle);
    return lifecycle;
  }

  // Shell notifies when MFE is mounted:
  notifyMounted(name: string): void {
    this.lifecycles.get(name)?.mount$.next();
    this.events$.next({ type: 'MOUNTED', name });
  }

  notifyUnmounted(name: string): void {
    this.lifecycles.get(name)?.unmount$.next();
    this.events$.next({ type: 'UNMOUNTED', name });
  }

  // Watch for a specific MFE to become available:
  whenMounted(name: string): Observable<void> {
    return this.events$.pipe(
      filter(e => e.type === 'MOUNTED' && e.name === name),
      map(() => void 0),
      take(1)
    );
  }
}

// MFE usage — auto-clean subscriptions on unmount:
const lifecycle = registry.register('cart-mfe');

lifecycle.mount$.pipe(
  switchMap(() =>
    priceUpdates$.pipe(
      takeUntil(lifecycle.unmount$) // auto-cleanup when MFE unmounts
    )
  )
).subscribe(updatePriceDisplay);
```

---

## Pattern 5: Cross-MFE Request/Response

One MFE requests data from another using a correlation ID:

```typescript
import { Subject, Observable } from 'rxjs';
import { filter, take, timeout } from 'rxjs/operators';

interface MFERequest  { id: string; type: string; payload: unknown; from: string; }
interface MFEResponse { requestId: string; data: unknown; error?: string; }

class MFEMessaging {
  private requests$  = new Subject<MFERequest>();
  private responses$ = new Subject<MFEResponse>();

  // Send a request and await response:
  request<T>(type: string, payload: unknown, to: string): Observable<T> {
    const id = crypto.randomUUID();

    return new Observable<T>(subscriber => {
      // Listen for matching response first:
      const sub = this.responses$.pipe(
        filter(r => r.requestId === id),
        take(1),
        timeout({ each: 5000, with: () => throwError(() => new Error(`${type} timed out`)) }),
        map(r => {
          if (r.error) throw new Error(r.error);
          return r.data as T;
        })
      ).subscribe(subscriber);

      // Then send the request:
      this.requests$.next({ id, type, payload, from: 'this-mfe' });

      return () => sub.unsubscribe();
    });
  }

  // Handle incoming requests:
  handle<T>(
    type:    string,
    handler: (payload: T) => Observable<unknown>
  ): Subscription {
    return this.requests$.pipe(
      filter(r => r.type === type),
      mergeMap(req =>
        handler(req.payload as T).pipe(
          take(1),
          map(data => ({ requestId: req.id, data })),
          catchError(err => of({ requestId: req.id, data: null, error: err.message }))
        )
      )
    ).subscribe(res => this.responses$.next(res));
  }
}

// Cart MFE provides stock info:
messaging.handle<{ skuId: string }>('GET_STOCK', ({ skuId }) =>
  inventoryService.getStock(skuId)
);

// Product MFE requests it:
messaging.request<StockInfo>('GET_STOCK', { skuId: 'ABC123' }, 'cart-mfe').pipe(
  catchError(() => of({ available: false, quantity: 0 }))
).subscribe(stock => updateStockBadge(stock));
```

---

## Pattern 6: Feature Flags Across MFEs

Shell manages feature flags reactively; all MFEs observe:

```typescript
import { BehaviorSubject, combineLatest } from 'rxjs';

class FeatureFlagService {
  private flags$ = new BehaviorSubject<Record<string, boolean>>({});

  isEnabled(flag: string): Observable<boolean> {
    return this.flags$.pipe(
      map(flags => flags[flag] ?? false),
      distinctUntilChanged()
    );
  }

  update(flags: Record<string, boolean>): void {
    this.flags$.next({ ...this.flags$.getValue(), ...flags });
  }

  // Enable features based on user roles:
  syncFromUser(user$: Observable<User | null>): void {
    user$.subscribe(user => {
      this.update({
        'beta_checkout':   user?.roles.includes('beta') ?? false,
        'admin_panel':     user?.roles.includes('admin') ?? false,
        'new_search':      user !== null
      });
    });
  }
}

// Each MFE accesses the same instance:
const featureFlags: FeatureFlagService = (window as any).__mfe_features;

// Conditionally render components based on flags:
combineLatest([
  featureFlags.isEnabled('new_search'),
  featureFlags.isEnabled('beta_checkout')
]).pipe(
  takeUntilDestroyed()
).subscribe(([newSearch, betaCheckout]) => {
  toggleComponent('search-v2',       newSearch);
  toggleComponent('checkout-beta',   betaCheckout);
});
```

---

## Common Pitfalls

### Subscribing Without Cleanup on MFE Unmount

```typescript
// ❌ Subscription keeps running after MFE is removed from DOM:
ngOnInit() {
  eventBus.on('USER_LOGGED_IN').subscribe(user => updateUI(user));
  // No unsubscribe — leaks memory and may cause errors after unmount
}

// ✅ Always tie subscription lifetime to MFE lifecycle:
ngOnInit() {
  eventBus.on('USER_LOGGED_IN').pipe(
    takeUntil(this.destroy$)
  ).subscribe(user => updateUI(user));
}

ngOnDestroy() { this.destroy$.next(); }
// Or use takeUntilDestroyed() with DestroyRef in Angular 16+
```

### Tight Coupling Through Shared Types

```typescript
// ❌ Both MFEs import from a shared package that changes frequently:
import { AppEvent } from '@company/shared-types'; // coupling!
// One MFE's release can break another

// ✅ Define event contracts locally, validate at runtime:
// Each MFE defines its own event types; the bus accepts `unknown`
// and MFEs validate incoming data with a type guard or Zod schema
function isUserLoggedInEvent(e: unknown): e is { type: 'USER_LOGGED_IN'; user: User } {
  return typeof e === 'object' && e !== null && (e as any).type === 'USER_LOGGED_IN';
}

rawEvents$.pipe(
  filter(isUserLoggedInEvent)
).subscribe(e => handleLogin(e.user));
```
