# Subject — Advanced Patterns

For fundamentals see the core [Subject](./Subject) doc. This page covers Subject as a message bus, controlled lifecycle, facade patterns, multicasting coordination, and anti-pattern corrections.

---

## The Four Subject Flavors Revisited

```typescript
import { Subject, BehaviorSubject, ReplaySubject, AsyncSubject } from 'rxjs';

// Subject        — no initial value, no replay; observers miss past emissions
// BehaviorSubject — always has a current value; new subscribers get it immediately
// ReplaySubject   — replays last N emissions to new subscribers
// AsyncSubject    — emits only the last value, and only on complete

// Pick by answering: "what should a late subscriber receive?"
// Nothing (only future values)   → Subject
// Current state                  → BehaviorSubject
// Recent history (N items)       → ReplaySubject(N)
// Final result only              → AsyncSubject
```

---

## Pattern 1: Subject as Typed Event Bus

Replace ad-hoc event emitters with a strongly-typed Observable event bus:

```typescript
import { Subject, filter, map } from 'rxjs';

// Discriminated union for type safety:
type AppEvent =
  | { type: 'USER_LOGIN';   userId: string; sessionId: string }
  | { type: 'USER_LOGOUT';  userId: string }
  | { type: 'CART_UPDATED'; itemCount: number }
  | { type: 'ORDER_PLACED'; orderId: string; amount: number };

@Injectable({ providedIn: 'root' })
class EventBusService {
  private readonly bus$ = new Subject<AppEvent>();

  // Public typed event stream:
  readonly events$ = this.bus$.asObservable(); // hide Subject from consumers

  // Publish:
  emit(event: AppEvent) {
    this.bus$.next(event);
  }

  // Subscribe to specific event types with narrowed types:
  on<K extends AppEvent['type']>(
    type: K
  ): Observable<Extract<AppEvent, { type: K }>> {
    return this.bus$.pipe(
      filter((e): e is Extract<AppEvent, { type: K }> => e.type === type)
    );
  }
}

// Usage:
eventBus.on('ORDER_PLACED').subscribe(event => {
  // TypeScript knows: event.orderId and event.amount exist
  sendOrderConfirmation(event.orderId, event.amount);
});

eventBus.emit({ type: 'USER_LOGIN', userId: 'u1', sessionId: 's1' });
```

---

## Pattern 2: Subject Lifecycle — Deliberate Completion

A Subject that never completes is a resource leak. Design explicit lifecycle:

```typescript
import { Subject, takeUntil } from 'rxjs';

// Pattern: pair every long-lived Subject with a destroy$ Subject:
@Injectable()
class PollingService implements OnDestroy {
  private readonly destroy$   = new Subject<void>();
  private readonly stopPolling$ = new Subject<void>();
  private readonly results$   = new Subject<ApiResult>();

  readonly latestResult$ = this.results$.asObservable();

  start(endpoint: string) {
    interval(5000).pipe(
      switchMap(() => this.http.get<ApiResult>(endpoint)),
      takeUntil(this.stopPolling$),
      takeUntil(this.destroy$)
    ).subscribe(result => this.results$.next(result));
  }

  stop() {
    this.stopPolling$.next();
    this.stopPolling$.complete();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();  // complete prevents memory leaks in subscribers
    this.results$.complete();  // signal downstream that no more data
  }
}

// Note: complete() is important — operators like toArray() only emit on complete
// Subscribers with takeUntil(destroy$) will also clean up automatically
```

---

## Pattern 3: Subject as Command Channel

Decouple imperative commands from reactive data flow:

```typescript
import { Subject, BehaviorSubject, merge } from 'rxjs';
import { switchMap, scan, distinctUntilChanged } from 'rxjs/operators';

interface PaginationCommand
  | { type: 'GOTO_PAGE';  page: number }
  | { type: 'NEXT_PAGE'  }
  | { type: 'PREV_PAGE'  }
  | { type: 'SET_FILTER'; filter: string }

@Injectable()
class PaginatedListService {
  private readonly commands$ = new Subject<PaginationCommand>();

  // Derive state from command stream:
  private readonly state$ = this.commands$.pipe(
    scan((state, cmd) => {
      switch (cmd.type) {
        case 'GOTO_PAGE':  return { ...state, page: cmd.page };
        case 'NEXT_PAGE':  return { ...state, page: state.page + 1 };
        case 'PREV_PAGE':  return { ...state, page: Math.max(0, state.page - 1) };
        case 'SET_FILTER': return { ...state, page: 0, filter: cmd.filter };
        default:           return state;
      }
    }, { page: 0, filter: '' }),
    distinctUntilChanged(),
    shareReplay(1)
  );

  // Data stream driven by state:
  readonly items$ = this.state$.pipe(
    switchMap(({ page, filter }) => this.api.list$({ page, filter }))
  );

  // Command dispatchers:
  nextPage()               { this.commands$.next({ type: 'NEXT_PAGE' }); }
  prevPage()               { this.commands$.next({ type: 'PREV_PAGE' }); }
  gotoPage(page: number)   { this.commands$.next({ type: 'GOTO_PAGE', page }); }
  setFilter(filter:string) { this.commands$.next({ type: 'SET_FILTER', filter }); }
}
```

---

## Pattern 4: Subject as Multicast Coordinator

Use a Subject to create a controlled multicasting point in a pipeline:

```typescript
import { Subject, connectable } from 'rxjs';
import { tap, filter, map } from 'rxjs/operators';

// Manual multicast — share one source across multiple consumers:
const dataSubject = new Subject<DataPacket>();

// Source pushes into the Subject:
websocketStream$.pipe(
  map(msg => JSON.parse(msg.data) as DataPacket),
  takeUntilDestroyed()
).subscribe(packet => dataSubject.next(packet));

// Multiple consumers subscribe to the Subject:
const metrics$  = dataSubject.pipe(filter(p => p.type === 'metric'));
const alerts$   = dataSubject.pipe(filter(p => p.type === 'alert' && p.severity === 'high'));
const logs$     = dataSubject.pipe(filter(p => p.type === 'log'));

// Each subscriber gets the same packet without duplicate WS connections.

// Subject as a tap-fork — split a pipeline without breaking it:
const debugSubject = new Subject<ApiCall>();

apiCalls$.pipe(
  tap(call => debugSubject.next(call)),  // fork to debug stream
  mergeMap(call => this.http.execute$(call))
).subscribe(handleResponse);

// Debug stream — can be subscribed independently:
debugSubject.pipe(
  bufferTime(1000),
  map(calls => ({ count: calls.length, endpoints: [...new Set(calls.map(c => c.url))] }))
).subscribe(stats => devtools.logStats(stats));
```

---

## Pattern 5: Subject as Bridge Between Callback and Observable APIs

```typescript
import { Subject, fromEventPattern } from 'rxjs';

// Bridging a callback-based SDK to RxJS:
@Injectable()
class SdkBridgeService {
  private readonly sdkEvents$ = new Subject<SdkEvent>();
  private sdkInstance: ThirdPartySdk | null = null;

  initialize(config: SdkConfig) {
    this.sdkInstance = new ThirdPartySdk(config);

    // SDK uses callbacks — bridge via Subject:
    this.sdkInstance.onEvent((event: SdkEvent) => {
      this.sdkEvents$.next(event);
    });

    this.sdkInstance.onError((err: Error) => {
      this.sdkEvents$.error(err); // propagate as Observable error
    });

    this.sdkInstance.onShutdown(() => {
      this.sdkEvents$.complete();
    });
  }

  // Typed event streams:
  readonly events$   = this.sdkEvents$.asObservable();
  readonly connects$ = this.events$.pipe(filter(e => e.type === 'connect'));
  readonly messages$ = this.events$.pipe(filter(e => e.type === 'message'));

  destroy() {
    this.sdkInstance?.destroy();
    this.sdkEvents$.complete();
  }
}
```

---

## Common Pitfalls

### Exposing Subject Directly — Bypasses Encapsulation

```typescript
// ❌ Consumers can call next() on the Subject — bypasses service logic:
@Injectable()
class CounterService {
  readonly count$ = new BehaviorSubject<number>(0); // exposed Subject!
}

// External code can break invariants:
inject(CounterService).count$.next(-999); // no validation, no bounds check

// ✅ Expose only asObservable() and provide command methods:
@Injectable()
class CounterService {
  private readonly _count$ = new BehaviorSubject<number>(0);
  readonly count$ = this._count$.asObservable(); // read-only view

  increment() { this._count$.next(this._count$.value + 1); }
  decrement() { this._count$.next(Math.max(0, this._count$.value - 1)); }
  reset()     { this._count$.next(0); }
}
```

### Creating a Subject in a Component Without Cleanup

```typescript
// ❌ Subject used for takeUntil but never completed — minor leak:
@Component({ selector: 'app-bad' })
class BadComponent implements OnDestroy {
  private readonly destroy$ = new Subject<void>();

  ngOnDestroy() {
    this.destroy$.next();
    // Missing: this.destroy$.complete() — Subject itself is not GC'd immediately
  }
}

// ✅ Always complete the destroy$ Subject itself:
@Component({ selector: 'app-good', standalone: true })
class GoodComponent {
  // Modern Angular: use takeUntilDestroyed() — no Subject needed at all
  constructor() {
    someStream$.pipe(takeUntilDestroyed()).subscribe(this.handler.bind(this));
  }
}
```

### Using `Subject` Where `BehaviorSubject` Is Needed

```typescript
// ❌ New subscriber misses previous emissions — gets no initial value:
@Injectable()
class AuthService {
  private readonly user$ = new Subject<User | null>(); // misses past emissions!
}

// A component that subscribes after login already happened gets nothing.

// ✅ BehaviorSubject always provides the current value to late subscribers:
@Injectable()
class AuthService {
  private readonly _user$ = new BehaviorSubject<User | null>(null);
  readonly user$ = this._user$.asObservable();

  setUser(user: User | null) { this._user$.next(user); }
  get currentUser() { return this._user$.value; } // synchronous read
}
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key insight**: Subject is the escape hatch from pure Observable composition — it makes RxJS imperative. Use it sparingly and deliberately: as an event bus, a command channel, a callback bridge, or an explicit multicast point. Always expose `asObservable()` to consumers (never the Subject itself), always `complete()` in cleanup, and always ask whether a `BehaviorSubject` (with current-value semantics) would serve better than a plain `Subject` (which has no memory).
