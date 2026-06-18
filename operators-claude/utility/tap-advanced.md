# tap — Advanced Patterns

For fundamentals see the core [tap / finalize](./tap) doc. This page covers structured debugging pipelines, analytics instrumentation, conditional side effects, and multi-observer tap patterns.

---

## tap as a First-Class Tool (Not Just Debugging)

```typescript
import { tap } from 'rxjs/operators';

// tap(observer) — full Observer interface:
source$.pipe(
  tap({
    next:        v  => console.log('next:', v),
    error:       e  => console.error('error:', e),
    complete:    () => console.log('complete'),
    subscribe:   () => console.log('subscribed'),
    unsubscribe: () => console.log('unsubscribed')
  })
)

// tap is the ONLY correct place for synchronous side effects in a pipeline:
// - Logging / metrics / analytics
// - Caching without changing the stream value
// - Triggering external systems (loaders, badges)
// - Debugging intermediate values
//
// Never use tap for:
// - Logic that changes what the downstream receives (use map)
// - Error recovery (use catchError)
// - Cleanup (use finalize)
```

---

## Pattern 1: Structured Debug Operator

Build a reusable debug operator that can be toggled in production:

```typescript
import { tap, timestamp, pairwise } from 'rxjs/operators';
import { isDevMode } from '@angular/core'; // or process.env check

// Structured tap that logs with context and can be disabled in prod:
function debug<T>(
  label:   string,
  options: { logValues?: boolean; logTiming?: boolean; production?: boolean } = {}
): MonoTypeOperatorFunction<T> {
  const { logValues = true, logTiming = false, production = false } = options;

  if (!production && !isDevMode()) return identity; // no-op in production

  return (source$: Observable<T>) => {
    let count = 0;
    let lastMs = 0;

    return source$.pipe(
      tap({
        subscribe:   () => console.groupCollapsed(`[${label}] subscribed`),
        next: v => {
          count++;
          const now = Date.now();
          const gap = lastMs ? `+${now - lastMs}ms` : 'initial';
          lastMs = now;

          if (logValues) {
            console.log(`[${label}] #${count} ${logTiming ? gap : ''}`, v);
          } else {
            console.log(`[${label}] emission #${count} ${logTiming ? gap : ''}`);
          }
        },
        error:       e  => console.error(`[${label}] ERROR`, e),
        complete:    () => { console.log(`[${label}] complete after ${count} emissions`); console.groupEnd(); },
        unsubscribe: () => { console.log(`[${label}] unsubscribed`); console.groupEnd(); }
      })
    );
  };
}

// Usage — remove without changing logic:
userData$.pipe(
  debug('UserData', { logTiming: true }),
  map(user => user.profile),
  debug('Profile'),
  switchMap(profile => loadPrefs$(profile.id)),
  debug('Prefs', { logValues: false }) // just count, don't log values
).subscribe(handlePrefs);
```

---

## Pattern 2: Analytics Instrumentation

Instrument key pipeline points for production metrics:

```typescript
import { tap, timer, switchMap } from 'rxjs/operators';

// Track load time from subscription to first emission:
function trackLatency<T>(
  metricName: string,
  analytics:  AnalyticsService
): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => {
    let startTime = 0;
    let firstEmission = true;

    return source$.pipe(
      tap({
        subscribe: () => { startTime = Date.now(); firstEmission = true; },
        next: () => {
          if (firstEmission) {
            analytics.timing(metricName, Date.now() - startTime);
            firstEmission = false;
          }
        },
        error: (e) => {
          analytics.event(metricName + '_error', { message: e.message });
        }
      })
    );
  };
}

// Track conversion funnel steps:
function trackStep<T>(
  step:      string,
  analytics: AnalyticsService
): MonoTypeOperatorFunction<T> {
  return tap(() => analytics.event('funnel_step', { step }));
}

// Full instrumented checkout flow:
checkoutInitiated$.pipe(
  trackStep('checkout_started', analytics),
  switchMap(cart => paymentService.process$(cart).pipe(
    trackLatency('payment_latency', analytics)
  )),
  trackStep('payment_processed', analytics),
  switchMap(payment => orderService.place$(payment)),
  trackStep('order_placed', analytics)
).subscribe(handleSuccess);
```

---

## Pattern 3: Cache Population Without Breaking the Stream

Use `tap` to populate caches as data flows through:

```typescript
import { tap, shareReplay } from 'rxjs/operators';

// Populate an in-memory cache as HTTP responses flow through:
@Injectable({ providedIn: 'root' })
class EntityCacheService {
  private readonly cache = new Map<string, User>();

  readonly cachedUserFetch$ = (userId: string): Observable<User> =>
    this.cache.has(userId)
      ? of(this.cache.get(userId)!)
      : this.http.get<User>(`/api/users/${userId}`).pipe(
          tap(user => this.cache.set(user.id, user)) // populate cache as side effect
        );
}

// Index-building tap — build a lookup map while streaming:
function indexBy<T, K extends keyof T>(
  key: K,
  index: Map<T[K], T>
): MonoTypeOperatorFunction<T> {
  return tap(item => index.set(item[key], item));
}

const userIndex = new Map<string, User>();

userList$.pipe(
  indexBy('id', userIndex) // side effect: build index while streaming
).subscribe(renderUserList);

// userIndex is now populated and can be used for O(1) lookup:
const user = userIndex.get('user-123');
```

---

## Pattern 4: Conditional Side Effects

Execute side effects only when certain conditions are met:

```typescript
import { tap, filter, map } from 'rxjs/operators';

// Only log errors in development:
apiCalls$.pipe(
  tap({
    error: (e) => {
      if (isDevMode()) {
        console.error('API error:', e);
      } else {
        errorTracker.capture(e);
      }
    }
  })
)

// Only show a spinner for requests taking longer than 200ms:
function withSpinner<T>(spinnerService: SpinnerService): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => {
    let showSpinner = false;

    return source$.pipe(
      tap({
        subscribe: () => {
          // Show spinner only if still pending after 200ms:
          setTimeout(() => {
            if (!showSpinner) {
              showSpinner = true;
              spinnerService.show();
            }
          }, 200);
        },
        next:        () => { if (showSpinner) { spinnerService.hide(); showSpinner = false; } },
        error:       () => { if (showSpinner) { spinnerService.hide(); showSpinner = false; } },
        complete:    () => { if (showSpinner) { spinnerService.hide(); showSpinner = false; } },
        unsubscribe: () => { if (showSpinner) { spinnerService.hide(); showSpinner = false; } }
      })
    );
  };
}

// Usage:
this.http.get('/api/data').pipe(
  withSpinner(inject(SpinnerService))
).subscribe(handleData);
```

---

## Pattern 5: Cross-Tab Synchronization Side Effect

Use `tap` to notify BroadcastChannel on state changes:

```typescript
// Sync state changes across browser tabs via BroadcastChannel:
@Injectable({ providedIn: 'root' })
class CrossTabSyncService {
  private readonly channel = new BroadcastChannel('app-state');

  syncWith<T>(key: string): MonoTypeOperatorFunction<T> {
    return tap(value => {
      try {
        this.channel.postMessage({ key, value });
      } catch {
        // Ignore — value may not be serializable
      }
    });
  }
}

// Use in a store to broadcast state changes:
this.cartStore.items$.pipe(
  syncService.syncWith('cart'),  // side effect: broadcast to other tabs
  takeUntilDestroyed()
).subscribe(items => this.renderCart(items));
```

---

## `tap` vs `map` vs Side Effects in `subscribe`

```typescript
// tap — side effect, value passes through unchanged:
source$.pipe(tap(v => console.log(v)))  // v arrives unchanged downstream

// map — transform: value is replaced with return value:
source$.pipe(map(v => { console.log(v); return v; })) // works but is a tap misuse

// subscribe callback — terminal side effect, no operators chained after:
source$.subscribe(v => console.log(v));  // fine for terminal consumption

// Rules:
// 1. If you return the same value → use tap (makes intent clear)
// 2. If you transform the value → use map
// 3. If it's a terminal consumer → put it in subscribe
// 4. Never put logic in tap that should be in map (e.g., tap(v => v.processed = true))
```

---

## Common Pitfalls

### Mutating Values Inside `tap`

```typescript
// ❌ Mutating the value in tap — obscures the transformation, causes bugs:
source$.pipe(
  tap(item => {
    item.processed = true;  // mutates the object — visible outside this operator!
    item.timestamp = Date.now();
  }),
  map(item => item) // downstream silently sees mutated values
)

// ✅ Use map for transformations — tap for observation only:
source$.pipe(
  map(item => ({ ...item, processed: true, timestamp: Date.now() })),  // pure transform
  tap(item => console.log('Processed:', item.id))  // observation only
)
```

### Throwing Inside `tap`

```typescript
// ❌ Throwing in tap sends the error down the error channel — surprising:
source$.pipe(
  tap(v => {
    if (!isValid(v)) throw new Error('Invalid!'); // becomes an Observable error
  })
)

// ✅ Use filter or map for validation, tap for side effects only:
source$.pipe(
  filter(isValid),  // or throw with throwIfEmpty()
  tap(v => logAnalytics(v))  // guaranteed valid here
)
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key insight**: `tap` is the "look but don't touch" operator — it observes without changing. Its full Observer interface (`next`, `error`, `complete`, `subscribe`, `unsubscribe`) makes it a complete instrumentation hook. The golden rule: if you find yourself writing `tap(v => { ...; return v; })` or mutating `v` inside tap, stop — use `map` instead. Reserve `tap` for true side effects: logging, analytics, cache population, external system notification.
