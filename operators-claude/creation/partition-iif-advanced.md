# partition / iif — Advanced Patterns

For fundamentals see [partition](./partition) and [iif](./iif). This page covers multi-way routing with `partition`, dynamic stream switching with `iif`, and patterns for conditional data flow.

---

## `partition` — Synchronous Stream Forking

```typescript
import { partition, from } from 'rxjs';
import { filter } from 'rxjs/operators';

// partition(source$, predicate) → [trueStream$, falseStream$]
// Equivalent to two filter() calls but shares a single subscription:

const [evens$, odds$] = partition(
  from([1, 2, 3, 4, 5, 6]),
  n => n % 2 === 0
);

evens$.subscribe(v => console.log('even:', v)); // 2, 4, 6
odds$.subscribe(v => console.log('odd:', v));   // 1, 3, 5

// IMPORTANT: partition subscribes to the source twice if you subscribe to both outputs
// Use share() on the source if it has side effects:
const shared$ = coldSource$.pipe(share());
const [pass$, fail$] = partition(shared$, isValid);
```

---

## Pattern 1: Error/Success Routing

Route stream items into success and failure channels without breaking the main stream:

```typescript
import { partition, merge, Subject } from 'rxjs';
import { mergeMap, tap } from 'rxjs/operators';

interface ApiResult<T> { data: T | null; error: string | null; id: string }

// Split API results into success and failure channels:
const results$ = batchApiRequests$.pipe(share()); // share to avoid double-subscribe

const [successes$, failures$] = partition(
  results$,
  (result): result is ApiResult<User> & { data: User } => result.data !== null
);

// Process successes:
successes$.pipe(
  mergeMap(result => saveToDatabase$(result.data)),
  takeUntilDestroyed()
).subscribe(saved => console.log('Saved:', saved.id));

// Handle failures separately:
failures$.pipe(
  tap(failed => console.error('Failed:', failed.id, failed.error)),
  mergeMap(failed => retryQueue$.add$(failed.id)),
  takeUntilDestroyed()
).subscribe();

// Merge results back for UI update:
merge(
  successes$.pipe(map(r => ({ ...r, status: 'saved' as const }))),
  failures$.pipe(map(r => ({ ...r, status: 'failed' as const })))
).pipe(
  takeUntilDestroyed()
).subscribe(result => updateItemStatus(result.id, result.status));
```

---

## Pattern 2: Multi-Way Routing with Chained `partition`

```typescript
import { partition, merge } from 'rxjs';

type OrderStatus = 'pending' | 'processing' | 'shipped' | 'cancelled' | 'error';
interface Order { id: string; status: OrderStatus; amount: number }

// Multi-way routing via chained partition:
function routeOrders$(orders$: Observable<Order>) {
  const shared$ = orders$.pipe(share());

  // First split: terminal vs active orders:
  const [terminal$, active$] = partition(
    shared$,
    o => o.status === 'shipped' || o.status === 'cancelled' || o.status === 'error'
  );

  // Second split: further route active orders:
  const [pending$, processing$] = partition(
    active$,
    o => o.status === 'pending'
  );

  // Third split: terminal orders by type:
  const [shipped$, failed$] = partition(
    terminal$,
    o => o.status === 'shipped'
  );

  return {
    pending$:    pending$.pipe(tap(o => console.log('Queue:', o.id))),
    processing$: processing$.pipe(tap(o => console.log('In progress:', o.id))),
    shipped$:    shipped$.pipe(tap(o => sendShippingNotification(o))),
    failed$:     failed$.pipe(tap(o => escalateToSupport(o)))
  };
}

const routes = routeOrders$(orderStream$);

// Subscribe to each channel independently:
routes.shipped$.pipe(takeUntilDestroyed()).subscribe(updateInventory);
routes.failed$.pipe(takeUntilDestroyed()).subscribe(createSupportTicket);
```

---

## `iif` — Runtime Observable Selection

```typescript
import { iif, of, EMPTY, defer } from 'rxjs';

// iif(condition, trueObservable$, falseObservable$)
// condition is evaluated at SUBSCRIPTION TIME (lazy)

// Basic:
iif(
  () => isLoggedIn(),          // evaluated fresh on each subscribe
  userDashboard$,              // subscribed if true
  loginRedirect$               // subscribed if false
).subscribe();

// iif vs ternary:
// ternary: condition evaluated ONCE when the line runs
const stream$ = isLoggedIn() ? userDashboard$ : loginRedirect$; // EAGER

// iif: condition evaluated on EACH subscribe — essential for dynamic conditions
const stream$ = iif(() => isLoggedIn(), userDashboard$, loginRedirect$); // LAZY
```

---

## Pattern 3: `iif` for Feature-Flag-Based Routing

```typescript
import { iif, defer } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// Feature-flag gated Observable:
function featureGated$<T>(
  flagName:    string,
  newFeature$: Observable<T>,
  legacy$:     Observable<T>
): Observable<T> {
  return defer(() =>
    iif(
      () => featureFlagService.isEnabled(flagName),
      newFeature$,
      legacy$
    )
  );
}

// Usage — users with the flag get v2 API, others get v1:
const userData$ = featureGated$(
  'new-profile-api',
  userServiceV2.getProfile$(userId),
  userServiceV1.getProfile$(userId)
);

// Dynamic permission check:
function conditionalStream$<T>(
  permission: string,
  allowed$:   Observable<T>
): Observable<T> {
  return iif(
    () => authService.hasPermission(permission),
    allowed$,
    EMPTY  // silently empty for unauthorized users
  );
}

// Route-level data gating:
const adminData$ = iif(
  () => currentUser.role === 'admin',
  adminApiService.getData$(),
  of({ restricted: true, data: null })
);
```

---

## Pattern 4: `iif` with `switchMap` for Live Condition Re-Evaluation

`iif` evaluates the condition at subscription time — use `switchMap` to re-evaluate on changes:

```typescript
import { iif, BehaviorSubject } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// Re-evaluate iif condition whenever featureFlag$ changes:
const featureEnabled$ = new BehaviorSubject<boolean>(false);

const activeStream$ = featureEnabled$.pipe(
  switchMap(enabled =>
    // iif is re-subscribed every time featureEnabled$ changes:
    iif(
      () => enabled,              // captured in closure — always reflects `enabled`
      enhancedDataStream$,
      basicDataStream$
    )
  )
);

// Configuration-driven stream selection:
interface StreamConfig { mode: 'polling' | 'websocket' | 'sse'; url: string }

const config$ = configService.config$.pipe(
  distinctUntilChanged(
    (a, b) => a.mode === b.mode && a.url === b.url
  )
);

const dataStream$ = config$.pipe(
  switchMap(config =>
    iif(
      () => config.mode === 'websocket',
      createWebSocket$(config.url),
      iif(
        () => config.mode === 'sse',
        createSSE$(config.url),
        createPolling$(config.url, 5000)  // default: polling
      )
    )
  )
);
```

---

## `partition` vs `filter` × 2 vs `groupBy`

```typescript
// filter × 2 (two subscriptions — each subscribes independently):
const source$ = coldHttp$.pipe(share()); // must share to avoid double-request
const evens$ = source$.pipe(filter(n => n % 2 === 0));
const odds$  = source$.pipe(filter(n => n % 2 !== 0));
// Pro: composable, any predicate
// Con: two subscriptions (OK with share(), problem with cold sources)

// partition (two subscriptions, but hides the share concern):
const [evens$, odds$] = partition(source$, n => n % 2 === 0);
// Same as filter × 2 — actually subscribes twice!
// Pro: clean symmetrical API
// Con: ONLY binary splits, source subscribed twice

// groupBy (one subscription, N categories):
source$.pipe(
  groupBy(n => n % 3),        // three groups: 0, 1, 2
  mergeMap(group$ => group$.pipe(toArray()))
)
// Pro: N-way split with one subscription, categories are dynamic
// Con: more verbose, each group is an Observable (must be subscribed)

// Decision:
// Binary split, hot/shared source → partition
// N-way static split → chained partition or filter × N with share()
// N-way dynamic categories → groupBy
```

---

## Common Pitfalls

### `partition` Subscribes to the Source Twice

```typescript
// ❌ Cold source (HTTP request) is triggered twice:
const [success$, failure$] = partition(
  this.http.get('/api/data').pipe(  // cold Observable — creates new request per subscribe
    map(data => ({ data, error: null })),
    catchError(err => of({ data: null, error: err.message }))
  ),
  r => r.data !== null
);

success$.subscribe(r => this.data = r.data);
failure$.subscribe(r => this.error = r.error);
// TWO HTTP requests are made!

// ✅ Share the source before partitioning:
const shared$ = this.http.get('/api/data').pipe(
  map(data => ({ data, error: null })),
  catchError(err => of({ data: null, error: err.message })),
  share()  // or shareReplay(1) for late subscribers
);

const [success$, failure$] = partition(shared$, r => r.data !== null);
```

### `iif` Condition Must Be a Function (Lazy Evaluation)

```typescript
// ❌ Condition evaluated eagerly — captures value at creation, not subscription:
const isEnabled = featureService.isEnabled('new-feature'); // captured once

const stream$ = iif(
  () => isEnabled,      // this is fine — but isEnabled never updates
  newStream$,
  legacyStream$
);

// If feature flag changes after creation, stream$ still uses old value.

// ✅ Re-derive from live source on each subscription:
const stream$ = defer(() =>
  iif(
    () => featureService.isEnabled('new-feature'), // fresh read per subscribe
    newStream$,
    legacyStream$
  )
);

// Or better: use switchMap to react to flag changes:
featureService.flag$('new-feature').pipe(
  switchMap(enabled => enabled ? newStream$ : legacyStream$)
);
```

---

**Cognitive Load**: 3/5 (partition), 3/5 (iif) | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `partition` is syntactic sugar for two `filter()` calls — always `share()` the source before partitioning to avoid double subscription on cold Observables. `iif` is `defer` + ternary: its condition is evaluated lazily at subscription time, making it ideal for permission checks and feature flags that must be read fresh each time. For runtime condition changes, wrap `iif` in `switchMap` over the condition signal — `iif` alone doesn't react to condition changes after initial subscription.
