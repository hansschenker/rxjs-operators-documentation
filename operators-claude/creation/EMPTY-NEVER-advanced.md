# EMPTY / NEVER — Advanced Patterns

For fundamentals see the core [EMPTY / NEVER](./EMPTY-NEVER) doc. This page covers using `EMPTY` and `NEVER` as control-flow tools — conditional short-circuiting, silent cancellation, fallback chains, and testing harnesses.

---

## Mental Model

```typescript
import { EMPTY, NEVER } from 'rxjs';

// EMPTY — completes immediately with zero emissions
// Observable.create(sub => sub.complete())
EMPTY.subscribe({
  next:     v  => console.log('next', v),   // never called
  complete: () => console.log('complete')   // called immediately
});

// NEVER — emits nothing, never completes, never errors
// Observable.create(() => {}) — infinite silence
NEVER.subscribe({
  next:     v  => console.log('next', v),   // never called
  complete: () => console.log('complete'),  // never called
  error:    e  => console.log('error', e)   // never called
});

// Core rule:
// EMPTY → "this stream is done, proceed" (triggers completion handlers)
// NEVER → "this stream is suspended, don't proceed" (no-op placeholder)
```

---

## Pattern 1: Conditional Short-Circuit with `EMPTY`

Use `EMPTY` in `switchMap`/`mergeMap` to silently skip invalid inputs:

```typescript
import { EMPTY, from } from 'rxjs';
import { switchMap, mergeMap, filter } from 'rxjs/operators';

// ❌ Alternative: filter before mergeMap (fine for simple predicates)
events$.pipe(
  filter(e => e.userId !== null),
  mergeMap(e => processEvent$(e))
)

// ✅ EMPTY in mergeMap — useful when the condition is based on API response:
events$.pipe(
  mergeMap(event =>
    featureFlags$.pipe(
      take(1),
      switchMap(flags =>
        flags.enableProcessing ? processEvent$(event) : EMPTY
      )
    )
  )
).subscribe(result => handleResult(result));

// Guard pattern — return EMPTY on validation failure:
function validateAndProcess$(input: unknown): Observable<ProcessedResult> {
  if (!isValidInput(input)) return EMPTY;  // silently skip invalid
  return processInput$(input as ValidInput);
}

userInputs$.pipe(
  mergeMap(validateAndProcess$)
).subscribe(handleResult);

// Conditional HTTP call — only fetch if data isn't already cached:
entityIds$.pipe(
  mergeMap(id => {
    const cached = cache.get(id);
    return cached ? of(cached) : fetchEntity$(id).pipe(
      tap(entity => cache.set(id, entity))
    );
  })
).subscribe(updateView);
```

---

## Pattern 2: Cancellation and Navigation Control

Return `EMPTY` from resolvers, guards, and interceptors to abort navigation:

```typescript
import { EMPTY, Observable } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';

// Route resolver — EMPTY cancels the navigation:
export const orderResolver: ResolveFn<Order> = (route) => {
  const id = route.paramMap.get('id')!;
  return inject(OrderService).getOrder$(id).pipe(
    catchError(err => {
      inject(NotificationService).error('Order not found');
      inject(Router).navigate(['/orders']);
      return EMPTY;  // cancel navigation to the order page
    })
  );
};

// HTTP interceptor — EMPTY drops the request (silently cancels):
@Injectable()
class OfflineInterceptor implements HttpInterceptor {
  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    if (!navigator.onLine) {
      this.queueService.queue(req);
      return EMPTY;  // drop request — queued for later, nothing goes to next
    }
    return next.handle(req);
  }
}

// Effect/action stream — EMPTY means "no action dispatched":
@Effect()
loadUser$ = this.actions$.pipe(
  ofType(UserActions.load),
  mergeMap(action =>
    this.userService.getUser$(action.userId).pipe(
      map(user => UserActions.loadSuccess({ user })),
      catchError(err => {
        if (err.status === 404) return EMPTY;  // no error action for 404
        return of(UserActions.loadFailure({ error: err.message }));
      })
    )
  )
);
```

---

## Pattern 3: `NEVER` as a Placeholder in Dynamic Streams

Use `NEVER` when a stream "slot" exists but isn't active yet:

```typescript
import { NEVER, combineLatest, BehaviorSubject } from 'rxjs';

// Feature flag — swap between live stream and silent NEVER:
const featureEnabled$ = new BehaviorSubject(false);

const data$ = featureEnabled$.pipe(
  switchMap(enabled =>
    enabled ? liveDataService.stream$() : NEVER
    // When disabled: stream is silent (no emissions, no completion)
    // When re-enabled: switchMap cancels NEVER and subscribes to live stream
  )
);

// Placeholder in combineLatest — prevents combineLatest from blocking:
// combineLatest won't emit until ALL sources emit at least once
// Use NEVER when a source shouldn't block others:
combineLatest([
  primaryData$,
  optionalData$ ?? NEVER  // NEVER if optional feature is disabled
]).subscribe(([primary, optional]) => {
  // ⚠️ NEVER blocks combineLatest — prefer startWith(null) instead:
});

// ✅ Better: startWith(null) so combineLatest can proceed:
combineLatest([
  primaryData$,
  (optionalData$ ?? EMPTY.pipe(startWith(null)))
]).subscribe(([primary, optional]) => {
  render(primary, optional); // optional may be null — that's fine
});
```

---

## Pattern 4: `NEVER` in Testing

`NEVER` is the perfect stand-in for a stream that should never emit during a test:

```typescript
import { NEVER, EMPTY, of } from 'rxjs';

// Test a component that has a stream of notifications:
describe('DashboardComponent', () => {
  it('renders without live data', () => {
    // Provide NEVER — the component should render even if data stream never emits
    const component = new DashboardComponent(
      { getLiveData$: () => NEVER } as DataService
    );

    expect(component).toBeTruthy();
    // No subscriptions were leaked — NEVER prevents any side effects
  });

  it('handles immediate empty stream', () => {
    const component = new DashboardComponent(
      { getLiveData$: () => EMPTY } as DataService
    );

    // EMPTY triggers completion — test that empty-state UI renders:
    expect(component.isEmpty).toBe(true);
  });
});

// TestScheduler — NEVER as a "never-firing" timer:
scheduler.run(({ cold, expectObservable }) => {
  const timeout$ = NEVER; // inject NEVER to "disable" a timeout in test
  const source$  = cold('--a--b--|');

  const result$ = source$.pipe(
    takeUntil(timeout$)  // timeout$ is NEVER → no timeout in test
  );

  expectObservable(result$).toBe('--a--b--|');
});
```

---

## Pattern 5: `EMPTY` in Fallback Chains

Chain with `concat` to provide fallback behavior after an empty source:

```typescript
import { EMPTY, concat, of } from 'rxjs';

// Try primary source, fall back to secondary, then to default:
function withFallback$<T>(
  primary$:   Observable<T>,
  secondary$: Observable<T>,
  fallback:   T
): Observable<T> {
  return concat(
    primary$.pipe(defaultIfEmpty(null as T | null)),
    // if primary emits null (was empty), try secondary:
  ).pipe(
    switchMap(v => v !== null ? of(v) : secondary$),
    defaultIfEmpty(fallback)
  );
}

// Simpler: defaultIfEmpty + concat pattern:
const config$ = concat(
  localConfig$.pipe(defaultIfEmpty(null)),
  remoteConfig$.pipe(defaultIfEmpty(null)),
  of(DEFAULT_CONFIG)
).pipe(
  first(v => v !== null)  // take the first non-null config found
);

// EMPTY as "done" signal in mergeMap fan-out:
const fanOut$ = ids$.pipe(
  mergeMap(id =>
    processId$(id).pipe(
      catchError(() => EMPTY)  // failed items → EMPTY → silently skipped
    )
  )
);
// Only successful results make it through
```

---

## EMPTY vs NEVER vs `of()` vs `throwError`

```typescript
// EMPTY        — completes immediately, 0 emissions
//                Use: skip/cancel, empty fallback, short-circuit

// NEVER        — never emits, never completes
//                Use: placeholder, disabled feature, infinite silence

// of()         — completes immediately, 1+ synchronous emissions
//                Use: constant values, synchronous responses in tests

// throwError() — errors immediately, 0 emissions
//                Use: signal failure, error propagation

// EMPTY in switchMap → cancels inner work, outer continues
// NEVER in switchMap → outer waits forever (hangs!)

// ❌ NEVER inside switchMap on a hot source:
actions$.pipe(
  switchMap(action => {
    if (!action.enabled) return NEVER;  // outer stream stalls!
    return processAction$(action);
  })
)

// ✅ EMPTY inside switchMap:
actions$.pipe(
  switchMap(action => {
    if (!action.enabled) return EMPTY;  // action silently skipped
    return processAction$(action);
  })
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 5/5
**Key insight**: `EMPTY` and `NEVER` are RxJS's control-flow primitives. `EMPTY` says "this branch is done" — it triggers completion handlers and lets the outer stream continue. `NEVER` says "this branch is suspended" — it blocks combinators like `combineLatest` and `zip` from emitting. In `switchMap`/`mergeMap`, always prefer `EMPTY` over `NEVER` for conditional skipping — `NEVER` stalls the stream. `NEVER` belongs in testing (disabled features, placeholder slots) and `switchMap` "pause" semantics where you want an explicit pause-until-reconfiguration pattern.
