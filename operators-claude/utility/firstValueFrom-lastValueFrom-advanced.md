# firstValueFrom / lastValueFrom — Advanced Patterns

For fundamentals see the core [firstValueFrom / lastValueFrom](./firstValueFrom-lastValueFrom) doc. This page covers safe Observable→Promise bridges, timeout guards, Angular resolver integration, error channel mapping, and comparisons with `toPromise`.

---

## The Bridge Pattern

```typescript
import { firstValueFrom, lastValueFrom } from 'rxjs';

// firstValueFrom — resolves with the FIRST emission, then unsubscribes
// lastValueFrom  — resolves with the LAST emission (waits for completion)

// firstValueFrom throws EmptyError if Observable completes without emitting
// Use defaultValue option to provide a fallback:
const user = await firstValueFrom(
  userService.getUser$(id),
  { defaultValue: null }     // resolves null instead of throwing EmptyError
);

// lastValueFrom also throws EmptyError on empty completion:
const total = await lastValueFrom(
  priceList$.pipe(reduce((acc, p) => acc + p, 0)),
  { defaultValue: 0 }
);
```

---

## Pattern 1: Timeout Guards

Never `await` an Observable without a timeout — a stream that never emits or completes hangs the caller forever:

```typescript
import { firstValueFrom, TimeoutError } from 'rxjs';
import { timeout, catchError } from 'rxjs/operators';

// ❌ No timeout — hangs forever if stream stalls:
const data = await firstValueFrom(this.api.getData$());

// ✅ Always add a timeout:
async function fetchWithTimeout<T>(
  source$:   Observable<T>,
  timeoutMs: number,
  fallback:  T
): Promise<T> {
  return firstValueFrom(
    source$.pipe(
      timeout({ first: timeoutMs }),
      catchError(err => {
        if (err instanceof TimeoutError) {
          console.warn(`Stream timed out after ${timeoutMs}ms`);
          return of(fallback);
        }
        return throwError(() => err);
      })
    ),
    { defaultValue: fallback }
  );
}

// Usage:
const config = await fetchWithTimeout(
  configService.load$(),
  3000,
  DEFAULT_CONFIG
);
```

---

## Pattern 2: Angular Route Resolvers

Route resolvers are one of the most common `firstValueFrom`/`lastValueFrom` use cases in Angular:

```typescript
import { inject } from '@angular/core';
import { ResolveFn } from '@angular/router';
import { firstValueFrom } from 'rxjs';
import { timeout, catchError } from 'rxjs/operators';

// Resolver returns Promise — firstValueFrom bridges Observable → Promise:
export const orderResolver: ResolveFn<Order | null> = async (route) => {
  const orderId    = route.paramMap.get('id')!;
  const orderApi   = inject(OrderApiService);
  const router     = inject(Router);

  try {
    return await firstValueFrom(
      orderApi.getOrder$(orderId).pipe(
        timeout(5000)
      )
    );
  } catch (err) {
    if (err instanceof TimeoutError) {
      console.error('Order resolver timed out');
    }
    router.navigate(['/orders']);
    return null;
  }
};

// Observable resolver (avoid firstValueFrom — resolver handles Observable natively):
export const orderResolverObservable: ResolveFn<Order> = (route) => {
  return inject(OrderApiService).getOrder$(route.paramMap.get('id')!).pipe(
    timeout(5000),
    catchError(() => {
      inject(Router).navigate(['/orders']);
      return EMPTY;
    })
  );
};
// Prefer the Observable form — Angular router handles the subscription lifecycle
```

---

## Pattern 3: Interop with Legacy Promise-Based APIs

Wrap Observables for use with existing `async/await` or `Promise.all` code:

```typescript
import { firstValueFrom, forkJoin } from 'rxjs';

// Converting multiple Observables to Promises for Promise.all:
async function initializeDashboard(): Promise<DashboardData> {
  const [user, config, permissions] = await Promise.all([
    firstValueFrom(userService.getCurrentUser$(), { defaultValue: null }),
    firstValueFrom(configService.load$()),
    firstValueFrom(permissionsService.loadForUser$())
  ]);

  return { user, config, permissions };
}

// Prefer forkJoin + firstValueFrom when all streams should complete together:
async function initializeDashboardV2(): Promise<DashboardData> {
  return firstValueFrom(
    forkJoin({
      user:        userService.getCurrentUser$(),
      config:      configService.load$(),
      permissions: permissionsService.loadForUser$()
    })
  );
}
// forkJoin version is cleaner and cancels all if any fails
```

---

## Pattern 4: One-Shot Operations in Non-Reactive Contexts

Use `firstValueFrom` in class methods, utility functions, or middleware that can't use `subscribe`:

```typescript
import { firstValueFrom } from 'rxjs';

// Express middleware — must return Promise for async/await:
async function authMiddleware(req: Request, res: Response, next: NextFunction) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token' });

  try {
    const user = await firstValueFrom(
      authService.verifyToken$(token).pipe(
        timeout(2000)
      )
    );
    req.user = user;
    next();
  } catch {
    res.status(401).json({ error: 'Invalid token' });
  }
}

// Angular Guard as async function:
const canActivate: CanActivateFn = async () => {
  const authService = inject(AuthService);
  const router      = inject(Router);

  const isAuth = await firstValueFrom(
    authService.isAuthenticated$().pipe(take(1))
  );

  return isAuth ? true : router.createUrlTree(['/login']);
};

// NestJS interceptor:
@Injectable()
class LoggingInterceptor implements NestInterceptor {
  intercept(ctx: ExecutionContext, next: CallHandler): Observable<any> {
    const request = ctx.switchToHttp().getRequest();
    const start   = Date.now();

    return next.handle().pipe(
      tap(async () => {
        const duration = Date.now() - start;
        await firstValueFrom(
          this.auditService.log$({ path: request.url, duration })
        );
      })
    );
  }
}
```

---

## Pattern 5: Conditional Resolution

Select between Observable or immediate value at call time:

```typescript
import { firstValueFrom, isObservable, of } from 'rxjs';

// Accept either a value or an Observable — normalize to Promise:
async function resolveValue<T>(
  valueOrObservable: T | Observable<T>
): Promise<T> {
  if (isObservable(valueOrObservable)) {
    return firstValueFrom(valueOrObservable as Observable<T>);
  }
  return valueOrObservable;
}

// Retry-on-failure with firstValueFrom:
async function fetchWithRetry<T>(
  factory:    () => Observable<T>,
  maxRetries: number
): Promise<T> {
  return firstValueFrom(
    factory().pipe(
      retry({ count: maxRetries, delay: (err, attempt) => timer(attempt * 1000) })
    )
  );
}

// Usage:
const result = await fetchWithRetry(
  () => this.api.getReport$(reportId),
  3
);
```

---

## `firstValueFrom` vs `lastValueFrom` vs `toPromise`

```typescript
// toPromise() — DEPRECATED in RxJS 7, removed in RxJS 8
// Equivalent to lastValueFrom but with different empty-stream behavior:
source$.toPromise() // resolves undefined on empty completion — silent bug!

// firstValueFrom — resolves on FIRST emission, cancels the rest
// Use for: HTTP requests, initial values, any single-emission Observable
await firstValueFrom(http.get('/api/user'))        // resolves immediately

// lastValueFrom — waits for COMPLETE, resolves last value
// Use for: reduce/scan aggregations, streams that must finish before reading
await lastValueFrom(prices$.pipe(reduce((a, b) => a + b, 0)))

// Key difference:
// firstValueFrom(interval(1000).pipe(take(5))) → resolves 0 after 1 second
// lastValueFrom(interval(1000).pipe(take(5)))  → resolves 4 after 5 seconds

// EmptyError — both throw if Observable completes without emitting:
await firstValueFrom(EMPTY)  // throws EmptyError
await lastValueFrom(EMPTY)   // throws EmptyError
// Always use { defaultValue } option or ensure the source emits at least once
```

---

## Common Pitfalls

### Awaiting an Infinite Observable

```typescript
// ❌ BehaviorSubject never completes — lastValueFrom hangs forever:
const authSubject = new BehaviorSubject<User | null>(null);
const user = await lastValueFrom(authSubject); // HANGS

// ❌ firstValueFrom on a never-completing stream without take(1) is fine,
//    but lastValueFrom is always wrong for infinite streams:
const count = await lastValueFrom(interval(1000)); // HANGS

// ✅ firstValueFrom for "give me the current value":
const currentUser = await firstValueFrom(
  authSubject.pipe(take(1))  // explicit — though firstValueFrom already unsubscribes after first
);

// ✅ Or add take(1) for clarity when source might not be obviously finite:
const currentUser2 = await firstValueFrom(
  authSubject  // firstValueFrom handles this, but take(1) documents intent
);
```

### Not Handling EmptyError

```typescript
// ❌ EMPTY throws EmptyError — unhandled rejection:
try {
  const val = await firstValueFrom(EMPTY);
} catch (e) {
  // EmptyError: no elements in sequence
}

// ✅ Use defaultValue option — no try/catch needed:
const val = await firstValueFrom(EMPTY, { defaultValue: null });  // null

// ✅ Or filter at the Observable level to guarantee an emission:
const val = await firstValueFrom(
  source$.pipe(
    filter(v => v !== null),
    defaultIfEmpty(FALLBACK)
  )
);
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 2/5
**Key insight**: `firstValueFrom` is the correct replacement for `.toPromise()` — always prefer it over `lastValueFrom` unless you genuinely need to wait for completion (aggregations, `reduce`). The two non-negotiable rules: always add `timeout()` for network operations, and always provide `{ defaultValue }` when the source might complete empty. These two guards prevent 90% of production bugs from Observable→Promise bridges.
