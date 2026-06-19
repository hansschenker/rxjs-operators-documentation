# RxJS Error Boundary Patterns

Global error handling architecture for RxJS streams — containing failures, preserving liveness, and surfacing errors to the right layer.

---

## The Core Problem: Errors Terminate Streams

In RxJS, an unhandled error destroys the Observable. It cannot recover:

```
Source:  --1--2--#        (# = error)
         --------         (stream dead — no more emissions ever)
```

An error in a long-lived stream (WebSocket connection, user action stream, polling loop) is catastrophic if not contained. Error boundary patterns isolate failures so streams survive.

---

## Layer Architecture

```
┌─────────────────────────────────────────┐
│  Global Error Handler (uncaught sink)   │  ← last resort
├─────────────────────────────────────────┤
│  Feature Error Boundary                 │  ← per feature/route
├─────────────────────────────────────────┤
│  Stream-Level Recovery                  │  ← catchError / retry
├─────────────────────────────────────────┤
│  Operator-Level Guards                  │  ← safe transforms
└─────────────────────────────────────────┘
```

---

## Layer 1: Operator-Level Guards

Prevent errors from entering the stream in the first place.

```typescript
import { map, filter } from 'rxjs/operators';
import { Observable } from 'rxjs';

// Safe map — catches synchronous transform errors
function safeMap<T, R>(
  project: (value: T) => R,
  fallback: R,
): OperatorFunction<T, R> {
  return map(value => {
    try {
      return project(value);
    } catch {
      return fallback;
    }
  });
}

// Safe JSON parse
const parsed$ = rawMessages$.pipe(
  safeMap(msg => JSON.parse(msg) as ApiMessage, null),
  filter((msg): msg is ApiMessage => msg !== null),
);

// Safe type narrowing with type guard
function filterType<T, R extends T>(
  guard: (v: T) => v is R
): OperatorFunction<T, R> {
  return source$ => source$.pipe(
    filter(guard),
  );
}
```

---

## Layer 2: Stream-Level Recovery with catchError

`catchError` is the primary recovery mechanism. The key decision: **what to return from catchError**.

```typescript
import { catchError, retry, retryWhen, timer } from 'rxjs';
import { switchMap, delayWhen } from 'rxjs/operators';

// Pattern A: Replace with fallback value
const safeData$ = apiCall$.pipe(
  catchError(err => {
    logger.warn('API failed, using cache', err);
    return cache.get('last-known');
  }),
);

// Pattern B: Re-throw after logging (preserve error for caller)
const logged$ = apiCall$.pipe(
  catchError(err => {
    logger.error('API error', { err, context: 'dashboard' });
    return throwError(() => err); // re-throw
  }),
);

// Pattern C: Convert to loading state (error as value)
type LoadState<T> = { ok: true; data: T } | { ok: false; error: string };

const stateStream$: Observable<LoadState<User>> = userApi$.pipe(
  map(data => ({ ok: true, data }) as LoadState<User>),
  catchError(err => of({ ok: false, error: err.message } as LoadState<User>)),
);

// Pattern D: Exponential backoff retry
const withBackoff$ = apiCall$.pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      delayWhen((_, attempt) => timer(Math.min(1000 * 2 ** attempt, 30_000))),
      take(5), // max 5 retries
    )
  ),
);
```

---

## Layer 3: Feature Error Boundary

Isolate failures per feature so one broken feature doesn't crash the whole app.

```typescript
import { Subject, Observable, EMPTY } from 'rxjs';
import { catchError, shareReplay } from 'rxjs/operators';

interface ErrorBoundaryOptions<T> {
  fallback?: T;
  onError?: (err: unknown) => void;
}

function withErrorBoundary<T>(
  source$: Observable<T>,
  options: ErrorBoundaryOptions<T> = {},
): Observable<T> {
  const { fallback, onError = console.error } = options;

  return source$.pipe(
    catchError(err => {
      onError(err);
      return fallback !== undefined ? of(fallback) : EMPTY;
    }),
  );
}

// Feature-level usage
class DashboardService {
  private errorReporter = inject(ErrorReportingService);

  readonly metrics$ = withErrorBoundary(
    this.metricsApi.stream$,
    {
      fallback: DEFAULT_METRICS,
      onError: err => this.errorReporter.report('metrics-stream', err),
    }
  ).pipe(shareReplay(1));

  readonly notifications$ = withErrorBoundary(
    this.notificationsApi.stream$,
    { onError: err => this.errorReporter.report('notifications', err) }
  );
}
```

---

## Layer 4: Global Error Handler

Catch all errors that escape lower layers. In Angular this is `ErrorHandler`; in plain apps, a global Subject.

```typescript
import { Subject, Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';

// Framework-agnostic global error bus
class GlobalErrorBus {
  private errors$ = new Subject<{ error: unknown; context: string }>();
  readonly stream$ = this.errors$.asObservable();

  report(error: unknown, context: string): void {
    this.errors$.next({ error, context });
  }
}

const globalErrors = new GlobalErrorBus();

// Subscribe to global errors for reporting
globalErrors.stream$.pipe(
  tap(({ error, context }) => {
    Sentry.captureException(error, { tags: { context } });
  }),
  // Throttle to avoid flooding error service on rapid failures
  throttleTime(1000),
).subscribe();

// Angular integration
@Injectable()
export class RxJsErrorHandler implements ErrorHandler {
  handleError(error: unknown): void {
    globalErrors.report(error, 'angular');
    console.error('[GlobalErrorHandler]', error);
  }
}
```

---

## Long-Lived Stream Resurrection

For streams that must stay alive forever (WebSocket, polling), use `retry` with restart logic.

```typescript
import { webSocket } from 'rxjs/webSocket';
import { retryWhen, delayWhen, tap, timer, scan } from 'rxjs/operators';

interface ConnectionState {
  attempt: number;
  lastError: unknown;
}

function resilientWebSocket<T>(url: string): Observable<T> {
  return webSocket<T>(url).pipe(
    retryWhen(errors$ =>
      errors$.pipe(
        scan((state, error): ConnectionState => ({
          attempt: state.attempt + 1,
          lastError: error,
        }), { attempt: 0, lastError: null }),
        tap(({ attempt, lastError }) => {
          const delay = Math.min(1000 * 2 ** attempt, 60_000);
          console.warn(`WebSocket reconnecting in ${delay}ms (attempt ${attempt})`, lastError);
        }),
        delayWhen(({ attempt }) => timer(Math.min(1000 * 2 ** attempt, 60_000))),
      )
    ),
  );
}

// Usage: never terminates due to error
const liveData$ = resilientWebSocket<MarketData>('wss://feed.example.com').pipe(
  shareReplay(1),
);
```

---

## Error Boundary for Inner Observables

`mergeMap`/`switchMap` inner Observables can error without killing the outer stream — but only with explicit protection.

```typescript
import { mergeMap, catchError } from 'rxjs/operators';
import { EMPTY, of } from 'rxjs';

// ❌ One inner error kills the entire outer stream
userIds$.pipe(
  mergeMap(id => fetchUser(id)), // if fetchUser errors, outer stream dies
).subscribe();

// ✅ Isolate inner errors — outer stream continues
userIds$.pipe(
  mergeMap(id =>
    fetchUser(id).pipe(
      catchError(err => {
        console.error(`Failed to fetch user ${id}:`, err);
        return EMPTY; // skip this user, continue with others
      })
    )
  ),
).subscribe(user => renderUser(user));

// ✅ With typed error result
type UserResult = { ok: true; user: User } | { ok: false; id: string; error: string };

userIds$.pipe(
  mergeMap(id =>
    fetchUser(id).pipe(
      map(user => ({ ok: true, user }) as UserResult),
      catchError(err => of({ ok: false, id, error: err.message } as UserResult)),
    )
  ),
).subscribe(result => {
  if (result.ok) renderUser(result.user);
  else logFailure(result.id, result.error);
});
```

---

## Error Boundary Decision Guide

```
Error should kill the stream (expected terminal error)?
  → Let it propagate naturally → subscribe error handler

Error should be logged but stream should continue?
  → catchError → EMPTY (or restart source)

Error should show a fallback value?
  → catchError → of(fallback)

Error should be visible as a value (loading state pattern)?
  → catchError → of({ ok: false, error })

Error in inner Observable (mergeMap/switchMap)?
  → catchError inside the inner pipe → EMPTY or of(fallback)

Network error with retry?
  → retryWhen with exponential backoff

Permanent long-lived stream resurrection?
  → retryWhen with cap + logging
```

---

## Anti-Patterns

```typescript
// ❌ INCORRECT — empty catchError swallows errors silently
source$.pipe(
  catchError(() => EMPTY), // error disappears with no trace
).subscribe();

// ✅ CORRECT — always log before returning EMPTY
source$.pipe(
  catchError(err => {
    console.error('[source$] error:', err);
    return EMPTY;
  }),
).subscribe();


// ❌ INCORRECT — catchError outside inner pipe (kills outer stream)
requests$.pipe(
  switchMap(req => http.post(req.url, req.body)),
  catchError(() => EMPTY), // if any request fails, ALL future requests stop
).subscribe();

// ✅ CORRECT — catchError inside the inner Observable
requests$.pipe(
  switchMap(req =>
    http.post(req.url, req.body).pipe(
      catchError(err => of({ error: err, req })), // isolate per request
    )
  ),
).subscribe();
```

---

## Related Guides

- **[Error Handling Patterns](./error-handling-patterns.md)** — operator-level guide
- **[RxJS Error Resilience Patterns](./rxjs-error-resilience-patterns.md)** — resilience patterns
- **[Debugging Guide](./debugging-guide.md)** — diagnosing stream failures
- **[WebSocket Patterns](./rxjs-websocket-patterns.md)** — reconnection strategies
