# RxJS HTTP Interceptor Patterns

Angular's `HttpInterceptor` (and the newer functional `HttpInterceptorFn`) is a reactive pipeline — every interceptor returns an `Observable<HttpEvent<T>>`. This guide covers advanced RxJS patterns for auth tokens, retry, caching, and request deduplication.

---

## Interceptor Architecture

```
Request →  [Auth]  →  [Retry]  →  [Cache]  →  [Dedup]  →  HTTP
Response ←  [Auth]  ←  [Log]   ←  [Cache]  ←          ←  HTTP
```

Each interceptor wraps the `next.handle(req)` Observable with RxJS operators. Order matters: the first interceptor in the array is outermost (handles both request and response first/last).

---

## Auth Token Interceptor

### Bearer Token with Automatic Refresh

```typescript
import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError, BehaviorSubject, filter, take, switchMap, catchError } from 'rxjs';

@Injectable()
export class AuthInterceptor implements HttpInterceptor {
  private isRefreshing = false;
  private refreshToken$ = new BehaviorSubject<string | null>(null);

  constructor(private authService: AuthService) {}

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    const token = this.authService.getAccessToken();

    return next.handle(this.addToken(req, token)).pipe(
      catchError((err: HttpErrorResponse) => {
        if (err.status === 401 && !req.url.includes('/auth/refresh')) {
          return this.handle401(req, next);
        }
        return throwError(() => err);
      }),
    );
  }

  private addToken(req: HttpRequest<unknown>, token: string | null): HttpRequest<unknown> {
    if (!token) return req;
    return req.clone({ setHeaders: { Authorization: `Bearer ${token}` } });
  }

  private handle401(
    req: HttpRequest<unknown>,
    next: HttpHandler,
  ): Observable<HttpEvent<unknown>> {
    if (this.isRefreshing) {
      // Queue: wait for ongoing refresh, then retry with new token
      return this.refreshToken$.pipe(
        filter((token): token is string => token !== null),
        take(1),
        switchMap(token => next.handle(this.addToken(req, token))),
      );
    }

    this.isRefreshing = true;
    this.refreshToken$.next(null); // signal "refreshing" to queued requests

    return this.authService.refreshToken().pipe(
      switchMap(({ accessToken }) => {
        this.isRefreshing = false;
        this.refreshToken$.next(accessToken); // unblock queued requests
        this.authService.setAccessToken(accessToken);
        return next.handle(this.addToken(req, accessToken));
      }),
      catchError(err => {
        this.isRefreshing = false;
        this.authService.logout();
        return throwError(() => err);
      }),
    );
  }
}
```

**Key pattern**: `BehaviorSubject<string | null>` as a gate. `null` = refreshing; queued requests `filter(token => token !== null)` and wait. When refresh completes, `next(newToken)` unblocks all waiting requests simultaneously.

---

## Retry Interceptor with Exponential Backoff

```typescript
import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError, timer } from 'rxjs';
import { retryWhen, delayWhen, scan, switchMap, catchError } from 'rxjs/operators';

const RETRYABLE_STATUS_CODES = new Set([408, 429, 500, 502, 503, 504]);

function exponentialBackoff(maxRetries = 3, baseMs = 1000) {
  return retryWhen<HttpEvent<unknown>>(errors$ =>
    errors$.pipe(
      scan((retryCount, error) => {
        if (
          retryCount >= maxRetries ||
          !(error instanceof HttpErrorResponse) ||
          !RETRYABLE_STATUS_CODES.has(error.status)
        ) {
          throw error; // non-retryable — propagate immediately
        }
        return retryCount + 1;
      }, 0),
      delayWhen(retryCount => {
        const delay = Math.min(baseMs * 2 ** retryCount, 30_000);
        console.log(`Retry ${retryCount} in ${delay}ms`);
        return timer(delay);
      }),
    )
  );
}

@Injectable()
export class RetryInterceptor implements HttpInterceptor {
  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    // Skip retry for non-idempotent methods unless explicitly opted in
    if (['POST', 'PATCH'].includes(req.method) && !req.headers.has('X-Retry')) {
      return next.handle(req);
    }

    return next.handle(req).pipe(
      exponentialBackoff(3, 1000),
    );
  }
}
```

---

## Caching Interceptor

```typescript
import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent, HttpResponse } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { tap, shareReplay } from 'rxjs/operators';

interface CacheEntry {
  response: HttpResponse<unknown>;
  expiresAt: number;
}

@Injectable({ providedIn: 'root' })
export class HttpCacheService {
  private cache = new Map<string, CacheEntry>();

  get(key: string): HttpResponse<unknown> | null {
    const entry = this.cache.get(key);
    if (!entry || Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return null;
    }
    return entry.response;
  }

  set(key: string, response: HttpResponse<unknown>, ttlMs: number): void {
    this.cache.set(key, { response, expiresAt: Date.now() + ttlMs });
  }

  invalidate(urlPattern: RegExp): void {
    for (const key of this.cache.keys()) {
      if (urlPattern.test(key)) this.cache.delete(key);
    }
  }
}

@Injectable()
export class CacheInterceptor implements HttpInterceptor {
  constructor(private cache: HttpCacheService) {}

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    // Only cache GET requests with cache-control header
    if (req.method !== 'GET') return next.handle(req);

    const ttl = Number(req.headers.get('X-Cache-TTL'));
    if (!ttl) return next.handle(req);

    const cacheKey = req.urlWithParams;
    const cached = this.cache.get(cacheKey);

    if (cached) {
      return of(cached.clone()); // serve from cache
    }

    return next.handle(req).pipe(
      tap(event => {
        if (event instanceof HttpResponse) {
          this.cache.set(cacheKey, event, ttl);
        }
      }),
    );
  }
}

// Usage: set TTL per request
http.get('/api/config', {
  headers: { 'X-Cache-TTL': '300000' } // 5 minutes
});
```

---

## Request Deduplication Interceptor

Prevent duplicate in-flight GET requests — e.g. when multiple components request the same data simultaneously.

```typescript
import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent } from '@angular/common/http';
import { Observable } from 'rxjs';
import { shareReplay, finalize } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class DedupInterceptor implements HttpInterceptor {
  private inflightRequests = new Map<string, Observable<HttpEvent<unknown>>>();

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    if (req.method !== 'GET') return next.handle(req);

    const key = req.urlWithParams;
    const inflight = this.inflightRequests.get(key);

    if (inflight) {
      return inflight; // return shared Observable — no duplicate HTTP call
    }

    const shared$ = next.handle(req).pipe(
      shareReplay(1),
      finalize(() => this.inflightRequests.delete(key)),
    );

    this.inflightRequests.set(key, shared$);
    return shared$;
  }
}
```

**How it works**: The first request creates a `shareReplay(1)` Observable and stores it. Subsequent identical requests receive the same Observable — they share one HTTP call. `finalize` cleans up after the response.

---

## Logging Interceptor with Timing

```typescript
import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent, HttpResponse } from '@angular/common/http';
import { Observable } from 'rxjs';
import { tap, finalize } from 'rxjs/operators';

@Injectable()
export class LoggingInterceptor implements HttpInterceptor {
  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    const startTime = Date.now();
    let status = 'pending';

    return next.handle(req).pipe(
      tap({
        next: event => {
          if (event instanceof HttpResponse) {
            status = `${event.status}`;
          }
        },
        error: err => {
          status = `error:${err.status ?? 'network'}`;
        },
      }),
      finalize(() => {
        const elapsed = Date.now() - startTime;
        console.log(`[HTTP] ${req.method} ${req.url} → ${status} (${elapsed}ms)`);
      }),
    );
  }
}
```

---

## Functional Interceptors (Angular 15+)

The newer `HttpInterceptorFn` is a pure function — easier to test, no class boilerplate.

```typescript
import { HttpInterceptorFn, HttpRequest, HttpHandlerFn, HttpEvent } from '@angular/common/http';
import { inject } from '@angular/core';
import { Observable } from 'rxjs';

// Auth interceptor as a function
export const authInterceptor: HttpInterceptorFn = (
  req: HttpRequest<unknown>,
  next: HttpHandlerFn,
): Observable<HttpEvent<unknown>> => {
  const authService = inject(AuthService); // DI works via inject()
  const token = authService.getAccessToken();

  const authedReq = token
    ? req.clone({ setHeaders: { Authorization: `Bearer ${token}` } })
    : req;

  return next(authedReq);
};

// Register in app config
export const appConfig: ApplicationConfig = {
  providers: [
    provideHttpClient(
      withInterceptors([
        authInterceptor,
        retryInterceptor,
        cacheInterceptor,
        loggingInterceptor,
      ])
    ),
  ],
};
```

---

## Interceptor Ordering Guide

```
Recommended order (outermost → innermost):
1. Logging       — wrap everything, measure total time
2. Auth          — add token before sending
3. Retry         — retry the authenticated request
4. Cache         — check cache before going to network
5. Dedup         — deduplicate identical in-flight requests
```

**Note**: In Angular's `withInterceptors([a, b, c])`, interceptors are applied in array order for requests and reverse order for responses — `a` sees the request first and the response last.

---

## Related Guides

- **[Authentication Patterns](./rxjs-authentication-patterns.md)** — broader auth with RxJS
- **[Caching Patterns](./rxjs-caching-patterns.md)** — caching beyond HTTP
- **[Error Resilience Patterns](./rxjs-error-resilience-patterns.md)** — retry and recovery
- **[RxJS Error Boundary Patterns](./rxjs-error-boundary-patterns.md)** — error containment architecture
