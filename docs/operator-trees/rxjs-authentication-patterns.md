# Authentication Patterns with RxJS

Token refresh, interceptors, guards, and session management.

---

## Pattern 1: Token Refresh Interceptor

Automatically refresh an expired token, retry the original request:

```typescript
import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent } from '@angular/common/http';
import { BehaviorSubject, throwError, Observable } from 'rxjs';
import { catchError, filter, switchMap, take, tap } from 'rxjs/operators';

@Injectable()
export class TokenRefreshInterceptor implements HttpInterceptor {
  private refreshing$ = new BehaviorSubject<boolean>(false);
  private token$      = new BehaviorSubject<string | null>(null);

  constructor(private auth: AuthService) {}

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    return next.handle(this.addToken(req)).pipe(
      catchError(err => {
        if (err.status !== 401 || req.url.includes('/auth/refresh')) {
          return throwError(() => err);
        }

        if (this.refreshing$.getValue()) {
          // Another request already refreshing — wait for new token:
          return this.token$.pipe(
            filter(t => t !== null),
            take(1),
            switchMap(token => next.handle(this.addToken(req, token!)))
          );
        }

        this.refreshing$.next(true);

        return this.auth.refreshToken().pipe(
          tap(token => {
            this.token$.next(token);
            this.refreshing$.next(false);
          }),
          switchMap(token => next.handle(this.addToken(req, token))),
          catchError(refreshErr => {
            this.refreshing$.next(false);
            this.auth.logout();
            return throwError(() => refreshErr);
          })
        );
      })
    );
  }

  private addToken(req: HttpRequest<unknown>, token?: string): HttpRequest<unknown> {
    const t = token ?? this.auth.getToken();
    return t ? req.clone({ setHeaders: { Authorization: `Bearer ${t}` } }) : req;
  }
}
```

---

## Pattern 2: Auth State Stream

Single source of truth for auth state across the app:

```typescript
import { BehaviorSubject, of } from 'rxjs';
import { switchMap, shareReplay, map } from 'rxjs/operators';

interface AuthState {
  user:  User | null;
  token: string | null;
}

@Injectable({ providedIn: 'root' })
export class AuthService {
  private state$ = new BehaviorSubject<AuthState>({ user: null, token: null });

  readonly isLoggedIn$ = this.state$.pipe(map(s => s.user !== null), distinctUntilChanged());
  readonly user$       = this.state$.pipe(map(s => s.user));
  readonly token$      = this.state$.pipe(map(s => s.token));

  // Rehydrate from storage on app init:
  init(): void {
    const token = localStorage.getItem('token');
    if (token) {
      this.api.getProfile(token).subscribe({
        next:  user  => this.state$.next({ user, token }),
        error: ()    => localStorage.removeItem('token')
      });
    }
  }

  login(creds: Credentials): Observable<User> {
    return this.api.login(creds).pipe(
      tap(({ user, token }) => {
        localStorage.setItem('token', token);
        this.state$.next({ user, token });
      }),
      map(r => r.user)
    );
  }

  logout(): void {
    localStorage.removeItem('token');
    this.state$.next({ user: null, token: null });
    this.router.navigate(['/login']);
  }

  getToken(): string | null { return this.state$.getValue().token; }
}
```

---

## Pattern 3: Auth Guard

```typescript
import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { map, take } from 'rxjs/operators';

export const authGuard: CanActivateFn = () => {
  const auth   = inject(AuthService);
  const router = inject(Router);

  return auth.isLoggedIn$.pipe(
    take(1),
    map(loggedIn => loggedIn || router.createUrlTree(['/login']))
  );
};

// Role guard:
export const roleGuard = (requiredRole: string): CanActivateFn => () => {
  const auth   = inject(AuthService);
  const router = inject(Router);

  return auth.user$.pipe(
    take(1),
    map(user =>
      user?.role === requiredRole
        ? true
        : router.createUrlTree(['/unauthorized'])
    )
  );
};
```

---

## Pattern 4: Protect API Calls — Wait for Auth

Defer API calls until the user is authenticated:

```typescript
import { switchMap, filter, take } from 'rxjs/operators';

// Only execute API call after user is confirmed logged in:
function whenAuthenticated<T>(apiCall$: Observable<T>): Observable<T> {
  return inject(AuthService).isLoggedIn$.pipe(
    filter(Boolean),
    take(1),
    switchMap(() => apiCall$)
  );
}

// Usage:
whenAuthenticated(this.api.getProfile()).subscribe(renderProfile);
```

---

## Pattern 5: Session Timeout Detection

```typescript
import { fromEvent, merge, timer } from 'rxjs';
import { switchMap, tap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class SessionTimeoutService {
  private readonly TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

  readonly sessionExpired$ = merge(
    fromEvent(document, 'click'),
    fromEvent(document, 'keydown'),
    fromEvent(document, 'mousemove')
  ).pipe(
    switchMap(() => timer(this.TIMEOUT_MS)), // reset timer on any activity
    tap(() => this.auth.logout())
  );

  start(): Subscription {
    return this.sessionExpired$.subscribe();
  }
}
```

---

## Common Pitfall: Multiple Concurrent Refresh Requests

The pattern in Pattern 1 handles this correctly — the `refreshing$` gate ensures only one token refresh runs at a time while queuing other requests to retry with the new token.

```typescript
// ❌ Without the gate: 3 simultaneous 401s → 3 refresh calls → race condition
// ✅ With BehaviorSubject gate: first refresh runs; others wait on token$
```
