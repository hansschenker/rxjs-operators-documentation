# defer — Advanced Patterns

For `defer` fundamentals see the core [defer](./defer) doc. This page covers stale closure prevention, retry-with-fresh-state, lazy factories, and `defer` vs `of`/`from`.

---

## The Core Problem `defer` Solves

Without `defer`, an Observable captures values at **creation time**. With `defer`, the factory runs at **subscription time** — giving you fresh values each time.

```typescript
import { defer, of, timer } from 'rxjs';

// ❌ Without defer — captures value at CREATION time:
let count = 0;
const stale$ = of(count); // count is 0 when this runs

count = 42;
stale$.subscribe(v => console.log(v)); // logs 0, not 42 — stale closure!

// ✅ With defer — captures value at SUBSCRIPTION time:
const fresh$ = defer(() => of(count)); // factory runs on each subscribe

count = 42;
fresh$.subscribe(v => console.log(v)); // logs 42 — fresh!
```

---

## Pattern 1: Retry with Fresh State

The most important `defer` use case — when a retry should produce a **new** Observable, not re-use the old one:

```typescript
import { defer, retry } from 'rxjs';

// ❌ Without defer — retry re-subscribes to the SAME Observable instance:
// Works fine for HTTP (new request on re-subscribe), but fails for:
let token = this.auth.getToken(); // captured once
this.api.authenticatedRequest(token).pipe(
  retry(3) // all retries use the original, now-expired token!
)

// ✅ With defer — token is fresh on every retry:
defer(() => {
  const token = this.auth.getToken(); // evaluated on each subscription
  return this.api.authenticatedRequest(token);
}).pipe(
  retry({ count: 3, delay: 1000 }) // each retry gets a fresh token
)
```

---

## Pattern 2: Lazy HTTP Requests

Delay an HTTP request until someone actually subscribes:

```typescript
import { defer } from 'rxjs';

// ❌ Eagerly creates the request — fires even if no one subscribes:
const users$ = this.http.get<User[]>('/api/users'); // request starts NOW

// ✅ defer — request only fires on first subscribe:
const users$ = defer(() => this.http.get<User[]>('/api/users'));

// Subscribe later, request fires then:
setTimeout(() => users$.subscribe(render), 5000); // request fires after 5s
```

---

## Pattern 3: `defer` for Stateful Factories

Each subscriber gets their own independent state:

```typescript
import { defer, interval, scan, takeWhile } from 'rxjs';

// Each subscriber gets their own independent countdown:
function countdown(from: number): Observable<number> {
  return defer(() => {
    let remaining = from; // fresh state per subscription
    return interval(1000).pipe(
      map(() => --remaining),
      takeWhile(n => n >= 0, true)
    );
  });
}

// Two independent countdowns — each starts fresh:
countdown(5).subscribe(n => console.log('Timer A:', n));
countdown(10).subscribe(n => console.log('Timer B:', n));
```

---

## Pattern 4: Conditional Observable Selection at Subscribe Time

```typescript
import { defer, iif } from 'rxjs';

// ❌ iif evaluates the condition at CREATION time:
const isLoggedIn = () => this.auth.isLoggedIn();
const stream$ = iif(
  isLoggedIn,            // called once at creation — may be wrong by subscribe time
  this.api.getPrivateData(),
  this.api.getPublicData()
);

// ✅ defer evaluates at SUBSCRIPTION time:
const stream$ = defer(() =>
  this.auth.isLoggedIn()
    ? this.api.getPrivateData()
    : this.api.getPublicData()
);

// iif is actually sugar for defer with a ternary — but only if sources are also deferred:
// The sources (getPrivateData(), getPublicData()) are evaluated immediately in iif,
// so defer is safer for side-effect-free lazy selection.
```

---

## Pattern 5: `defer` for Mutable State Snapshots

```typescript
import { defer, of } from 'rxjs';
import { map } from 'rxjs/operators';

class CartService {
  private items: CartItem[] = [];

  add(item: CartItem): void { this.items.push(item); }
  remove(id: string): void  { this.items = this.items.filter(i => i.id !== id); }

  // ❌ Snapshot at service construction time — always stale:
  readonly itemsStale$ = of(this.items); // captures array reference once

  // ✅ Fresh snapshot on each subscription:
  readonly items$ = defer(() => of([...this.items])); // copies current array on subscribe
}
```

---

## Pattern 6: Retry with Exponential Backoff and Token Refresh

```typescript
import { defer, timer } from 'rxjs';
import { retry, switchMap, catchError } from 'rxjs/operators';

function authenticatedRequest<T>(
  requestFn: (token: string) => Observable<T>,
  authService: AuthService
): Observable<T> {
  return defer(() => {
    const token = authService.getAccessToken();
    return requestFn(token);
  }).pipe(
    retry({
      count: 3,
      delay: (err, attempt) => {
        if (err.status === 401) {
          // Token expired — refresh before retry:
          return authService.refreshToken().pipe(
            switchMap(() => timer(0)) // retry immediately after refresh
          );
        }
        return timer(1000 * attempt); // exponential backoff for other errors
      }
    })
  );
}
```

---

## Pattern 7: `defer` for Resource Cleanup

```typescript
import { defer, using } from 'rxjs';

// Pair resource acquisition with cleanup using using():
function withDatabaseConnection<T>(
  queryFn: (conn: DbConnection) => Observable<T>
): Observable<T> {
  return using(
    () => {
      const conn = db.connect(); // acquire resource
      return conn;
    },
    (conn) => defer(() => queryFn(conn as DbConnection)) // use resource
    // conn.unsubscribe() = conn.release() — cleanup on unsubscribe
  );
}

withDatabaseConnection(conn =>
  from(conn.query('SELECT * FROM users'))
).subscribe(users => render(users));
// Connection auto-released when Observable completes or unsubscribes
```

---

## `defer` vs `of` / `from` / Direct Creation

| | Creates Observable | Executes factory | Use when |
|---|---|---|---|
| `of(value)` | Immediately | At creation | Value is static / already computed |
| `from(promise)` | Immediately | Promise started at creation | Promise already in-flight |
| `defer(() => of(fn()))` | On subscribe | At each subscription | Value may change; factory has side effects |
| `new Observable(fn)` | On subscribe | At each subscription | Full control over subscribe/unsubscribe logic |

`defer(() => someObservable$)` is equivalent to `new Observable(sub => someObservable$.subscribe(sub))` for most purposes — `defer` is the simpler syntax.

---

## Common Pitfalls

### Forgetting That HTTP Already Creates a New Request on Each Subscribe

```typescript
// ❌ Unnecessary defer — http.get() already creates a new request on each subscribe:
const users$ = defer(() => this.http.get<User[]>('/api/users'));
// http.get() is already "lazy" — it starts a request on each subscription

// ✅ defer is needed for synchronous values or closures that must be fresh:
const token$ = defer(() => of(this.auth.getToken())); // token read at subscribe time
```

### Using `defer` When `BehaviorSubject` Is More Appropriate

```typescript
// ❌ defer re-reads state on every subscribe — not reactive to changes:
const currentUser$ = defer(() => of(this.userService.currentUser));
// Only fires once per subscription — won't react to user changes

// ✅ BehaviorSubject for reactive mutable state:
const currentUser$ = this.userService.currentUser$; // BehaviorSubject — emits on change
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Primary use case**: Prevent stale closures in retry scenarios, especially `retry` with auth tokens or mutable configuration. If a value should be read at subscription time rather than creation time, `defer` is the answer.
