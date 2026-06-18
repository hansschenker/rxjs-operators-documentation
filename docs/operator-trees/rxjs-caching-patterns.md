# Caching Patterns with RxJS

HTTP response caching, invalidation strategies, and stale-while-revalidate patterns using RxJS operators.

---

## Why RxJS Caching?

Angular's `HttpClient` (and `fetch`) makes a new request on every call. RxJS caching adds:

- **Deduplication** — multiple components requesting the same resource → one HTTP call
- **Instant first render** — cached data renders immediately while fresh data loads
- **Offline resilience** — fall back to cache when network fails
- **Bandwidth reduction** — skip unnecessary re-fetches

---

## Pattern 1: `shareReplay(1)` — The Simplest Cache

For data that rarely changes and doesn't need invalidation:

```typescript
import { shareReplay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class ConfigService {
  // One HTTP request, result cached forever (until service is destroyed):
  readonly config$ = this.http.get<AppConfig>('/api/config').pipe(
    shareReplay(1)
  );
}

// Component A subscribes → HTTP request made → result cached
// Component B subscribes → gets cached result immediately, no HTTP request
```

**Caveat**: `shareReplay(1)` with default `refCount: false` keeps the subscription alive even when all subscribers unsubscribe. For application-level singletons this is fine. For component-level use, prefer `shareReplay({ bufferSize: 1, refCount: true })`.

---

## Pattern 2: Cache with Time-Based Expiry

```typescript
import { BehaviorSubject, timer, of } from 'rxjs';
import { switchMap, shareReplay, take } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class ProductCacheService {
  private cache = new Map<string, { data: Product; expires: number }>();

  getProduct(id: string): Observable<Product> {
    const cached = this.cache.get(id);
    if (cached && cached.expires > Date.now()) {
      return of(cached.data); // cache hit — return synchronously
    }

    return this.http.get<Product>(`/api/products/${id}`).pipe(
      tap(product => {
        this.cache.set(id, {
          data:    product,
          expires: Date.now() + 5 * 60 * 1000 // 5 minutes TTL
        });
      }),
      shareReplay(1) // deduplicate concurrent requests for the same id
    );
  }

  invalidate(id: string): void { this.cache.delete(id); }
  invalidateAll(): void        { this.cache.clear(); }
}
```

---

## Pattern 3: Stale-While-Revalidate

Show cached data immediately, fetch fresh data in the background:

```typescript
import { merge, of } from 'rxjs';
import { map, shareReplay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class UserService {
  private cache$ = new BehaviorSubject<User[] | null>(null);

  readonly users$ = this.cache$.pipe(
    switchMap(cached =>
      cached !== null
        ? merge(
            of(cached),                        // 1. Emit cache immediately
            this.fetchUsers().pipe(            // 2. Fetch fresh in background
              tap(fresh => this.cache$.next(fresh))
            )
          )
        : this.fetchUsers().pipe(             // No cache — just fetch
            tap(users => this.cache$.next(users))
          )
    ),
    shareReplay(1)
  );

  private fetchUsers(): Observable<User[]> {
    return this.http.get<User[]>('/api/users');
  }
}

// Consumer sees:
// t=0ms:   old cached data (instant render)
// t=200ms: fresh data replaces it (background fetch complete)
```

---

## Pattern 4: Request Deduplication

Prevent duplicate in-flight requests when multiple components mount simultaneously:

```typescript
import { shareReplay, tap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class DataService {
  private inFlight = new Map<string, Observable<unknown>>();

  fetch<T>(url: string): Observable<T> {
    if (this.inFlight.has(url)) {
      return this.inFlight.get(url) as Observable<T>; // join in-flight request
    }

    const request$ = this.http.get<T>(url).pipe(
      shareReplay(1),
      finalize(() => this.inFlight.delete(url)) // remove when complete
    );

    this.inFlight.set(url, request$);
    return request$;
  }
}

// Three components requesting the same URL simultaneously:
// First call starts HTTP request + stores Observable
// Second + third calls join the same Observable (shareReplay)
// All three get the same response — only one HTTP request made
```

---

## Pattern 5: Optimistic Cache Update

Update cache immediately on write, roll back on error:

```typescript
import { BehaviorSubject, throwError } from 'rxjs';
import { tap, catchError } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class TodoService {
  private todos$ = new BehaviorSubject<Todo[]>([]);

  readonly list$ = this.todos$.asObservable();

  addTodo(text: string): Observable<Todo> {
    const optimistic: Todo = { id: `temp-${Date.now()}`, text, done: false };

    // Optimistic update:
    const current = this.todos$.getValue();
    this.todos$.next([...current, optimistic]);

    return this.http.post<Todo>('/api/todos', { text }).pipe(
      tap(saved => {
        // Replace optimistic with real:
        this.todos$.next(
          this.todos$.getValue().map(t => t.id === optimistic.id ? saved : t)
        );
      }),
      catchError(err => {
        // Roll back:
        this.todos$.next(this.todos$.getValue().filter(t => t.id !== optimistic.id));
        return throwError(() => err);
      })
    );
  }
}
```

---

## Pattern 6: Selective Cache Invalidation

Invalidate cache entries based on events:

```typescript
import { BehaviorSubject, Subject, merge } from 'rxjs';
import { switchMap, filter, tap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class ProductService {
  private invalidate$ = new Subject<string | 'all'>();
  private cache = new Map<string, Observable<Product>>();

  getProduct(id: string): Observable<Product> {
    if (!this.cache.has(id)) {
      const product$ = this.http.get<Product>(`/api/products/${id}`).pipe(
        shareReplay(1),
        takeUntil(
          this.invalidate$.pipe(
            filter(key => key === id || key === 'all')
          )
        )
      );
      this.cache.set(id, product$);
    }
    return this.cache.get(id)!;
  }

  // Call after a product is updated:
  invalidateProduct(id: string): void {
    this.cache.delete(id);
    this.invalidate$.next(id);
  }

  invalidateAll(): void {
    this.cache.clear();
    this.invalidate$.next('all');
  }
}
```

---

## Pattern 7: Pagination Cache

Cache paginated results, invalidate on data change:

```typescript
import { map, shareReplay, tap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class PaginatedService {
  private pageCache = new Map<number, Observable<Page<Item>>>();

  getPage(page: number): Observable<Page<Item>> {
    if (!this.pageCache.has(page)) {
      const page$ = this.http.get<Page<Item>>(`/api/items?page=${page}`).pipe(
        shareReplay(1)
      );
      this.pageCache.set(page, page$);
    }
    return this.pageCache.get(page)!;
  }

  // On item create/update/delete: clear all pages (safest)
  clearCache(): void { this.pageCache.clear(); }

  // Prefetch adjacent pages:
  prefetch(currentPage: number, total: number): void {
    [currentPage - 1, currentPage + 1]
      .filter(p => p >= 1 && p <= total && !this.pageCache.has(p))
      .forEach(p => this.getPage(p).subscribe()); // warm cache
  }
}
```

---

## Cache Strategy Decision Table

| Scenario | Strategy | Implementation |
|---|---|---|
| Rarely changes (config, feature flags) | Permanent cache | `shareReplay(1)` |
| Changes occasionally (user profile) | TTL cache (5-15 min) | `Map` + expiry timestamp |
| Changes frequently but needs instant render | Stale-while-revalidate | `merge(of(cache), fetch$)` |
| Concurrent requests for same resource | Deduplication | `shareReplay(1)` + `inFlight` Map |
| Write → read consistency | Optimistic update | `BehaviorSubject` + rollback |
| Paginated data | Page cache + prefetch | `Map<page, Observable>` |
| Must be fresh on each component mount | No cache | Plain `http.get()` |

---

## Common Pitfalls

### `shareReplay` Without `refCount` on Mutable Data

```typescript
// ❌ Cache never expires — stale data served forever:
const users$ = this.http.get<User[]>('/api/users').pipe(
  shareReplay(1) // refCount: false → lives forever in memory
);

// ✅ For mutable data, invalidate explicitly or use TTL:
// See Pattern 2 (TTL) or Pattern 6 (Selective Invalidation)
```

### Caching Errors

```typescript
// ❌ shareReplay caches errors too — all future subscribers get the error:
const data$ = this.http.get('/api/data').pipe(
  shareReplay(1)
);

data$.subscribe(); // fails, caches the error
data$.subscribe(); // gets the cached error immediately — no retry!

// ✅ Don't cache errors — let them propagate normally:
const data$ = this.http.get('/api/data').pipe(
  shareReplay({ bufferSize: 1, refCount: false, resetOnError: true })
  // RxJS 7: resetOnError resets the cache on error
);
```
