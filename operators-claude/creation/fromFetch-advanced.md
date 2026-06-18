# fromFetch — Advanced Patterns

For `fromFetch` fundamentals see the core [fromFetch](./fromFetch) doc. This page covers automatic cancellation, streaming responses, request interceptors, and error normalization.

---

## Why `fromFetch` Over `HttpClient`?

`fromFetch` is the native browser `fetch` wrapped as an Observable. It automatically calls `AbortController.abort()` when the subscriber unsubscribes — perfect for `switchMap` scenarios where in-flight requests should be cancelled.

```typescript
import { fromFetch } from 'rxjs/fetch';
import { switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

// Auto-cancels previous request when query changes:
searchQuery$.pipe(
  debounceTime(300),
  switchMap(q =>
    fromFetch(`/api/search?q=${encodeURIComponent(q)}`).pipe(
      switchMap(res => res.ok ? res.json() : of({ error: res.status })),
      catchError(() => of({ error: 'network' }))
    )
  )
).subscribe(renderResults);
// When new query arrives mid-flight: AbortController cancels the previous fetch
```

---

## Pattern 1: JSON Fetching with Error Normalization

```typescript
import { fromFetch } from 'rxjs/fetch';
import { switchMap, catchError } from 'rxjs/operators';

interface ApiError { status: number; message: string; }

function fetchJson<T>(url: string, init?: RequestInit): Observable<T> {
  return fromFetch(url, init).pipe(
    switchMap(async res => {
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw Object.assign(new Error(body.message ?? res.statusText), {
          status: res.status,
          body
        });
      }
      return res.json() as Promise<T>;
    }),
    catchError(err => {
      // Network error (no response):
      if (!err.status) throw new Error('Network unavailable');
      throw err;
    })
  );
}

// Usage:
fetchJson<User[]>('/api/users').subscribe(renderUsers);
```

---

## Pattern 2: Streaming Response (NDJSON / Server-Sent Events)

```typescript
import { fromFetch } from 'rxjs/fetch';
import { switchMap, expand, filter, map, takeWhile } from 'rxjs/operators';
import { EMPTY, from } from 'rxjs';

// Read newline-delimited JSON stream line by line:
function fetchNDJSON<T>(url: string): Observable<T> {
  return fromFetch(url).pipe(
    switchMap(res => {
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      return new Observable<T>(observer => {
        const pump = (): Promise<void> =>
          reader.read().then(({ done, value }) => {
            if (done) { observer.complete(); return; }

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() ?? '';  // keep incomplete last line

            for (const line of lines) {
              if (line.trim()) {
                try   { observer.next(JSON.parse(line) as T); }
                catch { observer.error(new Error(`Invalid JSON: ${line}`)); }
              }
            }
            return pump();
          });

        pump().catch(err => observer.error(err));
        return () => reader.cancel();  // cancel stream on unsubscribe
      });
    })
  );
}

fetchNDJSON<LogEntry>('/api/logs/stream').pipe(
  takeUntilDestroyed()
).subscribe(appendLog);
```

---

## Pattern 3: Request Interceptor Pattern

```typescript
import { fromFetch } from 'rxjs/fetch';

type RequestInterceptor = (url: string, init: RequestInit) => [string, RequestInit];
type ResponseInterceptor = (res: Response) => Observable<Response>;

class FetchClient {
  private requestInterceptors:  RequestInterceptor[]  = [];
  private responseInterceptors: ResponseInterceptor[] = [];

  addRequestInterceptor(fn: RequestInterceptor): this {
    this.requestInterceptors.push(fn);
    return this;
  }

  addResponseInterceptor(fn: ResponseInterceptor): this {
    this.responseInterceptors.push(fn);
    return this;
  }

  fetch<T>(url: string, init: RequestInit = {}): Observable<T> {
    // Apply request interceptors:
    let [finalUrl, finalInit] = this.requestInterceptors.reduce(
      ([u, i], fn) => fn(u, i),
      [url, init] as [string, RequestInit]
    );

    return fromFetch(finalUrl, finalInit).pipe(
      // Apply response interceptors in sequence:
      ...this.responseInterceptors.map(fn => switchMap(fn)),
      switchMap(res => res.json() as Promise<T>)
    );
  }
}

// Usage:
const client = new FetchClient()
  .addRequestInterceptor((url, init) => [
    url,
    { ...init, headers: { ...init.headers, Authorization: `Bearer ${getToken()}` } }
  ])
  .addResponseInterceptor(res => {
    if (res.status === 401) {
      return refreshToken().pipe(switchMap(() => fromFetch(res.url)));
    }
    return of(res);
  });

client.fetch<User[]>('/api/users').subscribe(renderUsers);
```

---

## Pattern 4: Request Deduplication (In-Flight Cache)

```typescript
import { fromFetch } from 'rxjs/fetch';
import { shareReplay, finalize } from 'rxjs/operators';

class DeduplicatingFetch {
  private inFlight = new Map<string, Observable<unknown>>();

  fetch<T>(url: string, init?: RequestInit): Observable<T> {
    const key = `${init?.method ?? 'GET'}:${url}`;

    if (this.inFlight.has(key)) {
      return this.inFlight.get(key) as Observable<T>;
    }

    const request$ = fromFetch(url, init).pipe(
      switchMap(res => res.json() as Promise<T>),
      shareReplay(1),
      finalize(() => this.inFlight.delete(key))
    );

    this.inFlight.set(key, request$);
    return request$;
  }
}
```

---

## Pattern 5: Fetch with Timeout

```typescript
import { fromFetch } from 'rxjs/fetch';
import { timeout, catchError } from 'rxjs/operators';

function fetchWithTimeout<T>(url: string, ms = 5000): Observable<T> {
  return fromFetch(url).pipe(
    timeout({
      each: ms,
      with: () => throwError(() => new Error(`Request to ${url} timed out after ${ms}ms`))
    }),
    switchMap(res => {
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      return res.json() as Promise<T>;
    })
  );
}
```

---

## `fromFetch` vs `HttpClient` vs `fetch` directly

```typescript
// fromFetch — native fetch as Observable, auto-cancels on unsubscribe:
fromFetch('/api/data').pipe(switchMap(r => r.json()))
// ✓ Auto-cancel via AbortController
// ✓ No Angular dependency
// ✗ No interceptor system, no type safety, no progress events

// HttpClient — Angular's HTTP client:
this.http.get<Data>('/api/data')
// ✓ Interceptor pipeline (auth, logging, error handling)
// ✓ Typed responses
// ✓ Progress events (reportProgress: true)
// ✗ Angular-only
// Note: HttpClient also auto-cancels with switchMap

// fetch directly — promise-based:
from(fetch('/api/data').then(r => r.json()))
// ✗ No auto-cancel
// ✗ Not cold — fetch starts immediately
// ✓ Familiar API
```

**Rule**: Use `fromFetch` in framework-agnostic code (libraries, web components). Use `HttpClient` in Angular apps. Use `defer(() => fetch(...))` when you just need lazy evaluation without auto-cancel.

---

## Common Pitfalls

### Not Handling the Response Body

```typescript
// ❌ fromFetch emits the Response object, not the body:
fromFetch('/api/data').subscribe(data => renderData(data));
// data is a Response, not parsed JSON!

// ✅ Always pipe through res.json() or res.text():
fromFetch('/api/data').pipe(
  switchMap(res => res.json())
).subscribe(renderData);
```

### Non-OK Responses Are Not Errors

```typescript
// ❌ A 404 or 500 response doesn't trigger catchError:
fromFetch('/api/data').pipe(
  switchMap(res => res.json()),
  catchError(err => of(fallback)) // 404 never reaches here!
)

// ✅ Check res.ok explicitly:
fromFetch('/api/data').pipe(
  switchMap(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }),
  catchError(err => of(fallback))
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key advantage**: `fromFetch` automatically cancels in-flight requests via `AbortController` when the subscriber unsubscribes — making it the ideal pairing with `switchMap` in search/autocomplete/routing scenarios. Its main limitation vs `HttpClient` is the absence of a built-in interceptor system; use Pattern 3 above to add one.
