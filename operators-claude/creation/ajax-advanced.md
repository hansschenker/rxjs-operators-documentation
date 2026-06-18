# ajax — Advanced Patterns

For `ajax` fundamentals see the core [ajax](./ajax) doc. This page covers typed requests, interceptors, retry, progress events, and `ajax` vs `fromFetch` vs `HttpClient`.

---

## Why `ajax` Over `fetch`

`rxjs/ajax` provides a fully Observable HTTP client — observable from subscription, cancellable on unsubscribe, with built-in JSON parsing, progress events, and cross-browser XHR compatibility. It works in both browser and Node.js (via `XMLHttpRequest` polyfill).

```typescript
import { ajax } from 'rxjs/ajax';

ajax.getJSON<User[]>('/api/users').subscribe(users => renderUsers(users));
// Cancels in-flight XHR when subscriber unsubscribes (e.g., via switchMap)
```

---

## Pattern 1: Typed CRUD Client

```typescript
import { ajax, AjaxConfig } from 'rxjs/ajax';

const API_BASE = '/api/v1';

function get<T>(path: string, config?: Partial<AjaxConfig>): Observable<T> {
  return ajax.getJSON<T>(`${API_BASE}${path}`, config?.headers);
}

function post<T, B = unknown>(path: string, body: B): Observable<T> {
  return ajax<T>({
    method:  'POST',
    url:     `${API_BASE}${path}`,
    headers: { 'Content-Type': 'application/json' },
    body
  }).pipe(map(res => res.response));
}

function put<T, B = unknown>(path: string, body: B): Observable<T> {
  return ajax<T>({
    method:  'PUT',
    url:     `${API_BASE}${path}`,
    headers: { 'Content-Type': 'application/json' },
    body
  }).pipe(map(res => res.response));
}

function del<T>(path: string): Observable<T> {
  return ajax<T>({ method: 'DELETE', url: `${API_BASE}${path}` })
    .pipe(map(res => res.response));
}

// Usage:
get<User[]>('/users').subscribe(renderUsers);
post<User, CreateUserDto>('/users', { name: 'Alice' }).subscribe(newUser => {
  console.log('Created:', newUser.id);
});
```

---

## Pattern 2: Request Interceptor Pipeline

Add auth headers, logging, and error normalization to every request:

```typescript
import { ajax, AjaxConfig, AjaxResponse } from 'rxjs/ajax';
import { catchError, tap } from 'rxjs/operators';

type Interceptor = (config: AjaxConfig) => AjaxConfig;
type ResponseInterceptor<T> = (res: AjaxResponse<T>) => AjaxResponse<T>;

class AjaxClient {
  private requestInterceptors:  Interceptor[]        = [];
  private responseInterceptors: ResponseInterceptor<unknown>[] = [];

  useRequest(interceptor: Interceptor): this {
    this.requestInterceptors.push(interceptor);
    return this;
  }

  useResponse(interceptor: ResponseInterceptor<unknown>): this {
    this.responseInterceptors.push(interceptor);
    return this;
  }

  request<T>(config: AjaxConfig): Observable<T> {
    const finalConfig = this.requestInterceptors.reduce(
      (cfg, fn) => fn(cfg), config
    );

    return ajax<T>(finalConfig).pipe(
      ...this.responseInterceptors.map(fn => map(fn as ResponseInterceptor<T>)),
      map(res => res.response),
      catchError(err => {
        // Normalize AjaxError to application error:
        const appErr = new Error(
          err.response?.message ?? err.message ?? 'Request failed'
        );
        (appErr as any).status  = err.status;
        (appErr as any).details = err.response;
        return throwError(() => appErr);
      })
    );
  }
}

// Setup:
const client = new AjaxClient()
  .useRequest(cfg => ({
    ...cfg,
    headers: {
      ...cfg.headers,
      Authorization: `Bearer ${tokenStore.getToken()}`
    }
  }))
  .useRequest(cfg => ({
    ...cfg,
    headers: { ...cfg.headers, 'X-Request-Id': crypto.randomUUID() }
  }));

client.request<User[]>({ method: 'GET', url: '/api/users' }).subscribe(renderUsers);
```

---

## Pattern 3: Upload with Progress Events

`ajax` exposes XHR progress via `includeUploadProgress`:

```typescript
import { ajax } from 'rxjs/ajax';
import { filter, map, scan } from 'rxjs/operators';

interface UploadProgress {
  loaded:    number;
  total:     number;
  percent:   number;
  done:      boolean;
  response?: unknown;
}

function uploadWithProgress<T>(
  url:  string,
  file: File
): Observable<UploadProgress & { response?: T }> {
  const formData = new FormData();
  formData.append('file', file);

  return ajax<T>({
    method:               'POST',
    url,
    body:                 formData,
    includeUploadProgress: true
  }).pipe(
    map(event => {
      if (event.type === 'upload_progress') {
        return {
          loaded:  event.loaded,
          total:   event.total ?? 0,
          percent: event.total ? Math.round((event.loaded / event.total) * 100) : 0,
          done:    false
        };
      }
      // Final AjaxResponse (type === 'download_load'):
      return {
        loaded:   file.size,
        total:    file.size,
        percent:  100,
        done:     true,
        response: event.response
      };
    })
  );
}

// Usage:
uploadWithProgress<UploadResult>('/api/uploads', selectedFile).subscribe({
  next: ({ percent, done, response }) => {
    updateProgressBar(percent);
    if (done) showSuccess(response);
  },
  error: err => showError(err.message)
});
```

---

## Pattern 4: Retry with Backoff and Token Refresh

```typescript
import { ajax } from 'rxjs/ajax';
import { retry, catchError, switchMap } from 'rxjs/operators';
import { timer } from 'rxjs';

function resilientAjax<T>(config: AjaxConfig): Observable<T> {
  return ajax<T>(config).pipe(
    map(res => res.response),
    retry({
      count: 3,
      delay: (error, attempt) => {
        if (error.status === 401) {
          // Token expired — refresh and retry:
          return authService.refreshToken().pipe(
            tap(token => tokenStore.setToken(token))
          );
        }
        if (error.status === 429) {
          // Rate limited — respect Retry-After header:
          const retryAfter = error.xhr.getResponseHeader('Retry-After');
          return timer(retryAfter ? parseInt(retryAfter) * 1000 : 5000);
        }
        if (error.status >= 500) {
          // Server error — exponential backoff:
          return timer(1000 * Math.pow(2, attempt - 1));
        }
        // Client errors (4xx except 401/429) — don't retry:
        return throwError(() => error);
      }
    })
  );
}
```

---

## Pattern 5: Request Cancellation with `switchMap`

```typescript
import { ajax } from 'rxjs/ajax';
import { switchMap, debounceTime } from 'rxjs/operators';

// Search: cancels previous XHR when new query arrives:
searchQuery$.pipe(
  debounceTime(300),
  switchMap(q =>
    ajax.getJSON<SearchResult[]>(`/api/search?q=${encodeURIComponent(q)}`).pipe(
      catchError(() => of([]))
    )
  )
).subscribe(renderResults);
// When switchMap unsubscribes from previous ajax$, RxJS calls xhr.abort()
```

---

## `ajax` vs `fromFetch` vs Angular `HttpClient`

```typescript
// ajax (rxjs/ajax) — XMLHttpRequest-based:
ajax.getJSON<T>('/api/data')
// ✓ Auto-cancels via xhr.abort() on unsubscribe
// ✓ Upload progress events (includeUploadProgress)
// ✓ Works in Node.js (with polyfill)
// ✓ No framework dependency
// ✗ Older API; less composable than fetch
// Use: framework-agnostic apps, file upload with progress, Node.js

// fromFetch (rxjs/fetch) — Fetch API-based:
fromFetch('/api/data').pipe(switchMap(r => r.json()))
// ✓ Auto-cancels via AbortController on unsubscribe
// ✓ Modern, composable API
// ✗ No upload progress (fetch limitation)
// ✗ Requires manual response body parsing + error checking
// Use: modern browsers, streaming responses (NDJSON, SSE)

// Angular HttpClient — Angular DI-based:
this.http.get<T>('/api/data')
// ✓ Interceptor pipeline (auth, logging, retry)
// ✓ Auto-cancels on unsubscribe
// ✓ Response progress, typed responses
// ✗ Angular-only
// Use: Angular applications
```

---

## Common Pitfalls

### Forgetting `.response` on `ajax()` vs `ajax.getJSON()`

```typescript
// ❌ ajax() returns AjaxResponse, not the response body:
ajax<User[]>({ method: 'GET', url: '/api/users' }).subscribe(
  res => renderUsers(res) // res is AjaxResponse<User[]>, not User[]!
);

// ✅ Either use ajax.getJSON() or map to .response:
ajax.getJSON<User[]>('/api/users').subscribe(renderUsers);
// OR:
ajax<User[]>({ method: 'GET', url: '/api/users' }).pipe(
  map(res => res.response)
).subscribe(renderUsers);
```

### Ajax Errors Are `AjaxError`, Not Standard HTTP Errors

```typescript
// ❌ Assuming error.status exists like a plain Error:
ajax.getJSON('/api/users').pipe(
  catchError((err: Error) => {
    console.log(err.message); // message exists
    console.log(err.status);  // undefined! Error doesn't have .status
    return EMPTY;
  })
)

// ✅ Type as AjaxError to access HTTP details:
import { AjaxError } from 'rxjs/ajax';

ajax.getJSON('/api/users').pipe(
  catchError((err: AjaxError) => {
    console.log(err.status);   // 404, 500, etc.
    console.log(err.response); // parsed response body
    console.log(err.xhr);      // raw XMLHttpRequest
    return EMPTY;
  })
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**When to use `ajax`**: Choose `ajax` over `fromFetch` when you need upload progress events, Node.js compatibility, or a simple typed JSON client without Angular. For browser-only fetch with streaming, prefer `fromFetch`. For Angular, `HttpClient` wins every time.
