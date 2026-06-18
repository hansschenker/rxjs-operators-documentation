# ajax

## Identity

- **Name**: ajax
- **Category**: Creation Operators (Interop)
- **Type**: HTTP client — creates an Observable that sends an HTTP request on subscription and emits the response
- **Import**:
  ```typescript
  import { ajax } from 'rxjs/ajax';
  import { AjaxResponse, AjaxError } from 'rxjs/ajax';
  ```
- **Signature**:
  ```typescript
  // Shorthand for GET requests
  ajax(url: string): Observable<AjaxResponse<unknown>>
  ajax.getJSON<T>(url: string, headers?: Record<string, string>): Observable<T>
  ajax.post<T>(url: string, body?: any, headers?: Record<string, string>): Observable<AjaxResponse<T>>
  ajax.put<T>(url: string, body?: any, headers?: Record<string, string>): Observable<AjaxResponse<T>>
  ajax.patch<T>(url: string, body?: any, headers?: Record<string, string>): Observable<AjaxResponse<T>>
  ajax.delete<T>(url: string, headers?: Record<string, string>): Observable<AjaxResponse<T>>

  // Full config form
  ajax(config: AjaxConfig): Observable<AjaxResponse<unknown>>

  interface AjaxConfig {
    url: string;
    method?: string;
    headers?: Record<string, string>;
    body?: any;
    async?: boolean;
    timeout?: number;
    crossDomain?: boolean;
    withCredentials?: boolean;
    responseType?: XMLHttpRequestResponseType;
  }
  ```

## Functional Specification

**Concept**: `ajax` is RxJS's built-in HTTP creation operator. It wraps `XMLHttpRequest` (XHR) in an Observable, making HTTP requests naturally composable with the full RxJS operator ecosystem.

**Key properties**:
- **Cold Observable**: request is sent only on subscription; each subscriber sends its own request
- **Single emission**: emits exactly one `AjaxResponse` (or `AjaxError`) then completes
- **Cancellable**: unsubscription aborts the XHR — works naturally with `switchMap` to cancel in-flight requests
- **`getJSON<T>`**: convenience method that extracts `response.body` directly, typed as `T`
- **Error handling**: HTTP errors (4xx, 5xx) are thrown as `AjaxError` — unlike native `fetch`, which only rejects on network failure

## Marble Diagram

```
ajax.getJSON('/api/users'):

subscribe() → XHR starts
              ↓ (server responds)
Result:    --------[User[]]|    (single emission, then complete)

On network error or 4xx/5xx:
Result:    --------#            (AjaxError thrown)

With switchMap cancellation:
search$.pipe(switchMap(q => ajax.getJSON(`/api?q=${q}`))):

q='rx':     ----[results]|
q='rxjs' arrives mid-flight:
q='rx' request → ABORTED (switchMap unsubscribes → XHR aborted)
q='rxjs':        --------[results]|
```

## Type System Integration

```typescript
import { ajax } from 'rxjs/ajax';
import { AjaxResponse, AjaxError } from 'rxjs/ajax';

interface User { id: number; name: string; email: string }

// getJSON — returns T directly
const users$: Observable<User[]> = ajax.getJSON<User[]>('/api/users');

// Full config — returns AjaxResponse<T>
const response$: Observable<AjaxResponse<User>> = ajax<User>({
  url: '/api/users/1',
  method: 'GET',
  headers: { 'Authorization': `Bearer ${token}` }
});

response$.subscribe(res => {
  console.log(res.status);   // 200
  console.log(res.response); // User object
});

// Error handling with AjaxError type
users$.subscribe({
  error: (err: AjaxError) => {
    console.log(err.status);   // 404, 500, etc.
    console.log(err.message);
    console.log(err.response); // error response body
  }
});
```

## Examples

### Basic Usage
```typescript
import { ajax } from 'rxjs/ajax';
import { map, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// GET — simplest form
ajax.getJSON<User[]>('/api/users').subscribe(console.log);

// POST with body
ajax.post('/api/users', { name: 'Alice', email: 'alice@example.com' }).subscribe(
  res => console.log('created:', res.response)
);

// Full config for auth headers
ajax<User>({
  url: '/api/profile',
  method: 'GET',
  headers: {
    'Authorization': `Bearer ${authToken}`,
    'Content-Type': 'application/json'
  }
}).pipe(
  map(res => res.response),
  catchError(err => {
    console.error('Profile fetch failed:', err.status);
    return EMPTY;
  })
).subscribe(renderProfile);
```

### Common Pattern — Search with `switchMap` (Cancellation)
```typescript
import { ajax } from 'rxjs/ajax';
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged, switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

interface SearchResult { id: number; title: string }

const input = document.querySelector<HTMLInputElement>('#search')!;

fromEvent<InputEvent>(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query =>
    query.length === 0
      ? of([])
      : ajax.getJSON<SearchResult[]>(`/api/search?q=${encodeURIComponent(query)}`).pipe(
          catchError(() => of([]))  // silently empty on error
        )
  )
).subscribe(results => renderResults(results));
// Each new keystroke cancels the previous in-flight XHR via switchMap
```

### Common Pattern — CRUD Operations
```typescript
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';

const API = '/api/todos';
const authHeader = () => ({ 'Authorization': `Bearer ${getToken()}` });

// Read
const getTodos = () =>
  ajax.getJSON<Todo[]>(API, authHeader());

// Create
const createTodo = (todo: Partial<Todo>) =>
  ajax.post<Todo>(API, todo, { ...authHeader(), 'Content-Type': 'application/json' }).pipe(
    map(res => res.response)
  );

// Update
const updateTodo = (id: number, changes: Partial<Todo>) =>
  ajax.patch<Todo>(`${API}/${id}`, changes, { ...authHeader(), 'Content-Type': 'application/json' }).pipe(
    map(res => res.response)
  );

// Delete
const deleteTodo = (id: number) =>
  ajax.delete(`${API}/${id}`, authHeader()).pipe(
    map(() => id)
  );
```

### Common Pattern — Retry on Failure
```typescript
import { ajax } from 'rxjs/ajax';
import { retry, catchError } from 'rxjs/operators';
import { of, timer } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

ajax.getJSON<Data>('/api/data').pipe(
  retry({
    count: 3,
    delay: (error: AjaxError, retryCount) => {
      // Don't retry 4xx client errors
      if (error.status >= 400 && error.status < 500) {
        return throwError(() => error);
      }
      return timer(retryCount * 1000); // exponential-ish backoff for 5xx
    }
  }),
  catchError((err: AjaxError) => {
    console.error(`Failed after retries. Status: ${err.status}`);
    return of(null);
  })
).subscribe(data => data && renderData(data));
```

## Common Pitfalls

### Anti-pattern: Subscribing to `ajax` Multiple Times Without `shareReplay`
```typescript
import { ajax } from 'rxjs/ajax';

const users$ = ajax.getJSON<User[]>('/api/users'); // cold Observable

// ❌ TWO REQUESTS — each subscribe() sends a new XHR
users$.subscribe(renderTable);
users$.subscribe(renderCount); // second HTTP request!

// ✅ CORRECT — share one request across multiple subscribers
import { shareReplay } from 'rxjs/operators';
const users$ = ajax.getJSON<User[]>('/api/users').pipe(shareReplay(1));

users$.subscribe(renderTable);  // one request
users$.subscribe(renderCount);  // reuses cached response
```

### Anti-pattern: Using `ajax` for Fire-and-Forget Without Subscribing
```typescript
import { ajax } from 'rxjs/ajax';

// ❌ NOTHING HAPPENS — ajax is cold; no subscribe = no XHR sent
function logEvent(event: Event): void {
  ajax.post('/api/log', event); // creates Observable but never subscribes!
}

// ✅ CORRECT — subscribe to send the request (and handle errors)
function logEvent(event: Event): void {
  ajax.post('/api/log', event).subscribe({
    error: err => console.warn('Log failed (non-critical):', err.status)
  });
}

// WHY: ajax returns a cold Observable. The HTTP request is sent ONLY when
// subscribe() is called. Calling ajax(...) without subscribe() is a no-op.
```

### Anti-pattern: Ignoring `AjaxError` on 4xx/5xx
```typescript
import { ajax } from 'rxjs/ajax';

// ❌ MISSING ERROR HANDLER — 404/500 throw AjaxError, crash if unhandled
ajax.getJSON('/api/user/999').subscribe(user => {
  renderUser(user); // never called on 404; AjaxError propagates uncaught
});

// ✅ CORRECT — always handle errors
ajax.getJSON<User>('/api/user/999').subscribe({
  next:  user => renderUser(user),
  error: (err: AjaxError) => {
    if (err.status === 404) showNotFound();
    else showGenericError(err.message);
  }
});

// WHY: Unlike fetch(), ajax() throws AjaxError for HTTP error status codes
// (4xx, 5xx), not just network failures. Always provide an error handler.
```

## Related Operators

- **`fromFetch`** (`rxjs/fetch`): Wraps the native `fetch` API — returns `Response` (not parsed body); no automatic 4xx/5xx error throwing
- **`defer(() => ajax(...))`**: Ensure fresh token/config on each subscription/retry
- **`switchMap`**: Essential companion — cancels previous in-flight requests when new trigger arrives
- **`retry`**: Automatic retry on `AjaxError` — use `delay` function to skip 4xx errors
- **`shareReplay(1)`**: Cache the response for multiple subscribers

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/ajax/ajax](https://rxjs.dev/api/ajax/ajax)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key teaching points**:
1. Cold Observable — HTTP fires only on `subscribe()`. No subscribe = no request.
2. `getJSON<T>` returns `T` directly; `ajax<T>(config)` returns `AjaxResponse<T>`
3. 4xx/5xx throw `AjaxError` — unlike native `fetch` (which only rejects on network failure)
4. `switchMap` + `ajax` = automatic XHR cancellation on new trigger
