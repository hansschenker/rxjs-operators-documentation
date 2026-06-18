# fromFetch

## Identity

- **Name**: fromFetch
- **Category**: Creation Operators (Interop)
- **Type**: Fetch wrapper — wraps the native `fetch` API as a cold, cancellable Observable
- **Import**:
  ```typescript
  import { fromFetch } from 'rxjs/fetch';
  ```
- **Signature**:
  ```typescript
  function fromFetch<T>(
    input: string | Request,
    init?: RequestInit & { selector?: (response: Response) => ObservableInput<T> }
  ): Observable<Response | T>
  ```

## Functional Specification

`fromFetch` wraps `window.fetch` (or a configured fetch implementation) as an Observable. It is:
- **Cold**: A new `fetch` call is made per subscription
- **Cancellable**: Unsubscribing triggers `AbortController.abort()` — the network request is cancelled
- **Lazy**: The fetch doesn't start until subscription

**Without `selector`**: Emits the `Response` object, then completes. You are responsible for calling `.json()`, `.text()`, etc.

**With `selector`**: Passes the `Response` to your function and emits the selector's Observable result. Errors if `response.ok` is false unless you check it in the selector.

**`fromFetch` vs `ajax`**:

| | `fromFetch` | `ajax` |
|---|---|---|
| Underlying API | `fetch` (native/Streams) | `XMLHttpRequest` |
| Cancellation | `AbortController` | XHR abort |
| Upload progress | No | Yes (XHR) |
| Streaming body | Yes (ReadableStream) | No |
| `4xx/5xx` errors | NOT errors by default | Throws `AjaxError` |
| Import | `rxjs/fetch` | `rxjs/ajax` |

**Important**: `fetch` resolves on HTTP 4xx/5xx responses — they are NOT errors. You must check `response.ok` yourself (or use a `selector`).

## Marble Diagram

```
fromFetch('/api/data')    [cold — new fetch per subscribe]
                          subscribed
Result:   ---Response--|  (one Response object then complete)

fromFetch('/api/data', { selector: r => r.json() })
Result:   ---{data}--|    (parsed JSON then complete)

Unsubscribe before response:
subscribe → network request starts
unsubscribe → AbortController.abort() → request cancelled, network freed
```

## Type System Integration

```typescript
import { fromFetch } from 'rxjs/fetch';

// Without selector: Observable<Response>
fromFetch('/api/user').subscribe((r: Response) => r.json().then(console.log));

// With selector: Observable<T>
fromFetch<User>('/api/user', {
  selector: response => {
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<User>;
  }
}).subscribe((user: User) => console.log(user.name));
```

## Examples

### Basic Usage — GET Request
```typescript
import { fromFetch } from 'rxjs/fetch';
import { switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

// Without selector — handle Response manually
fromFetch('/api/users').pipe(
  switchMap(response => {
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  }),
  catchError(err => {
    console.error(err);
    return of([]);
  })
).subscribe(users => renderList(users));
```

### Common Pattern — With Selector (Recommended)
```typescript
import { fromFetch } from 'rxjs/fetch';

// selector form — cleaner HTTP error handling
fromFetch<User[]>('/api/users', {
  selector: response => {
    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
    }
    return response.json() as Promise<User[]>;
  }
}).subscribe({
  next:  users => renderList(users),
  error: err   => showError(err.message)
});
```

### Common Pattern — POST with Body
```typescript
import { fromFetch } from 'rxjs/fetch';

fromFetch<{ id: string }>('/api/users', {
  method:  'POST',
  headers: { 'Content-Type': 'application/json' },
  body:    JSON.stringify({ name: 'Alice', email: 'alice@example.com' }),
  selector: response => {
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<{ id: string }>;
  }
}).subscribe(({ id }) => console.log('Created user:', id));
```

### Common Pattern — Cancellation with `switchMap`
```typescript
import { fromEvent } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';
import { switchMap, map } from 'rxjs/operators';

// Auto-cancel previous request when new search term arrives
fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  switchMap(query =>
    fromFetch<Result[]>(`/api/search?q=${encodeURIComponent(query)}`, {
      selector: r => r.json() as Promise<Result[]>
    })
  )
).subscribe(results => renderResults(results));
// switchMap unsubscribes from previous fromFetch → AbortController aborts the XHR
```

### Common Pattern — Streaming Response
```typescript
import { fromFetch } from 'rxjs/fetch';
import { switchMap } from 'rxjs/operators';
import { Observable } from 'rxjs';

// Stream a large response line by line
fromFetch('/api/stream').pipe(
  switchMap(response => new Observable<string>(observer => {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();

    function read() {
      reader.read().then(({ done, value }) => {
        if (done) { observer.complete(); return; }
        observer.next(decoder.decode(value));
        read();
      }).catch(err => observer.error(err));
    }
    read();
    return () => reader.cancel();
  }))
).subscribe(chunk => processChunk(chunk));
```

## Common Pitfalls

### Anti-pattern: Missing `response.ok` Check
```typescript
import { fromFetch } from 'rxjs/fetch';

// ❌ WRONG — 404/500 responses are NOT errors in fetch
fromFetch('/api/users').pipe(
  switchMap(r => r.json()) // parses 404 response body as "success"!
).subscribe(data => console.log(data)); // logs error body, not data

// ✅ CORRECT — always check response.ok
fromFetch('/api/users', {
  selector: r => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }
}).subscribe({
  next:  data => console.log(data),
  error: err  => console.error(err.message)
});

// WHY: Unlike XMLHttpRequest-based ajax(), native fetch() treats HTTP
// error responses (4xx, 5xx) as resolved Promises — not rejections.
// The Observable contract follows the same: no automatic error on bad status.
```

### Anti-pattern: Using `fromFetch` for Upload Progress
```typescript
// ❌ fromFetch cannot report upload progress — fetch API limitation
fromFetch('/api/upload', {
  method: 'POST',
  body: largeFile,
  // no onUploadProgress option — fetch doesn't support it
});

// ✅ Use ajax() for upload progress monitoring
import { ajax } from 'rxjs/ajax';
ajax({
  url: '/api/upload',
  method: 'POST',
  body: largeFile,
  // XHR supports progress events via underlying XMLHttpRequest
}).subscribe(response => console.log('uploaded'));
// For actual progress events, use XMLHttpRequest directly
```

## Related Operators

- **`ajax`**: XHR-based alternative — auto-throws on 4xx/5xx, supports upload progress
- **`defer`**: Like `fromFetch` for custom lazy creation — create the Observable factory at subscription time
- **`switchMap`**: The standard companion — cancel-and-replace on new trigger

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/fetch/fromFetch](https://rxjs.dev/api/fetch/fromFetch)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching points**:
1. Import from `rxjs/fetch` (not `rxjs`) — separate entry point
2. HTTP 4xx/5xx are NOT errors — always check `response.ok` (or use `selector`)
3. Cancels the network request on unsubscribe via `AbortController` — pairs naturally with `switchMap`
