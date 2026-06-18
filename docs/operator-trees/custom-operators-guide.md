# Writing Custom RxJS Operators

Custom operators let you encapsulate reusable pipeline logic into named, composable functions. Any repeated `pipe(...)` sequence is a candidate.

---

## The Two Operator Types

| Type | TypeScript signature | Use when |
|---|---|---|
| `MonoTypeOperatorFunction<T>` | `Observable<T> → Observable<T>` | Output type = input type (filter, delay, tap) |
| `OperatorFunction<T, R>` | `Observable<T> → Observable<R>` | Output type differs from input (map, scan, buffer) |

---

## Pattern 1 — Wrapping Existing Operators (Most Common)

The simplest custom operator is a function that returns the result of `pipe(...)`:

```typescript
import { pipe } from 'rxjs';
import { MonoTypeOperatorFunction } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter } from 'rxjs/operators';

// Reusable search input pipeline
function searchInput(debounceMs = 300): MonoTypeOperatorFunction<string> {
  return pipe(
    debounceTime(debounceMs),
    distinctUntilChanged(),
    filter(q => q.length >= 2)
  );
}

// Usage:
searchBox$.pipe(
  searchInput(400),
  switchMap(q => api.search(q))
).subscribe(render);
```

Use `pipe()` (the standalone function from `rxjs`) to compose operators — it produces a function `Observable<T> → Observable<T>` directly.

---

## Pattern 2 — Operator with Transform (`OperatorFunction<T, R>`)

```typescript
import { OperatorFunction } from 'rxjs';
import { map, filter } from 'rxjs/operators';

interface ApiResponse<T> { data: T; status: number; }

// Extract successful HTTP responses
function extractData<T>(): OperatorFunction<ApiResponse<T>, T> {
  return pipe(
    filter((res): res is ApiResponse<T> & { status: 200 } => res.status === 200),
    map(res => res.data)
  );
}

// Usage:
http.get<ApiResponse<User>>('/api/user').pipe(
  extractData<User>()
).subscribe(user => render(user));
// TypeScript knows result is User, not ApiResponse<User>
```

---

## Pattern 3 — Full Custom Operator with `new Observable`

For operators that need to manage internal state or subscriptions not expressible via composition:

```typescript
import { Observable, OperatorFunction } from 'rxjs';

// Emit values with their running index
function withIndex<T>(): OperatorFunction<T, [T, number]> {
  return (source: Observable<T>): Observable<[T, number]> => {
    return new Observable(subscriber => {
      let index = 0;
      return source.subscribe({
        next:     value    => subscriber.next([value, index++]),
        error:    err      => subscriber.error(err),
        complete: ()       => subscriber.complete(),
      });
    });
  };
}

// Usage:
from(['a', 'b', 'c']).pipe(
  withIndex()
).subscribe(([v, i]) => console.log(i, v));
// 0 a
// 1 b
// 2 c
```

**Important**: Always forward `error` and `complete` unless your operator intentionally intercepts them. Missing `complete` causes memory leaks; missing `error` swallows failures.

---

## Pattern 4 — Reusable Debug Operator

```typescript
import { tap } from 'rxjs/operators';
import { MonoTypeOperatorFunction } from 'rxjs';

function debug<T>(tag: string): MonoTypeOperatorFunction<T> {
  return tap({
    subscribe:   ()  => console.log(`[${tag}] subscribed`),
    next:        v   => console.log(`[${tag}]`, v),
    error:       e   => console.error(`[${tag}] error:`, e),
    complete:    ()  => console.log(`[${tag}] complete`),
    unsubscribe: ()  => console.log(`[${tag}] unsubscribed`)
  });
}

source$.pipe(
  debug('raw'),
  debounceTime(300),
  debug('debounced')
).subscribe(render);
```

---

## Pattern 5 — Retry with Exponential Backoff

```typescript
import { pipe, timer } from 'rxjs';
import { retry } from 'rxjs/operators';
import { MonoTypeOperatorFunction } from 'rxjs';

function retryWithBackoff<T>(
  maxRetries = 3,
  baseDelayMs = 1000
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (error, attempt) => {
      const delay = Math.min(baseDelayMs * 2 ** (attempt - 1), 30_000);
      console.warn(`Retry ${attempt}/${maxRetries} in ${delay}ms`, error.message);
      return timer(delay);
    }
  });
}

// Usage:
apiCall$.pipe(
  retryWithBackoff(4, 500),
  catchError(() => of(FALLBACK))
).subscribe(render);
```

---

## Pattern 6 — Operator That Tracks Loading State

```typescript
import { Observable, BehaviorSubject } from 'rxjs';
import { OperatorFunction } from 'rxjs';
import { tap, finalize } from 'rxjs/operators';

function withLoading<T>(loading$: BehaviorSubject<boolean>): MonoTypeOperatorFunction<T> {
  return (source: Observable<T>) => {
    loading$.next(true);
    return source.pipe(
      finalize(() => loading$.next(false))
    );
  };
}

const isLoading$ = new BehaviorSubject(false);

apiCall$.pipe(
  withLoading(isLoading$)
).subscribe({
  next: render,
  error: showError // loading clears via finalize regardless
});
```

---

## TypeScript Typing Rules

```typescript
// MonoTypeOperatorFunction<T>: input and output types are the same
// Use when your operator doesn't change the value type
function myFilter<T>(pred: (v: T) => boolean): MonoTypeOperatorFunction<T> {
  return filter(pred);
}

// OperatorFunction<T, R>: different input and output types
// Use when your operator transforms the type
function myMap<T, R>(fn: (v: T) => R): OperatorFunction<T, R> {
  return map(fn);
}

// Generic operator that preserves type:
function withTimestamp<T>(): OperatorFunction<T, { value: T; time: number }> {
  return map(value => ({ value, time: Date.now() }));
}
```

---

## Common Mistakes

### Not Forwarding Complete / Error

```typescript
// ❌ MEMORY LEAK — complete never fires downstream
function broken<T>(): MonoTypeOperatorFunction<T> {
  return (source: Observable<T>) => new Observable(subscriber => {
    return source.subscribe({
      next: v => subscriber.next(v),
      // missing error and complete!
    });
  });
}

// ✅ CORRECT — always forward all three
function correct<T>(): MonoTypeOperatorFunction<T> {
  return (source: Observable<T>) => new Observable(subscriber => {
    return source.subscribe({
      next:     v  => subscriber.next(v),
      error:    e  => subscriber.error(e),
      complete: () => subscriber.complete(),
    });
  });
  // WHY: Subscribers and operators downstream rely on complete/error
  // to know the stream has ended. Missing them keeps the stream open
  // forever and prevents downstream cleanup (finalize, takeUntil, etc.)
}
```

### Calling `pipe()` Inside Every Subscription

```typescript
// ❌ INEFFICIENT — pipe() is called fresh per subscribe
function debounced<T>(ms: number): MonoTypeOperatorFunction<T> {
  return (source: Observable<T>) => source.pipe(debounceTime(ms)); // fine actually

  // But this creates unnecessary overhead vs the idiomatic form:
}

// ✅ IDIOMATIC — use the standalone pipe() helper
import { pipe } from 'rxjs';
function debounced<T>(ms: number): MonoTypeOperatorFunction<T> {
  return pipe(debounceTime(ms));
  // pipe() returns a reusable operator function — more efficient, more readable
}
```

---

## When to Extract a Custom Operator

Extract when:
- The same `pipe(op1, op2, op3)` sequence appears 3+ times
- The pipeline has a meaningful domain name (e.g., `normalizeSearchInput`, `retryWithBackoff`)
- You want to unit-test the transformation in isolation
- The operator has configuration parameters that vary by call site

Don't extract:
- One-off pipelines used in a single place
- Simple single-operator aliases (`const doubled = map((x: number) => x * 2)` — just use `map`)
