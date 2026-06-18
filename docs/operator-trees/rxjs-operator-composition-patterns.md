# RxJS Operator Composition Patterns

Building custom pipelines from primitives — reusable `OperatorFunction` factories, composing higher-order operators, memoization, and the taxonomy of operator shapes.

---

## The Shape of an Operator

Every RxJS pipeable operator is a function that takes an Observable and returns an Observable:

```typescript
type OperatorFunction<T, R> = (source: Observable<T>) => Observable<R>;
type MonoTypeOperatorFunction<T> = OperatorFunction<T, T>; // input = output type
type UnaryFunction<T, R> = (source: T) => R;

// The simplest possible custom operator:
function double(): MonoTypeOperatorFunction<number> {
  return (source$: Observable<number>) => source$.pipe(
    map(v => v * 2)
  );
}

// Usage identical to built-in operators:
of(1, 2, 3).pipe(double()).subscribe(console.log); // 2, 4, 6

// Parameterized:
function multiply(factor: number): MonoTypeOperatorFunction<number> {
  return source$ => source$.pipe(map(v => v * factor));
}

of(1, 2, 3).pipe(multiply(10)).subscribe(console.log); // 10, 20, 30
```

---

## Pattern 1: Composing Domain-Specific Operators

Group multiple operators into a single named abstraction for your domain:

```typescript
import { pipe, OperatorFunction } from 'rxjs';
import { filter, map, distinctUntilChanged, debounceTime, share } from 'rxjs/operators';

// Domain-specific: normalize and deduplicate search queries:
function normalizeQuery(): OperatorFunction<string, string> {
  return pipe(
    map(q => q.trim().toLowerCase()),
    filter(q => q.length >= 2),
    distinctUntilChanged(),
    debounceTime(300)
  );
}

// Domain-specific: filter to only authenticated user events:
function onlyAuthenticated<T extends { userId: string | null }>(): OperatorFunction<T, T & { userId: string }> {
  return filter((e): e is T & { userId: string } => e.userId !== null);
}

// Domain-specific: retry with exponential backoff and logging:
function withRetry<T>(
  maxAttempts = 3,
  baseDelayMs = 1000
): OperatorFunction<T, T> {
  return pipe(
    retry({
      count: maxAttempts,
      delay: (error, attempt) => {
        const delay = baseDelayMs * Math.pow(2, attempt - 1);
        console.warn(`Retry ${attempt}/${maxAttempts} after ${delay}ms:`, error.message);
        return timer(delay);
      }
    })
  );
}

// Compose all three into a higher-level operator:
function resilientSearch<R>(
  searchFn: (query: string) => Observable<R>
): OperatorFunction<string, R> {
  return pipe(
    normalizeQuery(),
    switchMap(q => searchFn(q).pipe(withRetry(3, 500))),
    share()
  );
}

// Usage — entire search pipeline in one operator:
this.searchInput.valueChanges.pipe(
  resilientSearch(q => this.api.search$(q)),
  takeUntilDestroyed()
).subscribe(results => this.render(results));
```

---

## Pattern 2: Stateful Operator Factories

Some operators need state that lives across emissions — build it inside the factory closure:

```typescript
import { Observable, OperatorFunction } from 'rxjs';

// Rate tracker — emit the emission rate (per second) alongside each value:
function withRate<T>(): OperatorFunction<T, { value: T; ratePerSec: number }> {
  return (source$: Observable<T>) => new Observable(subscriber => {
    const window: number[] = [];

    return source$.subscribe({
      next: value => {
        const now = Date.now();
        window.push(now);
        const cutoff = now - 1000;
        while (window[0] < cutoff) window.shift();

        subscriber.next({ value, ratePerSec: window.length });
      },
      error:    e  => subscriber.error(e),
      complete: () => subscriber.complete()
    });
  });
}

// Running index — attach a monotonic counter to each emission:
function indexed<T>(): OperatorFunction<T, { value: T; index: number }> {
  return (source$: Observable<T>) => {
    let index = 0;
    return source$.pipe(
      map(value => ({ value, index: index++ }))
    );
  };
}

// Change detector — emit only when a projected key changes:
function changedBy<T, K>(keyFn: (v: T) => K): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => {
    let lastKey: K | typeof UNSET = UNSET;
    const UNSET = Symbol('UNSET');

    return source$.pipe(
      filter(v => {
        const key = keyFn(v);
        if (key === lastKey) return false;
        lastKey = key;
        return true;
      })
    );
  };
}

// Usage:
userUpdates$.pipe(
  changedBy(u => u.role),         // only when role changes
  indexed(),                       // attach sequence number
  withRate()                       // attach emission rate
).subscribe(({ value, index, ratePerSec }) =>
  logRoleChange(value.role, index, ratePerSec)
);
```

---

## Pattern 3: Higher-Order Operator Composition

Build operators that themselves compose higher-order operators:

```typescript
import { OperatorFunction, Observable } from 'rxjs';

// Cache results by key — avoid re-fetching for the same key:
function cacheBy<T, R>(
  keyFn:   (v: T) => string,
  fetchFn: (v: T) => Observable<R>
): OperatorFunction<T, R> {
  const cache = new Map<string, R>();

  return (source$: Observable<T>) => source$.pipe(
    mergeMap(value => {
      const key = keyFn(value);
      if (cache.has(key)) {
        return of(cache.get(key)!);
      }
      return fetchFn(value).pipe(
        tap(result => cache.set(key, result))
      );
    })
  );
}

// Paginator — transform a page-number stream into paginated data:
function paginate<T>(
  pageSize: number,
  fetchFn:  (page: number, size: number) => Observable<T[]>
): OperatorFunction<number, { items: T[]; page: number; hasMore: boolean }> {
  return (page$: Observable<number>) => page$.pipe(
    distinctUntilChanged(),
    switchMap(page =>
      fetchFn(page, pageSize).pipe(
        map(items => ({
          items,
          page,
          hasMore: items.length === pageSize
        }))
      )
    )
  );
}

// Usage:
currentPage$.pipe(
  paginate(20, (page, size) => this.api.getOrders$(page, size)),
  takeUntilDestroyed()
).subscribe(({ items, page, hasMore }) => {
  this.orders = items;
  this.showLoadMore = hasMore;
});
```

---

## Pattern 4: The `pipe` Function for Reusable Pipelines

`pipe()` from RxJS composes `OperatorFunction`s into a new operator without needing a full function wrapper:

```typescript
import { pipe } from 'rxjs';

// pipe() creates a pre-composed pipeline:
const searchPipeline = pipe(
  debounceTime(300),
  distinctUntilChanged(),
  filter((q: string) => q.length >= 2),
  map(q => q.trim().toLowerCase())
);

// Reuse the same pipeline in multiple places:
usernameField.valueChanges.pipe(searchPipeline).subscribe(checkUsername$);
productSearch.valueChanges.pipe(searchPipeline).subscribe(searchProducts$);

// pipe() with type parameters for complex operator composition:
const httpSafetyPipeline = <T>() => pipe(
  retry<T>({ count: 3, delay: 1000 }),
  timeout<T>(10_000),
  catchError<T, Observable<T>>(err => {
    notificationService.error(err.message);
    return EMPTY;
  })
);

// Usage:
this.http.get<User[]>('/api/users').pipe(
  httpSafetyPipeline<User[]>()
).subscribe(users => this.users = users);
```

---

## Pattern 5: Operator Testing

Custom operators should be tested in isolation with `TestScheduler` or synchronous sources:

```typescript
import { TestScheduler } from 'rxjs/testing';

// Test the normalizeQuery operator:
describe('normalizeQuery', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) =>
      expect(actual).toEqual(expected)
    );
  });

  it('deduplicates and debounces', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-a-b-c|', {
        a: 'hello',
        b: 'hello', // duplicate — should be dropped
        c: 'world'
      });

      const result$ = source$.pipe(
        normalizeQuery()
      );

      // debounceTime(300) in virtual time = 300 frames
      expectObservable(result$).toBe(
        '300ms a 300ms b|',
        { a: 'hello', b: 'world' }
      );
    });
  });

  it('filters queries shorter than 2 chars', () => {
    const result: string[] = [];
    cold('a-b-c|', { a: 'h', b: 'he', c: 'hel' }).pipe(
      // short-circuit debounce for unit test
      map(q => q.trim().toLowerCase()),
      filter(q => q.length >= 2),
      distinctUntilChanged()
    ).subscribe(v => result.push(v));

    expect(result).toEqual(['he', 'hel']);
  });
});
```

---

## Operator Taxonomy

Understanding what category an operator belongs to guides composition:

```typescript
// TRANSFORM — one-to-one value conversion (no timing, no flattening)
// map, pluck, mapTo, scan, pairwise, timestamp, timeInterval
// Compose: freely, before/after any other operator

// FILTER — reduce emission count without changing values
// filter, take, skip, debounceTime, throttleTime, distinctUntilChanged
// Compose: early in pipeline to reduce work downstream

// FLATTEN — unpack inner Observable into outer stream
// mergeMap, switchMap, concatMap, exhaustMap, mergeAll, switchAll
// Compose: one per pipeline (two flatteners = smell)

// COMBINE — merge multiple streams
// combineLatest, forkJoin, zip, merge, race, withLatestFrom
// Compose: near the top of a pipeline

// SIDE EFFECT — observe without changing
// tap, finalize
// Compose: for logging/debugging, never for logic

// MULTICAST — share subscription
// share, shareReplay, publish, connectable
// Compose: at the END of a pipeline that will be subscribed multiple times

// LIFECYCLE — control subscription boundaries
// takeUntil, takeWhile, takeUntilDestroyed, retry, repeat
// Compose: at the END (lifecycle operators terminate the pipeline)

// Rule of thumb for ordering:
// combine → filter → flatten → transform → side-effect → multicast → lifecycle
```

---

**Key insight**: A custom operator is just a named function returning `source$ => source$.pipe(...)`. Use `pipe()` from RxJS for static compositions, a function returning `pipe(...)` for parameterized pipelines, and a full Observable constructor (`new Observable(...)`) only when you need closure state. Composing domain operators (`normalizeQuery`, `withRetry`, `cacheBy`) is what separates a codebase with raw RxJS scattered everywhere from one with a coherent reactive vocabulary.
