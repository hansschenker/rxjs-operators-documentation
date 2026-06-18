# Building Custom Operators — Advanced Guide

Pipeable operator signatures, TypeScript generics, higher-order composition, testing, and production-quality operator design patterns.

---

## The Pipeable Operator Signature

A custom pipeable operator is a function that takes an Observable and returns an Observable:

```typescript
import { Observable, MonoTypeOperatorFunction, OperatorFunction } from 'rxjs';

// MonoTypeOperatorFunction<T>: same input and output type
function myOperator<T>(): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>): Observable<T> => {
    return new Observable<T>(subscriber => {
      const sub = source$.subscribe({
        next:     value  => subscriber.next(value),
        error:    err    => subscriber.error(err),
        complete: ()     => subscriber.complete()
      });
      return () => sub.unsubscribe(); // teardown
    });
  };
}

// OperatorFunction<T, R>: transforms input type T to output type R
function mapToString<T>(): OperatorFunction<T, string> {
  return (source$: Observable<T>): Observable<string> =>
    source$.pipe(map(v => String(v)));
}
```

---

## Pattern 1: Composing Built-In Operators

The simplest and most reliable approach — compose existing operators into a named unit:

```typescript
import { pipe } from 'rxjs';
import { filter, map, distinctUntilChanged, debounceTime } from 'rxjs/operators';

// Reusable search input operator:
function searchInput(minLength = 2, debounceMs = 300) {
  return pipe(
    debounceTime(debounceMs),
    map((v: string) => v.trim()),
    filter(v => v.length >= minLength || v.length === 0),
    distinctUntilChanged()
  );
}

// Usage:
searchBox$.pipe(searchInput(3, 400)).subscribe(query => search(query));

// Strongly typed predicate operator:
function filterNonNull<T>(): OperatorFunction<T | null | undefined, T> {
  return filter((v): v is T => v !== null && v !== undefined);
}

stream$.pipe(filterNonNull()).subscribe(v => {
  // v is T, never null/undefined
});
```

---

## Pattern 2: Stateful Custom Operators

Operators that maintain state across emissions:

```typescript
import { Observable, OperatorFunction } from 'rxjs';

// Emit only when value changes from the perspective of a selector:
function distinctUntilChangedBy<T, K>(
  selector: (value: T) => K,
  equals:   (a: K, b: K) => boolean = (a, b) => a === b
): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) =>
    new Observable<T>(subscriber => {
      let lastKey: K;
      let hasLast = false;

      return source$.subscribe({
        next: value => {
          const key = selector(value);
          if (!hasLast || !equals(lastKey, key)) {
            lastKey  = key;
            hasLast  = true;
            subscriber.next(value);
          }
        },
        error:    err => subscriber.error(err),
        complete: ()  => subscriber.complete()
      });
    });
}

// Usage:
userEvents$.pipe(
  distinctUntilChangedBy(
    e => e.userId,
    (a, b) => a === b
  )
).subscribe(handleUserChange);
```

---

## Pattern 3: Higher-Order Operator (Wraps Inner Observables)

```typescript
import { Observable, OperatorFunction } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// retryWithBackoff: a higher-order operator wrapping retry logic
function retryWithBackoff<T>(
  maxRetries:  number,
  baseDelayMs: number
): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) =>
    source$.pipe(
      retry({
        count: maxRetries,
        delay: (err, attempt) => {
          if (err.status >= 400 && err.status < 500) {
            return throwError(() => err); // don't retry client errors
          }
          return timer(baseDelayMs * Math.pow(2, attempt - 1));
        }
      })
    );
}

// Operator that adds a loading state wrapper:
interface WithLoading<T> { loading: boolean; value: T | null; error: Error | null; }

function withLoadingState<T>(): OperatorFunction<T, WithLoading<T>> {
  return (source$: Observable<T>) =>
    source$.pipe(
      map(value  => ({ loading: false, value, error: null })),
      startWith(  { loading: true,  value: null, error: null }),
      catchError(err => of({ loading: false, value: null, error: err as Error }))
    );
}

// Usage:
userRequest$.pipe(
  switchMap(id => this.api.getUser(id).pipe(withLoadingState()))
).subscribe(state => renderWithLoading(state));
```

---

## Pattern 4: Operator with Side-Effects (Tap-Style)

```typescript
// Debug operator — logs emissions with a label, passes values through unchanged:
function debug<T>(
  label:   string,
  options: { next?: boolean; error?: boolean; complete?: boolean } = {}
): MonoTypeOperatorFunction<T> {
  const { next = true, error = true, complete = true } = options;

  return tap({
    next:     next     ? v   => console.log(`[${label}] next:`,     v)   : undefined,
    error:    error    ? err => console.error(`[${label}] error:`,  err) : undefined,
    complete: complete ? ()  => console.log(`[${label}] complete`)       : undefined
  });
}

// Performance profiling operator:
function profile<T>(label: string): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => {
    return new Observable<T>(subscriber => {
      const start = performance.now();
      let count   = 0;

      return source$.subscribe({
        next: value => {
          count++;
          subscriber.next(value);
        },
        error: err => subscriber.error(err),
        complete: () => {
          console.log(`[${label}] ${count} emissions in ${(performance.now() - start).toFixed(1)}ms`);
          subscriber.complete();
        }
      });
    });
  };
}
```

---

## Pattern 5: Parameterized Operator Factory

```typescript
// Buffer emissions until a signal fires, then flush:
function bufferUntil<T>(
  signal$: Observable<unknown>
): OperatorFunction<T, T[]> {
  return (source$: Observable<T>) =>
    new Observable<T[]>(subscriber => {
      let buffer: T[] = [];

      const sub = source$.subscribe({
        next:     value => { buffer.push(value); },
        error:    err   => subscriber.error(err),
        complete: ()    => {
          if (buffer.length) subscriber.next(buffer);
          subscriber.complete();
        }
      });

      const signalSub = signal$.subscribe(() => {
        if (buffer.length) {
          subscriber.next(buffer);
          buffer = [];
        }
      });

      return () => {
        sub.unsubscribe();
        signalSub.unsubscribe();
      };
    });
}

// Usage:
mouseMove$.pipe(
  bufferUntil(mouseUp$) // collect drag positions, flush on release
).subscribe(positions => processDragPath(positions));
```

---

## Pattern 6: TypeScript-Safe `ofType` (Discriminated Unions)

```typescript
type AppEvent =
  | { type: 'USER_LOADED'; user: User }
  | { type: 'ERROR';       error: Error }
  | { type: 'RESET' };

// Type-safe event filter:
function ofType<T extends { type: string }, K extends T['type']>(
  ...types: K[]
): OperatorFunction<T, Extract<T, { type: K }>> {
  return filter((event): event is Extract<T, { type: K }> =>
    types.includes(event.type as K)
  );
}

// Usage — TypeScript narrows the type correctly:
events$.pipe(
  ofType<AppEvent, 'USER_LOADED'>('USER_LOADED')
).subscribe(event => {
  // event.type === 'USER_LOADED', event.user: User ✓
  renderUser(event.user);
});

events$.pipe(
  ofType<AppEvent, 'USER_LOADED' | 'ERROR'>('USER_LOADED', 'ERROR')
).subscribe(event => {
  // event is { type: 'USER_LOADED'; user: User } | { type: 'ERROR'; error: Error }
});
```

---

## Testing Custom Operators

```typescript
import { TestScheduler } from 'rxjs/testing';

describe('filterNonNull', () => {
  let scheduler: TestScheduler;
  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) =>
      expect(actual).toEqual(expected)
    );
  });

  it('removes null and undefined', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-n-b-u-c|', { a: 1, n: null, b: 2, u: undefined, c: 3 });
      const result$ = source$.pipe(filterNonNull());
      expectObservable(result$).toBe('a---b---c|', { a: 1, b: 2, c: 3 });
    });
  });
});

describe('retryWithBackoff', () => {
  it('retries server errors with delay', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--#', {}, { status: 503 });
      const result$ = source$.pipe(retryWithBackoff(2, 10));
      // First attempt fails at 2, retry 1 waits 10ms (total 12), retry 2 waits 20ms (total 32):
      expectObservable(result$).toBe('-------------------------------#', {}, { status: 503 });
    });
  });
});
```

---

## Common Pitfalls

### Not Returning the Teardown in `new Observable`

```typescript
// ❌ Missing teardown — subscription leaks when unsubscribed:
function myOp<T>(): MonoTypeOperatorFunction<T> {
  return source$ => new Observable<T>(subscriber => {
    const sub = source$.subscribe(subscriber);
    // Missing: return () => sub.unsubscribe()
  });
}

// ✅ Always return teardown logic:
function myOp<T>(): MonoTypeOperatorFunction<T> {
  return source$ => new Observable<T>(subscriber => {
    const sub = source$.subscribe(subscriber);
    return () => sub.unsubscribe();
  });
}
```

### Forgetting to Propagate Errors and Completion

```typescript
// ❌ Only handling next — errors and completion swallowed:
return source$ => new Observable(subscriber => {
  return source$.subscribe(value => subscriber.next(value));
  // error and complete on source never reach subscriber!
});

// ✅ Forward all three notification types:
return source$ => new Observable(subscriber => {
  return source$.subscribe({
    next:     v   => subscriber.next(v),
    error:    err => subscriber.error(err),
    complete: ()  => subscriber.complete()
  });
});
// Or use: return source$ => source$.pipe(existingOp())
```

### Closing Over Mutable State Shared Between Subscriptions

```typescript
// ❌ State shared across all subscribers — concurrent subscribers corrupt each other:
let count = 0; // outside the operator factory!
function badCounter<T>(): MonoTypeOperatorFunction<T> {
  return source$ => source$.pipe(
    tap(() => count++) // shared mutable state
  );
}

// ✅ Create state inside the Observable callback — scoped per subscription:
function counter<T>(): MonoTypeOperatorFunction<T> {
  return source$ => new Observable(subscriber => {
    let count = 0; // local to this subscription
    return source$.subscribe({
      next: v => { count++; subscriber.next(v); },
      error: subscriber.error.bind(subscriber),
      complete: () => { console.log('Total:', count); subscriber.complete(); }
    });
  });
}
```
