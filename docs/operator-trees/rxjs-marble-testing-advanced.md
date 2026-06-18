# Marble Testing with RxJS — Advanced Guide

`TestScheduler` with virtual time, hot/cold marbles, error and completion notation, testing custom operators, and testing Angular/React components.

---

## The Marble Syntax

```
-      10ms (one frame)
a      a value emission (letter maps to values object)
|      completion
#      error (maps to errors object or default Error)
^      subscription point (hot observables only)
!      unsubscription point
( )    synchronous grouping — all emissions in one frame
```

```typescript
import { TestScheduler } from 'rxjs/testing';

const scheduler = new TestScheduler((actual, expected) => {
  expect(actual).toEqual(expected);
});
```

---

## Pattern 1: Testing Simple Operators

```typescript
import { TestScheduler } from 'rxjs/testing';
import { map, filter } from 'rxjs/operators';

describe('map operator', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) =>
      expect(actual).toEqual(expected)
    );
  });

  it('transforms values', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--a--b--c--|', { a: 1, b: 2, c: 3 });
      const result$ = source$.pipe(map(x => x * 2));

      expectObservable(result$).toBe('--a--b--c--|', { a: 2, b: 4, c: 6 });
    });
  });

  it('filters values', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--a--b--c--|', { a: 1, b: 2, c: 3 });
      const result$ = source$.pipe(filter(x => x % 2 !== 0));

      expectObservable(result$).toBe('--a-----c--|', { a: 1, c: 3 });
    });
  });
});
```

---

## Pattern 2: Testing Time-Based Operators

Virtual time makes `debounceTime`, `throttleTime`, `delay`, and `interval` testable without real waits:

```typescript
describe('debounceTime', () => {
  it('debounces rapid emissions', () => {
    scheduler.run(({ cold, expectObservable }) => {
      // Emit a, b, c quickly — only last (c) passes 50ms debounce:
      const source$ = cold('a-b--c------|', { a: 'a', b: 'b', c: 'c' });
      const result$ = source$.pipe(debounceTime(50));

      // c emits at frame 5, debounce fires at frame 5+50=55:
      expectObservable(result$).toBe('----------c-|', { c: 'c' });
      // In scheduler.run(), 1 frame = 1ms, so 50 frames = 50ms
    });
  });

  it('emits last value before completion', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a----------|', { a: 'a' });
      const result$ = source$.pipe(debounceTime(50));

      expectObservable(result$).toBe('-----------(a|)', { a: 'a' });
      // a + completion arrive synchronously (same frame) → grouped with ()
    });
  });
});

describe('interval', () => {
  it('emits on schedule', () => {
    scheduler.run(({ expectObservable }) => {
      const result$ = interval(10).pipe(take(3));
      expectObservable(result$).toBe('----------a---------b---------c|', {
        a: 0, b: 1, c: 2
      });
      // Each frame = 1ms; interval(10) = every 10 frames
    });
  });
});
```

---

## Pattern 3: Hot vs Cold Observables in Tests

Cold observables start fresh per subscriber. Hot observables share a timeline:

```typescript
describe('hot vs cold', () => {
  it('cold: each subscriber gets own sequence', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--a--b--c|');

      expectObservable(source$).toBe('--a--b--c|');
      expectObservable(source$).toBe('--a--b--c|'); // independent timeline
    });
  });

  it('hot: subscribers share timeline', () => {
    scheduler.run(({ hot, expectObservable }) => {
      //                           ^ = subscription point (subscriber joins here)
      const source$ = hot('--a--b--^--c--d--|');
      //                         subscriber joins at frame 8
      // Before ^ is invisible to subscriber

      expectObservable(source$).toBe('---c--d--|');
    });
  });

  it('hot: late subscriber misses early values', () => {
    scheduler.run(({ hot, cold, expectObservable }) => {
      const trigger$ = cold('------|');
      const shared$  = hot( 'a-b-c-d-e-|');

      const result$ = trigger$.pipe(
        switchMap(() => shared$) // subscribes at frame 6 (after trigger completes)
      );

      expectObservable(result$).toBe('------d-e-|');
    });
  });
});
```

---

## Pattern 4: Testing Error Handling

```typescript
describe('catchError', () => {
  it('recovers from error with fallback', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$  = cold('--a--#',    { a: 1 }, new Error('fail'));
      const fallback$ = cold('--b--|',   { b: 99 });
      const result$  = source$.pipe(
        catchError(() => fallback$)
      );

      // Error at frame 5, fallback starts from frame 5:
      expectObservable(result$).toBe('--a----b--|', { a: 1, b: 99 });
    });
  });

  it('retries on error', () => {
    scheduler.run(({ cold, expectObservable }) => {
      // cold() with multiple subscriptions: each ^ marks a new subscription:
      const source$ = cold('--a--#');
      const result$ = source$.pipe(retry(2));

      // First attempt: --a--# (error at 5)
      // Retry 1 starts at 5: --a--# (error at 10)
      // Retry 2 starts at 10: --a--# (error at 15, no more retries)
      expectObservable(result$).toBe('--a----a----a--#');
    });
  });

  it('custom error object in marble', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const myError  = new TypeError('invalid');
      const source$  = cold('--#', {}, myError);
      const result$  = source$.pipe(
        catchError(err => {
          expect(err).toBe(myError);
          return EMPTY;
        })
      );

      expectObservable(result$).toBe('--|');
    });
  });
});
```

---

## Pattern 5: Testing switchMap / mergeMap / concatMap

```typescript
describe('switchMap', () => {
  it('cancels previous inner on new outer', () => {
    scheduler.run(({ cold, hot, expectObservable }) => {
      const outer$  = hot( '-a---b------|');
      const inner$  = cold('--x--y--|');
      const result$ = outer$.pipe(
        switchMap(() => inner$)
      );

      // a triggers inner at 1 → starts at 1, would complete at 9
      // b triggers new inner at 5 → cancels a's inner, starts new one
      // b's inner: 5+2=7 (x), 5+5=10 (y), 5+7=12 (|)
      expectObservable(result$).toBe('---x---x---y----|');
      //                                 ^a's x  ^b's x ^b's y
    });
  });
});

describe('concatMap', () => {
  it('queues inner observables', () => {
    scheduler.run(({ cold, hot, expectObservable }) => {
      const outer$  = hot( '-a-b-|');
      const inner$  = cold('--x--|');
      const result$ = outer$.pipe(concatMap(() => inner$));

      // a's inner: 1..5 (x at 3, | at 5)
      // b's inner starts after a's completes at 5: x at 7, | at 9
      expectObservable(result$).toBe('---x---x--|');
    });
  });
});
```

---

## Pattern 6: Testing Custom Operators

```typescript
import { MonoTypeOperatorFunction } from 'rxjs';

// Custom operator: only emit if value changes AND is not null:
function distinctNonNull<T>(): MonoTypeOperatorFunction<T | null> {
  return source$ => source$.pipe(
    distinctUntilChanged(),
    filter((x): x is T => x !== null)
  );
}

describe('distinctNonNull', () => {
  it('filters null and deduplicates', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-a-b-n-b-c|', { a: 1, b: 2, n: null, c: 3 });
      const result$ = source$.pipe(distinctNonNull());

      expectObservable(result$).toBe('a---b-----c|', { a: 1, b: 2, c: 3 });
    });
  });
});
```

---

## Pattern 7: Testing Subscriptions and Unsubscriptions

```typescript
describe('subscription timing', () => {
  it('unsubscribes correctly with takeUntil', () => {
    scheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
      const source$ = cold('--a--b--c--|');
      const stop$   = hot( '------x----|');
      const result$ = source$.pipe(takeUntil(stop$));

      expectObservable(result$).toBe('--a--b|');

      // Verify the source was subscribed and unsubscribed at the right frames:
      expectSubscriptions(source$.subscriptions).toBe('^-----!');
    });
  });

  it('shareReplay shares subscription', () => {
    scheduler.run(({ cold, expectObservable, expectSubscriptions }) => {
      const source$ = cold('--a--b--c--|');
      const shared$ = source$.pipe(shareReplay(1));

      expectObservable(shared$).toBe('--a--b--c--|');
      expectObservable(shared$, '^-------!').toBe('--a--b--'); // second subscriber

      // Source subscribed only once:
      expectSubscriptions(source$.subscriptions).toBe('^----------!');
    });
  });
});
```

---

## Pattern 8: Testing Angular Services with TestScheduler

```typescript
import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

describe('SearchService', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) =>
      expect(actual).toEqual(expected)
    );
    TestBed.configureTestingModule({ imports: [HttpClientTestingModule] });
  });

  it('debounces search and cancels previous requests', fakeAsync(() => {
    scheduler.run(({ hot, expectObservable }) => {
      const service  = TestBed.inject(SearchService);
      const queries$ = hot('a-b--c----|', { a: 'rx', b: 'rxj', c: 'rxjs' });

      const results$ = queries$.pipe(
        debounceTime(300),
        switchMap(q => service.search(q))
      );

      expectObservable(results$).toBe('---------(r|)', { r: ['rxjs-operators'] });
    });
  }));
});
```

---

## Common Pitfalls

### Forgetting That `scheduler.run()` Uses 1 Frame = 1ms

```typescript
// ❌ Expecting debounceTime(300) to need 300 frames outside of run():
const scheduler = new TestScheduler(...);
// Don't use getTestScheduler() or scheduler.flush() manually —
// use scheduler.run() which virtualizes time correctly

// ✅ Always wrap in scheduler.run():
scheduler.run(({ cold, expectObservable }) => {
  const result$ = source$.pipe(debounceTime(300));
  expectObservable(result$).toBe('300ms a|', { a: 'x' });
  // '300ms a|' = 300 frames of silence then a then |
});
```

### Synchronous Emissions Need `( )`

```typescript
// ❌ Missing synchronous grouping for same-frame emissions:
expectObservable(of(1, 2, 3)).toBe('abc|', { a: 1, b: 2, c: 3 });
// Fails: of() emits synchronously, all at frame 0

// ✅ Group synchronous emissions with ():
expectObservable(of(1, 2, 3)).toBe('(abc|)', { a: 1, b: 2, c: 3 });
// All of a, b, c, and | happen at frame 0
```

### Hot Observable Subscription Before `^`

```typescript
// ❌ Assuming hot observable emits a before ^:
const source$ = hot('a-b-^-c-|'); // a and b are before subscription point
expectObservable(source$).toBe('a-b---c-|'); // WRONG — subscriber only sees from ^

// ✅ After ^ only:
expectObservable(source$).toBe('--c-|');
```
