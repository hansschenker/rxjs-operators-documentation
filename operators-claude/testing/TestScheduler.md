# TestScheduler

## Identity

- **Name**: TestScheduler
- **Category**: Testing / Debugging
- **Type**: Virtual time scheduler with marble syntax — enables deterministic, synchronous testing of time-based Observables
- **Import**:
  ```typescript
  import { TestScheduler } from 'rxjs/testing';
  ```
- **Signature**:
  ```typescript
  class TestScheduler extends VirtualTimeScheduler {
    constructor(assertDeepEqual: (actual: any, expected: any) => void)

    run<T>(callback: (helpers: RunHelpers) => T): T
  }

  interface RunHelpers {
    cold:              <T = string>(marbles: string, values?: Record<string, T>, error?: any) => ColdObservable<T>
    hot:               <T = string>(marbles: string, values?: Record<string, T>, error?: any) => HotObservable<T>
    expectObservable:  (observable: Observable<any>, unsubscriptionMarbles?: string) => { toBe: (marbles: string, values?: Record<string, any>, error?: any) => void }
    expectSubscriptions:(subscriptions: SubscriptionLog | SubscriptionLog[]) => { toBe: (marbles: string | string[]) => void }
    flush:             () => void
    animate:           (marbles: string) => void
    time:              (marbles: string) => number
  }
  ```

## Functional Specification

`TestScheduler` replaces real time with **virtual time**. Inside `testScheduler.run(fn)`, operators that use time (`debounceTime`, `delay`, `interval`, `timer`, etc.) are controlled by the virtual clock — 1 frame = 1ms. The entire test runs synchronously, regardless of how many "milliseconds" of virtual time are simulated.

**Marble syntax** (inside `run()`):

| Character | Meaning |
|-----------|---------|
| `-` | 1ms of virtual time |
| `a`–`z`, `A`–`Z`, `0`–`9` | Value emission (customizable via `values` map) |
| `\|` | Completion |
| `#` | Error (defaults to `'error'` string; use `error` param to customize) |
| `^` | Subscription point (hot Observables only) |
| `!` | Unsubscription point (in unsubscription marble) |
| `( )` | Synchronous group — multiple emissions in same frame |
| `空格` | No-op spacer for alignment |

**10ms shorthand** (legacy): In the OLD API (without `run()`), each `-` was 10ms. Inside `run()`, each `-` is **1ms**. Always use `run()`.

## Marble Diagram — Syntax Reference

```
cold('--a--b--|')
     ^         = subscription (t=0)
       a emits at t=2
          b emits at t=5
               completes at t=8

hot('--^--a--b--|')
       ^ = subscription marker (t=2)
            a emits at t=5 (3ms after subscription)
               b emits at t=8
                    completes at t=10

Synchronous group: '(ab|)'
  a, b, and completion all happen at t=1 (same frame)

Error: '--a--#'
  a emits at t=2, errors at t=5

With values map:
cold('--a--b--|', { a: { id: 1 }, b: { id: 2 } })
  emits objects instead of single characters
```

## Setup

```typescript
import { TestScheduler } from 'rxjs/testing';

// Use your test framework's assertion (Jest, Jasmine, etc.)
const testScheduler = new TestScheduler((actual, expected) => {
  expect(actual).toEqual(expected); // Jest
});
```

## Examples

### Basic Usage — Testing `debounceTime`
```typescript
import { TestScheduler } from 'rxjs/testing';
import { debounceTime } from 'rxjs/operators';

const testScheduler = new TestScheduler((actual, expected) => {
  expect(actual).toEqual(expected);
});

it('debounceTime(3ms) only emits after 3ms silence', () => {
  testScheduler.run(({ cold, expectObservable }) => {
    const source$  = cold('--a--b--------c--|');
    //                              ^ 3ms silence here
    const expected =      '-----------b-----c--|';
    //   debounce(3):
    //   a: next emission (b) is 3ms later → a suppressed
    //   b: 8ms silence after b → b emitted at 8+3=11
    //   c: no further emission → emitted at end

    expectObservable(source$.pipe(debounceTime(3))).toBe(expected);
  });
});
```

### Common Pattern — Testing `interval` / `timer`
```typescript
import { interval } from 'rxjs';
import { take, map } from 'rxjs/operators';

it('interval(1000) emits sequential integers', () => {
  testScheduler.run(({ expectObservable }) => {
    const source$  = interval(1000).pipe(take(3));
    const expected = '1000ms a 999ms b 999ms (c|)';
    //               ^ 1000ms gap between emissions

    expectObservable(source$).toBe(expected, { a: 0, b: 1, c: 2 });
  });
});
```

### Common Pattern — Testing `switchMap` Cancellation
```typescript
import { switchMap, debounceTime } from 'rxjs/operators';
import { of } from 'rxjs';

it('switchMap cancels previous inner on new source emission', () => {
  testScheduler.run(({ cold, hot, expectObservable }) => {
    const source$ = hot('--a--b------c--|');
    const inner   = (v: string) => cold('---x|', { x: v.toUpperCase() });

    // a emits at 2 → inner 'a' starts (would complete at 5 with 'A')
    // b emits at 5 → inner 'a' cancelled, inner 'b' starts (emits 'B' at 8)
    // c emits at 12 → inner 'b' already done; inner 'c' starts (emits 'C' at 15)
    const expected = '--------B--------C--|';

    expectObservable(source$.pipe(switchMap(inner))).toBe(expected);
  });
});
```

### Common Pattern — Testing Subscription Timing
```typescript
import { cold, hot } from 'rxjs';

it('tracks when subscriptions and unsubscriptions occur', () => {
  testScheduler.run(({ cold, expectSubscriptions }) => {
    const source$ = cold('--a--b--|');
    const subs    = '     ^------!'; // subscribed at 0, unsubscribed at 7

    // Force unsubscription using unsubscriptionMarbles
    expectSubscriptions(source$.subscribe()).toBe(subs);
  });
});
```

### Common Pattern — Custom Value Objects
```typescript
interface User { id: number; name: string }

it('maps IDs to user objects', () => {
  testScheduler.run(({ cold, expectObservable }) => {
    const users: Record<string, User> = {
      a: { id: 1, name: 'Alice' },
      b: { id: 2, name: 'Bob' }
    };

    const source$  = cold('--a--b--|', users);
    const expected =      '--x--y--|';

    expectObservable(
      source$.pipe(map(u => u.name))
    ).toBe(expected, { x: 'Alice', y: 'Bob' });
  });
});
```

### Common Pattern — Hot vs Cold in Tests
```typescript
it('hot Observable has pre-subscription history', () => {
  testScheduler.run(({ hot, expectObservable }) => {
    // hot: '^' marks where subscription happens; emissions before ^ are "past"
    const source$  = hot('--a--b--^--c--d--|');
    //                           ^ subscriber joins here
    // subscriber only sees c and d
    const expected =      '---c--d--|';

    expectObservable(source$).toBe(expected);
  });
});
```

## Common Pitfalls

### Anti-pattern: Using Real Timers in Time-Based Tests
```typescript
import { debounceTime } from 'rxjs/operators';
import { Subject } from 'rxjs';

// ❌ SLOW AND FRAGILE — real timers, real async, flaky in CI
it('debounceTime works', (done) => {
  const subject$ = new Subject<string>();
  const results: string[] = [];

  subject$.pipe(debounceTime(300)).subscribe(v => results.push(v));

  subject$.next('a');
  subject$.next('b');
  setTimeout(() => {
    subject$.next('c');
    setTimeout(() => {
      expect(results).toEqual(['b', 'c']);
      done();
    }, 400); // fragile: real 400ms wait
  }, 100);
});

// ✅ FAST AND DETERMINISTIC — virtual time
it('debounceTime(300) suppresses rapid emissions', () => {
  testScheduler.run(({ cold, expectObservable }) => {
    const source$  = cold('a-b 300ms c 300ms |');
    const expected =      '---------- 300ms b 300ms c 300ms |';
    //                    ^ simplification; exact timing depends on marble positions

    expectObservable(source$.pipe(debounceTime(300))).toBe(expected);
  });
});

// WHY: Real timer tests add hundreds of milliseconds to the test suite,
// are flaky under CPU load, and are impossible to run faster than wall clock.
// TestScheduler runs the same logic in microseconds.
```

### Anti-pattern: Using Legacy API (Outside `run()`)
```typescript
import { TestScheduler } from 'rxjs/testing';

// ❌ LEGACY — direct cold/hot on TestScheduler instance (pre-run() API)
const scheduler = new TestScheduler(assertFn);
const source = scheduler.createColdObservable('--a--b--|');
// This API uses 10ms per frame and has fewer helpers

// ✅ CORRECT — always use run() callback
scheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('--a--b--|'); // 1ms per frame inside run()
  expectObservable(source$).toBe('--a--b--|');
});

// WHY: The run() API uses 1ms per frame (matching real-time expectations),
// provides all helpers (cold, hot, expectObservable, time, animate),
// and is the only API that supports the 'Xms' shorthand for larger delays.
```

## Time Shorthand Syntax

Inside `run()`, you can write large delays concisely:
```typescript
// Instead of 1000 dashes:
cold('1000ms a 999ms b|')
// 'a' emits at 1000ms, 'b' at 2000ms, completes at 2001ms

// Mixed:
cold('--a 500ms b--|')
// 'a' at 2ms, 'b' at 503ms, completes at 506ms
```

## Related

- **`cold(marbles, values?)`**: Creates a cold Observable that starts from subscription; subscription point is implicit `^` at t=0
- **`hot(marbles, values?)`**: Creates a hot Observable with a shared timeline; subscription point marked by `^`
- **`observeOn(scheduler)`**: Production operator that accepts a scheduler for DI-based testing
- **`VirtualTimeScheduler`**: The base class TestScheduler extends; rarely used directly

## References
- **RxJS Official Docs**: [https://rxjs.dev/guide/testing/marble-testing](https://rxjs.dev/guide/testing/marble-testing)
- **TestScheduler API**: [https://rxjs.dev/api/testing/TestScheduler](https://rxjs.dev/api/testing/TestScheduler)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 3/5 | **Composability**: N/A (test utility)
**Key teaching points**:
1. Always use `run()` — 1ms per frame, time shorthand (`500ms`), all helpers available
2. `cold` = starts at subscription; `hot` = shared timeline with `^` subscription marker
3. Virtual time is synchronous — 1000ms of simulated time runs in microseconds
