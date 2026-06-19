# RxJS Marble Testing — Deep Reference

The complete cold/hot marble syntax reference, TestScheduler patterns, and advanced testing scenarios.

---

## Marble Syntax Reference

### Core Characters

| Character | Meaning | Notes |
|---|---|---|
| `-` | 10ms of virtual time | Each dash = one frame |
| `a`–`z`, `A`–`Z`, `0`–`9` | Emission with value | Default value = character itself |
| `\|` | Completion | Synchronous after last emission |
| `#` | Error | Default error = `new Error('error')` |
| `(` `)` | Synchronous group | Values inside emit at same frame |
| `^` | Subscription point | Hot observables only |
| `!` | Unsubscription point | `subscriptionMarbles` parameter |
| ` ` | No-op (ignored) | Use for alignment readability |

### Value Maps

When emissions need specific values (not their character), supply a `values` map:

```typescript
testScheduler.run(({ cold, hot, expectObservable }) => {
  const source$ = cold('--a--b--c|', {
    a: { id: 1, name: 'Alice' },
    b: { id: 2, name: 'Bob' },
    c: { id: 3, name: 'Carol' },
  });

  // Numeric values
  const nums$ = cold('-a-b-c|', { a: 10, b: 20, c: 30 });

  // Object values — reference equality matters in assertions
  const USERS = { a: { id: 1 }, b: { id: 2 } };
  const users$ = cold('a-b|', USERS);
  expectObservable(users$).toBe('a-b|', USERS); // same reference
});
```

### Error Values

```typescript
testScheduler.run(({ cold, expectObservable }) => {
  // Default error
  const source$ = cold('--#');
  expectObservable(source$).toBe('--#');

  // Custom error value
  const err = new TypeError('invalid input');
  const source2$ = cold('--#', {}, err);
  expectObservable(source2$).toBe('--#', {}, err);
});
```

---

## TestScheduler Setup

```typescript
import { TestScheduler } from 'rxjs/testing';

describe('MyOperator', () => {
  let testScheduler: TestScheduler;

  beforeEach(() => {
    testScheduler = new TestScheduler((actual, expected) => {
      // Use your test framework's deep-equality assertion
      expect(actual).toEqual(expected);
    });
  });

  it('description', () => {
    testScheduler.run(({ cold, hot, expectObservable, expectSubscriptions, flush }) => {
      // All marble operations here
    });
  });
});
```

### TestScheduler Helpers

| Helper | Purpose |
|---|---|
| `cold(marble, values?, error?)` | Create cold Observable |
| `hot(marble, values?, error?)` | Create hot Observable (has `^`) |
| `expectObservable(obs$, subscriptionMarble?)` | Assert emissions |
| `expectSubscriptions(subscriptions)` | Assert subscribe/unsubscribe timing |
| `flush()` | Synchronously advance all virtual time |
| `time(marble)` | Convert marble to milliseconds |

---

## Cold vs Hot Observable Marbles

### Cold Observables

A cold Observable starts its timeline at subscription time. Each subscriber gets a fresh timeline.

```typescript
testScheduler.run(({ cold, expectObservable }) => {
  // Cold: timeline starts when subscribed
  const source$ = cold('--a--b--|');

  // Two subscribers see independent timelines
  const sub1$ = source$;       // timeline: --a--b--|
  const sub2$ = source$.pipe(delay(20)); // starts 20ms later

  expectObservable(sub1$).toBe('--a--b--|');
  expectObservable(sub2$).toBe('----a--b--|');
  //                                ^^ 20ms delay shifts everything
});
```

### Hot Observables

A hot Observable has a global timeline. The `^` marks where subscription begins.

```typescript
testScheduler.run(({ hot, expectObservable }) => {
  // Hot: values before ^ are in the past (ignored by subscribers)
  const source$ = hot('--a--b--^--c--d--|');
  //                            ^ subscriber joins here

  // Subscriber only sees: --c--d--|
  expectObservable(source$).toBe('---c--d--|');
});
```

### Subscription Marbles

Assert when a source is subscribed and unsubscribed:

```typescript
testScheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
  const source$ = cold('--a--b--c--|');

  const result$ = source$.pipe(take(2));

  expectObservable(result$).toBe('--a--(b|)');
  expectSubscriptions(source$.subscriptions).toBe('^---!');
  //                                               ^   ^ = subscribed frame 0
  //                                                   ! = unsubscribed after 2 values
});
```

---

## Advanced Marble Patterns

### 1. Testing switchMap Cancellation

```typescript
testScheduler.run(({ cold, hot, expectObservable }) => {
  const trigger$ = hot('  -a---b-|');
  const inner   = cold('  --x--|  ');
  //                         ^ each trigger creates a new inner

  const result$ = trigger$.pipe(
    switchMap(() => inner),
  );

  // 'a' triggers inner, starts '--x--|'
  // 'b' arrives at frame 5, cancels first inner before it emits
  // 'b' triggers fresh inner, emits 'x' at frame 7
  expectObservable(result$).toBe('-------x--|');
  //                                      ^ only b's inner completes
});
```

### 2. Testing mergeMap Concurrency

```typescript
testScheduler.run(({ cold, hot, expectObservable }) => {
  const trigger$ = hot('--a--b----|');
  const inner   = cold('---x|     ');

  const result$ = trigger$.pipe(
    mergeMap(() => inner),
  );

  // 'a' at frame 2 → inner starts, emits 'x' at frame 5
  // 'b' at frame 5 → inner starts, emits 'x' at frame 8
  // Both run concurrently — no cancellation
  expectObservable(result$).toBe('-----x--x-|');
});
```

### 3. Testing combineLatest

```typescript
testScheduler.run(({ cold, expectObservable }) => {
  const a$ = cold('-a------c-|', { a: 1, c: 3 });
  const b$ = cold('---b------d|', { b: 2, d: 4 });

  const result$ = combineLatest([a$, b$]).pipe(
    map(([a, b]) => a + b),
  );

  // First emission requires both to have emitted at least once
  // frame 3: a=1, b=2 → emit 3
  // frame 6: a=3, b=2 → emit 5
  // frame 9: a=3, b=4 → emit 7
  expectObservable(result$).toBe('---c--e--g-|', { c: 3, e: 5, g: 7 });
});
```

### 4. Testing debounceTime

```typescript
testScheduler.run(({ cold, expectObservable, time }) => {
  const source$ = cold('-a-b-c---------d|');
  const debounce = time('------| ');  // 60ms (6 dashes × 10ms)

  const result$ = source$.pipe(debounceTime(debounce));

  // a, b, c all within 60ms of each other — only c passes
  // d has 90ms silence before it — passes
  expectObservable(result$).toBe('-------------c---------d|');
});
```

### 5. Testing Error Recovery

```typescript
testScheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('--a--#');
  const fallback$ = cold('--b--|');

  const result$ = source$.pipe(
    catchError(() => fallback$),
  );

  // a at frame 2, error at frame 5 → switch to fallback$
  // fallback starts from frame 5: --b--| = b at 7, complete at 10
  expectObservable(result$).toBe('--a----b--|');
});
```

### 6. Testing Retry

```typescript
testScheduler.run(({ cold, expectObservable }) => {
  let attempt = 0;

  const source$ = cold<string>('#').pipe(
    // create new source per attempt
  );

  // Simulate 2 failures then success
  const flaky$ = cold<string>('---#').pipe(
    retry(2),
  );
  // attempt 0: ---# (error at 3)
  // attempt 1: ---# (error at 6)
  // attempt 2: --- source$ with retry(2) exhausted → error propagates
  expectObservable(flaky$).toBe('---------#');

  // With success on third try using defer
  let tries = 0;
  const eventuallySucceeds$ = defer(() => {
    tries++;
    return tries < 3 ? cold('---#') : cold('---a|');
  }).pipe(retry(5));

  expectObservable(eventuallySucceeds$).toBe('---------a|');
});
```

### 7. Testing timer and interval

```typescript
testScheduler.run(({ expectObservable, time }) => {
  // interval
  const ticks$ = interval(100).pipe(take(3));
  expectObservable(ticks$).toBe('100ms a 99ms b 99ms (c|)', {
    a: 0, b: 1, c: 2,
  });

  // timer with delay
  const delayed$ = timer(200, 100).pipe(take(3));
  expectObservable(delayed$).toBe('200ms a 99ms b 99ms (c|)', {
    a: 0, b: 1, c: 2,
  });
});
```

### 8. Testing Hot Subjects

```typescript
import { Subject } from 'rxjs';

testScheduler.run(({ expectObservable, hot }) => {
  // Use hot() to simulate a Subject that emits over time
  const subject$ = hot('-a-b-c-|');

  const result$ = subject$.pipe(
    map(v => v.toUpperCase()),
  );

  expectObservable(result$).toBe('-A-B-C-|');
});

// Testing with a real Subject
it('Subject emits values to multiple subscribers', () => {
  testScheduler.run(({ expectObservable, cold }) => {
    const subject = new Subject<number>();

    const sub1$ = subject.asObservable().pipe(take(2));
    const sub2$ = subject.asObservable().pipe(take(2));

    // Schedule emissions
    cold('--a--b--c', { a: 1, b: 2, c: 3 }).subscribe(v => subject.next(v));

    expectObservable(sub1$).toBe('--(ab|)', { a: 1, b: 2 });
    expectObservable(sub2$).toBe('--(ab|)', { a: 1, b: 2 });
  });
});
```

---

## Common TestScheduler Pitfalls

```typescript
// ❌ INCORRECT — using real time in TestScheduler context
testScheduler.run(() => {
  const source$ = interval(1000); // real interval — TestScheduler can't control
  // Tests will hang or time out
});

// ✅ CORRECT — use TestScheduler's virtual time helpers
testScheduler.run(({ expectObservable }) => {
  const source$ = interval(1000).pipe(take(3)); // virtual time — instant
  expectObservable(source$).toBe('1000ms a 999ms b 999ms (c|)', { a: 0, b: 1, c: 2 });
});


// ❌ INCORRECT — comparing objects by reference without values map
testScheduler.run(({ cold, expectObservable }) => {
  const result$ = cold('-a|').pipe(map(v => ({ key: v })));
  expectObservable(result$).toBe('-a|'); // fails: { key: 'a' } ≠ 'a'
});

// ✅ CORRECT — provide values map with expected objects
testScheduler.run(({ cold, expectObservable }) => {
  const result$ = cold('-a|').pipe(map(v => ({ key: v })));
  expectObservable(result$).toBe('-a|', { a: { key: 'a' } });
});


// ❌ INCORRECT — forgetting that (abc) is synchronous
testScheduler.run(({ expectObservable, cold }) => {
  const source$ = cold('(ab|)');
  // Expecting sequential emissions at different times:
  expectObservable(source$).toBe('-a-b-|'); // FAILS — (ab|) is all frame 0
});

// ✅ CORRECT — (abc) all emit at the same virtual frame
testScheduler.run(({ expectObservable, cold }) => {
  const source$ = cold('(ab|)');
  expectObservable(source$).toBe('(ab|)'); // all synchronous
});
```

---

## Marble Testing Decision Guide

```
Need to test timing behavior (debounce, throttle, timer)?
  → TestScheduler with marble strings

Need to test operator logic without timing?
  → cold() with - separators for readability

Testing a Subject or BehaviorSubject?
  → hot() to simulate pre-existing emissions

Need to verify subscribe/unsubscribe timing?
  → expectSubscriptions() with subscription marbles

Testing error recovery (catchError, retry)?
  → cold() with # and values map for custom errors

Testing cancellation (switchMap, takeUntil)?
  → hot() for trigger + cold() for inner + expectSubscriptions()
```

---

## Related Guides

- **[TestScheduler Operator Doc](../operators-claude/testing/TestScheduler.md)** — TestScheduler API reference
- **[TestScheduler (Advanced)](../operators-claude/testing/TestScheduler-advanced.md)** — advanced scheduling patterns
- **[Marble Testing (Advanced)](./rxjs-marble-testing-advanced.md)** — prior marble testing guide
- **[Debugging Streams (Advanced)](../operators-claude/testing/debugging-operators-advanced.md)** — runtime diagnostics
- **[Testing Patterns Guide](./testing-patterns-guide.md)** — broader testing strategies
