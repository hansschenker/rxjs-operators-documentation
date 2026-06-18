# Testing Patterns Guide

Comprehensive guide to testing RxJS code — from unit testing operators to integration testing Angular components.

---

## 1. TestScheduler — Virtual Time

The gold standard for testing time-based operators. All timers run in virtual time — no real delays.

```typescript
import { TestScheduler } from 'rxjs/testing';
import { debounceTime, throttleTime, delay } from 'rxjs/operators';

describe('time-based operators', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
  });

  it('debounceTime waits for silence', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source   = cold('a-b-c-------d|');
      //                              ^ 300ms silence
      const expected =      '----------c-d|'; // c emits after 300ms quiet
      expectObservable(source.pipe(debounceTime(300))).toBe(expected);
    });
  });

  it('delay shifts emissions', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source   = cold('a--b--|');
      const expected =      '200ms a--b--|';
      expectObservable(source.pipe(delay(200))).toBe(expected);
    });
  });
});
```

See the [TestScheduler](../operators-claude/testing/TestScheduler) doc for full marble syntax reference.

---

## 2. Testing with `cold()` and `hot()`

```typescript
scheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
  // cold(): subscription starts at ^ (or t=0 if no ^)
  const source = cold('--a--b--|');

  // hot(): ^ marks subscription point; emissions before ^ are "past"
  const trigger = hot('--^--x--|');
  //                      ^ subscriber joins here — x visible, nothing before

  const result = source.pipe(takeUntil(trigger));
  expectObservable(result).toBe('--a--(b|)');

  // Verify subscription timing:
  expectSubscriptions(source.subscriptions).toBe('^----!');
});
```

---

## 3. Testing Custom Operators

```typescript
import { cold } from 'rxjs/testing'; // can use outside TestScheduler.run() with helpers

function retryOnce<T>(): MonoTypeOperatorFunction<T> {
  return retry(1);
}

it('retries once on error', () => {
  scheduler.run(({ cold, expectObservable }) => {
    const source   = cold('a-#');        // emits a, then errors
    const expected =      'a-a-#';      // retried: a again, then errors permanently
    expectObservable(source.pipe(retryOnce())).toBe(expected);
  });
});
```

---

## 4. Mocking HTTP with `of()` / `throwError()`

For testing services without `TestScheduler`:

```typescript
import { of, throwError } from 'rxjs';
import { delay } from 'rxjs/operators';

// Mock a successful HTTP call:
jest.spyOn(httpClient, 'get').mockReturnValue(of(mockUser).pipe(delay(0)));

// Mock an error:
jest.spyOn(httpClient, 'get').mockReturnValue(
  throwError(() => new Error('Not Found'))
);

// Test the service:
it('handles HTTP error gracefully', (done) => {
  jest.spyOn(httpClient, 'get').mockReturnValue(throwError(() => ({ status: 404 })));

  service.getUser('123').subscribe({
    next:     user  => { expect(user).toBeNull(); done(); },
    error:    ()    => done.fail('should not error'),
    complete: ()    => done()
  });
});
```

---

## 5. Testing Subscriptions with `done` Callback

For Jest/Jasmine without `TestScheduler`:

```typescript
it('emits three values then completes', (done) => {
  const results: number[] = [];

  of(1, 2, 3).subscribe({
    next:     v  => results.push(v),
    error:    e  => done.fail(e),
    complete: () => {
      expect(results).toEqual([1, 2, 3]);
      done();
    }
  });
});
```

---

## 6. Testing with `firstValueFrom` / `lastValueFrom`

Cleaner async/await style — no `done` callback needed:

```typescript
it('maps values correctly', async () => {
  const result = await lastValueFrom(
    of(1, 2, 3).pipe(
      map(x => x * 2),
      toArray()
    )
  );
  expect(result).toEqual([2, 4, 6]);
});

it('filters correctly', async () => {
  const first = await firstValueFrom(
    of(1, 2, 3, 4, 5).pipe(filter(x => x > 3))
  );
  expect(first).toBe(4);
});
```

---

## 7. Testing Angular Components with `async` Pipe

```typescript
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, BehaviorSubject } from 'rxjs';

describe('MyComponent', () => {
  let fixture: ComponentFixture<MyComponent>;
  let data$: BehaviorSubject<Item[]>;

  beforeEach(() => {
    data$ = new BehaviorSubject<Item[]>([]);

    TestBed.configureTestingModule({
      declarations: [MyComponent],
      providers: [
        { provide: DataService, useValue: { items$: data$ } }
      ]
    });
    fixture = TestBed.createComponent(MyComponent);
    fixture.detectChanges();
  });

  it('renders items when data arrives', () => {
    data$.next([{ id: '1', name: 'Test Item' }]);
    fixture.detectChanges();

    const items = fixture.debugElement.queryAll(By.css('.item'));
    expect(items.length).toBe(1);
    expect(items[0].nativeElement.textContent).toContain('Test Item');
  });

  it('renders empty state initially', () => {
    const empty = fixture.debugElement.query(By.css('.empty-state'));
    expect(empty).toBeTruthy();
  });
});
```

---

## 8. Testing Error Handling

```typescript
it('catches error and returns fallback', async () => {
  const source$ = throwError(() => new Error('oops')).pipe(
    catchError(() => of('fallback'))
  );
  const result = await firstValueFrom(source$);
  expect(result).toBe('fallback');
});

it('retries on error', () => {
  scheduler.run(({ cold, expectObservable }) => {
    const source   = cold('#');     // immediately errors
    const expected =      '---#'; // 3 frames delay × 1 retry
    expectObservable(
      source.pipe(retry({ count: 1, delay: () => cold('---') }))
    ).toBe(expected);
  });
});
```

---

## 9. Testing Subjects

```typescript
it('BehaviorSubject replays current value', async () => {
  const subject = new BehaviorSubject(42);

  // Late subscriber gets current value:
  const value = await firstValueFrom(subject);
  expect(value).toBe(42);

  subject.next(99);
  const updated = await firstValueFrom(subject);
  expect(updated).toBe(99);
});

it('ReplaySubject replays last N values', async () => {
  const subject = new ReplaySubject<number>(2);
  subject.next(1);
  subject.next(2);
  subject.next(3);

  const values = await lastValueFrom(subject.pipe(take(2), toArray()));
  expect(values).toEqual([2, 3]); // last 2 replayed
});
```

---

## 10. Common Testing Mistakes

### Not Completing Cold Observables in Marble Tests

```typescript
// ❌ If source doesn't complete, toArray() never emits:
scheduler.run(({ cold, expectObservable }) => {
  const source = cold('a-b-c'); // NO completion marker!
  expectObservable(source.pipe(toArray())).toBe(''); // nothing emits
});

// ✅ Always add | for completion:
scheduler.run(({ cold, expectObservable }) => {
  const source = cold('a-b-c|');
  expectObservable(source.pipe(toArray())).toBe('-----(abc|)', { abc: ['a','b','c'] });
});
```

### Forgetting `fakeAsync` / `tick` for Angular

```typescript
// ❌ timer(1000) won't resolve in synchronous test:
it('emits after 1s', () => {
  const results: number[] = [];
  timer(1000).subscribe(v => results.push(v));
  expect(results.length).toBe(1); // FAILS — timer hasn't fired
});

// ✅ Use fakeAsync + tick in Angular tests:
it('emits after 1s', fakeAsync(() => {
  const results: number[] = [];
  timer(1000).subscribe(v => results.push(v));
  tick(1000);
  expect(results.length).toBe(1); // passes
}));

// OR use TestScheduler outside Angular:
scheduler.run(({ cold, expectObservable }) => {
  expectObservable(timer(1000)).toBe('1000ms (0|)');
});
```

---

## Quick Reference

| Scenario | Recommended approach |
|---|---|
| Time-based operators | `TestScheduler.run()` with marble strings |
| Async/await style assertions | `firstValueFrom` / `lastValueFrom` |
| Synchronous pipelines | `done` callback or `lastValueFrom` |
| Mocking HTTP | `of(mockData)` / `throwError()` spy |
| Angular component tests | `BehaviorSubject` + `detectChanges()` |
| Angular timer tests | `fakeAsync` + `tick()` |
| Subscription timing | `expectSubscriptions()` in `TestScheduler` |
