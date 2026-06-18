# Integration Testing with RxJS

Testing Observable-based code in real environments — without marble diagrams — using Jest, Vitest, Jasmine, and Angular TestBed. Covers real timers, HTTP mocking, Subject-based test harnesses, and race-condition testing.

---

## When Integration Tests Beat Marble Tests

Marble tests are fast and deterministic, but they run in virtual time with a controlled scheduler. Integration tests catch a different class of bugs:

| Concern | Marble Test | Integration Test |
|---------|-------------|-----------------|
| Operator logic | ✅ Ideal | Overkill |
| Real DOM events | ❌ Can't simulate | ✅ |
| Real HTTP calls | ❌ Can't simulate | ✅ |
| Angular DI / lifecycle | ❌ | ✅ TestBed |
| Subscription leaks | ⚠️ Hard | ✅ Easy with spies |
| Race conditions (real time) | ❌ | ✅ fakeAsync + tick |

Use both: marble tests for operator logic, integration tests for the full data flow.

---

## Pattern 1: Testing with Subjects as Fakes

Subjects are the best seam for testing — inject them as stand-ins for real sources:

```typescript
import { Subject, BehaviorSubject } from 'rxjs';
import { first } from 'rxjs/operators';

describe('UserDashboardService', () => {
  let service: UserDashboardService;
  let userEvents$: Subject<UserEvent>;
  let auth$: BehaviorSubject<AuthState>;

  beforeEach(() => {
    userEvents$ = new Subject<UserEvent>();
    auth$ = new BehaviorSubject<AuthState>({ loggedIn: false, userId: null });

    service = new UserDashboardService(userEvents$, auth$);
  });

  it('should emit dashboard data when user logs in', async () => {
    const result$ = service.dashboardData$.pipe(first());
    const resultPromise = firstValueFrom(result$);

    // Simulate login:
    auth$.next({ loggedIn: true, userId: '42' });

    const result = await resultPromise;
    expect(result.userId).toBe('42');
  });

  it('should complete subscriptions on destroy', () => {
    const completeSpy = jest.fn();
    service.dashboardData$.subscribe({ complete: completeSpy });

    service.destroy(); // calls takeUntil(destroy$)

    expect(completeSpy).toHaveBeenCalled();
  });
});
```

---

## Pattern 2: Testing Real HTTP with `HttpClientTestingModule`

Angular's `HttpTestingController` gives you fine-grained control over HTTP flushes:

```typescript
import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

describe('ProductService', () => {
  let service: ProductService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [ProductService]
    });
    service     = TestBed.inject(ProductService);
    http        = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify()); // ensure no pending requests

  it('retries once on 503 then emits product', () => {
    const results: Product[] = [];
    service.getProduct('123').subscribe(p => results.push(p));

    // First request — server error:
    http.expectOne('/api/products/123').flush(
      { message: 'Service Unavailable' },
      { status: 503, statusText: 'Service Unavailable' }
    );

    // Retry request — success:
    http.expectOne('/api/products/123').flush({ id: '123', name: 'Widget' });

    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Widget');
  });

  it('emits error after max retries exceeded', () => {
    let error: Error | undefined;
    service.getProduct('bad-id').subscribe({ error: e => (error = e) });

    // Exhaust all retries:
    for (let i = 0; i <= 3; i++) {
      http.expectOne('/api/products/bad-id').flush(
        { message: 'Not Found' },
        { status: 404, statusText: 'Not Found' }
      );
    }

    expect(error?.message).toMatch(/404/);
  });
});
```

---

## Pattern 3: `fakeAsync` + `tick` for Time-Dependent Streams

Angular's `fakeAsync` zone patches timers and makes real-time Observables testable synchronously:

```typescript
import { fakeAsync, tick } from '@angular/core/testing';

describe('SearchService debounce', () => {
  it('should debounce search input by 300ms', fakeAsync(() => {
    const searchService = new SearchService();
    const apiSpy = jest.spyOn(searchService, 'callApi').mockReturnValue(of([]));

    searchService.search$.next('he');
    searchService.search$.next('hel');
    searchService.search$.next('hell');
    searchService.search$.next('hello');

    // Nothing called yet — still within debounce window:
    expect(apiSpy).not.toHaveBeenCalled();

    tick(300); // advance virtual time

    // Only called once with final value:
    expect(apiSpy).toHaveBeenCalledTimes(1);
    expect(apiSpy).toHaveBeenCalledWith('hello');
  }));

  it('should cancel previous request on new input', fakeAsync(() => {
    const searchService = new SearchService();
    const results: string[][] = [];

    searchService.results$.subscribe(r => results.push(r));

    searchService.search$.next('rx');
    tick(300);

    // Simulate slow first response:
    searchService.search$.next('rxjs');
    tick(300);

    // Flush responses in REVERSE order (second comes back first):
    // With switchMap, first response should be ignored:
    expect(results).toHaveLength(0); // waiting for latest
  }));
});
```

---

## Pattern 4: Subscription Leak Detection

Detect subscriptions that survive component destruction:

```typescript
import { Subscription } from 'rxjs';

describe('ComponentSubscriptionLeak', () => {
  it('should unsubscribe all streams on ngOnDestroy', () => {
    const component = new MyComponent(inject(DataService));
    const subscriptions: Subscription[] = [];

    // Spy on subscribe to track active subscriptions:
    const originalSubscribe = Observable.prototype.subscribe;
    jest.spyOn(Observable.prototype, 'subscribe').mockImplementation(function(this: Observable<unknown>, ...args) {
      const sub = originalSubscribe.apply(this, args);
      subscriptions.push(sub);
      return sub;
    });

    component.ngOnInit();
    const countOnInit = subscriptions.filter(s => !s.closed).length;
    expect(countOnInit).toBeGreaterThan(0);

    component.ngOnDestroy();

    const leakedSubscriptions = subscriptions.filter(s => !s.closed);
    expect(leakedSubscriptions).toHaveLength(0); // all cleaned up
  });
});

// Simpler approach — track via a destroy$ subject:
describe('DestroySubjectPattern', () => {
  it('should complete all pipelines when destroy$ emits', () => {
    const destroy$ = new Subject<void>();
    const completedStreams: string[] = [];

    // Simulate what the component does:
    interval(100).pipe(
      takeUntil(destroy$),
      finalize(() => completedStreams.push('interval'))
    ).subscribe();

    fromEvent(document, 'click').pipe(
      takeUntil(destroy$),
      finalize(() => completedStreams.push('clicks'))
    ).subscribe();

    destroy$.next();
    destroy$.complete();

    expect(completedStreams).toContain('interval');
    expect(completedStreams).toContain('clicks');
  });
});
```

---

## Pattern 5: Testing Race Conditions

Use Subjects to control emission order and verify race-condition behavior:

```typescript
describe('switchMap cancellation', () => {
  it('should cancel in-flight request when new search arrives', async () => {
    const searchQuery$ = new Subject<string>();
    const callCount = { n: 0 };

    // Service that tracks concurrent calls:
    const search = (q: string) => {
      callCount.n++;
      return timer(100).pipe(map(() => `results for ${q}`));
    };

    const results: string[] = [];
    searchQuery$.pipe(
      switchMap(q => search(q))
    ).subscribe(r => results.push(r));

    searchQuery$.next('a'); // starts first request
    // Immediately:
    searchQuery$.next('ab'); // cancels first, starts second

    await lastValueFrom(timer(150)); // wait for second to complete

    expect(callCount.n).toBe(2);        // both were initiated
    expect(results).toHaveLength(1);    // only second result arrived
    expect(results[0]).toBe('results for ab');
  });
});

describe('concatMap ordering', () => {
  it('should process queue in order even with variable latency', async () => {
    const actions$ = new Subject<string>();
    const processed: string[] = [];

    // Second action is faster than first:
    const process = (action: string) =>
      timer(action === 'slow' ? 100 : 10).pipe(map(() => action));

    actions$.pipe(
      concatMap(process)
    ).subscribe(r => processed.push(r));

    actions$.next('slow');
    actions$.next('fast');

    await lastValueFrom(timer(250));

    expect(processed).toEqual(['slow', 'fast']); // order preserved
  });
});
```

---

## Pattern 6: Testing `shareReplay` Caching

Verify that shared Observables don't re-execute the source:

```typescript
describe('shareReplay caching', () => {
  it('should call HTTP only once for multiple subscribers', () => {
    const httpCallCount = { n: 0 };
    const mockHttp = {
      get: () => {
        httpCallCount.n++;
        return of({ data: 'result' });
      }
    };

    const service = new ProductCacheService(mockHttp);

    // Subscribe twice:
    service.products$.subscribe();
    service.products$.subscribe();

    expect(httpCallCount.n).toBe(1); // cache hit on second subscribe
  });

  it('should replay to late subscribers', () => {
    const results1: unknown[] = [];
    const results2: unknown[] = [];

    const shared$ = of('value').pipe(shareReplay(1));

    shared$.subscribe(v => results1.push(v));
    // Late subscriber:
    shared$.subscribe(v => results2.push(v));

    expect(results1).toEqual(['value']);
    expect(results2).toEqual(['value']); // replayed
  });
});
```

---

## Pattern 7: Integration Test with Vitest + Real Timers

For non-Angular apps using Vitest:

```typescript
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { interval, Subject } from 'rxjs';
import { buffer, debounceTime } from 'rxjs/operators';

describe('EventBatcher', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  it('should batch events within 200ms window', () => {
    const events$ = new Subject<string>();
    const batches: string[][] = [];

    const close$ = interval(200);
    events$.pipe(
      buffer(close$)
    ).subscribe(batch => {
      if (batch.length > 0) batches.push(batch);
    });

    events$.next('a');
    events$.next('b');
    vi.advanceTimersByTime(200); // trigger buffer close

    events$.next('c');
    vi.advanceTimersByTime(200);

    expect(batches[0]).toEqual(['a', 'b']);
    expect(batches[1]).toEqual(['c']);
  });

  it('debounce: only emits after quiescence', () => {
    const input$ = new Subject<string>();
    const emitted: string[] = [];

    input$.pipe(debounceTime(300)).subscribe(v => emitted.push(v));

    input$.next('x');
    input$.next('xy');
    input$.next('xyz');
    vi.advanceTimersByTime(299);
    expect(emitted).toHaveLength(0);

    vi.advanceTimersByTime(1);
    expect(emitted).toEqual(['xyz']);
  });
});
```

---

## Pattern 8: Testing Angular Effects / NgRx Actions

```typescript
import { TestBed } from '@angular/core/testing';
import { Actions, provideMockActions } from '@ngrx/effects/testing';
import { of, ReplaySubject } from 'rxjs';

describe('ProductEffects', () => {
  let actions$: ReplaySubject<Action>;
  let effects: ProductEffects;
  let productService: jest.Mocked<ProductService>;

  beforeEach(() => {
    actions$ = new ReplaySubject<Action>(1);
    productService = { loadProducts: jest.fn() } as jest.Mocked<ProductService>;

    TestBed.configureTestingModule({
      providers: [
        ProductEffects,
        provideMockActions(() => actions$),
        { provide: ProductService, useValue: productService }
      ]
    });

    effects = TestBed.inject(ProductEffects);
  });

  it('dispatches loadProductsSuccess on successful load', () => {
    const products = [{ id: '1', name: 'Widget' }];
    productService.loadProducts.mockReturnValue(of(products));

    const results: Action[] = [];
    effects.loadProducts$.subscribe(a => results.push(a));

    actions$.next(ProductActions.loadProducts());

    expect(results).toEqual([ProductActions.loadProductsSuccess({ products })]);
  });

  it('dispatches loadProductsFailure on error', () => {
    const error = new Error('Network error');
    productService.loadProducts.mockReturnValue(throwError(() => error));

    const results: Action[] = [];
    effects.loadProducts$.subscribe(a => results.push(a));

    actions$.next(ProductActions.loadProducts());

    expect(results[0].type).toBe(ProductActions.loadProductsFailure.type);
  });
});
```

---

## Common Pitfalls

### Forgetting to `verify()` Pending HTTP Requests

```typescript
// ❌ Test passes even though HTTP requests were made but not flushed:
afterEach(() => {
  // No verify — leaked pending requests pollute next test
});

// ✅ Always call verify in afterEach:
afterEach(() => {
  TestBed.inject(HttpTestingController).verify();
  // Throws if any requests were made but not flushed
});
```

### Testing Time with Real Timers (Makes Tests Slow)

```typescript
// ❌ Real timer — test takes 500ms:
it('debounce', done => {
  subject$.pipe(debounceTime(500)).subscribe(v => {
    expect(v).toBe('final');
    done();
  });
  subject$.next('final');
  // wait 500ms for real...
});

// ✅ Use fakeAsync + tick (Angular) or vi.useFakeTimers() (Vitest/Jest):
it('debounce', fakeAsync(() => {
  subject$.pipe(debounceTime(500)).subscribe(v => expect(v).toBe('final'));
  subject$.next('final');
  tick(500); // instant
}));
```
