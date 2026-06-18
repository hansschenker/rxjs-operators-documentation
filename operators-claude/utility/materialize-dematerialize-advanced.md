# materialize / dematerialize — Advanced Patterns

For fundamentals see the core [materialize / dematerialize](./materialize-dematerialize) doc. This page covers error-channel manipulation, notification routing, and testing patterns where treating errors as values is essential.

---

## What `materialize` Gives You

`materialize` converts every Observable notification into a `Notification<T>` value object. The stream never errors or completes — it just emits `Notification` objects:

```typescript
import { materialize, dematerialize } from 'rxjs/operators';
import { Notification } from 'rxjs';

// source$ emits: 1, 2, ERROR
source$.pipe(materialize()).subscribe(n => {
  console.log(n.kind);   // 'N' | 'E' | 'C'
  console.log(n.value);  // defined for 'N'
  console.log(n.error);  // defined for 'E'
});
// Logs: { kind: 'N', value: 1 }
//       { kind: 'N', value: 2 }
//       { kind: 'E', error: Error(...) }
// Stream then COMPLETES (error notification emitted as value, not thrown)
```

After `materialize`, the stream can no longer error. Errors become `{ kind: 'E', error }` values.

---

## Pattern 1: Error-Proof `switchMap`

The classic use case — prevent inner errors from killing the outer stream:

```typescript
import { mergeMap, materialize, dematerialize, filter } from 'rxjs/operators';

// Without materialize: one inner error kills the whole stream
requests$.pipe(
  switchMap(req => this.api.call(req)) // error here ends requests$
)

// With materialize: errors become values, stream survives
requests$.pipe(
  switchMap(req =>
    this.api.call(req).pipe(
      materialize()                    // convert errors to Notification values
    )
  ),
  tap(n => {
    if (n.kind === 'E') logger.error(n.error);
  }),
  filter(n => n.kind === 'N'),         // only pass successful values
  dematerialize()                      // unwrap back to T
)
```

---

## Pattern 2: Routing Errors to a Separate Channel

```typescript
import { Subject } from 'rxjs';
import { materialize, partition, map, dematerialize } from 'rxjs/operators';

const errors$ = new Subject<unknown>();

function processWithErrorRouting<T>(source$: Observable<T>): Observable<T> {
  const notifications$ = source$.pipe(materialize());

  // Split into success and error notifications:
  const [success$, error$] = partition(
    notifications$,
    n => n.kind === 'N'
  );

  // Route errors to error channel:
  error$.pipe(
    filter(n => n.kind === 'E'),
    map(n => n.error)
  ).subscribe(errors$);

  // Return only successful values:
  return success$.pipe(dematerialize());
}

// Consumer:
errors$.subscribe(e => showErrorToast(e));
processWithErrorRouting(this.api.getData()).subscribe(render);
```

---

## Pattern 3: Timeout With Error Capture

Capture which requests timed out vs completed:

```typescript
import { materialize, timeout, map } from 'rxjs/operators';

type Result<T> =
  | { status: 'success'; value: T }
  | { status: 'timeout'; operationId: string }
  | { status: 'error'; error: unknown; operationId: string };

function withCapture<T>(operationId: string, source$: Observable<T>): Observable<Result<T>> {
  return source$.pipe(
    timeout(5000),
    materialize(),
    map((n): Result<T> => {
      if (n.kind === 'N') return { status: 'success',  value: n.value! };
      if (n.kind === 'E') {
        const isTimeout = (n.error as Error).name === 'TimeoutError';
        return isTimeout
          ? { status: 'timeout', operationId }
          : { status: 'error',   error: n.error, operationId };
      }
      return null!; // 'C' notification — filtered out upstream
    }),
    filter(r => r !== null)
  );
}

// Aggregate results from multiple operations:
const ops = ['users', 'products', 'orders'];
forkJoin(
  ops.map(id => withCapture(id, this.api.fetch(id)))
).subscribe(results => {
  const failed = results.filter(r => r.status !== 'success');
  renderDashboard(results, failed);
});
```

---

## Pattern 4: `materialize` for Testing Marble Diagrams

The standard RxJS testing pattern for asserting error and completion notifications:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { materialize } from 'rxjs/operators';

describe('my operator', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
  });

  it('should convert error to notification', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--a--#', { a: 1 });
      const result$ = source$.pipe(materialize());

      // After materialize, errors are values — use 'N' notifications in marble:
      expectObservable(result$).toBe('--a--(b|)', {
        a: Notification.createNext(1),
        b: Notification.createError(new Error())
      });
    });
  });
});
```

---

## Pattern 5: Re-emit Notifications With Delay

Notifications-as-values lets you apply any operator to them — including `delay`:

```typescript
import { materialize, delay, dematerialize } from 'rxjs/operators';

// Delay error notifications by 500ms (e.g., to show loading state first):
source$.pipe(
  materialize(),
  delay(500),         // delays everything: values AND errors
  dematerialize()
)

// Delay ONLY errors (not values):
source$.pipe(
  materialize(),
  mergeMap(n =>
    n.kind === 'E'
      ? timer(500).pipe(map(() => n))  // delay error notifications
      : of(n)                           // pass values immediately
  ),
  dematerialize()
)
```

---

## Pattern 6: Combining Streams That May Error

Merge multiple streams that might fail, collect all results including errors:

```typescript
import { merge, materialize, toArray, map } from 'rxjs/operators';

const streams = [
  this.api.getUsers(),
  this.api.getProducts(),
  this.api.getOrders()
];

// Collect all notifications including errors:
merge(...streams.map((s$, i) =>
  s$.pipe(
    materialize(),
    map(n => ({ source: i, notification: n }))
  )
)).pipe(
  filter(({ notification: n }) => n.kind !== 'C'),
  toArray()
).subscribe(allResults => {
  const successes = allResults.filter(r => r.notification.kind === 'N');
  const errors    = allResults.filter(r => r.notification.kind === 'E');
  buildDashboard(successes, errors);
});
```

---

## Pattern 7: Notification Transformation

Modify values inside a notification without unwrapping:

```typescript
import { materialize, map, dematerialize } from 'rxjs/operators';
import { Notification } from 'rxjs';

// Transform only 'N' (next) notifications, pass errors unchanged:
function mapNotification<T, R>(
  project: (value: T) => R
): OperatorFunction<T, R> {
  return pipe(
    materialize(),
    map(n =>
      n.kind === 'N' && n.value !== undefined
        ? Notification.createNext(project(n.value))
        : n as unknown as Notification<R>
    ),
    dematerialize()
  );
}

// Usage — guaranteed not to lose error semantics:
source$.pipe(
  mapNotification(user => ({ ...user, displayName: `${user.first} ${user.last}` }))
)
```

---

## `materialize` vs `catchError` vs `onErrorResumeNext`

```typescript
// catchError — intercepts error, you decide the recovery Observable:
source$.pipe(
  catchError(err => of(FALLBACK))
)
// ✓ Stream continues. Error is caught, recovery Observable returned.
// ✗ Error is "consumed" — downstream doesn't know there was an error.

// materialize — converts error to Notification value, stream continues:
source$.pipe(
  materialize()
)
// ✓ Error is visible as a value downstream — can inspect, log, route.
// ✓ Stream never errors after materialize.
// ✗ Changes the type: Observable<T> → Observable<Notification<T>>

// onErrorResumeNext — ignores errors, subscribes to next source:
onErrorResumeNext(source1$, source2$, source3$)
// ✓ Simple. Any error in source N → move to source N+1.
// ✗ Error information is discarded silently.
```

---

## Common Pitfalls

### Forgetting That `materialize` Changes the Stream Type

```typescript
// ❌ Trying to use value directly after materialize:
source$.pipe(
  materialize(),
  map(x => x.toUpperCase()) // x is Notification<string>, not string!
)

// ✅ Access through .value for 'N' notifications:
source$.pipe(
  materialize(),
  filter(n => n.kind === 'N'),
  map(n => n.value!.toUpperCase()), // n.value is string
  // OR: dematerialize first then map
)
```

### Not Filtering Completion Notifications

```typescript
// ❌ Completion notification is emitted as { kind: 'C' } — may cause issues:
source$.pipe(
  materialize(),
  map(n => processNotification(n))  // processes 'C' too
)

// ✅ Filter to only 'N' and 'E' if 'C' is not meaningful:
source$.pipe(
  materialize(),
  filter(n => n.kind !== 'C')
)
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key use cases**: (1) Prevent inner Observable errors from killing an outer stream in `switchMap`/`mergeMap`. (2) Route errors to a separate error-handling channel. (3) Assert error and completion notifications in tests. For most error handling needs, `catchError` is simpler — reach for `materialize` when you need errors as *values* rather than as exceptional events.
