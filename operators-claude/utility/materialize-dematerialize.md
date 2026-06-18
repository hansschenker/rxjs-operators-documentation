# materialize / dematerialize

## Identity

| | `materialize` | `dematerialize` |
|---|---|---|
| **Import** | `import { materialize } from 'rxjs/operators'` | `import { dematerialize } from 'rxjs/operators'` |
| **Signature** | `materialize<T>(): OperatorFunction<T, Notification<T>>` | `dematerialize<T>(): OperatorFunction<ObservableNotification<T>, T>` |
| **Category** | Utility Operators | Utility Operators |
| **Direction** | Converts emissions → `Notification` objects | Converts `Notification` objects → emissions |

```typescript
function materialize<T>(): OperatorFunction<T, Notification<T>>
function dematerialize<T>(): OperatorFunction<ObservableNotification<T>, T>

// Notification shape:
interface Notification<T> {
  kind: 'N' | 'E' | 'C'  // Next, Error, Complete
  value?: T               // present when kind === 'N'
  error?: any             // present when kind === 'E'
  hasValue: boolean
}
```

## Functional Specification

**`materialize()`**: Wraps every notification (next, error, complete) into a `Notification<T>` object and emits it as a `next` value. The resulting stream never errors — errors become `Notification<T>` objects with `kind: 'E'`. The source's completion is also materialized as a final `Notification<T>` with `kind: 'C'`, and then the outer stream completes.

**`dematerialize()`**: The inverse — unwraps `Notification<T>` objects back into actual next/error/complete signals. Used after `materialize` or when working with stored notification sequences.

**Why use them?**:
- Make errors "safe" to work with as values (e.g., merge multiple streams without one error killing all)
- Store/replay notification sequences (e.g., in tests or caching)
- Apply operators to errors without `catchError` (filter, delay, map over any notification kind)

## Marble Diagram

```
Source:  --1--2--#(err)

materialize():
Result:  --N(1)--N(2)--E(err)--|
         (N=next notification, E=error notification, outer completes normally)

Source:  --1--2--|

materialize():
Result:  --N(1)--N(2)--C--|
         (C=complete notification, then outer completes)

dematerialize() is the exact inverse:
--N(1)--N(2)--E(err)--|  →  --1--2--#(err)
```

## Type System Integration

```typescript
import { of, throwError } from 'rxjs';
import { materialize, dematerialize, map } from 'rxjs/operators';

// materialize: Observable<T> → Observable<Notification<T>>
of(1, 2, 3).pipe(
  materialize()
).subscribe(n => {
  if (n.kind === 'N') console.log('value:', n.value);
  if (n.kind === 'C') console.log('complete');
});
// value: 1, value: 2, value: 3, complete

// dematerialize: Observable<Notification<T>> → Observable<T>
import { Notification } from 'rxjs';
const notifications = [
  Notification.createNext(1),
  Notification.createNext(2),
  Notification.createComplete()
] as Notification<number>[];

from(notifications).pipe(dematerialize())
  .subscribe(console.log); // 1, 2
```

## Examples

### Common Pattern — Merge Streams Without One Error Killing All
```typescript
import { merge, from } from 'rxjs';
import { materialize, dematerialize, filter } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const urls = ['/api/a', '/api/b', '/api/c'];

// Without materialize: one 404 kills the entire merge
// With materialize: errors become values; we can filter/handle them

merge(
  ...urls.map(url =>
    ajax.getJSON(url).pipe(materialize()) // errors become Notification objects
  )
).pipe(
  filter(n => n.kind === 'N'),  // keep only successful responses
  dematerialize()                // unwrap back to values
).subscribe(data => processData(data));
// Failed requests are silently dropped; successful ones continue
```

### Common Pattern — Delay Errors
```typescript
import { throwError } from 'rxjs';
import { materialize, dematerialize, delay, map } from 'rxjs/operators';

// delay() only delays next/complete — errors bypass it
// materialize lets you delay errors too
throwError(() => new Error('oops')).pipe(
  materialize(),                              // error → Notification
  delay(1000),                                // delay ALL notifications including error
  dematerialize()                             // unwrap back to error
).subscribe({ error: e => console.log(e.message) }); // logs after 1s
```

### Common Pattern — Testing (Marble Test Assertions)
```typescript
import { TestScheduler } from 'rxjs/testing';
import { materialize } from 'rxjs/operators';

// materialize is used internally by TestScheduler to record notification sequences
// You can use it to capture a stream's full history including errors/completion

const recorded: Notification<number>[] = [];

source$.pipe(materialize()).subscribe(n => recorded.push(n));
// recorded contains the full notification history as plain values
// Useful for assertions: expect(recorded[0]).toEqual(Notification.createNext(42))
```

### Common Pattern — Conditional Error Suppression
```typescript
import { mergeMap, materialize, dematerialize, filter, of } from 'rxjs/operators';

// Retry only specific error types; convert others to empty
source$.pipe(
  materialize(),
  mergeMap(notification => {
    if (notification.kind === 'E') {
      if (notification.error instanceof NetworkError) {
        return of(notification); // keep network errors for retry
      }
      return of(Notification.createComplete()); // suppress other errors → complete
    }
    return of(notification); // pass through next and complete as-is
  }),
  dematerialize()
).subscribe(handleValue);
```

## Common Pitfalls

### Anti-pattern: Using `materialize` When `catchError` Is Sufficient
```typescript
import { materialize, dematerialize, filter } from 'rxjs/operators';

// ❌ OVERENGINEERED — using materialize just to suppress errors
source$.pipe(
  materialize(),
  filter(n => n.kind !== 'E'),
  dematerialize()
).subscribe(console.log);

// ✅ SIMPLER — catchError + EMPTY is the standard pattern
import { catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';
source$.pipe(
  catchError(() => EMPTY)
).subscribe(console.log);

// WHY: materialize/dematerialize add complexity. Most error-handling patterns
// (suppress, recover, retry) are more clearly expressed with catchError, retry,
// and EMPTY. Reach for materialize only when you genuinely need to treat
// notifications as values — merging erroring streams, delaying errors,
// or recording full notification histories.
```

## Related Operators

- **`catchError`**: Standard error recovery — simpler than materialize for most cases
- **`retry`**: Resubscribe on error — no need to materialize for basic retry
- **`tap`**: Observe notifications without converting them — simpler for side effects
- **`Notification`**: The static class with `createNext`, `createError`, `createComplete` factory methods

## References
- **RxJS materialize**: [https://rxjs.dev/api/operators/materialize](https://rxjs.dev/api/operators/materialize)
- **RxJS dematerialize**: [https://rxjs.dev/api/operators/dematerialize](https://rxjs.dev/api/operators/dematerialize)

---

**`materialize`** — Cognitive Load: 3/5 | Usage: 2/5 | Errors become values — enables merging fallible streams without one killing all.
**`dematerialize`** — Cognitive Load: 3/5 | Usage: 2/5 | Always paired with `materialize` — inverse operation.
**Key teaching point**: Use `materialize/dematerialize` when you need to treat notifications (including errors) as first-class values. For simple error suppression, `catchError + EMPTY` is clearer.
