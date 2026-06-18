# endWith / ignoreElements

---

## `endWith`

### Identity
- **Import**: `import { endWith } from 'rxjs/operators'`
- **Signature**: `endWith<T>(...values: T[]): MonoTypeOperatorFunction<T>`
- **Category**: Utility — appends one or more values before completion

### Functional Specification

Emits all source values, then emits the provided `values` in order, then completes. The complement of `startWith`.

```
Source:  --a--b--c--|
endWith('d', 'e'):
Result:  --a--b--c--d--e--|
```

### Examples

```typescript
import { of, interval } from 'rxjs';
import { endWith, take } from 'rxjs/operators';

// Append a sentinel value
of('alice', 'bob', 'charlie').pipe(
  endWith('END')
).subscribe(console.log); // alice, bob, charlie, END

// Append loading state transitions
userActions$.pipe(
  endWith({ type: 'SESSION_END' })
).subscribe(action => dispatch(action));

// endWith + startWith to bracket a stream
interval(1000).pipe(
  take(3),
  startWith(-1),
  endWith(99)
).subscribe(console.log); // -1, 0, 1, 2, 99
```

### Pitfall
```typescript
// ❌ WRONG — endWith doesn't fire on error (only on completion)
throwError(() => new Error('oops')).pipe(
  endWith('done') // never emits — error prevents completion
).subscribe({ error: e => console.log('error, not done') });

// ✅ CORRECT — use finalize for guaranteed teardown regardless of outcome
import { finalize } from 'rxjs/operators';
source$.pipe(
  finalize(() => cleanup()) // runs on complete, error, AND unsubscription
).subscribe();
// WHY: endWith is about emitting values before normal completion.
// For cleanup or teardown, finalize is the right tool.
```

---

## `ignoreElements`

### Identity
- **Import**: `import { ignoreElements } from 'rxjs/operators'`
- **Signature**: `ignoreElements<T>(): OperatorFunction<T, never>`
- **Category**: Utility — suppresses all `next` emissions, passes only `error` and `complete`

### Functional Specification

Drops every `next` notification from the source. Only `error` and `complete` are forwarded. The output type is `Observable<never>` — it will never emit a value to `next`.

```
Source:   --a--b--c--|
ignoreElements():
Result:   ------------|   (all values dropped, completion passed through)

Source:   --a--b--#
ignoreElements():
Result:   -----------#    (error passed through)
```

### Examples

```typescript
import { of, throwError } from 'rxjs';
import { ignoreElements, catchError } from 'rxjs/operators';

// Only care about completion signal, not values
longRunningOperation$.pipe(
  ignoreElements()
).subscribe({
  complete: () => console.log('operation finished'),
  error:    e  => console.error('operation failed:', e)
});

// Test that a stream completes without errors — ignore all values
source$.pipe(ignoreElements()).subscribe({
  complete: () => testPassed(),
  error:    e  => testFailed(e)
});

// Use as a "fire and forget completion signal"
const done$ = upload$.pipe(ignoreElements());
done$.subscribe({ complete: () => showSuccessBanner() });
```

### Common Pattern — Error-Only Stream
```typescript
import { merge } from 'rxjs';
import { ignoreElements } from 'rxjs/operators';

// Monitor multiple streams for errors — ignore their values
const errors$ = merge(
  streamA$.pipe(ignoreElements()),
  streamB$.pipe(ignoreElements()),
  streamC$.pipe(ignoreElements())
);

errors$.subscribe({
  error: e => logError(e) // only errors from any stream reach here
});
```

---

## `endWith` vs `startWith` vs `concat`

| | `startWith(v)` | `endWith(v)` | `concat(source$, of(v))` |
|---|---|---|---|
| Prepends | Yes | No | No |
| Appends | No | Yes | Yes (equivalent) |
| Multiple values | Yes | Yes | Via `of(a, b, c)` |
| Readability | High | High | Verbose |

## References
- [endWith](https://rxjs.dev/api/operators/endWith)
- [ignoreElements](https://rxjs.dev/api/operators/ignoreElements)

---

**`endWith`** — Cognitive Load: 1/5 | Usage: 3/5 | Symmetric complement to `startWith` — append sentinel/transition values before completion.
**`ignoreElements`** — Cognitive Load: 1/5 | Usage: 2/5 | Output type is `Observable<never>` — useful when only the completion/error signal matters.
