# endWith / ignoreElements — Advanced Patterns

> **Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
> **Teaching Sequence**: After `startWith` — introduces stream bracketing and notification filtering

---

## Advanced Behavioral Model

### `endWith` — Append Before Completion

`endWith(...values)` appends values synchronously after the source completes, before the downstream receives the completion signal.

```
Source:      --a--b--c--|
endWith('d', 'e'):
Result:      --a--b--c--d--e--|
                        ^^^^
                 synchronous on completion
```

Key invariant: **`endWith` does not fire on error**. If the source errors, downstream sees the error directly — the appended values are never emitted.

```
Source:      --a--b--#          (# = error)
endWith('sentinel'):
Result:      --a--b--#          (sentinel never emitted)
```

### `ignoreElements` — Filter to Completion/Error Only

`ignoreElements()` drops all `next` notifications, passing only `complete` and `error` through.

```
Source:      --1--2--3--|
ignoreElements():
Result:      -----------|       (values gone, completion passes)

Source:      --1--2--#
ignoreElements():
Result:      ----------#        (values gone, error passes)
```

Mental model: turns an Observable into a pure lifecycle signal.

---

## Type System Integration

```typescript
import { endWith } from 'rxjs/operators';
import { ignoreElements } from 'rxjs/operators';
import { NEVER, merge } from 'rxjs';

// endWith widens the type to T | A
const source$: Observable<number> = of(1, 2, 3);
const withSentinel$: Observable<number | string> = source$.pipe(
  endWith('DONE')   // Observable<number | string>
);

// To keep the type as T, the appended value must match T
const numeric$: Observable<number> = source$.pipe(
  endWith(-1)       // Observable<number> — type preserved
);

// ignoreElements(): Observable<never>
// Returns Observable<never> because no values pass through
const lifecycle$: Observable<never> = dataStream$.pipe(
  ignoreElements()
);

// Use in merge to react to completion without values
const withCompletion$: Observable<DataEvent | CompletionEvent> = merge(
  dataStream$,
  dataStream$.pipe(
    ignoreElements(),
    endWith({ type: 'STREAM_COMPLETE' } as CompletionEvent),
  )
);
```

---

## Advanced Patterns

### 1. Stream Bracketing with startWith + endWith

Wrap a stream with lifecycle markers — useful for loading states, audit logs, and protocol framing.

```typescript
import { of, interval } from 'rxjs';
import { startWith, endWith, take, map } from 'rxjs/operators';

type LoadState =
  | { type: 'LOADING' }
  | { type: 'DATA'; value: number }
  | { type: 'COMPLETE' };

const dataWithState$: Observable<LoadState> = interval(500).pipe(
  take(5),
  map(n => ({ type: 'DATA' as const, value: n })),
  startWith({ type: 'LOADING' as const }),
  endWith({ type: 'COMPLETE' as const }),
);

dataWithState$.subscribe(state => {
  if (state.type === 'LOADING') showSpinner();
  if (state.type === 'DATA')    renderRow(state.value);
  if (state.type === 'COMPLETE') hideSpinner();
});
// Expected output: LOADING → DATA(0) → DATA(1) … → DATA(4) → COMPLETE
```

### 2. Protocol Framing for WebSocket Messages

WebSocket protocols often require explicit start/end framing. `startWith`/`endWith` compose cleanly for this.

```typescript
import { webSocket } from 'rxjs/webSocket';
import { startWith, endWith, map } from 'rxjs/operators';

interface WsFrame {
  type: 'START' | 'DATA' | 'END';
  payload?: unknown;
}

function framedStream<T>(ws$: Observable<T>): Observable<WsFrame> {
  return ws$.pipe(
    map(payload => ({ type: 'DATA' as const, payload })),
    startWith({ type: 'START' as const }),
    endWith({ type: 'END' as const }),
  );
}

framedStream(webSocket('wss://api.example.com/stream'))
  .subscribe(frame => sendToProtocolHandler(frame));
```

### 3. ignoreElements for Side-Effect-Only Completion Handling

When you want to run a side effect only on completion/error of a stream, not on its values.

```typescript
import { ignoreElements } from 'rxjs/operators';
import { finalize, tap } from 'rxjs/operators';

// Track when a background job finishes without subscribing to its output
function monitorJob(job$: Observable<JobUpdate>): Observable<never> {
  return job$.pipe(
    tap({
      error: err => logger.error('job failed', err),
    }),
    ignoreElements(), // suppress JobUpdate values — only lifecycle matters
  );
}

// Compose with other streams using merge
const ui$ = merge(
  userActions$,
  monitorJob(backgroundJob$), // merge lifecycle signals into the main stream
);
// Observable<UserAction | never> = Observable<UserAction>
// Only emits on completion/error of backgroundJob$
```

### 4. endWith + ignoreElements — Completion as a Value

Transform an Observable's completion into an emitted value, then ignore all prior values.

```typescript
import { endWith, ignoreElements } from 'rxjs/operators';

// Convert any observable's completion into a single emission
function whenComplete<R>(source$: Observable<unknown>, value: R): Observable<R> {
  return source$.pipe(
    ignoreElements(),
    endWith(value),
  ) as Observable<R>;
}

// Wait for all three phases to finish, emit 'ready'
const allPhasesComplete$ = merge(
  whenComplete(phase1$, 'phase1'),
  whenComplete(phase2$, 'phase2'),
  whenComplete(phase3$, 'phase3'),
);

forkJoin([phase1$, phase2$, phase3$]).pipe(
  ignoreElements(),
  endWith('all-done'),
).subscribe(() => initializeApp());
```

### 5. Audit Trail with endWith

Append immutable audit records to data streams before persistence.

```typescript
import { endWith, map, toArray } from 'rxjs/operators';

interface AuditRecord {
  timestamp: number;
  event: string;
  itemCount: number;
}

function withAuditTrail<T>(
  source$: Observable<T>,
  streamName: string,
): Observable<T | AuditRecord> {
  let count = 0;

  return source$.pipe(
    map(value => { count++; return value; }),
    endWith({
      timestamp: Date.now(),
      event: `${streamName}:complete`,
      itemCount: count,
    } as AuditRecord),
  );
}

withAuditTrail(orderEvents$, 'orders')
  .pipe(toArray())
  .subscribe(records => saveToAuditLog(records));
// Last element is always the audit record
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — expecting endWith to fire on error
source$.pipe(
  endWith('cleanup')
).subscribe({
  next: v => console.log(v),
  error: e => console.error(e),   // 'cleanup' never emitted here
});

// ✅ CORRECT — use finalize for guaranteed teardown on any termination
source$.pipe(
  finalize(() => console.log('cleanup'))  // always runs: complete OR error
).subscribe({
  next: v => console.log(v),
  error: e => console.error(e),
});
// WHY: endWith is for appending values before completion only.
// finalize runs as a side effect on any termination including error.


// ❌ INCORRECT — using ignoreElements thinking it affects errors
errorProne$.pipe(
  ignoreElements(),
).subscribe({
  error: e => console.error(e), // error STILL propagates
});

// ✅ CORRECT — pair with catchError to suppress errors too
errorProne$.pipe(
  ignoreElements(),
  catchError(() => EMPTY),  // suppress error, treat as completion
).subscribe({ complete: () => console.log('done') });
// WHY: ignoreElements only filters next notifications.
// Errors pass through unmodified.


// ❌ INCORRECT — type mismatch with endWith
const nums$: Observable<number> = of(1, 2, 3).pipe(
  endWith('end') // TypeScript error: string not assignable to number
);

// ✅ CORRECT — annotate with union or match the type
const withMarker$: Observable<number | string> = of(1, 2, 3).pipe(
  endWith('end')
);
// OR keep type as number:
const withSentinel$: Observable<number> = of(1, 2, 3).pipe(
  endWith(-1)
);
// WHY: endWith<T>(...values: T[]) infers from source; appended values
// must be assignable to T or the type widens to T | AppendedType.
```

---

## Operator Comparison: Stream Termination Utilities

| Need | Operator |
|---|---|
| Append values before completion | `endWith(...values)` |
| Prepend values at subscription | `startWith(...values)` |
| Side effect on any termination | `finalize(fn)` |
| Drop all values, keep lifecycle | `ignoreElements()` |
| Suppress errors | `catchError(() => EMPTY)` |
| Emit only on completion, as value | `ignoreElements()` + `endWith(value)` |

---

## Related Operators

- **`startWith`** — the symmetric counterpart; prepends values at subscription
- **`finalize`** — guaranteed side effect on completion OR error; use instead of `endWith` for teardown
- **`materialize`** — wraps all notifications (next/error/complete) as `Notification` objects
- **`dematerialize`** — reverse of materialize
- **`defaultIfEmpty`** — emit a fallback value if the source completes without emitting
- **`catchError`** — recover from errors (pair with `ignoreElements` to suppress error propagation)
