# timestamp

**Category**: Utility  
**Import**: `import { timestamp } from 'rxjs';`

## Description

`timestamp` attaches an absolute timestamp to each emitted value, producing objects of shape `{ value: T, timestamp: number }`. The timestamp is obtained from the provided `TimestampProvider`'s `now()` method, which defaults to `dateTimestampProvider` — a thin wrapper around `Date.now()` that returns milliseconds since the Unix epoch (January 1, 1970 UTC).

Use `timestamp` when you need to record when each event occurred for logging, auditing, or displaying time-based information alongside your data.

## Signature

```typescript
function timestamp<T>(timestampProvider: TimestampProvider = dateTimestampProvider): OperatorFunction<T, Timestamp<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| timestampProvider | `TimestampProvider` | An object with a `now()` method that returns the current time. Defaults to `dateTimestampProvider` (milliseconds since epoch via `Date.now()`). |

## Return Type

`OperatorFunction<T, Timestamp<T>>` — an operator that transforms each source value into `{ value: T, timestamp: number }`.

## Marble Diagram

```
Source:  --a--------b--c--|
         timestamp()
Output:  --{v:a,t:T1}--------{v:b,t:T2}--{v:c,t:T3}--|
                    (T1, T2, T3 are epoch ms at emission time)
```

## Examples

### Example 1: Record when user events occur

```typescript
import { fromEvent, timestamp } from 'rxjs';

fromEvent(document, 'click').pipe(
  timestamp()
).subscribe(({ value, timestamp }) => {
  const date = new Date(timestamp);
  console.log(`Click at ${date.toISOString()}`, value);
});

// Click at 2026-06-18T10:43:21.345Z PointerEvent {...}
```

### Example 2: Log API response arrival times

```typescript
import { from, timestamp, map } from 'rxjs';

const apiCall$ = from(fetch('/api/data').then(r => r.json()));

apiCall$.pipe(
  timestamp(),
  map(({ value, timestamp }) => ({
    receivedAt: new Date(timestamp).toISOString(),
    data: value
  }))
).subscribe(record => {
  console.log('Response logged:', record);
});
```

### Example 3: Calculate age of cached values

```typescript
import { Subject, timestamp, map } from 'rxjs';

const cache$ = new Subject<string>();
const MAX_AGE_MS = 5000;

cache$.pipe(
  timestamp(),
  map(({ value, timestamp }) => ({
    value,
    ageMs: Date.now() - timestamp,
    stale: Date.now() - timestamp > MAX_AGE_MS
  }))
).subscribe(entry => {
  if (entry.stale) {
    console.warn('Stale cache entry:', entry.value);
  } else {
    console.log('Fresh entry:', entry.value, `(${entry.ageMs}ms old)`);
  }
});
```

## Common Pitfalls

- **Timestamp is captured at emission, not subscription**: The `timestamp` reflects when the source emitted the value, not when the downstream subscriber processed it.
- **Custom providers for testing**: When unit testing time-sensitive code, pass a custom `TimestampProvider` with a controlled `now()` to make assertions deterministic.
- **Changed output type**: `timestamp` changes the emitted type from `T` to `{ value: T, timestamp: number }`. Downstream operators need to destructure accordingly.

## Related Operators

- `timeInterval` — records elapsed milliseconds between consecutive emissions rather than absolute wall-clock time
- `delay` — shifts emission times without adding metadata
- `tap` — log data without changing the emitted type
