# skipWhile

**Category**: Filtering  
**Import**: `import { skipWhile } from 'rxjs';`

## Description

`skipWhile` suppresses values emitted by the source Observable as long as a predicate function returns `true`. Once the predicate returns `false` for the first time, that value and all subsequent values are forwarded — the predicate is never called again after the first failure.

This is distinct from `filter`: `filter` evaluates the predicate for every value; `skipWhile` stops evaluating after the first value that fails the predicate, and passes everything through from that point on.

## Signature

```typescript
function skipWhile<T>(predicate: (value: T, index: number) => boolean): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number) => boolean` | A function called for each value. Values are skipped while this returns `true`. Once it returns `false`, all subsequent values (including this one) pass through and the predicate is not called again. |

## Return Type

`MonoTypeOperatorFunction<T>` — skips initial values while the predicate holds, then passes all remaining values.

## Marble Diagram

```
Source: --1--2--3--4--2--1--|
        skipWhile(x => x < 3)
Output: --------3--4--2--1--|
        (note: 2 and 1 at end are emitted because the gate already opened)
```

## Examples

### Example 1: Skip until a threshold value is reached

```typescript
import { from } from 'rxjs';
import { skipWhile } from 'rxjs';

const temperatures$ = from([15, 18, 22, 19, 25, 30]);

temperatures$.pipe(
  skipWhile(temp => temp < 20)
).subscribe(temp => console.log('Temperature above threshold:', temp));

// Logs: 22, 19, 25, 30
// Note: 19 is included because the gate opened at 22
```

### Example 2: Skip loading states in a state stream

```typescript
import { BehaviorSubject } from 'rxjs';
import { skipWhile } from 'rxjs';

type AppState = 'loading' | 'ready' | 'error';
const state$ = new BehaviorSubject<AppState>('loading');

state$.pipe(
  skipWhile(s => s === 'loading')
).subscribe(s => console.log('App state:', s));

state$.next('loading'); // still skipped
state$.next('ready');   // Logs: App state: ready
state$.next('error');   // Logs: App state: error
```

### Example 3: Skip by index (positional skip)

```typescript
import { from } from 'rxjs';
import { skipWhile } from 'rxjs';

const values$ = from([10, 20, 30, 40, 50]);

values$.pipe(
  skipWhile((_, index) => index < 3)
).subscribe(v => console.log(v));

// Logs: 40, 50  (first 3 values at index 0,1,2 are skipped)
```

## Common Pitfalls

- **Once open, always open**: Unlike `filter`, `skipWhile` does not continue to evaluate the predicate after it first returns `false`. Values that previously would have been "skipped" will be emitted if they appear later in the stream.
- **Predicate is called with the index of emission, not position among skipped values**: The `index` argument counts all source emissions since subscription, regardless of whether they were skipped.
- **Side effects in the predicate**: The predicate should be side-effect-free. Since it stops being called once the gate opens, any side effects would only run for the initial segment of the stream.

## Related Operators

- `takeWhile` — the complement: passes values while the predicate holds
- `skip` — skips a fixed number of initial values
- `skipUntil` — skips until a notifier Observable fires
- `filter` — evaluates predicate for every value, does not stop after first failure
