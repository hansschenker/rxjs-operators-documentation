# combineLatestAll

**Category**: Combination  
**Import**: `import { combineLatestAll } from 'rxjs';`

## Description

`combineLatestAll` collects all inner Observables emitted by a higher-order source, waits for the source to complete, then subscribes to all collected inner Observables simultaneously using the `combineLatest` strategy. Once every inner Observable has emitted at least one value, it emits an array containing the latest value from each inner Observable. After that, every time any inner Observable emits, a new array of the latest values is emitted.

An optional `project` function can transform the array of latest values into a custom output type before emission. This operator is useful when you need to react to changes across a dynamic set of streams that are not known until the source completes.

## Signature

```typescript
function combineLatestAll<T>(): OperatorFunction<ObservableInput<T>, T[]>
function combineLatestAll<T, R>(project: (...values: T[]) => R): OperatorFunction<ObservableInput<T>, R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| project | `(...values: T[]) => R` | Optional. A function that takes the latest value from each inner Observable as arguments and returns the value to emit. If omitted, an array of the latest values is emitted. |

## Return Type

`OperatorFunction<ObservableInput<T>, T[]>` — without `project`, emits arrays of the latest values. With `project`, emits whatever the projection function returns.

## Marble Diagram

```
Source:   --A--B--|   (source completes, then combineLatest begins)
A:                 --1-----3--|
B:                 -----2-----|
          combineLatestAll()
Output:             -----[1,2]-[3,2]--|
```

## Examples

### Example 1: Combining the latest price for a dynamic set of stock symbols

```typescript
import { Subject, map, take, combineLatestAll } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const symbols = ['AAPL', 'GOOG', 'MSFT'];

// Create a stream that emits one Observable per symbol, then completes
const symbolStreams$ = from(symbols).pipe(
  map(symbol =>
    interval(2000).pipe(
      map(() => ({ symbol, price: Math.random() * 1000 })),
      take(5)
    )
  )
  // from() completes after emitting all three Observables
);

symbolStreams$.pipe(
  combineLatestAll()
  // Emits [latestAAPL, latestGOOG, latestMSFT] whenever any price updates
).subscribe(prices => {
  console.log('Latest prices:', prices);
});
```

### Example 2: Monitoring multiple sensor streams with a projection function

```typescript
import { from, interval, map, take, combineLatestAll } from 'rxjs';

const sensorIds = ['temperature', 'humidity', 'pressure'];

from(sensorIds).pipe(
  map(sensorId =>
    interval(1000).pipe(
      map(() => ({ sensorId, value: Math.random() * 100 })),
      take(10)
    )
  ),
  combineLatestAll(
    (temp, humidity, pressure) => ({
      temperature: temp.value.toFixed(1),
      humidity: humidity.value.toFixed(1),
      pressure: pressure.value.toFixed(1),
      timestamp: new Date().toISOString(),
    })
  )
).subscribe(dashboard => console.log('Dashboard:', dashboard));
```

### Example 3: Waiting for all form field streams before combining

```typescript
import { Subject, take, combineLatestAll } from 'rxjs';

// Dynamically built list of form field observables
const formFields$ = new Subject<Observable<string>>();

// After emitting all fields, complete the subject
formFields$.pipe(
  take(3), // Collect exactly 3 field observables then complete
  combineLatestAll()
).subscribe(([username, email, password]) => {
  const isValid = username.length > 2 && email.includes('@') && password.length > 8;
  console.log('Form valid:', isValid, { username, email });
});

// Push field streams onto the subject
formFields$.next(usernameField$);
formFields$.next(emailField$);
formFields$.next(passwordField$);
```

## Common Pitfalls

- **Source must complete before any inner Observables are subscribed**: All inner Observables are collected first, and subscription only begins after the outer source completes. If the source never completes, `combineLatestAll` will never subscribe to any inner Observable and will never emit.
- **All inner Observables must emit at least once**: Following the same rule as `combineLatest`, no output is produced until every inner Observable has emitted its first value. An inner Observable that completes without emitting will cause the output to never emit.
- **Contrast with `combineLatestWith`**: Use `combineLatestWith` when the set of streams is known at construction time. Use `combineLatestAll` when the streams are produced dynamically by a higher-order source.

## Related Operators

- `combineLatestWith` — pipeable operator for combining a known, static set of Observables with `combineLatest` semantics
- `combineLatest` — creation operator that takes a fixed array of Observable inputs
- `mergeAll` — flattens a higher-order Observable concurrently without waiting for the source to complete
- `zipAll` — like `combineLatestAll` but combines values by index rather than by latest value
