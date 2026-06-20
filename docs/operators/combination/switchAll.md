# switchAll

**Category**: Combination  
**Import**: `import { switchAll } from 'rxjs';`

## Description

`switchAll` converts a higher-order Observable into a first-order Observable using a "switch" strategy: whenever the source emits a new inner Observable, `switchAll` unsubscribes from the previously active inner Observable and immediately subscribes to the new one. Only the most recently emitted inner Observable is active at any given time.

The output Observable completes only when both the source Observable has completed and the most recently subscribed inner Observable has also completed. `switchAll` is ideal for "latest wins" scenarios such as type-ahead searches or live previews, where stale in-flight operations from previous interactions should be cancelled as soon as the user acts again.

## Signature

```typescript
function switchAll<O extends ObservableInput<any>>(): OperatorFunction<O, ObservedValueOf<O>>
```

## Parameters

`switchAll` takes no parameters.

## Return Type

`OperatorFunction<O, ObservedValueOf<O>>` — An Observable that emits values only from the most recently subscribed inner Observable, cancelling prior inner subscriptions when a new inner Observable arrives.

## Marble Diagram

```
Source:   --A-----B----|
A:          --1--2--3--4--|
B:                --a--b--|
          switchAll()
Output:   ----1--2--a--b--|
          (3 and 4 from A are never emitted because B replaced A)
```

## Examples

### Example 1: Live search with automatic cancellation of stale requests

```typescript
import { fromEvent, map, debounceTime, switchAll } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const searchInput = document.getElementById('search') as HTMLInputElement;

fromEvent(searchInput, 'input').pipe(
  debounceTime(300),
  map(event => {
    const term = (event.target as HTMLInputElement).value.trim();
    return ajax.getJSON<SearchResult[]>(`/api/search?q=${encodeURIComponent(term)}`);
  }),
  switchAll() // Cancel the previous search request when the user types again
).subscribe({
  next: results => renderResults(results),
  error: err => console.error('Search failed:', err),
});
```

### Example 2: Restarting an interval timer on each user click

```typescript
import { fromEvent, tap, map, interval, switchAll } from 'rxjs';

const clicks$ = fromEvent(document, 'click').pipe(
  tap(() => console.log('click — restarting timer'))
);

clicks$.pipe(
  map(() => interval(1000)),
  switchAll() // Each new click cancels the previous interval and starts fresh
).subscribe(tick => console.log('Tick:', tick));

// Output:
// click — restarting timer
// Tick: 0
// Tick: 1
// click — restarting timer  <-- previous interval is cancelled
// Tick: 0
// Tick: 1
// ...
```

### Example 3: Live route preview — cancel previous route calculation on new destination

```typescript
import { Subject, switchAll, map } from 'rxjs';

const destinationChanges$ = new Subject<string>();

destinationChanges$.pipe(
  map(destination =>
    // calculateRoute returns an Observable that emits route updates
    calculateRoute(currentLocation, destination)
  ),
  switchAll() // When destination changes, discard the in-progress calculation
).subscribe(route => {
  displayRouteOnMap(route);
  console.log(`Route updated: ${route.distance} km, ${route.duration} min`);
});

// As the user types a new destination, previous route calculations are cancelled
destinationChanges$.next('123 Main St');
destinationChanges$.next('456 Oak Ave'); // Cancels route to 123 Main St
destinationChanges$.next('789 Pine Rd'); // Cancels route to 456 Oak Ave
```

## Common Pitfalls

- **Values from superseded inner Observables are lost**: As soon as a new inner Observable arrives, all future values from the previous one are discarded. This is intentional for "latest wins" use cases, but is a bug if you need all values. Use `mergeAll` or `concatAll` instead.
- **Ongoing side effects may be abandoned**: If an inner Observable triggers a side effect (e.g. a database write), unsubscribing from it via `switchAll` cancels the Observable but may not cancel the underlying operation. Ensure side-effectful Observables handle cancellation properly, or use `exhaustAll` to prevent overlapping operations.
- **Inner Observable completion vs source completion**: The output completes only when the source completes AND the last active inner Observable completes. If the source completes but the most recent inner Observable is still active, the output remains open until that inner Observable also completes.

## Related Operators

- `exhaustAll` — ignores new inner Observables while the current one is active (opposite strategy)
- `concatAll` — queues inner Observables and processes them one at a time without cancellation
- `mergeAll` — subscribes to all inner Observables concurrently, cancelling none
- `switchMap` — combines the mapping and switch steps in one operator
