# exhaustAll

**Category**: Combination  
**Import**: `import { exhaustAll } from 'rxjs';`

## Description

`exhaustAll` converts a higher-order Observable into a first-order Observable using an "exhaust" strategy: when the source emits a new inner Observable, `exhaustAll` subscribes to it and starts forwarding its values. If the source emits another inner Observable while the current one is still active, that new Observable is silently ignored and dropped. Only once the current inner Observable completes will `exhaustAll` accept and subscribe to the next incoming inner Observable.

This behaviour is the opposite of `switchAll` (which cancels the current inner Observable in favour of the new one) and makes `exhaustAll` ideal for preventing duplicate or re-entrant operations — for example, ignoring a button click that triggers a long-running request while that request is already in flight.

## Signature

```typescript
function exhaustAll<O extends ObservableInput<any>>(): OperatorFunction<O, ObservedValueOf<O>>
```

## Parameters

`exhaustAll` takes no parameters.

## Return Type

`OperatorFunction<O, ObservedValueOf<O>>` — An Observable that emits values from the first active inner Observable and ignores new inner Observables until the current one completes.

## Marble Diagram

```
Source:   --A-----B--C-----D----|
A:          --1--2--3--|
B:                  (ignored, A still active)
C:                     --4--5--|
D:                              --6--|
          exhaustAll()
Output:   ----1--2--3-----4--5----6--|
```

## Examples

### Example 1: Preventing duplicate form submissions

```typescript
import { fromEvent, map, exhaustAll } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const submitBtn = document.getElementById('submit-btn')!;
const form = document.getElementById('my-form') as HTMLFormElement;

fromEvent(submitBtn, 'click').pipe(
  map(() => {
    const formData = new FormData(form);
    return ajax({
      url: '/api/submit',
      method: 'POST',
      body: Object.fromEntries(formData),
    });
  }),
  exhaustAll() // Ignore extra clicks while a submission is in flight
).subscribe({
  next: response => console.log('Submission successful:', response),
  error: err => console.error('Submission failed:', err),
});
```

### Example 2: Running a finite timer for each click, ignoring clicks during active timer

```typescript
import { fromEvent, map, interval, take, exhaustAll } from 'rxjs';

const button = document.getElementById('start-timer')!;

fromEvent(button, 'click').pipe(
  map(() => interval(1000).pipe(take(5))),
  exhaustAll() // If user clicks again while timer runs, the extra click is ignored
).subscribe(tick => console.log('Tick:', tick));
```

### Example 3: Rate-limiting search requests — only one in-flight at a time

```typescript
import { fromEvent, map, debounceTime, exhaustAll } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const searchInput = document.getElementById('search') as HTMLInputElement;

fromEvent(searchInput, 'input').pipe(
  debounceTime(300),
  map(event => {
    const term = (event.target as HTMLInputElement).value;
    return ajax.getJSON<SearchResult[]>(`/api/search?q=${encodeURIComponent(term)}`);
  }),
  exhaustAll() // If a search is still running, ignore the newer input event
).subscribe(results => console.log('Search results:', results));
```

## Common Pitfalls

- **New inner Observables are silently dropped**: Unlike `concatAll`, which queues pending inner Observables, `exhaustAll` discards them entirely. If you need to process every inner Observable in order, use `concatAll` instead.
- **Can feel unresponsive to users**: Because user interactions are discarded while a previous one is processing, the UI may appear to ignore input. Always provide visual feedback (e.g. a loading indicator or disabling the button) so users know the action is in progress.
- **Source completion does not wait for dropped Observables**: The output completes once the source completes and the currently active inner Observable completes. Any inner Observables that were ignored are not accounted for at all.

## Related Operators

- `switchAll` — cancels the current inner Observable when a new one arrives (opposite behaviour)
- `concatAll` — queues inner Observables and processes them one at a time in order
- `mergeAll` — subscribes to all inner Observables concurrently
- `exhaustMap` — combines the mapping and exhaustion steps in one operator
