# switchMap

**Category**: Transformation  
**Import**: `import { switchMap } from 'rxjs';`

## Description

Projects each source value to an Observable which is merged in the output Observable, emitting values only from the most recently projected Observable. Each time a new value arrives from the source, `switchMap` unsubscribes from the previous inner Observable and subscribes to the new one.

This "switch" behavior makes `switchMap` ideal for scenarios where only the result of the most recent request matters — for example, a search-as-you-type feature where earlier search results should be discarded when the user keeps typing.

## Signature

```typescript
function switchMap<T, O extends ObservableInput<any>>(
  project: (value: T, index: number) => O
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `project` | `(value: T, index: number) => ObservableInput<O>` | A function that returns an Observable (or Promise, array, etc.) for each source value. The previous inner Observable is automatically unsubscribed. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — emits values only from the most recently projected inner Observable.

## Marble Diagram

```
Source:  --a---------b----c------|
            switchMap(x => inner)
Inner a: ----1--2--3--...
Inner b:             --4--5--...
Inner c:                  --6--|
Output:  ----1--2----4----6--|
                    (a cancelled when b arrives)
                         (b cancelled when c arrives)
```

## Examples

### Example 1: Search as you type

```typescript
import { fromEvent, switchMap, debounceTime, map, from } from 'rxjs';

const searchInput = document.querySelector<HTMLInputElement>('#search')!;

fromEvent(searchInput, 'input').pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300),
  switchMap(query =>
    from(fetch(`/api/search?q=${encodeURIComponent(query)}`).then(r => r.json()))
  )
).subscribe(results => {
  console.log('Search results:', results);
});
```

### Example 2: Restart a timer on every user interaction

```typescript
import { fromEvent, switchMap, interval, map } from 'rxjs';

const button = document.querySelector('#reset-btn')!;

// Every click restarts the countdown from 10
fromEvent(button, 'click').pipe(
  switchMap(() => interval(1000).pipe(map(i => 10 - i)))
).subscribe(countdown => {
  console.log('Countdown:', countdown);
});
```

### Example 3: Route navigation — cancel previous page data load

```typescript
import { Subject, switchMap, from } from 'rxjs';

const route$ = new Subject<string>();

route$.pipe(
  switchMap(routeId =>
    from(fetch(`/api/pages/${routeId}`).then(r => r.json()))
  )
).subscribe(pageData => {
  console.log('Page data loaded:', pageData);
});

// Navigating quickly — only the last navigation's data arrives
route$.next('home');
route$.next('about');
route$.next('contact'); // Only contact page data is delivered
```

## Common Pitfalls

- **Losing in-flight work**: Because `switchMap` cancels the previous inner Observable, it can drop in-progress HTTP requests. If every request must complete (e.g., a form submission), use `concatMap` or `exhaustMap` instead.
- **Completing too early**: If the source completes while no inner Observable is active, the output completes immediately. If the source completes while an inner Observable is still running, the output waits for that inner Observable to complete.
- **No concurrency parameter**: Unlike `mergeMap`, `switchMap` does not accept a `concurrent` argument — it always switches to the latest.

## Related Operators

- `mergeMap` — subscribes to all inner Observables concurrently; does not cancel previous ones
- `concatMap` — queues inner subscriptions; waits for each to complete before starting the next
- `exhaustMap` — ignores new source values while the current inner Observable is active
- `switchMapTo` — deprecated variant that maps all values to the same inner Observable
