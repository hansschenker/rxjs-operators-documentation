# connect

**Category**: Utility  
**Import**: `import { connect } from 'rxjs';`

## Description

`connect` creates an observable by multicasting the source within a selector function, giving you a `shared$` observable that you can subscribe to multiple times inside the selector before the actual connection to the source is made. This sets it apart from `share`, which can fail to share a single subscription with multiple consumers for purely synchronous sources.

When you subscribe to the result of `connect`, the `selector` function is called. The function receives a `shared$` observable (not yet connected to the source). You build your multicast result inside the selector using `shared$`. Once the selector's returned observable is subscribed, the operator then connects the multicast to the source. This ordering guarantees that all derived subscriptions are wired up before values start flowing.

## Signature

```typescript
function connect<T, O extends ObservableInput<unknown>>(
  selector: (shared: Observable<T>) => O,
  config?: ConnectConfig<T>
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| selector | `(shared: Observable<T>) => O` | A function that receives the multicast observable and returns the observable to subscribe to. Inside this function, you define how `shared$` is used by different consumers. |
| config | `ConnectConfig<T>` | Optional configuration. `connector` defaults to `() => new Subject<T>()`. |

### `ConnectConfig<T>` properties

| Property | Type | Description |
|----------|------|-------------|
| `connector` | `() => SubjectLike<T>` | Factory for the subject used to multicast. Use `() => new ReplaySubject(1)` for replay behavior. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — the operator returns the observable produced by `selector`, wired to the multicasted source.

## Marble Diagram

```
Source: --(1)(2)(3)(4)(5)|  (synchronous)

connect(shared$ => merge(
  shared$.pipe(filter(n => n % 2 === 0), map(n => `even ${n}`)),
  shared$.pipe(filter(n => n % 2 === 1), map(n => `odd ${n}`))
))

Output: --(odd 1)(even 2)(odd 3)(even 4)(odd 5)|
```

## Examples

### Example 1: Share a synchronous observable across multiple derived streams

```typescript
import { of, tap, connect, merge, map, filter } from 'rxjs';

const source$ = of(1, 2, 3, 4, 5).pipe(
  tap(n => console.log(`source emitted ${n}`))
);

source$.pipe(
  connect(shared$ => merge(
    shared$.pipe(map(n => `all: ${n}`)),
    shared$.pipe(filter(n => n % 2 === 0), map(n => `even: ${n}`)),
    shared$.pipe(filter(n => n % 2 === 1), map(n => `odd: ${n}`))
  ))
).subscribe(console.log);

// source emitted 1 (only once per value!)
// all: 1
// odd: 1
// source emitted 2
// all: 2
// even: 2
// ...
```

### Example 2: Use a `ReplaySubject` connector for late subscribers inside selector

```typescript
import { interval, connect, take, Subject, ReplaySubject, merge, skip } from 'rxjs';

interval(500).pipe(
  take(5),
  connect(
    shared$ => merge(
      shared$,
      shared$.pipe(skip(2)) // receives replayed values from ReplaySubject
    ),
    { connector: () => new ReplaySubject(2) }
  )
).subscribe(console.log);
```

### Example 3: Build a multicast pipeline with separate hot consumers

```typescript
import { fromEvent, connect, map, filter, merge } from 'rxjs';

const clicks$ = fromEvent<MouseEvent>(document, 'click');

clicks$.pipe(
  connect(shared$ => merge(
    shared$.pipe(
      filter(e => e.clientX < window.innerWidth / 2),
      map(() => 'Left click')
    ),
    shared$.pipe(
      filter(e => e.clientX >= window.innerWidth / 2),
      map(() => 'Right click')
    )
  ))
).subscribe(console.log);
```

## Common Pitfalls

- **`share` is insufficient for synchronous sources**: If your source is synchronous, by the time a second subscriber subscribes to `share()`'s output, the first subscription has already completed and the ref count is back at zero, causing a reset. `connect` solves this by wiring all subscriptions before connecting to the source.
- **Do not subscribe outside the selector**: Only subscribe to `shared$` inside the `selector` function. Subscribing outside means those subscriptions won't be counted before the connection is made.
- **`takeUntil` inside `connect` vs `takeWhile`**: Using `takeUntil` on `shared$` inside `connect`'s selector can cause unexpected unsubscription behavior. Prefer `takeWhile` in those scenarios.

## Related Operators

- `share` — simpler multicast for asynchronous sources; may not work for synchronous ones
- `shareReplay` — multicast with replay; built on `share` with a `ReplaySubject`
- `connectable` — a lower-level primitive that returns a `ConnectableObservable` for manual `connect()` calls
