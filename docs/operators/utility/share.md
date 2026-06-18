# share

**Category**: Utility  
**Import**: `import { share } from 'rxjs';`

## Description

`share` returns a new Observable that multicasts (shares) the original Observable among multiple subscribers. As long as there is at least one subscriber, the source Observable is subscribed to and emitting data. When all subscribers have unsubscribed, `share` will unsubscribe from the source, making the stream "cold" again. This turns the stream "hot" — all active subscribers receive the same emissions at the same time.

By default, `share` uses a plain `Subject` internally and resets (goes cold) when the source errors, completes, or when all subscribers unsubscribe. Each reset behavior can be controlled individually via the optional configuration object's `resetOnError`, `resetOnComplete`, and `resetOnRefCountZero` properties. Each can be `true` (immediate reset), `false` (no reset), or a notifier factory that returns an observable controlling when the reset should happen.

Unlike `shareReplay`, `share` does not cache and replay previous values to late subscribers.

## Signature

```typescript
function share<T>(): MonoTypeOperatorFunction<T>
function share<T>(options: ShareConfig<T>): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| options | `ShareConfig<T>` | Optional configuration object. |

### `ShareConfig<T>` properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `connector` | `() => SubjectLike<T>` | `() => new Subject<T>()` | Factory for the subject used to multicast. |
| `resetOnError` | `boolean \| ((error: any) => ObservableInput<any>)` | `true` | Whether/when to reset on source error. |
| `resetOnComplete` | `boolean \| (() => ObservableInput<any>)` | `true` | Whether/when to reset on source completion. |
| `resetOnRefCountZero` | `boolean \| (() => ObservableInput<any>)` | `true` | Whether/when to reset when all subscribers unsubscribe. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns a multicasting Observable backed by a `Subject`.

## Marble Diagram

```
Source:   --a----b----c--|  (subscribed once)
sub1:     --a----b----c--|
sub2:         ---b----c--|  (subscribes late, misses 'a')
              share()
Output:   --a----b----c--|
```

## Examples

### Example 1: Share an expensive computation between two consumers

```typescript
import { interval, tap, map, take, share } from 'rxjs';

const source$ = interval(1000).pipe(
  tap(x => console.log('Processing:', x)),
  map(x => x * x),
  take(4),
  share()
);

source$.subscribe(x => console.log('sub 1:', x));
source$.subscribe(x => console.log('sub 2:', x));

// Processing: 0
// sub 1: 0
// sub 2: 0
// Processing: 1
// sub 1: 1
// sub 2: 1
// (source is only subscribed once despite two consumers)
```

### Example 2: Delayed reset with `resetOnRefCountZero`

```typescript
import { interval, take, share, timer } from 'rxjs';

const source$ = interval(1000).pipe(
  take(5),
  share({
    resetOnRefCountZero: () => timer(2000) // wait 2s before resetting
  })
);

const sub1 = source$.subscribe(x => console.log('sub 1:', x));
setTimeout(() => {
  sub1.unsubscribe();
  // Source is NOT reset immediately — 2s grace period starts
}, 1500);

setTimeout(() => {
  // Subscribes within grace period: reuses existing Subject, gets live values
  source$.subscribe(x => console.log('sub 2 (resumed):', x));
}, 2500);
```

### Example 3: No reset on completion (keep alive for future subscribers)

```typescript
import { of, share, delay } from 'rxjs';

const request$ = of('data').pipe(
  delay(1000),
  share({ resetOnComplete: false }) // completed source stays completed
);

request$.subscribe(d => console.log('sub 1:', d));

setTimeout(() => {
  // Subscribes after source has completed — nothing is emitted (no replay)
  request$.subscribe({
    next: d => console.log('sub 2:', d),
    complete: () => console.log('sub 2 complete')
  });
}, 2000);
```

## Common Pitfalls

- **Late subscribers miss values**: `share` does not replay. A subscriber joining after values have already been emitted will miss them. Use `shareReplay` if late subscribers need previous values.
- **Synchronous sources and `share`**: For purely synchronous sources, `share` may not work as expected because by the time the second subscriber subscribes, the ref count may have already gone from 1 back to 0 and reset. Use `connect` for synchronous multicast scenarios.
- **Reset behavior**: By default, `share` resets on error, complete, and zero subscribers. If you want the source to keep running after all subscribers leave (until it completes), set `resetOnRefCountZero: false`.

## Related Operators

- `shareReplay` — like `share` but uses a `ReplaySubject` to cache and replay N values to late subscribers
- `connect` — more explicit multicast control; handles synchronous sources correctly
- `publish` (deprecated) — the older multicast API replaced by `share`/`connect`
