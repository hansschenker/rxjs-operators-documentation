# shareReplay

**Category**: Utility  
**Import**: `import { shareReplay } from 'rxjs';`

## Description

`shareReplay` multicasts the source Observable and replays a specified number of emissions to any new subscriber, even if they subscribe after those values were emitted. It is built on top of `share` using a `ReplaySubject` as the internal connector.

**Why use `shareReplay`?** You generally want it when:
- You have side-effects or expensive computations that should only run once regardless of how many subscribers there are.
- You have late subscribers that need access to previously emitted values (e.g., caching an HTTP request so the second component gets the same data without triggering a new request).

**Key difference from `share`**: `share` uses a plain `Subject` (no replay). `shareReplay` uses a `ReplaySubject` that buffers up to `bufferSize` values and replays them to new subscribers.

**Reference counting (`refCount`)**: By default `shareReplay` uses `refCount: false`, meaning the source subscription continues even when all subscribers have unsubscribed. Set `refCount: true` to unsubscribe from the source when the reference count drops to zero.

## Signature

```typescript
function shareReplay<T>(bufferSize?: number, windowTime?: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>
function shareReplay<T>(config: ShareReplayConfig): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| bufferSize | `number` | Maximum number of emissions to cache for replay. Defaults to `Infinity`. |
| windowTime | `number` | Maximum age (in ms) of cached values. Defaults to `Infinity`. |
| scheduler | `SchedulerLike` | Optional scheduler for the `ReplaySubject`. |

### `ShareReplayConfig` properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `bufferSize` | `number` | `Infinity` | Number of values to replay. |
| `windowTime` | `number` | `Infinity` | Max age of buffered values in ms. |
| `refCount` | `boolean` | `false` | If `true`, unsubscribe from source when ref count reaches zero. |
| `scheduler` | `SchedulerLike` | — | Optional scheduler. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns a multicasting Observable that replays the last `bufferSize` values to new subscribers.

## Marble Diagram

```
Source:  --a--b--c--d--e--|
sub 1:   --a--b--c--d--e--|
sub 2 (subscribes late, after 'c'):
         shareReplay(2)
sub 2:            --b--c--d--e--|
                    (replays last 2 values: b, c)
```

## Examples

### Example 1: Cache an HTTP request result for multiple components

```typescript
import { from, shareReplay } from 'rxjs';

// Create the shared request once at the service level
const userProfile$ = from(fetch('/api/profile').then(r => r.json())).pipe(
  shareReplay(1) // Cache the single response; replay to any late subscriber
);

// Component A subscribes — triggers the HTTP request
userProfile$.subscribe(profile => console.log('Component A:', profile.name));

// Component B subscribes 2 seconds later — gets the cached result, no new request
setTimeout(() => {
  userProfile$.subscribe(profile => console.log('Component B:', profile.name));
}, 2000);
```

### Example 2: Late subscriber receives last N values

```typescript
import { interval, take, shareReplay } from 'rxjs';

const stream$ = interval(1000).pipe(
  take(6),
  shareReplay(3)
);

stream$.subscribe(x => console.log('sub A:', x));
stream$.subscribe(x => console.log('sub B:', x));

setTimeout(() => {
  // sub C joins after ~5 values have been emitted
  // It immediately receives the last 3 buffered values
  stream$.subscribe(x => console.log('sub C:', x));
}, 5500);
```

### Example 3: Use `refCount: true` to auto-unsubscribe from source

```typescript
import { interval, shareReplay, take } from 'rxjs';

const source$ = interval(1000).pipe(
  shareReplay({ bufferSize: 1, refCount: true }),
  take(5)
);

const sub1 = source$.subscribe(x => console.log('sub 1:', x));
const sub2 = source$.subscribe(x => console.log('sub 2:', x));

setTimeout(() => {
  sub1.unsubscribe();
  sub2.unsubscribe();
  // refCount drops to 0 → source is unsubscribed
  console.log('Both unsubscribed, source stopped');
}, 2500);
```

## Common Pitfalls

- **`refCount: false` (default) keeps the source alive forever**: If you use `shareReplay(1)` on an HTTP call and nobody subscribes, the source keeps running. Use `refCount: true` or `shareReplay({ bufferSize: 1, refCount: true })` when you want cleanup on zero subscribers.
- **Completed source is cached indefinitely (with `resetOnComplete: false`)**: `shareReplay` sets `resetOnComplete: false` internally via `share`. A completed source will serve its cached buffer to all future subscribers without re-subscribing to the source.
- **Errored sources reset**: Unlike completion, errors do reset the internal state (via `resetOnError: true`). A new subscriber after an error will trigger a fresh subscription to the source.
- **Buffer can grow large**: `shareReplay()` with no arguments has `bufferSize: Infinity`. Always specify a `bufferSize` for long-running streams.

## Related Operators

- `share` — multicasts without replay; use when late subscribers should only receive new values
- `connect` — explicit multicast with a selector function; handles synchronous sources correctly
- `ReplaySubject` — the underlying implementation; use directly when you need a subject with replay semantics
