# sample

**Category**: Filtering  
**Import**: `import { sample } from 'rxjs';`

## Description

`sample` emits the most recently received value from the source Observable whenever a second Observable (the `notifier`) emits. If the source has not emitted a new value since the last sample, nothing is emitted. The notifier is subscribed immediately when the output Observable is subscribed.

This is the event-driven counterpart of `sampleTime`: instead of sampling on a fixed timer, you control when sampling happens via any Observable.

## Signature

```typescript
function sample<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| notifier | `ObservableInput<any>` | An Observable (or Promise, array, etc.) that triggers sampling. Each emission causes the most recent source value to be forwarded (if one exists since the last sample). |

## Return Type

`MonoTypeOperatorFunction<T>` — emits the most recent source value each time the notifier fires.

## Marble Diagram

```
Source:   --a--b--c-----d--e--|
Notifier: -----s-----s--s-----|
          sample(notifier)
Output:   -----b-----c--d-----|
          (most recent source value when each notifier fires)
```

## Examples

### Example 1: Sample a counter when the user clicks

```typescript
import { interval, fromEvent } from 'rxjs';
import { sample } from 'rxjs';

const counter$ = interval(1000);
const click$ = fromEvent(document, 'click');

counter$.pipe(
  sample(click$)
).subscribe(n => console.log('Counter value at click:', n));

// Logs the current counter value each time the user clicks
```

### Example 2: Capture the most recent game state when the player scores

```typescript
import { Subject } from 'rxjs';
import { sample } from 'rxjs';

interface GameState { score: number; lives: number; level: number; }

const gameState$ = new Subject<GameState>();
const playerScored$ = new Subject<void>();

gameState$.pipe(
  sample(playerScored$)
).subscribe(state => {
  console.log('State at scoring event:', state);
});

gameState$.next({ score: 0, lives: 3, level: 1 });
gameState$.next({ score: 10, lives: 3, level: 1 });
playerScored$.next(); // Logs: { score: 10, lives: 3, level: 1 }
gameState$.next({ score: 20, lives: 2, level: 1 });
playerScored$.next(); // Logs: { score: 20, lives: 2, level: 1 }
```

### Example 3: Coordinate two streams — emit one based on the other

```typescript
import { fromEvent, animationFrames } from 'rxjs';
import { sample, map } from 'rxjs';

// Read mouse position, but only render on animation frames
const mouseMove$ = fromEvent<MouseEvent>(document, 'mousemove');
const frame$ = animationFrames();

mouseMove$.pipe(
  sample(frame$),
  map(ev => ({ x: ev.clientX, y: ev.clientY }))
).subscribe(pos => {
  // Guaranteed to run at most once per animation frame
  updateCursor(pos);
});

function updateCursor(pos: { x: number; y: number }) {
  console.log('Cursor at:', pos);
}
```

## Common Pitfalls

- **No emission if source has no new value**: If the notifier fires but the source hasn't emitted since the previous sample, nothing is output. `sample` tracks whether a new value has arrived, not simply what the current value is.
- **Notifier completion is ignored**: When the notifier completes, `sample` does not automatically complete. The source Observable still controls completion.
- **vs `withLatestFrom`**: `withLatestFrom` combines each notifier emission with the latest source value within a `pipe` transformation; `sample` treats the source as the primary stream and the notifier as the trigger.

## Related Operators

- `sampleTime` — like `sample` with a fixed-interval timer as the notifier
- `audit` — emits the most recent value after a duration Observable fires (started by each source value)
- `throttle` — emits the first value in each window
- `debounce` — emits only after a silence period
