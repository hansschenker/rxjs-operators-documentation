# withLatestFrom

**Category**: Combination  
**Import**: `import { withLatestFrom } from 'rxjs';`

## Description

`withLatestFrom` combines each value emitted by the source Observable with the most recent value from one or more other Observable inputs, but only when the source emits. The other Observables are subscribed to immediately and their values are cached internally — they are never the trigger for an emission. The output emits an array `[sourceValue, latestOtherA, latestOtherB, ...]` each time the source emits, provided all other sources have emitted at least one value.

This is the key distinction from `combineLatestWith`: `combineLatestWith` triggers an emission whenever any source emits. `withLatestFrom` triggers only when the primary (piped) source emits. Use `withLatestFrom` to sample the current state of auxiliary streams without being driven by them.

An optional `project` function can be provided as the last argument to transform the combined values into a custom output.

## Signature

```typescript
function withLatestFrom<T, O extends unknown[]>(
  ...inputs: [...ObservableInputTuple<O>]
): OperatorFunction<T, [T, ...O]>

function withLatestFrom<T, O extends unknown[], R>(
  ...inputs: [...ObservableInputTuple<O>, (...value: [T, ...O]) => R]
): OperatorFunction<T, R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| inputs | `...ObservableInputTuple<O>` | One or more Observable inputs whose latest values will be sampled when the source emits. The last argument may optionally be a projection function. |
| project (optional) | `(...values: [T, ...O]) => R` | If the last argument is a function, it is called with the source value and each latest value, and its return value is emitted. |

## Return Type

`OperatorFunction<T, [T, ...O]>` — without `project`, emits a tuple of `[sourceValue, ...latestValues]`. With `project`, emits the result of calling the projection function.

## Marble Diagram

```
Source:   ------1-----------2---------3--|
Other:    --A-------B---C---------------|
          withLatestFrom(Other)
Output:   ------[1,A]-------[2,C]----[3,C]--|
          ^                 ^-- Other emitted C before source emitted 2
          |-- Other emitted A before source emitted 1

Note: if source emits before Other has emitted anything, that emission is dropped.
```

## Examples

### Example 1: Handling a click event enriched with the current application state

```typescript
import { fromEvent, withLatestFrom, map } from 'rxjs';

const saveBtn = document.getElementById('save-btn')!;
const clicks$ = fromEvent(saveBtn, 'click');

// appState$ is a BehaviorSubject tracking the current editor state
const appState$ = new BehaviorSubject<AppState>(initialState);

clicks$.pipe(
  withLatestFrom(appState$),
  map(([_click, state]) => state)
).subscribe(state => {
  saveToServer(state);
  console.log('Saved state:', state.documentTitle);
});
// Only fires when the button is clicked, using whatever the latest state is at that moment
```

### Example 2: Augmenting keyboard shortcuts with the current route

```typescript
import { fromEvent, withLatestFrom, filter, map } from 'rxjs';
import { Router } from '@angular/router'; // or any router that exposes an Observable

declare const router: { url$: Observable<string> };

const keydown$ = fromEvent<KeyboardEvent>(document, 'keydown');

keydown$.pipe(
  filter(e => e.ctrlKey && e.key === 's'), // Ctrl+S
  withLatestFrom(router.url$),
  map(([_event, url]) => url)
).subscribe(currentUrl => {
  // We know the current URL at the moment of the shortcut press
  if (currentUrl.startsWith('/editor')) {
    triggerEditorSave();
  } else if (currentUrl.startsWith('/settings')) {
    triggerSettingsSave();
  }
});
```

### Example 3: Using a projection function to compute a combined result

```typescript
import { interval, BehaviorSubject, withLatestFrom, map } from 'rxjs';

// A game loop tick combined with the player's current position
const gameTick$ = interval(16); // ~60 fps
const playerPosition$ = new BehaviorSubject<{ x: number; y: number }>({ x: 0, y: 0 });
const enemies$ = new BehaviorSubject<Enemy[]>([]);

gameTick$.pipe(
  withLatestFrom(
    playerPosition$,
    enemies$,
    (_tick, position, enemies) => ({
      position,
      nearbyEnemies: enemies.filter(e => distance(e, position) < 100),
    })
  )
).subscribe(({ position, nearbyEnemies }) => {
  renderFrame(position);
  if (nearbyEnemies.length > 0) {
    triggerCombat(nearbyEnemies);
  }
});
```

## Common Pitfalls

- **Source emissions before other sources have emitted are silently dropped**: If the source emits before one of the other Observable inputs has produced any value, that source emission is ignored — no output is produced. To avoid dropped values, use `startWith` on the other sources to give them an immediate initial value, or use a `BehaviorSubject` which emits on subscribe.
- **Not the same as `combineLatestWith`**: `combineLatestWith` emits when any source emits. `withLatestFrom` emits only when the primary source emits. Choosing the wrong one is a common source of unexpected behaviour: use `withLatestFrom` when you are reacting to the primary source and sampling auxiliary state; use `combineLatestWith` when any change to any source should trigger recalculation.
- **Side effects in other sources still run**: The other Observable inputs are subscribed to immediately, even if the source never emits. Their side effects (subscriptions, HTTP calls, etc.) execute regardless. This is intentional — `withLatestFrom` needs to be "warm" and ready with the latest value before the source emits.
- **Completed other sources still supply their last value**: If one of the auxiliary sources completes, `withLatestFrom` continues to use its last emitted value for all future source emissions. This is usually correct but can lead to stale data if the completed source represented live state.

## Related Operators

- `combineLatestWith` — emits when any combined source emits, not just the primary source
- `combineLatest` — creation operator equivalent to `combineLatestWith`
- `zipWith` — pairs values by index rather than by recency; requires each source to emit in lock-step
- `switchMap` — if you need to react to the source by starting a new inner subscription to the latest value
