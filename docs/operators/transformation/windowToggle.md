# windowToggle

**Category**: Transformation  
**Import**: `import { windowToggle } from 'rxjs';`

## Description

Branches the source Observable values as a nested Observable starting from an emission from `openings` and ending when the Observable returned by `closingSelector` emits. Like `bufferToggle`, but emits nested Observables (windows) instead of arrays.

Multiple windows can be open simultaneously if `openings` emits before a previous window closes. Each open window is an Observable that receives source values until it is closed by its corresponding `closingSelector`.

## Signature

```typescript
function windowToggle<T, O>(
  openings: ObservableInput<O>,
  closingSelector: (openValue: O) => ObservableInput<any>
): OperatorFunction<T, Observable<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `openings` | `ObservableInput<O>` | An Observable of notifications that open a new window. The emitted value is passed to `closingSelector`. |
| `closingSelector` | `(openValue: O) => ObservableInput<any>` | A function called with each value from `openings`. Returns an Observable; when that Observable emits, the corresponding window is completed and emitted. |

## Return Type

`OperatorFunction<T, Observable<T>>` — emits a new window Observable for each value from `openings`; each window completes when the corresponding closing Observable emits.

## Marble Diagram

```
Source:   --1--2--3--4--5--6--7--|
Openings: --o1-----------o2------|
Close o1:       --x
Close o2:                    --x
Output:   --w1-----------w2------|
  w1:     --1--2--3|
  w2:               --4--5--6--7|
```

## Examples

### Example 1: Capture click events in alternating 500ms windows

```typescript
import { fromEvent, interval, windowToggle, EMPTY, mergeAll } from 'rxjs';

const clicks = fromEvent(document, 'click');
const openings = interval(1000);

clicks.pipe(
  windowToggle(openings, i => i % 2 === 0 ? interval(500) : EMPTY),
  mergeAll()
).subscribe(click => console.log('Captured click during window:', click));
```

### Example 2: Track user activity during active sessions

```typescript
import { Subject, windowToggle, mergeMap, toArray } from 'rxjs';

const event$ = new Subject<string>();
const sessionStart$ = new Subject<string>(); // emits sessionId
const sessionEnd$ = new Subject<string>();   // emits sessionId

event$.pipe(
  windowToggle(sessionStart$, () => sessionEnd$),
  mergeMap(win => win.pipe(toArray()))
).subscribe(sessionEvents => {
  console.log('Session events:', sessionEvents);
});

sessionStart$.next('session-1');
event$.next('page-view');
event$.next('button-click');
sessionEnd$.next('session-1');
```

### Example 3: Record audio segments between start/stop commands

```typescript
import { Subject, windowToggle, mergeMap, reduce } from 'rxjs';

interface AudioSample { timestamp: number; amplitude: number }

const sample$ = new Subject<AudioSample>();
const recordStart$ = new Subject<void>();
const recordStop$ = new Subject<void>();

sample$.pipe(
  windowToggle(recordStart$, () => recordStop$),
  mergeMap(win =>
    win.pipe(
      reduce((clip, sample) => [...clip, sample], [] as AudioSample[])
    )
  )
).subscribe(clip => {
  console.log(`Recorded ${clip.length} samples`);
});
```

## Common Pitfalls

- **Higher-order Observable**: The output emits Observable objects; use `mergeAll` or `mergeMap` to access their values.
- **Unsubscribed window Observable**: After a window closes (its `closingSelector` fires), the underlying Subject is unsubscribed. Any subscriber that still holds a reference to that window and tries to receive values will get an `ObjectUnsubscribedError`.
- **Overlapping windows**: If `openings` fires faster than windows close, multiple windows are active simultaneously, each buffering independently. This can increase memory usage.

## Related Operators

- `bufferToggle` — like `windowToggle` but collects into arrays instead of Observables
- `window` — single concurrent window with a single boundary Observable
- `windowCount` — boundary determined by item count
- `windowTime` — boundary determined by time
- `windowWhen` — single buffer with dynamic closing factory
