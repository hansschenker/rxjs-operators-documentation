# bufferTime

**Category**: Transformation  
**Import**: `import { bufferTime } from 'rxjs';`

## Description

Buffers the source Observable values for a specific time period, then emits the collected values as an array. By default, it emits and resets the buffer every `bufferTimeSpan` milliseconds. Two optional parameters add more control: `bufferCreationInterval` sets how often a new buffer opens, and `maxBufferSize` caps the number of items per buffer.

## Signature

```typescript
function bufferTime<T>(bufferTimeSpan: number, scheduler?: SchedulerLike): OperatorFunction<T, T[]>
function bufferTime<T>(bufferTimeSpan: number, bufferCreationInterval: number | null | undefined, scheduler?: SchedulerLike): OperatorFunction<T, T[]>
function bufferTime<T>(bufferTimeSpan: number, bufferCreationInterval: number | null | undefined, maxBufferSize: number, scheduler?: SchedulerLike): OperatorFunction<T, T[]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `bufferTimeSpan` | `number` | The amount of time (ms) to fill each buffer. |
| `bufferCreationInterval` | `number \| null` | Optional. The interval (ms) at which to open new buffers. If omitted or `null`, a new buffer opens immediately when the previous one closes. |
| `maxBufferSize` | `number` | Optional. The maximum number of items in a buffer. A buffer closes early if this limit is reached before `bufferTimeSpan`. |
| `scheduler` | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. The scheduler used for timing. |

## Return Type

`OperatorFunction<T, T[]>` — emits arrays of buffered values on a time-based schedule.

## Marble Diagram

```
Source (bufferTime(1000)):
  --a-b----c-d-e--|
  -----[a,b]------[c,d,e]--|
  (emits every 1 second)

Source (bufferTime(2000, 5000)):
  --a-b-c---d-e-f--g-h-|
  (new buffer every 5s, closes after 2s)
```

## Examples

### Example 1: Batch user clicks every second

```typescript
import { fromEvent, bufferTime, filter } from 'rxjs';

fromEvent(document, 'click').pipe(
  bufferTime(1000),
  filter(clicks => clicks.length > 0)
).subscribe(clicks => {
  console.log(`${clicks.length} clicks in the last second`);
});
```

### Example 2: Rate-limit writes to a database

```typescript
import { Subject, bufferTime, filter, mergeMap, from } from 'rxjs';

interface Event { type: string; data: unknown }

const event$ = new Subject<Event>();

event$.pipe(
  bufferTime(2000),              // collect for 2 seconds
  filter(batch => batch.length > 0),
  mergeMap(batch =>
    from(fetch('/api/events/bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(batch),
    }))
  )
).subscribe(() => console.log('Batch saved'));
```

### Example 3: Overlapping time windows (every 5s, capture last 2s)

```typescript
import { fromEvent, bufferTime } from 'rxjs';

// Opens a 2-second capture window every 5 seconds
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  bufferTime(2000, 5000)
).subscribe(moves => {
  console.log(`Captured ${moves.length} mouse positions in 2s window`);
});
```

## Common Pitfalls

- **Empty arrays emitted**: When no source values arrive during a buffer period, `bufferTime` still emits an empty array. Use `filter(arr => arr.length > 0)` to suppress empty emissions.
- **Scheduler dependency**: The default `asyncScheduler` is macrotask-based and may not be precise for very short durations. For testing, inject a `TestScheduler` via the optional `scheduler` parameter.
- **`bufferCreationInterval` vs `bufferTimeSpan`**: When both are set, `bufferCreationInterval` determines how often a new buffer opens, and `bufferTimeSpan` determines when each buffer closes. These are independent and can overlap.

## Related Operators

- `buffer` — triggers buffer flush via an Observable, not a time span
- `bufferCount` — buffers a fixed number of values
- `bufferToggle` — opens and closes buffers with separate Observables
- `bufferWhen` — uses a factory function for dynamic buffer boundaries
- `windowTime` — like `bufferTime` but emits nested Observables instead of arrays
