# switchScan

**Category**: Transformation  
**Import**: `import { switchScan } from 'rxjs';`

## Description

Applies an accumulator function over the source Observable where the accumulator function returns an Observable, emitting values only from the most recently returned Observable. Like `mergeScan`, but uses `switchMap` semantics — when the source emits a new value, the previously returned inner Observable is cancelled and the new one takes over.

The accumulated state is updated with values emitted by each active inner Observable. When a new source value arrives, the state at that point is passed to the accumulator along with the new source value.

## Signature

```typescript
function switchScan<T, R, O extends ObservableInput<any>>(
  accumulator: (acc: R, value: T, index: number) => O,
  seed: R
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `accumulator` | `(acc: R, value: T, index: number) => ObservableInput<O>` | The accumulator function. Receives the current state, the source value, and the index. Returns an Observable whose emissions update the state. The previous inner Observable is cancelled when a new source value arrives. |
| `seed` | `R` | The initial accumulated state. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — emits values only from the most recently projected inner Observable.

## Marble Diagram

```
Source:  --a---------b---------|
         switchScan((acc, x) => inner, seed)
Inner a: --r1--r2--...
  (cancelled when b arrives, state = r1 or r2)
Inner b:             --r3--r4--|
Output:  --r1--r2----r3--r4--|
```

## Examples

### Example 1: Debounce a search with accumulated context

```typescript
import { fromEvent, map, switchScan, from, debounceTime } from 'rxjs';

interface SearchState {
  previousQuery: string;
  results: string[];
}

const input = document.querySelector<HTMLInputElement>('#search')!;

fromEvent(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),
  switchScan(
    (state, query) =>
      from(
        fetch(`/api/search?q=${query}&context=${state.previousQuery}`)
          .then(r => r.json())
          .then(results => ({ previousQuery: query, results }))
      ),
    { previousQuery: '', results: [] } as SearchState
  )
).subscribe(state => console.log('Results:', state.results));
```

### Example 2: Live calculation that cancels previous async computation

```typescript
import { Subject, switchScan, from, timer, map } from 'rxjs';

const input$ = new Subject<number>();

// Simulate an async calculation that takes 500ms
function calculate(acc: number, value: number) {
  return timer(500).pipe(map(() => acc + value));
}

input$.pipe(
  switchScan((acc, value) => calculate(acc, value), 0)
).subscribe(result => console.log('Result:', result));

input$.next(10); // Starts calculating 0+10
input$.next(5);  // Cancels previous, starts calculating state+5
// Only the last calculation completes
```

### Example 3: Chat room — track message history per room switch

```typescript
import { Subject, switchScan, from } from 'rxjs';

interface ChatState {
  roomId: string;
  messages: string[];
}

const roomSwitch$ = new Subject<string>();

roomSwitch$.pipe(
  switchScan(
    (state, roomId) =>
      from(
        fetch(`/api/rooms/${roomId}/messages`)
          .then(r => r.json())
          .then(messages => ({ roomId, messages }))
      ),
    { roomId: '', messages: [] } as ChatState
  )
).subscribe(state => console.log(`Room ${state.roomId}:`, state.messages));

roomSwitch$.next('general');
roomSwitch$.next('random'); // Cancels general room fetch if still in-flight
```

## Common Pitfalls

- **State at cancellation point**: When a new source value cancels the current inner Observable, the accumulated state passed to the next accumulator call is whatever was last emitted by the cancelled inner Observable (or the seed if it hadn't emitted yet). Plan the state type accordingly.
- **No concurrency control**: Unlike `mergeScan`, `switchScan` always uses switch semantics — there is no `concurrent` parameter.
- **Inner Observable not completing**: If an inner Observable never emits, the state is never updated. Subsequent source values will still be processed with the old state.

## Related Operators

- `mergeScan` — like `switchScan` but does not cancel previous inner Observables
- `scan` — synchronous accumulation; the accumulator returns a plain value
- `switchMap` — like `switchScan` but does not carry accumulated state
