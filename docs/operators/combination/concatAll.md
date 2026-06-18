# concatAll

**Category**: Combination  
**Import**: `import { concatAll } from 'rxjs';`

## Description

`concatAll` converts a higher-order Observable (an Observable that emits other Observables) into a first-order Observable by subscribing to each inner Observable only after the previous one has completed. Inner Observables are processed strictly in the order they are received, with no concurrency — the next inner Observable waits in a queue until the current one finishes.

This serialised behaviour makes `concatAll` the right choice when ordering matters and inner Observables must not overlap. It is equivalent to `mergeAll(1)`. Be aware that if the source emits inner Observables faster than they complete, the internal queue will grow without bound and can cause memory pressure.

## Signature

```typescript
function concatAll<O extends ObservableInput<any>>(): OperatorFunction<O, ObservedValueOf<O>>
```

## Parameters

`concatAll` takes no parameters.

## Return Type

`OperatorFunction<O, ObservedValueOf<O>>` — An Observable that emits all values from each inner Observable in sequence, subscribing to the next only after the current one completes.

## Marble Diagram

```
Source:   --A--B--C----|
A:          --1--2--|
B:                   --3--4--|
C:                            --5--|
          concatAll()
Output:   ----1--2----3--4----5--|
```

## Examples

### Example 1: Sequencing HTTP requests (load details after list)

```typescript
import { of, concatAll, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// First fetch the list of order IDs, then fetch each order's details in order
const orderIds = [101, 102, 103];

of(...orderIds).pipe(
  map(id => ajax.getJSON<Order>(`https://api.example.com/orders/${id}`)),
  concatAll() // Fetch order 101, wait for it, then 102, then 103
).subscribe({
  next: order => console.log('Order loaded:', order),
  complete: () => console.log('All orders loaded in sequence'),
});
```

### Example 2: Playing a queue of audio clips in order

```typescript
import { Subject, concatAll } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

const audioQueue$ = new Subject<Observable<AudioBuffer>>();

// Each item added to the queue plays only after the previous clip finishes
audioQueue$.pipe(
  concatAll()
).subscribe(audioBuffer => {
  const source = audioCtx.createBufferSource();
  source.buffer = audioBuffer;
  source.connect(audioCtx.destination);
  source.start();
});

// Add clips to the queue
audioQueue$.next(loadClip('intro.mp3'));
audioQueue$.next(loadClip('chapter1.mp3'));
audioQueue$.next(loadClip('outro.mp3'));
```

### Example 3: Running migration steps one at a time

```typescript
import { from, concatAll, map } from 'rxjs';

const migrationSteps = [
  () => runMigration('001_create_users_table'),
  () => runMigration('002_add_email_index'),
  () => runMigration('003_create_posts_table'),
];

from(migrationSteps).pipe(
  map(step => step()), // Each step() returns an Observable
  concatAll()          // Run steps strictly in order
).subscribe({
  next: result => console.log('Step complete:', result),
  error: err => console.error('Migration failed:', err),
  complete: () => console.log('All migrations complete'),
});
```

## Common Pitfalls

- **Memory growth with fast sources**: If the source emits inner Observables quicker than they complete, they accumulate in an unbounded buffer. For long-running or never-ending inner Observables, prefer `mergeAll` with a `concurrent` limit, `switchAll`, or `exhaustAll` depending on your desired strategy.
- **Source must complete for all items to be processed**: `concatAll` queues inner Observables emitted by the source. If the source never completes and each inner Observable runs for a long time, later items are simply waiting — they are not dropped, but they may wait a very long time.
- **Not suitable for parallel work**: Because only one inner Observable runs at a time, `concatAll` is slower than `mergeAll` for independent operations. Use `mergeAll` when ordering does not matter.

## Related Operators

- `mergeAll` — processes inner Observables concurrently; use when order does not matter
- `switchAll` — cancels the current inner Observable when a new one arrives
- `exhaustAll` — ignores new inner Observables while the current one is active
- `concatMap` — combines mapping and concatenation in one step
- `concat` — creation operator equivalent; concatenates static Observable inputs
