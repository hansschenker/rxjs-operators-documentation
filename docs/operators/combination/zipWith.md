# zipWith

**Category**: Combination  
**Import**: `import { zipWith } from 'rxjs';`

## Description

`zipWith` is the pipeable equivalent of the `zip` creation operator. It subscribes to the source Observable and all provided Observable inputs simultaneously, then combines their values by index: the first value emitted by every source is grouped into an array and emitted, then the second value from each, and so on. The output completes as soon as the shortest source has emitted its last value.

Combine by index means each group requires one value from every participating source. Faster sources buffer their values while waiting for the slower ones to catch up, which can cause memory growth if the rates differ significantly. `zipWith` is best suited to finite streams of known or similar lengths, or to streams that naturally emit in lock-step.

## Signature

```typescript
function zipWith<T, A extends readonly unknown[]>(
  ...otherInputs: [...ObservableInputTuple<A>]
): OperatorFunction<T, Cons<T, A>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| otherInputs | `...ObservableInputTuple<A>` | One or more Observable inputs whose values will be combined with the source by index. |

## Return Type

`OperatorFunction<T, Cons<T, A>>` — Emits a tuple `[T, ...A]` where each element is the nth value from the corresponding source, in the order the sources were provided.

## Marble Diagram

```
Source A: --1-----2-----3--|
Source B: -----a-----b--c--|
          zipWith(B)
Output:   -----[1,a]--[2,b]--[3,c]--|
          (each pair waits for one value from every source)
```

## Examples

### Example 1: Pairing user actions with server-side acknowledgements

```typescript
import { Subject, zipWith, map } from 'rxjs';

const userActions$ = new Subject<UserAction>();
const serverAcks$ = new Subject<AckMessage>();

// Each action is paired with the acknowledgement that arrives in the same position
userActions$.pipe(
  zipWith(serverAcks$),
  map(([action, ack]) => ({
    action,
    acknowledged: ack.success,
    latencyMs: ack.timestamp - action.sentAt,
  }))
).subscribe(record => {
  console.log(`Action "${record.action.type}" acknowledged in ${record.latencyMs}ms`);
  auditLog.push(record);
});
```

### Example 2: Matching uploaded files with their upload progress events

```typescript
import { from, Subject, zipWith } from 'rxjs';

const files: File[] = Array.from(fileInput.files ?? []);
const fileStreams$ = from(files); // Each file as a stream value

const progressStreams$ = new Subject<ProgressEvent>();

fileStreams$.pipe(
  zipWith(progressStreams$)
).subscribe(([file, progress]) => {
  const percent = Math.round((progress.loaded / progress.total) * 100);
  console.log(`${file.name}: ${percent}%`);
});
```

### Example 3: Combining two paginated API streams page-by-page

```typescript
import { interval, map, take, zipWith } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Simulate two paginated sources that emit one page at a time
const usersPage$ = interval(1000).pipe(
  take(5),
  map(page => ajax.getJSON<User[]>(`/api/users?page=${page}`))
);
const postsPage$ = interval(1200).pipe(
  take(5),
  map(page => ajax.getJSON<Post[]>(`/api/posts?page=${page}`))
);

usersPage$.pipe(
  zipWith(postsPage$),
  map(([users$, posts$]) =>
    // Combine matching page observables — here we'd mergeMap to resolve them
    ({ users$, posts$ })
  )
).subscribe(({ users$, posts$ }) => {
  console.log('Processing matched page pair');
});
```

## Common Pitfalls

- **Memory pressure from mismatched rates**: If one source emits much faster than the others, its values accumulate in an internal buffer waiting for matching values from slower sources. For streams with very different rates, prefer `combineLatestWith` which does not buffer — it simply uses the most recent value.
- **Completion of the shortest source ends everything**: Once any source completes, the output completes too, discarding all buffered values from longer sources. Pad shorter streams with a default value if you need them to match the length of longer ones.
- **Often confused with `combineLatestWith`**: `zipWith` pairs by index (nth value with nth value). `combineLatestWith` pairs by recency (latest value with latest value). Choose based on whether positional matching or real-time responsiveness is needed.
- **Not suitable for event streams**: `zipWith` waits for all sources to have a value at the same index, which means fast event streams (like mouse moves) will cause the other streams to buffer heavily. Use `withLatestFrom` or `combineLatestWith` for event-driven scenarios.

## Related Operators

- `zip` — creation operator equivalent; takes a static array of Observable inputs without a pipe source
- `zipAll` — zips a dynamically produced set of inner Observables from a higher-order source
- `combineLatestWith` — combines by latest value instead of by index; no buffering
- `withLatestFrom` — combines with the latest value from other sources only when the primary source emits
