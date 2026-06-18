# expand

**Category**: Transformation  
**Import**: `import { expand } from 'rxjs';`

## Description

Recursively projects each source value to an Observable which is merged in the output Observable. Unlike `mergeMap`, which only applies the projection to source values, `expand` also feeds every *output* value back through the `project` function. This creates a recursive expansion that continues until the projected Observables complete without emitting.

`expand` is useful for tree traversal, paginated API consumption, and any algorithm that requires recursive fan-out.

## Signature

```typescript
function expand<T, O extends ObservableInput<unknown>>(
  project: (value: T, index: number) => O,
  concurrent?: number
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `project` | `(value: T, index: number) => ObservableInput<O>` | A function applied to each source value *and* to each output value. Return `EMPTY` to stop the recursive branch. |
| `concurrent` | `number` | Optional. Defaults to `Infinity`. Maximum number of concurrent inner subscriptions. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — emits the original source values followed by all recursively projected values.

## Marble Diagram

```
Source:  --1--|
           expand(x => x < 8 ? of(x * 2) : EMPTY)
Output:  --1--2--4--8--|
         (each output fed back through project)
```

## Examples

### Example 1: Consume all pages of a paginated API

```typescript
import { of, expand, concatMap, EMPTY, from } from 'rxjs';

interface Page<T> {
  data: T[];
  nextCursor: string | null;
}

function fetchPage(cursor: string | null): Promise<Page<string>> {
  const url = cursor ? `/api/items?cursor=${cursor}` : '/api/items';
  return fetch(url).then(r => r.json());
}

from(fetchPage(null)).pipe(
  expand(page => page.nextCursor ? from(fetchPage(page.nextCursor)) : EMPTY),
  concatMap(page => page.data)
).subscribe({
  next: item => console.log('Item:', item),
  complete: () => console.log('All pages loaded'),
});
```

### Example 2: Traverse a tree structure depth-first

```typescript
import { of, expand, EMPTY, concatMap, from } from 'rxjs';

interface TreeNode {
  id: number;
  children?: TreeNode[];
}

const root: TreeNode = {
  id: 1,
  children: [
    { id: 2, children: [{ id: 4 }, { id: 5 }] },
    { id: 3 },
  ],
};

of(root).pipe(
  expand(node => node.children ? from(node.children) : EMPTY)
).subscribe(node => console.log('Visiting node:', node.id));
// 1, 2, 4, 5, 3
```

### Example 3: Generate powers of two on click

```typescript
import { fromEvent, map, expand, of, delay, take } from 'rxjs';

const clicks = fromEvent(document, 'click');

clicks.pipe(
  map(() => 1),
  expand(x => of(x * 2).pipe(delay(1000))),
  take(10)
).subscribe(x => console.log(x));
// 1, 2, 4, 8, 16, 32, 64, 128, 256, 512
```

## Common Pitfalls

- **Infinite recursion**: If the `project` function never returns `EMPTY`, the expansion never terminates. Always include a stopping condition (e.g., return `EMPTY` when a sentinel value is reached), and/or pair with `take` or `takeWhile`.
- **Unbounded concurrency**: The default `concurrent = Infinity` means all branches expand simultaneously. For deep or wide trees, pass a lower `concurrent` value to control resource usage.
- **Memory from overlapping subscriptions**: Unlike `scan` or `reduce`, `expand` holds all active inner subscriptions open. Monitor memory when processing large recursive structures.

## Related Operators

- `mergeMap` — like `expand` but does not feed output values back through the project function
- `mergeScan` — accumulates state across emissions using an Observable-returning accumulator
- `scan` — synchronous recursive accumulation without inner Observables
