# expand

## Identity

- **Name**: expand
- **Category**: Transformation Operators
- **Type**: Recursive projection — applies a function to each value and merges the results back into the stream, enabling tree traversal and recursive async patterns
- **Import**:
  ```typescript
  import { expand } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function expand<T, R>(
    project: (value: T | R, index: number) => ObservableInput<R>,
    concurrent?: number,
    scheduler?: SchedulerLike
  ): OperatorFunction<T, T | R>
  ```

## Functional Specification

**Concept**: `expand` is like `mergeMap` but recursive. Each value emitted by the source OR by the projected inner Observables is fed back into `project`, creating a self-sustaining expansion. To terminate, `project` must return `EMPTY` for some values.

**Execution model**:
1. Source emits value `v`
2. `project(v)` is called → returns Observable
3. That Observable's emissions are forwarded to the output stream AND fed back into `project`
4. This recurses until `project` returns `EMPTY`

**Termination**: `project` MUST return `EMPTY` for leaf nodes, otherwise the stream never completes.

**`concurrent`**: Max number of active inner Observables at once (default: `Infinity`). Use to limit parallel recursion depth/width.

**Output type** — union of source and projected:
```typescript
expand<T, R>(project: (v: T | R) => Observable<R>): OperatorFunction<T, T | R>
```

## Marble Diagram

```
Source:   --1|

expand(n => n < 4 ? of(n + 1) : EMPTY):

Execution trace:
  source emits 1 → project(1) = of(2)
  of(2) emits 2 → project(2) = of(3)
  of(3) emits 3 → project(3) = of(4)
  of(4) emits 4 → project(4) = EMPTY  → terminates this branch

Result:   --1--2--3--4--|
          (1 from source, 2/3/4 from recursive projections)

With depth > 1 (tree):
  source emits root → project(root) = of(childA, childB)
  childA → project(childA) = of(leaf1)
  childB → project(childB) = EMPTY  (leaf)
  leaf1  → project(leaf1)  = EMPTY  (leaf)

Result:   root--childA--childB--leaf1--|
```

## Type System Integration

```typescript
import { of, EMPTY } from 'rxjs';
import { expand, map } from 'rxjs/operators';

// Type: Observable<number> — T=number, R=number
of(1).pipe(
  expand(n => n < 5 ? of(n * 2) : EMPTY)
).subscribe((v: number) => console.log(v));
// 1, 2, 4  (1→2→4→8 but 8 > 5 so EMPTY)
// Wait: 1*2=2, 2*2=4, 4*2=8 > 5 → EMPTY
// Output: 1, 2, 4

// When T ≠ R — union type
interface TreeNode { id: number; children?: number[] }

of<TreeNode>({ id: 1, children: [2, 3] }).pipe(
  expand((node: TreeNode | number) => {
    if (typeof node === 'number') {
      return fetchNode(node); // Observable<TreeNode>
    }
    return node.children
      ? of(...node.children)   // Observable<number>
      : EMPTY;
  })
)
// Observable<TreeNode | number>
```

## Examples

### Basic Usage — Numeric Sequence
```typescript
import { of, EMPTY } from 'rxjs';
import { expand, take } from 'rxjs/operators';

// Generate 1, 2, 4, 8, 16 (powers of 2, stop at 16)
of(1).pipe(
  expand(n => n < 16 ? of(n * 2) : EMPTY)
).subscribe(console.log);
// 1, 2, 4, 8, 16

// Fibonacci sequence (first 8 values)
of([0, 1]).pipe(
  expand(([a, b]) => of([b, a + b])),
  take(8),
  map(([a]) => a)
).subscribe(console.log);
// 0, 1, 1, 2, 3, 5, 8, 13
```

### Common Pattern — Paginated API Traversal
```typescript
import { of, EMPTY } from 'rxjs';
import { expand, mergeMap, map, toArray } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface PageResult<T> {
  items: T[];
  nextPageToken?: string;
}

// Fetch all pages of a paginated API
function fetchAllPages<T>(url: string): Observable<T> {
  return ajax.getJSON<PageResult<T>>(url).pipe(
    expand(page =>
      page.nextPageToken
        ? ajax.getJSON<PageResult<T>>(`${url}?pageToken=${page.nextPageToken}`)
        : EMPTY
    ),
    mergeMap(page => page.items) // flatten pages into individual items
  );
}

fetchAllPages<User>('/api/users').pipe(
  toArray()
).subscribe(allUsers => console.log(`Total: ${allUsers.length}`));
```

### Common Pattern — Recursive Tree Traversal
```typescript
import { of, EMPTY, from } from 'rxjs';
import { expand, mergeMap, filter, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface TreeNode {
  id: number;
  name: string;
  childIds: number[];
}

// Traverse a tree structure from an API, breadth-first
function traverseTree(rootId: number): Observable<TreeNode> {
  return ajax.getJSON<TreeNode>(`/api/nodes/${rootId}`).pipe(
    expand(node =>
      node.childIds.length > 0
        ? from(node.childIds).pipe(
            mergeMap(id => ajax.getJSON<TreeNode>(`/api/nodes/${id}`))
          )
        : EMPTY  // leaf node — stop recursion
    )
  );
}

traverseTree(1).subscribe(node => console.log(`Visited: ${node.name}`));
```

### Common Pattern — Polling Until Condition
```typescript
import { timer, EMPTY } from 'rxjs';
import { expand, switchMap, filter, take } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface JobStatus { id: string; status: 'pending' | 'running' | 'done' | 'failed' }

// Poll a job status endpoint until done/failed
function pollUntilComplete(jobId: string): Observable<JobStatus> {
  return ajax.getJSON<JobStatus>(`/api/jobs/${jobId}`).pipe(
    expand(status =>
      status.status === 'done' || status.status === 'failed'
        ? EMPTY  // terminal state — stop polling
        : timer(2000).pipe(
            switchMap(() => ajax.getJSON<JobStatus>(`/api/jobs/${jobId}`))
          )
    )
  );
}

pollUntilComplete('job-123').pipe(
  filter(s => s.status === 'done' || s.status === 'failed')
).subscribe(finalStatus => console.log('Job finished:', finalStatus));
```

## Common Pitfalls

### Anti-pattern: Forgetting `EMPTY` Termination Condition
```typescript
import { of } from 'rxjs';
import { expand } from 'rxjs/operators';

// ❌ INFINITE STREAM — project never returns EMPTY
of(1).pipe(
  expand(n => of(n + 1)) // 1, 2, 3, 4, 5, ... forever
).subscribe(console.log); // never stops, potential stack overflow

// ✅ CORRECT — always have a terminal condition
import { EMPTY } from 'rxjs';
of(1).pipe(
  expand(n => n < 10 ? of(n + 1) : EMPTY)
).subscribe(console.log); // 1, 2, ..., 10 then completes

// For genuinely infinite streams, use take() to limit:
of(1).pipe(
  expand(n => of(n + 1)),
  take(10)
).subscribe(console.log); // 1–10

// WHY: expand feeds each output value back into project. Without a termination
// condition (returning EMPTY for leaf values), the stream grows indefinitely.
// Always define when recursion stops, or bound with take().
```

### Anti-pattern: Using `expand` When `mergeMap` Is Sufficient
```typescript
import { from, of, EMPTY } from 'rxjs';
import { expand, mergeMap } from 'rxjs/operators';

const ids = [1, 2, 3];

// ❌ OVERENGINEERED — expand for a flat mapping (no recursion needed)
from(ids).pipe(
  expand(id => typeof id === 'number'
    ? fetchUser(id).pipe(mergeMap(user => [user]))
    : EMPTY
  )
).subscribe(console.log);

// ✅ CORRECT — mergeMap for flat async transformation
from(ids).pipe(
  mergeMap(id => fetchUser(id))
).subscribe(console.log);

// WHY: expand is for RECURSIVE patterns where output feeds back as input.
// For flat async transformation (each input → one or more outputs, no
// feedback loop), mergeMap/concatMap/switchMap are simpler and clearer.
// Only reach for expand when you need output values to drive further fetches.
```

## Related Operators

- **`mergeMap`**: Flat async projection — use when outputs don't feed back as inputs
- **`concatMap`**: Sequential flat projection — like expand but ordered, no recursion
- **`BFS/DFS traversal`**: expand is the natural RxJS primitive for tree/graph traversal
- **`repeat`**: Resubscribes on completion — simpler for fixed-count loops without feedback
- **`retryWhen / retry`**: Error-driven resubscription — not for value-driven recursion

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/expand](https://rxjs.dev/api/operators/expand)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching points**:
1. `project` MUST return `EMPTY` for leaf nodes — no termination = infinite stream
2. `expand` = `mergeMap` + feedback loop; use only when outputs drive further emissions
3. Best use cases: paginated APIs, tree traversal, polling until done, Fibonacci/sequences
