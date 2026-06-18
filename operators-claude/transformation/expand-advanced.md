# expand — Advanced Patterns

For `expand` fundamentals, see the core [expand](./expand) doc. This page covers recursive tree traversal, BFS/DFS, cursor-based pagination, and graph walking.

---

## Quick Recap

`expand(project)` recursively applies `project` to each emission — including the results of the project itself. It's the RxJS equivalent of an unfold/anamorphism.

```
expand(x => next(x)):
  seed → next(seed) → next(next(seed)) → next(next(next(seed))) → ...
```

**Termination**: return `EMPTY` from the project to stop recursion for that branch.

---

## Pattern 1: Cursor-Based Pagination (REST)

```typescript
import { expand, mergeMap, takeWhile, scan, map, EMPTY } from 'rxjs';

interface Page<T> { items: T[]; nextCursor: string | null; }

function fetchAllPages<T>(url: string): Observable<T[]> {
  return this.http.get<Page<T>>(url).pipe(
    expand(page =>
      page.nextCursor
        ? this.http.get<Page<T>>(`${url}?cursor=${page.nextCursor}`)
        : EMPTY                          // no more pages — stop
    ),
    map(page => page.items),
    scan((all, items) => [...all, ...items], [] as T[])
  );
}

// Usage:
fetchAllPages<User>('/api/users').pipe(last()).subscribe(allUsers => render(allUsers));
```

---

## Pattern 2: Link-Based Pagination (HATEOAS)

```typescript
interface HateoasResponse<T> {
  data:  T[];
  links: { next?: string };
}

function fetchAll<T>(startUrl: string): Observable<T> {
  return this.http.get<HateoasResponse<T>>(startUrl).pipe(
    expand(res =>
      res.links.next
        ? this.http.get<HateoasResponse<T>>(res.links.next)
        : EMPTY
    ),
    mergeMap(res => from(res.data))   // flatten items from each page
  );
}

// Emits each item individually as pages load:
fetchAll<Product>('/api/products').pipe(
  filter(p => p.inStock),
  take(50)
).subscribe(renderProduct);
```

---

## Pattern 3: BFS Tree Traversal

Breadth-first search — visit all nodes level by level.

```typescript
import { expand, mergeMap, from, EMPTY } from 'rxjs';

interface TreeNode {
  id:       string;
  children: string[]; // child IDs
}

function bfsTraversal(rootId: string): Observable<TreeNode> {
  return this.api.getNode(rootId).pipe(
    expand(node =>
      node.children.length > 0
        ? from(node.children).pipe(
            mergeMap(childId => this.api.getNode(childId))
          )
        : EMPTY
    )
  );
}

bfsTraversal('root').pipe(
  toArray()
).subscribe(allNodes => buildTreeIndex(allNodes));
```

---

## Pattern 4: DFS Tree Traversal

Depth-first search — follow each branch to its leaf before backtracking.

```typescript
import { expand, concatMap, from, EMPTY } from 'rxjs';

function dfsTraversal(rootId: string): Observable<TreeNode> {
  return this.api.getNode(rootId).pipe(
    expand(node =>
      node.children.length > 0
        ? from(node.children).pipe(
            concatMap(childId => this.api.getNode(childId)) // concatMap = depth-first order
          )
        : EMPTY
    )
  );
}

// Key: mergeMap gives BFS-like order, concatMap gives DFS-like order
```

---

## Pattern 5: File System Directory Walk

```typescript
import * as fs from 'fs/promises';
import * as path from 'path';

interface DirEntry { path: string; type: 'file' | 'dir'; }

function walkDirectory(rootPath: string): Observable<DirEntry> {
  return from(fs.readdir(rootPath, { withFileTypes: true })).pipe(
    mergeMap(entries => from(entries)),
    mergeMap(entry => {
      const fullPath = path.join(rootPath, entry.name);
      return of({ path: fullPath, type: entry.isDirectory() ? 'dir' as const : 'file' as const });
    }),
    expand(entry =>
      entry.type === 'dir'
        ? from(fs.readdir(entry.path, { withFileTypes: true })).pipe(
            mergeMap(entries => from(entries)),
            mergeMap(e => {
              const p = path.join(entry.path, e.name);
              return of({ path: p, type: e.isDirectory() ? 'dir' as const : 'file' as const });
            })
          )
        : EMPTY
    )
  );
}

walkDirectory('./src').pipe(
  filter(e => e.type === 'file' && e.path.endsWith('.ts')),
  map(e => e.path)
).subscribe(tsFile => console.log(tsFile));
```

---

## Pattern 6: Graph Walking (Cycle Detection)

```typescript
import { expand, filter, EMPTY } from 'rxjs';

function walkGraph(startId: string): Observable<GraphNode> {
  const visited = new Set<string>();

  return this.api.getNode(startId).pipe(
    expand(node => {
      if (visited.has(node.id)) return EMPTY; // cycle detected — stop
      visited.add(node.id);

      return node.edges.length > 0
        ? from(node.edges).pipe(
            mergeMap(edgeId => this.api.getNode(edgeId))
          )
        : EMPTY;
    })
  );
}
```

---

## Pattern 7: Retry with Exponential State

```typescript
import { expand, map, take, delay } from 'rxjs';

// Generate exponential delay sequence: 100, 200, 400, 800, 1600
const backoff$ = of(100).pipe(
  expand(delay => of(delay * 2)),
  take(5)
);

backoff$.subscribe(console.log); // 100, 200, 400, 800, 1600

// Use for retry delays:
apiCall$.pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      mergeMap((err, i) =>
        of(err).pipe(delay(100 * 2 ** i))
      ),
      take(5)
    )
  )
).subscribe();
```

---

## Pattern 8: Recursive Data Enrichment

```typescript
// Enrich a comment tree — load replies for each comment
function enrichComments(comments: Comment[]): Observable<EnrichedComment[]> {
  return from(comments).pipe(
    mergeMap(comment =>
      comment.hasReplies
        ? this.api.getReplies(comment.id).pipe(
            expand(replies =>
              replies.some(r => r.hasReplies)
                ? from(replies).pipe(
                    mergeMap(r => r.hasReplies
                      ? this.api.getReplies(r.id)
                      : EMPTY
                    )
                  )
                : EMPTY
            ),
            toArray(),
            map(allReplies => ({ ...comment, replies: allReplies.flat() }))
          )
        : of({ ...comment, replies: [] })
    ),
    toArray()
  );
}
```

---

## Common Pitfalls

### Infinite Recursion — Missing `EMPTY` Terminator

```typescript
// ❌ INFINITE — project never returns EMPTY
of(1).pipe(
  expand(n => of(n + 1)) // always returns a value
).subscribe(console.log); // 1, 2, 3, 4... forever

// ✅ Always return EMPTY to terminate:
of(1).pipe(
  expand(n => n < 10 ? of(n + 1) : EMPTY)
).subscribe(console.log); // 1, 2, 3, ..., 10
// WHY: expand keeps recursing as long as the project emits values.
// Without EMPTY as a base case, it's an infinite Observable.
```

### Stack Overflow on Deep Recursion (without `queueScheduler`)

```typescript
// ❌ Deep synchronous recursion can overflow the call stack
of(root).pipe(
  expand(node => node.children.length ? from(node.children) : EMPTY)
).subscribe(); // may crash on very deep trees

// ✅ Use queueScheduler to process recursively without stack growth
import { queueScheduler, scheduled } from 'rxjs';

of(root).pipe(
  expand(node =>
    node.children.length
      ? scheduled(node.children, queueScheduler)
      : EMPTY
  )
).subscribe(); // safe for arbitrarily deep trees
```

## Related Operators

- **`expand`** (core): Signature, marble diagram, basic fibonacci example
- **`mergeMap`**: Non-recursive fan-out (no feedback)
- **`scan`**: Accumulate state without recursion
- **`repeat`**: Re-subscribe rather than recursively expand
- **`generate`**: Loop-based sequence without recursion

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `expand` is the right tool whenever you need to follow links — paginated APIs, tree nodes, graph edges. The pattern is always: emit the seed, return the "next step" Observable, return `EMPTY` at the leaf.
