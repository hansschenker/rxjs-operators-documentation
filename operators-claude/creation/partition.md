# partition

## Identity

- **Name**: partition
- **Category**: Creation Operators (Join Creation)
- **Type**: Binary splitter — splits one Observable into two based on a predicate, returning a tuple `[matching$, nonMatching$]`
- **Import**:
  ```typescript
  import { partition } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function partition<T>(
    source: ObservableInput<T>,
    predicate: (value: T, index: number) => boolean
  ): [Observable<T>, Observable<T>]
  ```

## Functional Specification

**Concept**: `partition` takes a source and a predicate, returning a two-element tuple:
- Index 0: Observable of values where `predicate` returns `true`
- Index 1: Observable of values where `predicate` returns `false`

**Key properties**:
- Both Observables share the same underlying source subscription (subscribing to either or both subscribes to the source once via multicasting)
- Both Observables complete and error together — they are backed by the same Subject
- Lazy: the source is not subscribed until one of the returned Observables is subscribed to
- Equivalent to `[source.pipe(filter(p)), source.pipe(filter(v => !p(v)))]` but with shared subscription

**`partition` vs `filter` + `filter`**:

| | `partition` | Two `filter` calls |
|---|---|---|
| Source subscriptions | 1 shared | 2 separate |
| Use case | When you need both branches | When you only need one branch |
| Syntax | Destructuring tuple | Separate pipe chains |

## Marble Diagram

```
Source:  --1--2--3--4--5--6--|

partition(n => n % 2 === 0):

even$:   -----2-----4-----6--|
odd$:    --1-----3-----5-----|

Both observables share one source subscription.
Values are routed to even$ or odd$ depending on the predicate.

partition(s => s.startsWith('a')):
Source:  --apple--banana--avocado--cherry--|

aWords$: --apple----------avocado---------|
others$: ---------banana----------cherry--|
```

## Type System Integration

```typescript
import { partition, of } from 'rxjs';

// Destructure the tuple — both are Observable<T>
const [evens$, odds$] = partition(
  of(1, 2, 3, 4, 5),
  n => n % 2 === 0
);
// evens$: Observable<number>
// odds$:  Observable<number>

evens$.subscribe(console.log); // 2, 4
odds$.subscribe(console.log);  // 1, 3, 5

// With union types — both branches retain T
interface Event { type: 'user' | 'system'; payload: unknown }

const [userEvents$, systemEvents$] = partition(
  events$,
  (e): e is Event & { type: 'user' } => e.type === 'user'
);
// Type narrowing works with type predicates
```

## Examples

### Basic Usage
```typescript
import { partition, from } from 'rxjs';
import { map } from 'rxjs/operators';

const numbers = from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

const [evens$, odds$] = partition(numbers, n => n % 2 === 0);

evens$.pipe(map(n => `even: ${n}`)).subscribe(console.log);
odds$.pipe(map(n => `odd: ${n}`)).subscribe(console.log);
// even: 2, even: 4, ..., odd: 1, odd: 3, ...
```

### Common Pattern — Route Events to Different Handlers
```typescript
import { partition, fromEvent } from 'rxjs';
import { map, mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface ApiResponse { success: boolean; data?: unknown; error?: string }

const response$ = apiCalls$.pipe(
  mergeMap(req => ajax.getJSON<ApiResponse>(req.url))
);

const [success$, failure$] = partition(
  response$,
  res => res.success === true
);

success$.pipe(map(res => res.data)).subscribe(renderData);
failure$.pipe(map(res => res.error)).subscribe(showErrorToast);
```

### Common Pattern — Priority Queue Split
```typescript
import { partition, Subject } from 'rxjs';
import { mergeMap, concatMap } from 'rxjs/operators';

interface Task { priority: 'high' | 'normal'; work: () => Promise<void> }

const tasks$ = new Subject<Task>();

const [highPriority$, normalPriority$] = partition(
  tasks$,
  task => task.priority === 'high'
);

// High-priority tasks run concurrently
highPriority$.pipe(mergeMap(t => t.work())).subscribe();

// Normal tasks run sequentially
normalPriority$.pipe(concatMap(t => t.work())).subscribe();

tasks$.next({ priority: 'high',   work: urgentTask });
tasks$.next({ priority: 'normal', work: backgroundTask });
```

### Common Pattern — Error/Success Stream Split
```typescript
import { partition, Subject } from 'rxjs';
import { map } from 'rxjs/operators';

type Result<T> = { ok: true; value: T } | { ok: false; error: Error };

const results$ = new Subject<Result<User>>();

const [users$, errors$] = partition(
  results$,
  (r): r is { ok: true; value: User } => r.ok
);

users$.pipe(map(r => r.value)).subscribe(updateUserList);
errors$.pipe(map(r => r.error)).subscribe(logError);
```

## Common Pitfalls

### Anti-pattern: Subscribing to Only One Branch Without the Other
```typescript
import { partition, interval } from 'rxjs';
import { take } from 'rxjs/operators';

// ❌ POTENTIAL ISSUE — partition uses a Subject internally;
// if you only subscribe to one branch, the other branch's Subject buffers
// values (Subject doesn't buffer, so values are lost to the unsubscribed branch)
const [evens$, odds$] = partition(
  interval(100).pipe(take(10)),
  n => n % 2 === 0
);

evens$.subscribe(console.log); // subscribes
// odds$ is never subscribed — its values are simply not delivered anywhere
// (not a memory leak, but values are discarded, which may be intentional)

// ✅ IF YOU ONLY NEED ONE BRANCH — use filter instead
import { filter } from 'rxjs/operators';
interval(100).pipe(
  take(10),
  filter(n => n % 2 === 0)
).subscribe(console.log);

// WHY: partition is most useful when you genuinely need BOTH branches and
// want to avoid subscribing to the source twice. If you only need one branch,
// filter is simpler and doesn't create an unused Observable.
```

### Anti-pattern: `partition` for N-Way Split (Use `groupBy`)
```typescript
import { partition } from 'rxjs';

// ❌ NESTED PARTITIONS — clunky for N-way splits
const [typeA$, notA$] = partition(source$, e => e.type === 'A');
const [typeB$, typeC$] = partition(notA$, e => e.type === 'B');
// typeC$ is actually "not A and not B" — confusing

// ✅ CORRECT — groupBy for N-way dynamic splits
import { groupBy, mergeMap } from 'rxjs/operators';
source$.pipe(
  groupBy(e => e.type),
  mergeMap(group$ => processGroup(group$.key, group$))
).subscribe();

// WHY: partition is binary only — two outputs. For splitting into 3+ categories,
// groupBy handles dynamic N-way splits more cleanly. partition shines for
// clean true/false decisions; groupBy handles enum/string keys.
```

## Related Operators

- **`filter`**: Single-branch filtering — use when you only need matching values
- **`groupBy`**: N-way dynamic split by key — use for 3+ categories or runtime-determined splits
- **`iif`**: Conditional Observable selection at subscription time — different from partition (not a splitter)
- **`merge`**: Inverse of partition — combine two streams into one

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/partition](https://rxjs.dev/api/index/function/partition)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching point**: `partition` = `filter` + `filter(not)` with a shared subscription. Use it when you need BOTH branches; use `filter` when you only need one; use `groupBy` for N-way splits.
