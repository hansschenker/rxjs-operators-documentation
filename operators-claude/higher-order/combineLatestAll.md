# combineLatestAll

## Identity

- **Name**: combineLatestAll
- **Category**: Higher-Order Operators (Join)
- **Type**: Higher-order flattener — collects all inner Observables, then combines their latest values reactively
- **Import**:
  ```typescript
  import { combineLatestAll } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function combineLatestAll<T>(): OperatorFunction<ObservableInput<T>, T[]>
  function combineLatestAll<T, R>(
    project: (...values: T[]) => R
  ): OperatorFunction<ObservableInput<T>, R>
  ```

## Functional Specification

`combineLatestAll` operates on a higher-order Observable (an Observable that emits Observables). It:
1. **Collects** all inner Observables emitted by the source until the source completes
2. **Subscribes** to all of them simultaneously once the source completes
3. **Combines** their latest values exactly like `combineLatest([...])`, emitting a new array whenever any inner Observable emits

**The outer source MUST complete** for `combineLatestAll` to subscribe to the inners. If the source never completes, nothing ever emits.

**Relationship to `combineLatest`**:

| | `combineLatest([a$, b$, c$])` | `combineLatestAll` |
|---|---|---|
| Sources | Known statically | Dynamic — determined at runtime |
| Source count | Fixed at construction | Variable |
| Outer complete required | N/A (no outer) | Yes — must complete to start |
| Use when | Known set of streams | Unknown count of streams |

## Marble Diagram

```
Outer (source):  --a$--b$--c$--|   (outer completes at |)
                               ↓ outer completes → subscribe to all inners

a$: --1--------3--|
b$: ----2--4------|
c$: -------5--6--|

combineLatestAll (waits for all three to have emitted at least once):
Result: -------[1,2,5]--[3,2,5]--[3,4,5]--[3,4,6]--|

Note: result starts only after outer completes AND all inners have emitted once.
```

## Type System Integration

```typescript
import { of } from 'rxjs';
import { map, combineLatestAll } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Dynamic set of HTTP requests, combined reactively
const ids = [1, 2, 3];

of(...ids).pipe(
  map(id => ajax.getJSON<User>(`/api/users/${id}`)),
  combineLatestAll()  // Observable<User[]>
).subscribe((users: User[]) => renderAll(users));

// With project function
of(...ids).pipe(
  map(id => ajax.getJSON<User>(`/api/users/${id}`)),
  combineLatestAll((a, b, c) => ({ a, b, c }))
).subscribe(result => console.log(result));
```

## Examples

### Basic Usage — Dynamic Stream Combination
```typescript
import { of, interval } from 'rxjs';
import { map, combineLatestAll, take } from 'rxjs/operators';

// Combine N timer streams where N is determined at runtime
const streamCount = 3;

of(...Array.from({ length: streamCount }, (_, i) => i)).pipe(
  map(i => interval(100 * (i + 1)).pipe(
    map(v => `stream${i}:${v}`),
    take(5)
  )),
  combineLatestAll()
).subscribe(latest => console.log(latest));
// ['stream0:0', 'stream1:0', 'stream2:0']
// ['stream0:1', 'stream1:0', 'stream2:0']
// ...
```

### Common Pattern — Multi-Source Dashboard
```typescript
import { from } from 'rxjs';
import { map, combineLatestAll } from 'rxjs/operators';

const dashboardSources = [
  '/api/metrics/cpu',
  '/api/metrics/memory',
  '/api/metrics/network',
  '/api/metrics/disk',
];

// Number of sources determined by config — can't use static combineLatest
from(dashboardSources).pipe(
  map(url => webSocket<MetricUpdate>(url.replace('http', 'ws'))),
  combineLatestAll()  // emits [cpu, memory, network, disk] on any update
).subscribe(([cpu, memory, network, disk]) => {
  updateDashboard({ cpu, memory, network, disk });
});
```

### Common Pattern — Parallel Requests, Reactive Updates
```typescript
import { of } from 'rxjs';
import { map, combineLatestAll } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// forkJoin waits for all to complete; combineLatestAll re-emits on any update
const userIds = getUserIdsFromRoute();

of(...userIds).pipe(
  map(id => ajax.getJSON<User>(`/api/users/${id}`)),
  combineLatestAll()
).subscribe(users => renderUserList(users));

// vs forkJoin — use when sources are finite HTTP calls and you want one emission:
// forkJoin(userIds.map(id => ajax.getJSON(`/api/users/${id}`)))
```

## Common Pitfalls

### Anti-pattern: Outer Source That Never Completes
```typescript
import { Subject } from 'rxjs';
import { combineLatestAll } from 'rxjs/operators';

// ❌ HANGS — Subject never completes; combineLatestAll never subscribes to inners
const source$ = new Subject<Observable<number>>();

source$.pipe(
  combineLatestAll()
).subscribe(console.log); // never emits

source$.next(of(1));
source$.next(of(2));
// Still nothing — combineLatestAll is waiting for source$ to complete

// ✅ CORRECT — complete the outer source when done emitting inners
source$.next(of(1));
source$.next(of(2));
source$.complete(); // NOW combineLatestAll subscribes and emits

// WHY: combineLatestAll must know the full set of inner Observables before
// it can subscribe to all of them simultaneously.
```

### Anti-pattern: Confusing with `mergeAll` for One-Shot HTTP
```typescript
import { of } from 'rxjs';
import { map, combineLatestAll, mergeAll } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ WRONG TOOL — using combineLatestAll for independent parallel requests
of(1, 2, 3).pipe(
  map(id => ajax.getJSON(`/api/items/${id}`)),
  combineLatestAll() // waits for all to have emitted — overkill for one-shots
).subscribe(console.log); // works but emits [item1, item2, item3] once

// ✅ CORRECT — use forkJoin for "all parallel, one result"
import { forkJoin } from 'rxjs';
forkJoin([1, 2, 3].map(id => ajax.getJSON(`/api/items/${id}`)))
  .subscribe(([item1, item2, item3]) => console.log(item1, item2, item3));

// WHY: combineLatestAll is designed for reactive combination of long-lived
// streams. For parallel HTTP calls, forkJoin is semantically correct and
// requires no outer Observable construction.
```

## Related Operators

- **`combineLatest([a$, b$])`**: Static version — sources known at construction time
- **`forkJoin`**: Wait for ALL to complete, emit one combined result — for finite sources
- **`mergeAll`**: Subscribe to each inner as it arrives, no combination
- **`zipAll`**: Combines inners by index (like `zip`) instead of latest values

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/combineLatestAll](https://rxjs.dev/api/operators/combineLatestAll)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key teaching points**:
1. Outer source MUST complete before any inner subscriptions happen
2. Use when the number of streams is determined at runtime — use static `combineLatest` when the count is known
3. For one-shot parallel requests, `forkJoin` is simpler
