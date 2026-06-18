# groupBy

## Identity

- **Name**: groupBy
- **Category**: Transformation Operators
- **Type**: Partitioning — splits a flat stream into a stream of keyed inner Observables
- **Import**:
  ```typescript
  import { groupBy } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function groupBy<T, K, R = T>(
    keySelector: (value: T) => K,
    options?: {
      element?: (value: T) => R;
      duration?: (grouped: GroupedObservable<K, R>) => Observable<any>;
      connector?: () => SubjectLike<R>;
    }
  ): OperatorFunction<T, GroupedObservable<K, R>>

  // GroupedObservable extends Observable<R>
  interface GroupedObservable<K, T> extends Observable<T> {
    readonly key: K;
  }
  ```

## Functional Specification

**Concept**: `groupBy` emits a `GroupedObservable` for each new key seen in the source. Each `GroupedObservable` has a `.key` property and forwards all source values with that key.

**Key rules**:
- Outer Observable emits one `GroupedObservable` per unique key (emitted on first encounter of that key)
- Subsequent values with the same key are forwarded to the existing group Observable
- Groups live as long as the source — subscribing to a group Observable after its values have passed will miss them (unless `connector` adds replay behavior)
- Groups complete when the source completes (default) or when the `duration` Observable fires for that group

**`duration` option**: Lets you close and re-open groups. When the duration Observable emits, that group completes and a new one will be created the next time that key appears.

## Marble Diagram

```
Source:  --{k:'a',v:1}--{k:'b',v:2}--{k:'a',v:3}--{k:'b',v:4}--|

groupBy(item => item.k):

Outer:   --groupA---------groupB-------------------------------|
              |                |
         groupA: --1-----------3---------------------------|
         groupB: ----------------2-----------4-------------|

groupBy(k => k, { duration: group$ => timer(100) }):
         groupA created at t=0 → closes at t=100; re-opens on next 'a'
         groupB created at t=0 → closes at t=100; re-opens on next 'b'
```

## Behavioral Characteristics

**Two-level subscription**: You must subscribe to BOTH the outer Observable (to receive groups) and each inner `GroupedObservable` (to receive values). If you subscribe to the outer but not the inner, values are buffered then dropped.

**Memory**: By default groups use a plain `Subject` — no replay. Late subscribers to a group miss past values. Use `connector: () => new ReplaySubject(1)` to add replay.

**Hot inner Observables**: Group Observables are hot multicast streams — multiple subscribers to the same group share emissions.

## Type System Integration

```typescript
import { of } from 'rxjs';
import { groupBy, mergeMap, toArray } from 'rxjs/operators';

interface Item { category: string; value: number }

const items: Item[] = [
  { category: 'A', value: 1 },
  { category: 'B', value: 2 },
  { category: 'A', value: 3 },
];

of(...items).pipe(
  groupBy(item => item.category),  // OperatorFunction<Item, GroupedObservable<string, Item>>
  mergeMap(group$ =>               // group$.key: string
    group$.pipe(
      toArray(),
      // [{ category: 'A', value: 1 }, { category: 'A', value: 3 }]
    )
  )
).subscribe(console.log);
```

## Examples

### Basic Usage — Categorize Events
```typescript
import { from } from 'rxjs';
import { groupBy, mergeMap, toArray, map } from 'rxjs/operators';

const events = [
  { type: 'click', target: 'button' },
  { type: 'hover', target: 'link' },
  { type: 'click', target: 'icon' },
  { type: 'hover', target: 'button' },
  { type: 'click', target: 'link' },
];

from(events).pipe(
  groupBy(e => e.type),
  mergeMap(group$ =>
    group$.pipe(
      toArray(),
      map(items => ({ type: group$.key, count: items.length, items }))
    )
  )
).subscribe(console.log);
// { type: 'click', count: 3, items: [...] }
// { type: 'hover', count: 2, items: [...] }
```

### Common Pattern — Live Stream Partitioning
```typescript
import { Subject } from 'rxjs';
import { groupBy, mergeMap } from 'rxjs/operators';

interface LogEntry { level: 'info' | 'warn' | 'error'; message: string }

const logs$ = new Subject<LogEntry>();

logs$.pipe(
  groupBy(log => log.level),
  mergeMap(group$ => {
    // Wire each group to a different handler as soon as it appears
    switch (group$.key) {
      case 'error': return group$.pipe(
        mergeMap(log => alerting.send(log.message))
      );
      case 'warn': return group$.pipe(
        mergeMap(log => monitoring.record(log))
      );
      default: return group$; // info — pass through for general logging
    }
  })
).subscribe(entry => console.log(entry));

logs$.next({ level: 'info',  message: 'Server started' });
logs$.next({ level: 'error', message: 'DB connection failed' }); // → alerting
logs$.next({ level: 'warn',  message: 'High memory usage' });    // → monitoring
```

### Common Pattern — Group + Aggregate (Finite Source)
```typescript
import { from } from 'rxjs';
import { groupBy, mergeMap, reduce, map } from 'rxjs/operators';

interface Sale { region: string; amount: number }

const sales: Sale[] = [
  { region: 'North', amount: 100 },
  { region: 'South', amount: 200 },
  { region: 'North', amount: 150 },
  { region: 'South', amount: 50 },
  { region: 'East',  amount: 300 },
];

from(sales).pipe(
  groupBy(sale => sale.region),
  mergeMap(group$ =>
    group$.pipe(
      reduce((total, sale) => total + sale.amount, 0),
      map(total => ({ region: group$.key, total }))
    )
  )
).subscribe(console.log);
// { region: 'North', total: 250 }
// { region: 'South', total: 250 }
// { region: 'East',  total: 300 }
```

### Common Pattern — `duration` to Reset Groups Periodically
```typescript
import { timer, fromEvent } from 'rxjs';
import { groupBy, mergeMap } from 'rxjs/operators';

// Group user actions, but reset groups every 5 minutes
// (new group = new context for analysis)
fromEvent<CustomEvent>(document, 'userAction').pipe(
  groupBy(
    e => e.detail.userId,
    { duration: () => timer(5 * 60 * 1000) } // each group lives 5 minutes
  ),
  mergeMap(group$ =>
    group$.pipe(
      // analyze per-user action sequences within the 5-minute window
      toArray()
    )
  )
).subscribe(userActions => analyzeSession(userActions));
```

## Common Pitfalls

### Anti-pattern: Subscribing to Outer But Not Inner
```typescript
import { of } from 'rxjs';
import { groupBy } from 'rxjs/operators';

// ❌ MEMORY LEAK / LOST VALUES — inner groups never subscribed
of(1, 2, 1, 3, 2).pipe(
  groupBy(n => n)
).subscribe(group$ => {
  console.log('new group:', group$.key);
  // Values are buffered internally and never consumed — buffer grows
  // group$.subscribe() is never called!
});
// Prints: new group: 1, new group: 2, new group: 3
// All values are lost/buffered

// ✅ CORRECT — always subscribe to inner groups
of(1, 2, 1, 3, 2).pipe(
  groupBy(n => n),
  mergeMap(group$ =>
    group$.pipe(toArray(), map(arr => ({ key: group$.key, values: arr })))
  )
).subscribe(console.log);
// { key: 1, values: [1, 1] }
// { key: 2, values: [2, 2] }
// { key: 3, values: [3] }

// WHY: groupBy emits GroupedObservables. Each GroupedObservable is a Subject
// internally. If you don't subscribe to it, values are queued until the group
// buffer is exhausted or the source completes — effectively lost.
// Always use mergeMap (or similar) to immediately subscribe to each group.
```

### Anti-pattern: Using `groupBy` for a Simple Two-Way Split
```typescript
import { groupBy, mergeMap, filter } from 'rxjs/operators';

// ❌ OVERENGINEERED — groupBy for a binary split
source$.pipe(
  groupBy(n => n % 2 === 0 ? 'even' : 'odd'),
  mergeMap(group$ => group$.key === 'even' ? group$.pipe(map(processEven)) : group$.pipe(map(processOdd)))
).subscribe(console.log);

// ✅ SIMPLER — partition() for a binary split
import { partition } from 'rxjs';
const [even$, odd$] = partition(source$, n => n % 2 === 0);
even$.pipe(map(processEven)).subscribe(console.log);
odd$.pipe(map(processOdd)).subscribe(console.log);

// WHY: groupBy excels at N-way dynamic splits where keys are discovered at
// runtime. For a static two-way split, partition() is simpler and more readable.
```

## Related Operators

- **`partition(predicate)`**: Two-way static split — returns `[Observable, Observable]`; simpler than groupBy for binary conditions
- **`filter`**: Single-stream filtering — use when groups are consumed independently
- **`mergeMap`**: Always used with groupBy to subscribe to inner groups
- **`bufferTime / windowTime`**: Time-based grouping — collect values into arrays/windows by time rather than key
- **`scan`**: Accumulate state across a flat stream without splitting (alternative when the split isn't needed)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/groupBy](https://rxjs.dev/api/operators/groupBy)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 3/5 | **Composability**: 3/5
**Key teaching point**: `groupBy` produces a stream of streams — you MUST subscribe to inner groups (via `mergeMap`). The most common bug is subscribing to the outer Observable but never consuming the inner ones.
