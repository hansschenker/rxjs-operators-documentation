# groupBy — Advanced Patterns

For `groupBy` fundamentals, the base doc covers identity and basic marble behavior. This page covers real-world patterns.

## Quick Reference

```typescript
import { groupBy, mergeMap, toArray } from 'rxjs/operators';

source$.pipe(
  groupBy(item => item.category),          // key selector
  mergeMap(group$ => group$.pipe(toArray())) // MUST subscribe to inner groups
)
```

`groupBy` emits `GroupedObservable<K, T>` — each carries a `.key` and is itself an Observable. **Every group must be subscribed to** (via `mergeMap`/`concatMap`) or its values are dropped.

---

## Pattern 1: Aggregate Per Category

```typescript
import { groupBy, mergeMap, reduce, map } from 'rxjs/operators';

interface Sale { product: string; amount: number; }

sales$.pipe(
  groupBy(sale => sale.product),
  mergeMap(group$ =>
    group$.pipe(
      reduce((total, sale) => total + sale.amount, 0),
      map(total => ({ product: group$.key, total }))
    )
  )
).subscribe(({ product, total }) =>
  console.log(`${product}: $${total}`)
);
// Emits one result per product after source completes
```

---

## Pattern 2: Route Events by Type

```typescript
import { groupBy, mergeMap } from 'rxjs/operators';

interface AppEvent { type: 'click' | 'hover' | 'focus'; payload: unknown; }

// Route each event type to a different handler:
events$.pipe(
  groupBy(e => e.type)
).subscribe(group$ => {
  switch (group$.key) {
    case 'click': group$.subscribe(handleClick); break;
    case 'hover': group$.subscribe(handleHover); break;
    case 'focus': group$.subscribe(handleFocus); break;
  }
});
```

---

## Pattern 3: Partition into Two Streams

For binary split (pass/fail, valid/invalid), `groupBy` + `mergeMap` is more composable than `partition`:

```typescript
import { groupBy, mergeMap, filter } from 'rxjs/operators';

// Split valid and invalid items in one pass:
const grouped$ = items$.pipe(
  groupBy(item => item.isValid ? 'valid' : 'invalid'),
  share()
);

const valid$   = grouped$.pipe(filter(g => g.key === 'valid'),   mergeMap(g => g));
const invalid$ = grouped$.pipe(filter(g => g.key === 'invalid'), mergeMap(g => g));

valid$.subscribe(processItem);
invalid$.subscribe(logError);
```

---

## Pattern 4: Group With Duration (Timed Windows per Key)

Each group window expires after inactivity using `groupBy`'s `durationSelector`:

```typescript
import { groupBy, mergeMap, toArray, debounceTime } from 'rxjs/operators';

// Group events by userId; each user's window closes after 5s of silence
userEvents$.pipe(
  groupBy(
    event => event.userId,
    { duration: group$ => group$.pipe(debounceTime(5000)) }
    // ↑ durationSelector: close this group's window after 5s quiet
  ),
  mergeMap(group$ =>
    group$.pipe(
      toArray(),
      map(events => ({ userId: group$.key, events }))
    )
  )
).subscribe(session => saveSession(session));
// Emits one session object per user per activity burst
```

Without `duration`, groups never complete — `toArray()` would never emit. The `duration` option is what makes `groupBy` viable for unbounded streams.

---

## Pattern 5: Collect Per-Key Streams Live

Sometimes you want each group to remain open and process values as they arrive (rather than accumulating):

```typescript
import { groupBy, mergeMap, scan, map } from 'rxjs/operators';

// Running total per product, updated live:
transactions$.pipe(
  groupBy(tx => tx.productId),
  mergeMap(group$ =>
    group$.pipe(
      scan((total, tx) => total + tx.amount, 0),
      map(total => ({ productId: group$.key, total }))
    )
  )
).subscribe(({ productId, total }) =>
  updateDashboard(productId, total)
);
// Emits whenever any product's total changes
```

---

## Pattern 6: Top-N Per Category

```typescript
import { groupBy, mergeMap, toArray, map } from 'rxjs/operators';

// Top 3 items per category (source must complete):
items$.pipe(
  groupBy(item => item.category),
  mergeMap(group$ =>
    group$.pipe(
      toArray(),
      map(items =>
        items
          .sort((a, b) => b.score - a.score)
          .slice(0, 3)
          .map(item => ({ ...item, category: group$.key }))
      )
    )
  )
).subscribe(topItems => renderCategory(topItems));
```

---

## Common Pitfalls

### Not Subscribing to Inner Groups

```typescript
// ❌ GROUP OBSERVABLES IGNORED — all values are dropped
source$.pipe(
  groupBy(item => item.type)
).subscribe(group$ => {
  console.log('group key:', group$.key); // key visible
  // group$ is an Observable — never subscribed to!
});

// ✅ CORRECT — subscribe via mergeMap (or concatMap for ordering)
source$.pipe(
  groupBy(item => item.type),
  mergeMap(group$ => group$.pipe(toArray()))
).subscribe(console.log);
// WHY: groupBy emits GroupedObservable<K,T>. Values only flow when
// the inner Observable is subscribed. Not subscribing = silently dropped.
```

### Using `toArray()` on Infinite Groups Without `duration`

```typescript
// ❌ HANGS — toArray() waits for group$ to complete; without duration, it never does
userEvents$.pipe(
  groupBy(e => e.userId),
  mergeMap(group$ => group$.pipe(toArray())) // waits forever
).subscribe(console.log);

// ✅ Use duration to close groups after inactivity
userEvents$.pipe(
  groupBy(e => e.userId, { duration: g$ => g$.pipe(debounceTime(5000)) }),
  mergeMap(group$ => group$.pipe(toArray()))
).subscribe(console.log);
// WHY: toArray() only emits when the source completes. Without a duration,
// individual key groups never complete on an unbounded stream.
```

### Using `groupBy` When `partition` Is Enough

```typescript
// ❌ OVER-COMPLEX for a binary split
source$.pipe(
  groupBy(item => item.isValid),
  mergeMap(g => g.pipe(toArray()))
)

// ✅ Use partition for binary splits
const [valid$, invalid$] = partition(source$, item => item.isValid);
valid$.subscribe(process);
invalid$.subscribe(logError);
// WHY: partition is designed for binary splits. groupBy shines with
// 3+ dynamic categories where the key isn't known at compile time.
```

## Related Operators

- **`partition`**: Binary split into two Observables
- **`filter`**: Single-condition filtering (not grouping)
- **`scan`**: Running accumulation per key (combine with groupBy for per-key state)
- **`bufferToggle`**: Time/signal-based grouping rather than key-based

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `groupBy` without a `duration` is only safe when the source completes. For live/infinite streams, always provide a `durationSelector` to close stale groups.
