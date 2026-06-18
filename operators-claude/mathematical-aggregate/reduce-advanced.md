# reduce — Advanced Patterns

For `reduce` fundamentals see the core [reduce](./reduce) doc. This page covers aggregation pipelines, reporting, grouping, and the critical distinction from `scan`.

---

## `reduce` vs `scan` — When Each Is Right

```typescript
// scan — emit accumulated value AFTER EACH emission:
of(1, 2, 3, 4, 5).pipe(
  scan((acc, x) => acc + x, 0)
).subscribe(console.log);
// 1, 3, 6, 10, 15 — running total, emits 5 times

// reduce — emit ONLY when source completes:
of(1, 2, 3, 4, 5).pipe(
  reduce((acc, x) => acc + x, 0)
).subscribe(console.log);
// 15 — final total only, emits once

// Rule: scan for running state / UI. reduce for final aggregation / reporting.
```

---

## Pattern 1: Multi-Metric Aggregation

Compute several statistics in a single pass:

```typescript
import { reduce, map } from 'rxjs/operators';

interface Stats {
  count: number;
  sum:   number;
  min:   number;
  max:   number;
  first: number | null;
  last:  number | null;
}

const INITIAL_STATS: Stats = {
  count: 0, sum: 0,
  min: Infinity, max: -Infinity,
  first: null, last: null
};

measurements$.pipe(
  reduce((stats, value): Stats => ({
    count: stats.count + 1,
    sum:   stats.sum   + value,
    min:   Math.min(stats.min, value),
    max:   Math.max(stats.max, value),
    first: stats.first ?? value,
    last:  value
  }), INITIAL_STATS),
  map(stats => ({
    ...stats,
    avg: stats.count > 0 ? stats.sum / stats.count : 0,
    range: stats.max - stats.min
  }))
).subscribe(report => generateReport(report));
```

---

## Pattern 2: Grouping / Histogram

```typescript
import { reduce, map } from 'rxjs/operators';

interface Order { category: string; amount: number }

orders$.pipe(
  reduce((groups, order) => {
    const existing = groups.get(order.category) ?? { count: 0, total: 0 };
    return new Map([
      ...groups,
      [order.category, {
        count: existing.count + 1,
        total: existing.total + order.amount
      }]
    ]);
  }, new Map<string, { count: number; total: number }>()),
  map(groups => [...groups.entries()].map(([category, data]) => ({
    category,
    count:   data.count,
    total:   data.total,
    average: data.total / data.count
  }))),
  map(rows => rows.sort((a, b) => b.total - a.total)) // by total desc
).subscribe(renderGroupedReport);
```

---

## Pattern 3: Building a Lookup Map

```typescript
import { reduce } from 'rxjs/operators';

// Convert a stream of items into a keyed lookup:
users$.pipe(
  reduce((map, user) => {
    map.set(user.id, user);
    return map;
  }, new Map<string, User>())
).subscribe(userMap => {
  // Fast O(1) lookup after stream completes:
  cache.set('users', userMap);
});

// Or build a plain object index:
products$.pipe(
  reduce((index, product) => ({
    ...index,
    [product.sku]: product
  }), {} as Record<string, Product>)
).subscribe(productIndex => renderCatalog(productIndex));
```

---

## Pattern 4: Validation Accumulator

Collect all validation errors in a single pass:

```typescript
import { from, reduce, map } from 'rxjs';

interface ValidationResult {
  field: string;
  errors: string[];
}

const formFields = ['email', 'username', 'password', 'confirmPassword'];

from(formFields).pipe(
  reduce<string, ValidationResult[]>((errors, field) => {
    const fieldErrors = validateField(field, formData[field]);
    return fieldErrors.length > 0
      ? [...errors, { field, errors: fieldErrors }]
      : errors;
  }, []),
  map(errors => ({
    valid:  errors.length === 0,
    errors,
    firstError: errors[0] ?? null
  }))
).subscribe(result => {
  if (!result.valid) showErrors(result.errors);
  else submitForm();
});
```

---

## Pattern 5: Event Log Summary

Summarize a stream of events into a compact report:

```typescript
import { reduce, map } from 'rxjs/operators';

interface AuditEvent {
  type: 'CREATE' | 'UPDATE' | 'DELETE';
  entityId: string;
  userId: string;
  timestamp: number;
}

auditLog$.pipe(
  reduce((summary, event) => ({
    total:       summary.total + 1,
    byType:      { ...summary.byType, [event.type]: (summary.byType[event.type] ?? 0) + 1 },
    byUser:      { ...summary.byUser, [event.userId]: (summary.byUser[event.userId] ?? 0) + 1 },
    uniqueEntities: summary.uniqueEntities.add(event.entityId),
    startTime:   Math.min(summary.startTime, event.timestamp),
    endTime:     Math.max(summary.endTime, event.timestamp)
  }), {
    total:          0,
    byType:         {} as Record<string, number>,
    byUser:         {} as Record<string, number>,
    uniqueEntities: new Set<string>(),
    startTime:      Infinity,
    endTime:        -Infinity
  }),
  map(s => ({
    ...s,
    uniqueEntityCount: s.uniqueEntities.size,
    durationMs: s.endTime - s.startTime
  }))
).subscribe(sendSummaryEmail);
```

---

## Pattern 6: `reduce` After `groupBy`

Classic group-then-aggregate pattern:

```typescript
import { groupBy, mergeMap, reduce, toArray, map } from 'rxjs/operators';

// Sales by region:
sales$.pipe(
  groupBy(sale => sale.region),
  mergeMap(region$ =>
    region$.pipe(
      reduce((acc, sale) => ({
        region:      region$.key,
        count:       acc.count + 1,
        revenue:     acc.revenue + sale.amount,
        topProduct:  sale.amount > acc.topRevenue
                       ? sale.product
                       : acc.topProduct,
        topRevenue:  Math.max(acc.topRevenue, sale.amount)
      }), { region: '', count: 0, revenue: 0, topProduct: '', topRevenue: 0 })
    )
  ),
  toArray(),
  map(regions => regions.sort((a, b) => b.revenue - a.revenue))
).subscribe(renderRegionReport);
```

---

## Pattern 7: Pipeline Aggregation (ETL)

```typescript
import { from, filter, map, reduce } from 'rxjs';

interface RawRecord { date: string; value: string; category: string; valid?: boolean }
interface CleanRecord { date: Date; value: number; category: string }
interface Summary { count: number; total: number; invalid: number }

// ETL pipeline: extract → transform → load (into summary):
from(rawRecords).pipe(
  // Transform:
  map((r): RawRecord & { valid: boolean } => ({
    ...r,
    valid: !isNaN(Number(r.value)) && Boolean(r.date)
  })),
  // Load — accumulate into summary:
  reduce<RawRecord & { valid: boolean }, Summary>((summary, record) => {
    if (!record.valid) return { ...summary, invalid: summary.invalid + 1 };
    return {
      count:   summary.count + 1,
      total:   summary.total + Number(record.value),
      invalid: summary.invalid
    };
  }, { count: 0, total: 0, invalid: 0 })
).subscribe(summary => {
  console.log(`Processed: ${summary.count} valid, ${summary.invalid} invalid`);
  console.log(`Total: ${summary.total}, Average: ${summary.total / summary.count}`);
});
```

---

## `reduce` vs `toArray` vs `last`

```typescript
// toArray — collect all values into one array (no custom accumulator):
source$.pipe(toArray()).subscribe(arr => console.log(arr));

// last — get the final value (no accumulation):
source$.pipe(last()).subscribe(final => console.log(final));

// reduce — custom accumulation with transformation:
source$.pipe(reduce((acc, v) => acc + v, 0)).subscribe(sum => console.log(sum));

// All three wait for source to COMPLETE before emitting
```

---

## Common Pitfalls

### `reduce` on Infinite Observables

```typescript
// ❌ NEVER EMITS — interval never completes:
interval(1000).pipe(
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log); // nothing ever logged

// ✅ Use scan for ongoing streams:
interval(1000).pipe(
  scan((acc, v) => acc + v, 0) // running total, emits every second
).subscribe(console.log);

// ✅ Or bound the stream before reduce:
interval(1000).pipe(
  take(10),           // complete after 10 values
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log); // 45 (0+1+2+...+9)
```

### Mutating the Accumulator

```typescript
// ❌ INCORRECT — mutating accumulator causes shared state bugs:
source$.pipe(
  reduce((acc, item) => {
    acc.items.push(item); // mutates accumulator!
    return acc;
  }, { items: [] })
)

// ✅ Always return a new reference:
source$.pipe(
  reduce((acc, item) => ({
    items: [...acc.items, item] // new array each time
  }), { items: [] as Item[] })
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 3/5
**Key rule**: `reduce` for final answers (reports, summaries, lookup maps). `scan` for running state (UI, progressive feedback). If your source might not complete, `scan` is almost always the right choice.
