# range

## Identity

- **Name**: range
- **Category**: Creation Operators
- **Type**: Synchronous integer sequence — emits a contiguous range of integers
- **Import**:
  ```typescript
  import { range } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function range(start: number, count?: number, scheduler?: SchedulerLike): Observable<number>
  ```

## Functional Specification

`range(start, count)` emits `count` integers beginning at `start`, then completes. Without a scheduler, all emissions are synchronous.

| Call | Emits |
|---|---|
| `range(1, 5)` | 1, 2, 3, 4, 5 |
| `range(0, 3)` | 0, 1, 2 |
| `range(5, 3)` | 5, 6, 7 |
| `range(1)` | 1 (count defaults to `Infinity` — avoid!) |
| `range(0, 0)` | completes immediately, no values |

**Key constraint**: `range` emits only integers. For floats or non-linear sequences, use `from([...])` or `generate`.

## Marble Diagram

```
range(1, 4):
  (1234|)   ← synchronous: all values then complete in same frame
```

## Examples

### Basic Usage
```typescript
import { range } from 'rxjs';

range(1, 5).subscribe(console.log);
// 1
// 2
// 3
// 4
// 5
```

### Common Pattern — Batch ID Generator
```typescript
import { range } from 'rxjs';
import { map, mergeMap } from 'rxjs/operators';

// Fetch items 1–10 in parallel
range(1, 10).pipe(
  mergeMap(id => fetchItem(id))
).subscribe(renderItem);
```

### Common Pattern — Table Row Indices
```typescript
import { range } from 'rxjs';
import { map, toArray } from 'rxjs/operators';

// Generate table row data
range(0, pageSize).pipe(
  map(i => ({ index: i, row: pageData[i] })),
  toArray()
).subscribe(rows => renderTable(rows));
```

### Edge Case — Async Scheduling
```typescript
import { range } from 'rxjs';
import { asyncScheduler } from 'rxjs';

// Async: emissions are queued, not synchronous
range(1, 3, asyncScheduler).subscribe(console.log);
console.log('after subscribe');
// Output: "after subscribe", 1, 2, 3
// (vs sync: 1, 2, 3, "after subscribe")
```

## Common Pitfalls

### Missing `count` — Infinite Loop
```typescript
// ❌ HANGS — range(start) with no count → Infinity emissions
range(1).subscribe(console.log); // never stops

// ✅ CORRECT — always provide count
range(1, 10).subscribe(console.log);
// WHY: The count parameter is optional, defaulting to Infinity.
// Without it you get an unbounded synchronous loop that blocks the thread.
```

### Using `range` for Float Sequences
```typescript
// ❌ range only emits integers
range(0, 5).pipe(map(i => i * 0.1))  // workaround, not idiomatic

// ✅ For non-integer sequences, use from() or generate
import { generate } from 'rxjs';
generate(0, x => x < 0.5, x => x + 0.1)
  .subscribe(v => console.log(v.toFixed(1)));
// 0.0, 0.1, 0.2, 0.3, 0.4
```

## Related Operators

- **`generate`**: Loop-based sequence with custom condition and step (supports floats, custom types)
- **`interval`**: Time-based integer sequence (0, 1, 2, ... at regular intervals)
- **`timer`**: Single value after delay, or interval with initial delay
- **`from([...])`**: Sequence from any iterable

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
