# distinctUntilChanged

**Category**: Filtering  
**Import**: `import { distinctUntilChanged } from 'rxjs';`

## Description

`distinctUntilChanged` emits a value from the source only when it differs from the previously emitted value. It always emits the first value, then for every subsequent value it compares against the last emitted value using either strict equality (`===`) or a custom `comparator` function.

An optional `keySelector` function can project each value to a comparison key before the comparator is applied. This allows deep or partial comparison of complex objects without implementing a full comparator.

Unlike `distinct`, which tracks all previously seen values, `distinctUntilChanged` only stores the last emitted key — making it memory-efficient for long-running streams.

## Signature

```typescript
function distinctUntilChanged<T>(
  comparator?: (previous: T, current: T) => boolean
): MonoTypeOperatorFunction<T>

function distinctUntilChanged<T, K>(
  comparator: (previous: K, current: K) => boolean,
  keySelector: (value: T) => K
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| comparator | `(previous: K, current: K) => boolean` | Optional. Returns `true` if the two keys are considered equal (should suppress emission). Defaults to `===`. |
| keySelector | `(value: T) => K` | Optional. Extracts a comparison key from each value. Defaults to the identity function. |

## Return Type

`MonoTypeOperatorFunction<T>` — suppresses consecutive duplicate emissions.

## Marble Diagram

```
Source: --1--1--2--2--1--3--|
        distinctUntilChanged()
Output: --1-----2-----1--3--|
```

## Examples

### Example 1: Suppress consecutive duplicate numbers

```typescript
import { of } from 'rxjs';
import { distinctUntilChanged } from 'rxjs';

of(1, 1, 1, 2, 2, 2, 1, 1, 3, 3).pipe(
  distinctUntilChanged()
).subscribe(console.log);

// Logs: 1, 2, 1, 3
```

### Example 2: Compare objects by a specific field

```typescript
import { of } from 'rxjs';
import { distinctUntilChanged } from 'rxjs';

of(
  { userId: 1, page: 'home' },
  { userId: 1, page: 'profile' },
  { userId: 2, page: 'profile' },
  { userId: 2, page: 'settings' }
).pipe(
  distinctUntilChanged(undefined, event => event.userId)
).subscribe(event => console.log('User changed:', event.userId));

// Logs:
// User changed: 1
// User changed: 2
```

### Example 3: Custom comparator for partial change detection

```typescript
import { of } from 'rxjs';
import { distinctUntilChanged } from 'rxjs';

// Only emit when BOTH fields change simultaneously
const builds$ = of(
  { engine: '1.0', transmission: '1.0' },
  { engine: '1.0', transmission: '1.1' }, // only transmission changed — suppressed
  { engine: '1.1', transmission: '1.1' }, // both changed — emitted
  { engine: '2.0', transmission: '1.1' }  // only engine changed — suppressed
);

builds$.pipe(
  distinctUntilChanged(
    (prev, curr) =>
      prev.engine === curr.engine || prev.transmission === curr.transmission
  )
).subscribe(build => console.log('New build:', build));

// Logs:
// New build: { engine: '1.0', transmission: '1.0' }
// New build: { engine: '1.1', transmission: '1.1' }
```

## Common Pitfalls

- **Repeated non-consecutive values are not suppressed**: `1,2,1` through `distinctUntilChanged()` yields `1,2,1`. Only back-to-back repeats are suppressed. Use `distinct` if you need to suppress all previously seen values.
- **Object references vs deep equality**: Without a `comparator`, equality is checked with `===`. Two objects with identical properties are not equal by default. Use a `keySelector` or a deep-comparison `comparator` for objects.
- **Comparator semantics**: The comparator returns `true` to indicate the values are the **same** (suppress emission), and `false` to indicate they differ (allow emission). This is inverted compared to a typical sort comparator.

## Related Operators

- `distinct` — suppresses all previously seen values, not just consecutive ones
- `distinctUntilKeyChanged` — a simplified form when comparing a single object property
- `debounceTime` — suppresses rapid bursts by time rather than value equality
