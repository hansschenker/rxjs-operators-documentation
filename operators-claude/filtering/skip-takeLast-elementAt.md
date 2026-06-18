# skip / takeLast / elementAt

Three focused filtering operators for position-based value selection.

---

## `skip`

### Identity
- **Import**: `import { skip } from 'rxjs/operators'`
- **Signature**: `skip<T>(count: number): MonoTypeOperatorFunction<T>`
- **Category**: Filtering — skips the first `count` emissions, then passes all remaining

### Functional Specification
Discards the first `count` values from the source and forwards everything after. The complement of `take(n)`.

```
Source:   --1--2--3--4--5--|
skip(2):  --------3--4--5--|   (1 and 2 discarded)
skip(0):  --1--2--3--4--5--|   (no skip — same as source)
skip(10): ------------------|  (source has 5 items; all skipped; just completion)
```

### Examples
```typescript
import { of, BehaviorSubject } from 'rxjs';
import { skip } from 'rxjs/operators';

// Skip first N values
of(1, 2, 3, 4, 5).pipe(skip(2)).subscribe(console.log); // 3, 4, 5

// Skip BehaviorSubject's initial emission
const state$ = new BehaviorSubject<string>('initial');
state$.pipe(skip(1)).subscribe(v => console.log('changed to:', v));
state$.next('updated'); // logs: changed to: updated
// 'initial' was silently skipped
```

### Pitfall
```typescript
// ❌ WRONG TOOL — using skip to "filter" non-initial values
// skip(1) only skips the FIRST value, not every occurrence of a value
state$.pipe(skip(1)).subscribe(v => v && doSomething(v));
// If state$ emits '' (empty string) after initial, it passes through

// ✅ CORRECT — filter for condition-based skipping
state$.pipe(filter(v => v !== 'initial')).subscribe(doSomething);
```

---

## `takeLast`

### Identity
- **Import**: `import { takeLast } from 'rxjs/operators'`
- **Signature**: `takeLast<T>(count: number): OperatorFunction<T, T>`
- **Category**: Filtering — emits only the last `count` values on source completion

### Functional Specification
Buffers the last `count` values internally. When the source completes, emits the buffered values in order and completes. Like `last()` but for N values instead of 1. Requires source completion — does not emit on infinite sources.

```
Source:   --1--2--3--4--5--|
takeLast(2):  ---------------4--5|  (last 2 values, emitted on completion)
takeLast(1):  ---------------5|     (equivalent to last())
takeLast(10): --1--2--3--4--5|      (fewer than 10 — emits all)
```

### Examples
```typescript
import { of, interval } from 'rxjs';
import { takeLast, take } from 'rxjs/operators';

// Get last 3 items from a finite source
of(1, 2, 3, 4, 5).pipe(takeLast(3)).subscribe(console.log); // 3, 4, 5

// Sliding window tail — get last 5 ticks of a 10-tick interval
interval(100).pipe(
  take(10),
  takeLast(5)
).subscribe(console.log); // 5, 6, 7, 8, 9
```

### Pitfall
```typescript
import { interval } from 'rxjs';
import { takeLast } from 'rxjs/operators';

// ❌ HANGS — interval never completes; takeLast buffers forever
interval(100).pipe(takeLast(3)).subscribe(console.log); // nothing, ever

// ✅ CORRECT — make the source finite first
interval(100).pipe(take(10), takeLast(3)).subscribe(console.log); // 7, 8, 9
// WHY: takeLast waits for completion to know which values were "last."
```

---

## `elementAt`

### Identity
- **Import**: `import { elementAt } from 'rxjs/operators'`
- **Signature**: `elementAt<T, D = T>(index: number, defaultValue?: D): OperatorFunction<T, T | D>`
- **Category**: Filtering — emits the value at a specific 0-based index, then completes

### Functional Specification
Emits the Nth emission (0-indexed) from the source, then completes. Errors with `ArgumentOutOfRangeError` if the source completes before reaching that index — unless `defaultValue` is provided.

```
Source:   --a--b--c--d--|
elementAt(0):  --a|         (index 0)
elementAt(2):  --------c|   (index 2)
elementAt(10): ----------#  (ArgumentOutOfRangeError — only 4 values)
elementAt(10, 'x'): ------x| (defaultValue used)
```

### Examples
```typescript
import { of, EMPTY } from 'rxjs';
import { elementAt } from 'rxjs/operators';

// Get item at specific index
of('a', 'b', 'c', 'd').pipe(elementAt(2)).subscribe(console.log); // 'c'

// Safe with defaultValue
of('a', 'b').pipe(elementAt(5, 'missing')).subscribe(console.log); // 'missing'

// elementAt(0) ≡ first() semantics (but throws ArgumentOutOfRangeError, not EmptyError)
of(1, 2, 3).pipe(elementAt(0)).subscribe(console.log); // 1
```

### Pitfall
```typescript
import { EMPTY } from 'rxjs';
import { elementAt } from 'rxjs/operators';

// ❌ MISSING ERROR HANDLER — ArgumentOutOfRangeError on short source
EMPTY.pipe(elementAt(0)).subscribe({
  next:  v => console.log(v),
  error: e => console.log(e.name) // ArgumentOutOfRangeError
});

// ✅ CORRECT — provide defaultValue or handle error
EMPTY.pipe(elementAt(0, null)).subscribe(v => console.log(v ?? 'empty')); // 'empty'
```

---

## Comparison Table

| Operator | Selects | Completion required | No-match behavior |
|----------|---------|--------------------|--------------------|
| `first(p?)` | First (matching) | No | `EmptyError` |
| `last(p?)` | Last (matching) | **Yes** | `EmptyError` |
| `take(n)` | First N | No | Completes after all |
| `takeLast(n)` | Last N | **Yes** | Emits all if < n |
| `skip(n)` | All after first N | No | Just completes |
| `elementAt(i)` | One by index | No | `ArgumentOutOfRangeError` |

## References
- **skip**: [https://rxjs.dev/api/operators/skip](https://rxjs.dev/api/operators/skip)
- **takeLast**: [https://rxjs.dev/api/operators/takeLast](https://rxjs.dev/api/operators/takeLast)
- **elementAt**: [https://rxjs.dev/api/operators/elementAt](https://rxjs.dev/api/operators/elementAt)

---

**`skip`** — Cognitive Load: 1/5 | Usage: 4/5 | Skip initial BehaviorSubject emission is the primary use case.
**`takeLast`** — Cognitive Load: 1/5 | Usage: 2/5 | Requires completion; symmetric to take().
**`elementAt`** — Cognitive Load: 1/5 | Usage: 2/5 | Throws ArgumentOutOfRangeError (not EmptyError) — always provide defaultValue for uncertain sources.
