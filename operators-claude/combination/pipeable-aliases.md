# Pipeable Combination Aliases

## combineLatestWith / mergeWith / concatWith / zipWith

These are the **pipeable operator forms** of the corresponding creation functions. They exist so combination operators can be used inside a `.pipe()` chain without breaking the pipeline into a separate `combineLatest([source$, ...])` call.

## Identity

| Pipeable form | Equivalent creation call | Import |
|---|---|---|
| `combineLatestWith(...sources)` | `combineLatest([source$, ...sources])` | `import { combineLatestWith } from 'rxjs/operators'` |
| `mergeWith(...sources)` | `merge(source$, ...sources)` | `import { mergeWith } from 'rxjs/operators'` |
| `concatWith(...sources)` | `concat(source$, ...sources)` | `import { concatWith } from 'rxjs/operators'` |
| `zipWith(...sources)` | `zip(source$, ...sources)` | `import { zipWith } from 'rxjs/operators'` |

```typescript
function combineLatestWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, Cons<T, A>>

function mergeWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>

function concatWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>

function zipWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, Cons<T, A>>
```

## Functional Specification

Each pipeable alias behaves identically to its creation counterpart — the only difference is syntax. The pipeable form treats the upstream Observable as the first source.

**When to prefer pipeable form over creation form**:
- When the first source is itself the result of a long pipe chain (avoids nesting)
- For stylistic consistency — keep everything inside `.pipe()`
- When the first source isn't available as a standalone variable

## Examples

### `combineLatestWith` — Inline Combination
```typescript
import { combineLatestWith } from 'rxjs/operators';

// ❌ CREATION FORM — source$ must be extracted to pass as first arg
const result$ = combineLatest([
  source$.pipe(filter(x => x > 0), map(x => x * 2)),
  otherA$,
  otherB$
]);

// ✅ PIPEABLE FORM — natural pipe chain
const result$ = source$.pipe(
  filter(x => x > 0),
  map(x => x * 2),
  combineLatestWith(otherA$, otherB$)  // source is implicit first
);
// Both are equivalent; pipeable form reads better when source is already piped
```

### `mergeWith` — Merge in a Pipe
```typescript
import { fromEvent } from 'rxjs';
import { mergeWith } from 'rxjs/operators';

// Merge mouse and touch events into one stream
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  mergeWith(fromEvent<TouchEvent>(document, 'touchmove'))
).subscribe(handlePointerMove);
// All mousemove and touchmove events arrive on one stream
```

### `concatWith` — Sequential Completion in a Pipe
```typescript
import { of } from 'rxjs';
import { concatWith } from 'rxjs/operators';

// Play intro animation, then show content
introAnimation$.pipe(
  concatWith(mainContent$)  // mainContent$ starts only after introAnimation$ completes
).subscribe(render);
```

### `zipWith` — Index-Pair in a Pipe
```typescript
import { from } from 'rxjs';
import { zipWith, map } from 'rxjs/operators';

from(['Alice', 'Bob', 'Carol']).pipe(
  zipWith(from([95, 87, 92])),
  map(([name, score]) => `${name}: ${score}`)
).subscribe(console.log);
// Alice: 95, Bob: 87, Carol: 92
```

## Type System Integration

```typescript
// combineLatestWith — output is tuple [T, ...A]
of(1).pipe(
  combineLatestWith(of('a'), of(true))
).subscribe(([n, s, b]: [number, string, boolean]) => console.log(n, s, b));

// mergeWith — output is T | A[number] union
of(1).pipe(
  mergeWith(of('a'))
).subscribe((v: number | string) => console.log(v));

// zipWith — output is tuple [T, ...A] (same as combineLatestWith shape)
of(1).pipe(
  zipWith(of('a'))
).subscribe(([n, s]: [number, string]) => console.log(n, s));
```

## Common Pitfalls

### Anti-pattern: Using Creation Form When Pipeable Form Is Cleaner
```typescript
// ❌ AWKWARD — must extract source$ just to pass it to combineLatest
const processed$ = source$.pipe(
  debounceTime(300),
  map(transform)
);
const combined$ = combineLatest([processed$, other$]);

// ✅ CLEANER — combineLatestWith keeps it all in one chain
const combined$ = source$.pipe(
  debounceTime(300),
  map(transform),
  combineLatestWith(other$)
);
```

## Related Operators

Full behavioral documentation for each operator:
- **[combineLatest](./combineLatest-operator-documentation)** — fires on any source; all must emit once
- **[merge](./merge)** — concurrent union of all source emissions
- **[concat](./concat)** — sequential; next source starts after previous completes
- **[zip](./zip)** — index-based pairing; emits tuple when all sources have contributed index N

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 4/5
**Teaching note**: These are pure syntax conveniences — no new behavior to learn. Introduce after students know the creation forms.
