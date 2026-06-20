# filter

**Category**: Filtering  
**Import**: `import { filter } from 'rxjs';`

## Description

`filter` emits only those values from the source Observable that satisfy a specified predicate function. It behaves analogously to `Array.prototype.filter` — each value emitted by the source is passed through the predicate, and only values for which the predicate returns `true` are forwarded to the output Observable.

The predicate receives both the current value and a zero-based index representing how many values have been emitted since subscription. Errors and the completion notification always pass through regardless of the predicate.

## Signature

```typescript
function filter<T>(predicate: (value: T, index: number) => boolean): MonoTypeOperatorFunction<T>
```

For type-narrowing use cases, a type guard predicate is also supported:

```typescript
function filter<T, S extends T>(predicate: (value: T, index: number) => value is S): OperatorFunction<T, S>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number) => boolean` | A function that evaluates each emitted value. Return `true` to emit the value, `false` to drop it. The `index` is the zero-based count of values received since subscription. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that passes only values for which the predicate returns `true`.

## Marble Diagram

```
Source:  --a--b--c--d--e--|
         filter(x => x !== 'b' && x !== 'd')
Output:  --a-----c-----e--|
```

## Examples

### Example 1: Filter click events to a specific element

```typescript
import { fromEvent } from 'rxjs';
import { filter } from 'rxjs';

const clicks$ = fromEvent<MouseEvent>(document, 'click');

clicks$.pipe(
  filter(ev => (ev.target as HTMLElement).tagName === 'BUTTON')
).subscribe(ev => console.log('Button clicked:', ev));
```

### Example 2: Filter a stream of numbers

```typescript
import { from } from 'rxjs';
import { filter } from 'rxjs';

const numbers$ = from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

numbers$.pipe(
  filter(n => n % 2 === 0)
).subscribe(n => console.log('Even:', n));

// Logs: 2, 4, 6, 8, 10
```

### Example 3: Use a type guard to narrow the output type

```typescript
import { of } from 'rxjs';
import { filter } from 'rxjs';

const values$ = of(1, null, 'hello', undefined, true);

values$.pipe(
  filter((v): v is string => typeof v === 'string')
).subscribe(s => console.log('String value:', s.toUpperCase()));

// Logs: String value: HELLO
```

## Common Pitfalls

- **Mutating state in the predicate**: The predicate should be a pure function. Avoid side effects like incrementing counters outside the operator — use the provided `index` argument instead.
- **Confusing `filter` with `find`**: `filter` keeps the Observable open and emits every matching value; `find` emits only the first match and then completes.
- **TypeScript narrowing not applying**: To benefit from TypeScript type narrowing, write a proper type guard (`(v): v is S => ...`) rather than a plain boolean predicate.

## Related Operators

- `find` — emits only the first value matching the predicate, then completes
- `first` — emits only the first value (optionally matching a predicate), then completes
- `distinct` — filters out values that have already been emitted
- `ignoreElements` — drops all `next` emissions and passes only completion/error
- `skip` — skips the first N values unconditionally
