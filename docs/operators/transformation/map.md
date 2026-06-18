# map

**Category**: Transformation  
**Import**: `import { map } from 'rxjs';`

## Description

Applies a given `project` function to each value emitted by the source Observable, and emits the resulting values as an Observable. Like `Array.prototype.map()`, it passes each source value through a transformation function to get the corresponding output value.

This operator is one of the most commonly used in RxJS. It does not alter the timing or number of emissions — every source value produces exactly one output value. Both the transformed value and the zero-based emission index are passed to the projection function.

## Signature

```typescript
function map<T, R>(project: (value: T, index: number) => R): OperatorFunction<T, R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `project` | `(value: T, index: number) => R` | The transformation function applied to each emitted value. The `index` is the zero-based count of emissions since subscription. |

## Return Type

`OperatorFunction<T, R>` — emits one transformed value for every value from the source.

## Marble Diagram

```
Source: --a------b------c--|
              map(fn)
Output: --fn(a)--fn(b)--fn(c)--|
```

## Examples

### Example 1: Extract a property from DOM events

```typescript
import { fromEvent, map } from 'rxjs';

const clicks = fromEvent<PointerEvent>(document, 'click');

clicks.pipe(
  map(event => ({ x: event.clientX, y: event.clientY }))
).subscribe(pos => console.log(`Clicked at ${pos.x}, ${pos.y}`));
```

### Example 2: Transform HTTP response data

```typescript
import { from, map } from 'rxjs';

interface ApiUser {
  id: number;
  first_name: string;
  last_name: string;
  email: string;
}

const users$ = from(fetch('/api/users').then(res => res.json() as Promise<ApiUser[]>));

users$.pipe(
  map(users => users.map(u => ({
    id: u.id,
    fullName: `${u.first_name} ${u.last_name}`,
    email: u.email,
  })))
).subscribe(users => console.log(users));
```

### Example 3: Use the emission index

```typescript
import { interval, map, take } from 'rxjs';

// Tag each value with its sequence number (1-based)
interval(500).pipe(
  take(5),
  map((value, index) => `Item ${index + 1}: ${value}`)
).subscribe(console.log);
// Item 1: 0
// Item 2: 1
// Item 3: 2
// Item 4: 3
// Item 5: 4
```

## Common Pitfalls

- **Mutating the source value**: If you mutate the incoming object instead of returning a new one, you may cause hard-to-trace side effects. Always return a new object/array from the projection function.
- **Throwing inside `project`**: If the projection function throws, the error is forwarded to the subscriber's error handler and the stream terminates. Make sure to guard against unexpected `null`/`undefined` values.
- **Confusing `map` with `mergeMap`**: Use `map` when the transformation returns a plain value. If the transformation returns an Observable, use `mergeMap`, `concatMap`, or `switchMap` to flatten it.

## Related Operators

- `mapTo` — maps every emission to the same constant value (deprecated in v9, use `map(() => value)`)
- `mergeMap` — like `map`, but the project function returns an Observable that is then flattened
- `switchMap` — like `mergeMap` but cancels the previous inner Observable on each new source value
- `pluck` — shorthand for picking a nested property (removed in v8, use `map`)
