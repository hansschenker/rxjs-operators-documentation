# mapTo

**Category**: Transformation  
**Import**: `import { mapTo } from 'rxjs';`

> **Deprecated**: `mapTo` will be removed in RxJS v9. Use `map(() => value)` instead.

## Description

Emits the given constant value on the output Observable every time the source Observable emits a value. Like `map`, but it ignores the actual source value and simply uses the emission moment to know when to emit the given constant value.

This operator is useful when you care only about *when* something happens, not *what* value was emitted. For example, mapping all click events to a single string response.

## Signature

```typescript
function mapTo<R>(value: R): OperatorFunction<unknown, R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `value` | `R` | The constant value to map every source emission to. |

## Return Type

`OperatorFunction<unknown, R>` — emits the constant `value` for every source emission.

## Marble Diagram

```
Source: --a------b------c--|
          mapTo('x')
Output: --x------x------x--|
```

## Examples

### Example 1: Signal user activity with a constant action type

```typescript
import { fromEvent, mapTo } from 'rxjs';

// Deprecated — shown for reference only
const clicks = fromEvent(document, 'click');
const activity$ = clicks.pipe(mapTo('USER_ACTIVE'));

activity$.subscribe(action => console.log(action));
// USER_ACTIVE
// USER_ACTIVE
// ...
```

### Example 2: Preferred modern equivalent using `map`

```typescript
import { fromEvent, map } from 'rxjs';

const clicks = fromEvent(document, 'click');
const activity$ = clicks.pipe(map(() => 'USER_ACTIVE'));

activity$.subscribe(action => console.log(action));
```

### Example 3: Map button presses to boolean toggle signals

```typescript
import { fromEvent, map } from 'rxjs';

const button = document.querySelector('#toggle-btn')!;
const toggle$ = fromEvent(button, 'click').pipe(map(() => true));

toggle$.subscribe(signal => console.log('Toggle triggered:', signal));
```

## Common Pitfalls

- **Deprecated API**: `mapTo` is deprecated as of RxJS v8 and will be removed in v9. Migrate to `map(() => value)` to future-proof your code.
- **Mutable reference**: If you pass an object or array as `value`, all emissions share the same reference. Mutating it will affect all downstream consumers. Use `map(() => ({ ...template }))` to emit a fresh object each time.

## Related Operators

- `map` — the modern replacement; pass `() => value` for the same behavior
- `tap` — perform side effects on each emission without changing the value
- `ignoreElements` — suppress all `next` notifications entirely
