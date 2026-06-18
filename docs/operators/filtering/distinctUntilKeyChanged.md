# distinctUntilKeyChanged

**Category**: Filtering  
**Import**: `import { distinctUntilKeyChanged } from 'rxjs';`

## Description

`distinctUntilKeyChanged` is a convenience wrapper around `distinctUntilChanged` that compares a single named property of each emitted object. It emits a value only when the specified property's value differs from the same property on the previously emitted value.

An optional `compare` function can be provided to customize how the property values are compared. Without it, strict equality (`===`) is used.

## Signature

```typescript
function distinctUntilKeyChanged<T>(key: keyof T): MonoTypeOperatorFunction<T>

function distinctUntilKeyChanged<T, K extends keyof T>(
  key: K,
  compare: (x: T[K], y: T[K]) => boolean
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | `keyof T` | The property name to use for comparison. |
| compare | `(x: T[K], y: T[K]) => boolean` | Optional. A function that returns `true` when the two property values are considered equal (suppresses emission). Defaults to `===`. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits values only when the specified property changes.

## Marble Diagram

```
Source: --{name:'A'}--{name:'A',age:2}--{name:'B'}--{name:'B',age:3}--|
        distinctUntilKeyChanged('name')
Output: --{name:'A'}---------------------------{name:'B'}--------------|
```

## Examples

### Example 1: Emit only when a user's name changes

```typescript
import { of } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs';

of(
  { age: 4,  name: 'Foo' },
  { age: 7,  name: 'Bar' },
  { age: 5,  name: 'Foo' },
  { age: 6,  name: 'Foo' }
).pipe(
  distinctUntilKeyChanged('name')
).subscribe(x => console.log(x));

// Logs:
// { age: 4, name: 'Foo' }
// { age: 7, name: 'Bar' }
// { age: 5, name: 'Foo' }
// (last entry suppressed — name 'Foo' same as previous)
```

### Example 2: Detect route changes in a router state stream

```typescript
import { BehaviorSubject } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs';

interface RouterState {
  url: string;
  queryParams: Record<string, string>;
}

const router$ = new BehaviorSubject<RouterState>({ url: '/home', queryParams: {} });

router$.pipe(
  distinctUntilKeyChanged('url')
).subscribe(state => console.log('Navigated to:', state.url));

router$.next({ url: '/home', queryParams: { ref: 'nav' } }); // suppressed (url unchanged)
router$.next({ url: '/about', queryParams: {} });             // emitted
router$.next({ url: '/about', queryParams: { tab: '2' } });  // suppressed (url unchanged)
```

### Example 3: Custom comparison — ignore letter casing

```typescript
import { of } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs';

of(
  { id: 1, name: 'Foo1' },
  { id: 2, name: 'Bar' },
  { id: 3, name: 'Foo2' },
  { id: 4, name: 'Foo3' }
).pipe(
  distinctUntilKeyChanged(
    'name',
    (x, y) => x.substring(0, 3) === y.substring(0, 3)
  )
).subscribe(x => console.log(x));

// Logs:
// { id: 1, name: 'Foo1' }
// { id: 2, name: 'Bar' }
// { id: 3, name: 'Foo2' }
// (id:4 suppressed because 'Foo3' has same prefix 'Foo' as 'Foo2')
```

## Common Pitfalls

- **Only compares one key**: If your distinctness requirement involves multiple fields, use `distinctUntilChanged` with a custom `keySelector` or comparator instead.
- **Shallow comparison of the property value**: If the property value is itself an object or array, `===` checks reference equality. Use the `compare` function for deep equality when needed.
- **The full object is emitted, not just the key**: When the key changes, the entire source object is forwarded. The `key` parameter is used only for comparison, not for projection.

## Related Operators

- `distinctUntilChanged` — the general form; supports a `keySelector` and custom `comparator`
- `distinct` — suppresses all globally previously seen values
- `filter` — general-purpose value filtering
