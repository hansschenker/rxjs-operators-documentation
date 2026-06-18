# find / findIndex

## Identity

| | `find` | `findIndex` |
|---|---|---|
| **Import** | `import { find } from 'rxjs/operators'` | `import { findIndex } from 'rxjs/operators'` |
| **Signature** | `find<T>(predicate): OperatorFunction<T, T \| undefined>` | `findIndex<T>(predicate): OperatorFunction<T, number>` |
| **Category** | Filtering Operators | Filtering Operators |
| **Emits** | First matching value (or `undefined`) | Index of first matching value (or `-1`) |

```typescript
function find<T>(
  predicate: (value: T, index: number, source: Observable<T>) => boolean
): OperatorFunction<T, T | undefined>

function findIndex<T>(
  predicate: (value: T, index: number, source: Observable<T>) => boolean
): OperatorFunction<T, number>
```

## Functional Specification

**`find(predicate)`**: Emits the first value for which `predicate` returns `true`, then completes. If the source completes without a match, emits `undefined` and completes.

**`findIndex(predicate)`**: Emits the 0-based index of the first matching value, then completes. If the source completes without a match, emits `-1` and completes.

**Comparison with `first(predicate)`**:

| | `find(p)` | `first(p)` |
|---|---|---|
| No match found | Emits `undefined` | Throws `EmptyError` |
| Output type | `T \| undefined` | `T` (or `T \| D` with defaultValue) |
| Use when | "find or undefined" is acceptable | "must find — empty is a bug" |

**Both operators**:
- Subscribe to source until first match (or source completion)
- Unsubscribe from source immediately after first match (efficient)
- Always emit exactly once and complete

## Marble Diagram

```
Source:   --a--b--c--d--|
Predicate: v => v === 'c'

find(p):       --------c|    (emits 'c', then completes; d never processed)
findIndex(p):  --------2|    (index 2, then completes)

No match:
Source:   --a--b--|
find(v => v === 'z'):      --undefined|
findIndex(v => v === 'z'): ---1|        (wait — emits -1 on completion)

Actually:
find on empty/no-match:   --|  then undefined emitted at completion → undefined|
findIndex on no-match:    --|  then -1 emitted at completion → -1|
```

## Type System Integration

```typescript
import { of, EMPTY } from 'rxjs';
import { find, findIndex } from 'rxjs/operators';

// find — output is T | undefined
of(1, 2, 3, 4).pipe(
  find(n => n > 2)
).subscribe((v: number | undefined) => console.log(v)); // 3

EMPTY.pipe(
  find(n => n > 0)
).subscribe((v: number | undefined) => console.log(v)); // undefined

// findIndex — always number
of('a', 'b', 'c').pipe(
  findIndex(s => s === 'b')
).subscribe((i: number) => console.log(i)); // 1

of('a', 'b', 'c').pipe(
  findIndex(s => s === 'z')
).subscribe((i: number) => console.log(i)); // -1

// Type narrowing with type predicate
interface Shape { kind: 'circle' | 'square'; radius?: number }
of<Shape>({ kind: 'square' }, { kind: 'circle', radius: 5 }).pipe(
  find((s): s is Shape & { kind: 'circle'; radius: number } => s.kind === 'circle')
).subscribe(circle => circle?.radius); // circle is narrowed
```

## Examples

### Basic Usage
```typescript
import { of, from } from 'rxjs';
import { find, findIndex, map } from 'rxjs/operators';

const items = ['apple', 'banana', 'cherry', 'date'];

// find — get the item itself
from(items).pipe(
  find(item => item.startsWith('c'))
).subscribe(console.log); // 'cherry'

// findIndex — get the position
from(items).pipe(
  findIndex(item => item.startsWith('c'))
).subscribe(console.log); // 2

// No match
from(items).pipe(find(item => item.startsWith('z'))).subscribe(console.log); // undefined
from(items).pipe(findIndex(item => item.startsWith('z'))).subscribe(console.log); // -1
```

### Common Pattern — Find First Valid Item in a Stream
```typescript
import { from } from 'rxjs';
import { find, switchMap, filter } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface Server { id: number; url: string; healthy: boolean }

const servers: Server[] = [
  { id: 1, url: 'https://s1.api.com', healthy: false },
  { id: 2, url: 'https://s2.api.com', healthy: true  },
  { id: 3, url: 'https://s3.api.com', healthy: true  },
];

// Find the first healthy server, then use it
from(servers).pipe(
  find(server => server.healthy),
  filter((server): server is Server => server !== undefined),
  switchMap(server => ajax.getJSON(`${server.url}/data`))
).subscribe(data => processData(data));
```

### Common Pattern — `findIndex` for Array Manipulation
```typescript
import { from } from 'rxjs';
import { findIndex, map } from 'rxjs/operators';

interface Todo { id: number; text: string; done: boolean }

// Find the position of a todo to update it in the array
function updateTodo(todos: Todo[], id: number, done: boolean): Observable<Todo[]> {
  return from(todos).pipe(
    findIndex(todo => todo.id === id),
    map(index => {
      if (index === -1) return todos; // not found — unchanged
      const updated = [...todos];
      updated[index] = { ...todos[index], done };
      return updated;
    })
  );
}
```

### Edge Case — `find` vs `first` on Empty / Non-Matching Sources
```typescript
import { EMPTY, of } from 'rxjs';
import { find, first } from 'rxjs/operators';

// find — safe on empty/non-matching sources
EMPTY.pipe(find(() => true)).subscribe({
  next:  v => console.log('find:', v),    // find: undefined
  error: e => console.log('error:', e)    // never called
});

// first — throws EmptyError on empty/non-matching sources
EMPTY.pipe(first()).subscribe({
  next:  v => console.log('first:', v),
  error: e => console.log('error:', e.name) // EmptyError
});

// first with defaultValue — behaves like find
EMPTY.pipe(first(null, undefined)).subscribe({
  next: v => console.log('first+default:', v) // undefined
});

// Decision rule:
// find(p)  → safe sentinel: "first match or undefined"
// first(p) → strict: "first match or error (empty = bug)"
```

## Common Pitfalls

### Anti-pattern: Not Handling `undefined` From `find`
```typescript
import { of } from 'rxjs';
import { find, map } from 'rxjs/operators';

// ❌ UNSAFE — accessing properties on possibly-undefined result
of({ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }).pipe(
  find(u => u.id === 99),
  map(user => user.name.toUpperCase()) // ← TypeError: Cannot read 'name' of undefined
).subscribe(console.log);

// ✅ CORRECT — guard against undefined
import { filter } from 'rxjs/operators';
of({ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }).pipe(
  find(u => u.id === 99),
  filter((user): user is { id: number; name: string } => user !== undefined),
  map(user => user.name.toUpperCase()) // safe — filter removed undefined
).subscribe(console.log); // (no output — no match found, filter stops it)

// OR use optional chaining
of({ id: 1, name: 'Alice' }).pipe(
  find(u => u.id === 99),
  map(user => user?.name?.toUpperCase() ?? 'NOT FOUND')
).subscribe(console.log); // 'NOT FOUND'

// WHY: find() always emits — but emits undefined when no match is found.
// Unlike first() which throws EmptyError, find() silently emits undefined.
// Always guard downstream operators with filter() or optional chaining.
```

### Anti-pattern: Using `find` When `first` Semantics Are Needed
```typescript
import { of } from 'rxjs';
import { find, first } from 'rxjs/operators';

function getAdminUser(users$: Observable<User>): Observable<User> {
  // ❌ WRONG SEMANTICS — silently returns undefined if no admin exists
  // Caller receives undefined and may not notice
  return users$.pipe(find(u => u.role === 'admin'));
  // Return type: Observable<User | undefined> — caller must handle undefined
}

function getAdminUser(users$: Observable<User>): Observable<User> {
  // ✅ CORRECT — if no admin, it's an error, not a valid "not found"
  return users$.pipe(first(u => u.role === 'admin'));
  // Throws EmptyError if no admin — explicit about this being unexpected
}

// WHY: find() signals "not found" by emitting undefined — a value that callers
// may ignore. first() signals "not found" by throwing EmptyError — impossible
// to silently ignore. Use first() when absence is a program error; use find()
// when absence is a valid outcome to be handled explicitly.
```

## Related Operators

- **`first(predicate, defaultValue?)`**: Strict version — errors on no match (without defaultValue); use when absence is unexpected
- **`filter(predicate)`**: Keeps all matching values (not just first); stream continues
- **`take(1)`**: First value without predicate, no error on empty
- **`elementAt(n)`**: Emit value at a specific index position
- **`findIndex` ↔ `find`**: Twin operators — same predicate logic, different return value (index vs value)

## References
- **RxJS find**: [https://rxjs.dev/api/operators/find](https://rxjs.dev/api/operators/find)
- **RxJS findIndex**: [https://rxjs.dev/api/operators/findIndex](https://rxjs.dev/api/operators/findIndex)

---

**`find`** — Cognitive Load: 1/5 | Usage: 3/5 | Always emits (undefined on no-match) — guard downstream with `filter` or `?.` optional chaining.
**`findIndex`** — Cognitive Load: 1/5 | Usage: 2/5 | Always emits (-1 on no-match) — check for -1 before using the index.
**Teaching sequence**: After `first`/`last` — the find/first distinction (undefined vs EmptyError) is the key differentiator.
