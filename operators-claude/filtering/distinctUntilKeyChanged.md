# distinctUntilKeyChanged

## Identity

- **Name**: distinctUntilKeyChanged
- **Category**: Filtering Operators
- **Type**: Property-based duplicate suppressor — emits only when a specified key of the source object has changed from the previous emission
- **Import**:
  ```typescript
  import { distinctUntilKeyChanged } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function distinctUntilKeyChanged<T, K extends keyof T>(
    key: K,
    compare?: (x: T[K], y: T[K]) => boolean
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Concept**: Specialized form of `distinctUntilChanged` for objects. Instead of comparing entire values, compares only the property at `key`. Emits the full object whenever `obj[key]` differs from the previous emission's `obj[key]`.

**Equivalence**:
```typescript
distinctUntilKeyChanged('name')
// ≡
distinctUntilChanged((prev, curr) => prev.name === curr.name)
```

**`compare` function**: Optional custom comparator for the key's value. Defaults to `===` (strict equality). Use a custom comparator for deep equality on nested objects, case-insensitive string comparison, etc.

**Key invariants**:
- Only consecutive duplicates are suppressed (same as `distinctUntilChanged`)
- Non-consecutive duplicate objects with the same key value ARE emitted
- The entire object is emitted, not just the key value
- First emission always passes through (no previous to compare against)

## Marble Diagram

```
Source objects (tracking 'status' key):
--{id:1,status:'A'}--{id:2,status:'A'}--{id:3,status:'B'}--{id:4,status:'A'}--|

distinctUntilKeyChanged('status'):
--{id:1,status:'A'}------------------{id:3,status:'B'}--{id:4,status:'A'}--|

id:2 suppressed (status 'A' = previous 'A')
id:4 passes (status 'A' ≠ previous 'B')

With custom compare (case-insensitive):
--{name:'Alice'}--{name:'alice'}--{name:'Bob'}--|

distinctUntilKeyChanged('name', (a, b) => a.toLowerCase() === b.toLowerCase()):
--{name:'Alice'}--------------------{name:'Bob'}--|
```

## Type System Integration

```typescript
import { of } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs/operators';

interface User { id: number; name: string; role: 'admin' | 'user' }

const users: User[] = [
  { id: 1, name: 'Alice', role: 'user'  },
  { id: 2, name: 'Alice', role: 'admin' }, // name unchanged → suppressed
  { id: 3, name: 'Bob',   role: 'admin' }, // name changed → emitted
];

of(...users).pipe(
  distinctUntilKeyChanged('name')
).subscribe((u: User) => console.log(u));
// { id: 1, name: 'Alice', role: 'user' }
// { id: 3, name: 'Bob',   role: 'admin' }

// Key is type-checked — must be keyof T
// distinctUntilKeyChanged('nonexistent') // ← TypeScript error
```

## Examples

### Basic Usage
```typescript
import { of } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs/operators';

of(
  { page: 1, query: 'rxjs' },
  { page: 1, query: 'rxjs' },   // suppressed — page unchanged
  { page: 2, query: 'rxjs' },   // emitted — page changed
  { page: 2, query: 'angular' } // emitted — page changed? No — page 2 = 2... wait:
  // page 2 = previous page 2 → suppressed IF tracking 'page'
  // but query changed → emitted IF tracking 'query'
).pipe(
  distinctUntilKeyChanged('page')
).subscribe(console.log);
// { page: 1, query: 'rxjs' }
// { page: 2, query: 'rxjs' }
// (last entry suppressed — page 2 same as previous page 2)
```

### Common Pattern — Suppress Redundant State Updates
```typescript
import { BehaviorSubject } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs/operators';

interface AppState {
  user: User | null;
  theme: 'light' | 'dark';
  language: string;
}

const state$ = new BehaviorSubject<AppState>(initialState);

// Only re-render header when user identity changes (not theme or language)
state$.pipe(
  distinctUntilKeyChanged('user')
).subscribe(state => renderHeader(state.user));

// Only re-apply theme when theme key changes
state$.pipe(
  distinctUntilKeyChanged('theme')
).subscribe(state => applyTheme(state.theme));
```

### Common Pattern — Router / Navigation Dedup
```typescript
import { Router, NavigationEnd } from '@angular/router';
import { filter, map, distinctUntilKeyChanged } from 'rxjs/operators';

interface RouteState { path: string; queryParams: Record<string, string> }

routeState$.pipe(
  distinctUntilKeyChanged('path') // re-render only when path changes, not query params
).subscribe(state => loadPageData(state.path));
```

### Common Pattern — Custom Comparator for Deep Key Equality
```typescript
import { BehaviorSubject } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs/operators';

interface SearchState {
  filters: { category: string; minPrice: number; maxPrice: number };
  sortBy: string;
}

const searchState$ = new BehaviorSubject<SearchState>(initialSearch);

// Deep-compare the filters object (reference changes on each setState)
searchState$.pipe(
  distinctUntilKeyChanged(
    'filters',
    (a, b) =>
      a.category === b.category &&
      a.minPrice === b.minPrice &&
      a.maxPrice === b.maxPrice
  )
).subscribe(state => fetchResults(state.filters));
// Only re-fetches when filter VALUES actually change,
// not just when the filters object reference changes
```

## Common Pitfalls

### Anti-pattern: Expecting Object Identity to Matter
```typescript
import { of } from 'rxjs';
import { distinctUntilKeyChanged } from 'rxjs/operators';

// ❌ SURPRISE — different objects with same key value are suppressed
const obj1 = { status: 'active', data: { x: 1 } };
const obj2 = { status: 'active', data: { x: 2 } }; // different data!

of(obj1, obj2).pipe(
  distinctUntilKeyChanged('status')
).subscribe(console.log);
// Only obj1 is emitted — obj2 suppressed because status 'active' === 'active'
// Even though obj2 has different data!

// ✅ CORRECT — track the right key, or use a custom comparator
of(obj1, obj2).pipe(
  distinctUntilKeyChanged('data', (a, b) => a.x === b.x)
).subscribe(console.log); // both emitted — data.x differs

// WHY: distinctUntilKeyChanged ONLY compares the specified key (by === default).
// Other properties are invisible to the comparison. If you need to track
// multiple properties, chain multiple distinctUntilKeyChanged calls or use
// distinctUntilChanged with a custom comparator across all relevant fields.
```

### Anti-pattern: Using `distinctUntilKeyChanged` for Non-Consecutive Dedup
```typescript
import { of } from 'rxjs';
import { distinctUntilKeyChanged, distinct } from 'rxjs/operators';

// ❌ WRONG OPERATOR — expecting globally unique key values
of(
  { id: 1, status: 'A' },
  { id: 2, status: 'B' },
  { id: 3, status: 'A' }  // 'A' appeared before — but not consecutively
).pipe(
  distinctUntilKeyChanged('status')
).subscribe(console.log);
// Emits ALL THREE — 'A' reappears after 'B' so it's not a consecutive duplicate

// ✅ CORRECT — use distinct(keySelector) for globally unique values
of(
  { id: 1, status: 'A' },
  { id: 2, status: 'B' },
  { id: 3, status: 'A' }
).pipe(
  distinct(obj => obj.status)
).subscribe(console.log);
// Emits id:1 and id:2 only — 'A' is globally seen

// WHY: distinctUntilKeyChanged (like distinctUntilChanged) only suppresses
// CONSECUTIVE duplicates. For globally unique values across the entire stream,
// use distinct(keySelector) instead.
```

## Related Operators

- **`distinctUntilChanged(compareFn?)`**: General consecutive-duplicate suppressor — use when comparing entire values or need a custom full-object comparator
- **`distinct(keySelector?)`**: Global uniqueness — never re-emits a seen key value (uses a Set internally)
- **`filter`**: Per-value predicate — use for conditions unrelated to duplication
- **`debounceTime`**: Time-based dedup for rapidly changing streams

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/distinctUntilKeyChanged](https://rxjs.dev/api/operators/distinctUntilKeyChanged)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key teaching point**: Compares only the specified key (`===` by default) — other properties are invisible. Suppresses only CONSECUTIVE duplicates. Use `distinct(keySelector)` for global uniqueness.
