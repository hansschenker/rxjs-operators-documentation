# distinctUntilChanged — Advanced Patterns

For `distinctUntilChanged` fundamentals see the core [distinctUntilChanged](./distinctUntilChanged) doc. This page covers custom comparators, deep equality, object/array comparison, and composable equality helpers.

---

## Why Custom Comparators Matter

By default, `distinctUntilChanged` uses `===` (reference equality). This means:
- Primitives: works correctly (`1 === 1`, `'a' === 'a'`)
- Objects/arrays: **fails** — new object with same data is not `===` to the previous one

```typescript
// Default === comparison:
of({ a: 1 }, { a: 1 }).pipe(
  distinctUntilChanged()
).subscribe(console.log);
// Logs: { a: 1 }, { a: 1 } — BOTH emitted! Objects are different references
```

---

## Pattern 1: Key-Based Comparison

Compare by a single property (most common — avoids deep equality cost):

```typescript
import { distinctUntilChanged } from 'rxjs/operators';

// Compare by ID — only re-render when the user changes:
userStream$.pipe(
  distinctUntilChanged((prev, curr) => prev.id === curr.id)
).subscribe(renderUser);

// Compare by version number:
documentStream$.pipe(
  distinctUntilChanged((prev, curr) => prev.version === curr.version)
).subscribe(renderDocument);
```

The same thing with `distinctUntilKeyChanged` (syntactic sugar):

```typescript
import { distinctUntilKeyChanged } from 'rxjs/operators';

userStream$.pipe(
  distinctUntilKeyChanged('id')           // Observable<User>, filters by id
).subscribe(renderUser);

userStream$.pipe(
  distinctUntilKeyChanged('name', (a, b) => a.toLowerCase() === b.toLowerCase())
).subscribe(renderUser); // case-insensitive name comparison
```

---

## Pattern 2: Shallow Object Comparison

Check all top-level properties — faster than deep equality, safer than `===`:

```typescript
import { distinctUntilChanged } from 'rxjs/operators';

function shallowEqual<T extends object>(a: T, b: T): boolean {
  if (a === b) return true;
  const keysA = Object.keys(a) as (keyof T)[];
  const keysB = Object.keys(b) as (keyof T)[];
  if (keysA.length !== keysB.length) return false;
  return keysA.every(key => a[key] === b[key]);
}

stateStream$.pipe(
  distinctUntilChanged(shallowEqual)
).subscribe(render);
```

---

## Pattern 3: JSON Deep Equality

Simple deep equality using JSON serialization — good for small objects, avoid for large ones:

```typescript
import { distinctUntilChanged } from 'rxjs/operators';

configStream$.pipe(
  distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b))
).subscribe(applyConfig);
// NOTE: JSON.stringify doesn't handle undefined, functions, Dates, or circular references
// Use for plain data objects only
```

---

## Pattern 4: Structural Equality for Specific Shapes

Write comparators tailored to your data shape — faster than generic deep equality:

```typescript
import { distinctUntilChanged } from 'rxjs/operators';

interface FilterState {
  search:    string;
  category:  string;
  tags:      string[];
  page:      number;
}

function filtersEqual(a: FilterState, b: FilterState): boolean {
  return (
    a.search   === b.search   &&
    a.category === b.category &&
    a.page     === b.page     &&
    a.tags.length === b.tags.length &&
    a.tags.every((tag, i) => tag === b.tags[i]) // array item comparison
  );
}

filters$.pipe(
  distinctUntilChanged(filtersEqual)
).subscribe(applyFilters);
```

---

## Pattern 5: Array Content Comparison

```typescript
import { distinctUntilChanged } from 'rxjs/operators';

// Check if arrays have same items (same order):
function arrayEqual<T>(a: T[], b: T[]): boolean {
  return a.length === b.length && a.every((item, i) => item === b[i]);
}

// Check if sets of IDs are the same (order-independent):
function idSetEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const setA = new Set(a);
  return b.every(id => setA.has(id));
}

selectedIds$.pipe(
  distinctUntilChanged(idSetEqual)
).subscribe(loadItems);
```

---

## Pattern 6: Composable Equality Helpers

Build reusable equality operators for common patterns:

```typescript
import { OperatorFunction } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

// Compare by a selector function:
function distinctUntilMappedChanged<T, K>(
  selector: (value: T) => K,
  comparator?: (a: K, b: K) => boolean
): MonoTypeOperatorFunction<T> {
  return distinctUntilChanged((prev, curr) => {
    const a = selector(prev);
    const b = selector(curr);
    return comparator ? comparator(a, b) : a === b;
  });
}

// Usage:
userStream$.pipe(
  distinctUntilMappedChanged(u => u.permissions.sort().join(','))
).subscribe(updatePermissions);

priceStream$.pipe(
  distinctUntilMappedChanged(p => Math.round(p * 100)) // cents — ignore sub-cent noise
).subscribe(updateDisplay);
```

---

## Pattern 7: `distinctUntilChanged` for State Slices

Only react to the specific slice of state you care about:

```typescript
import { map, distinctUntilChanged, shareReplay } from 'rxjs/operators';

const appState$ = this.store.state$.pipe(shareReplay(1));

// Each slice only emits when its own data changes:
const users$ = appState$.pipe(
  map(state => state.users),
  distinctUntilChanged()               // reference equality — NgRx ensures immutability
);

const selectedUser$ = appState$.pipe(
  map(state => state.users.find(u => u.id === state.selectedId) ?? null),
  distinctUntilChanged((a, b) => a?.id === b?.id) // only re-emit on user change
);

const sortedUsers$ = appState$.pipe(
  map(state => [...state.users].sort((a, b) => a.name.localeCompare(b.name))),
  distinctUntilChanged(arrayEqual) // prevent re-render when sort result is same
);
```

---

## Pattern 8: Debounce + Distinct for Search

The classic combination:

```typescript
import { debounceTime, distinctUntilChanged, map, trim } from 'rxjs/operators';

searchInput$.pipe(
  debounceTime(300),
  map(v => v.trim()),                  // normalize first
  distinctUntilChanged(),              // then skip duplicates
  filter(q => q.length === 0 || q.length >= 2), // allow clear or meaningful queries
  switchMap(q => q ? this.api.search(q) : of([]))
).subscribe(renderResults);
```

---

## Common Pitfalls

### Using Default `===` on Objects

```typescript
// ❌ Always re-renders — new object reference every time even if data is same
this.store.select(selectUser).pipe(
  distinctUntilChanged()   // === fails for objects
).subscribe(render);

// ✅ Compare by meaningful property:
this.store.select(selectUser).pipe(
  distinctUntilChanged((a, b) => a?.id === b?.id && a?.updatedAt === b?.updatedAt)
).subscribe(render);
```

### Expensive Comparator Running on Every Emission

```typescript
// ❌ Deep JSON comparison on every event — could be slow for large objects
highFrequencyStream$.pipe(
  distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b))
).subscribe();

// ✅ Use a cheap structural check or debounce first:
highFrequencyStream$.pipe(
  debounceTime(16), // throttle to ~60fps first
  distinctUntilChanged((a, b) => a.version === b.version) // cheap check
).subscribe();
```

### Comparator Returns `true` for Different Values

```typescript
// ❌ BUG — returning true means "same" — this would SKIP all emissions
source$.pipe(
  distinctUntilChanged((a, b) => true) // always "same" → stream never emits
)

// The comparator contract: return true = same (skip), false = different (emit)
source$.pipe(
  distinctUntilChanged((a, b) => a.id === b.id) // true = same id → skip
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key insight**: Always provide a custom comparator when the stream emits objects or arrays. Choose the cheapest comparator that correctly identifies meaningful changes — key comparison beats shallow equality beats JSON comparison beats deep equality.
