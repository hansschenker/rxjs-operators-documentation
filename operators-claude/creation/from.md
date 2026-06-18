# from

## Identity
- **Name**: from
- **Category**: Creation Operators
- **Type**: Universal Observable converter — wraps arrays, Promises, iterables, async iterables, and Observable-like objects into an Observable
- **Import**:
  ```typescript
  import { from } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function from<T>(
    input: ObservableInput<T>,
    scheduler?: SchedulerLike
  ): Observable<T>

  type ObservableInput<T> =
    | Observable<T>
    | InteropObservable<T>    // any object with Symbol.observable
    | AsyncIterable<T>        // async generators, ReadableStream
    | PromiseLike<T>          // Promise, thenable
    | ArrayLike<T>            // arrays, strings (indexed + length)
    | Iterable<T>             // Set, Map, generator functions
  ```

## Functional Specification

**Input**: Any `ObservableInput<T>` — the broadest possible "things that produce values"

**Output**: `Observable<T>` — wraps the input so it emits values following Observable semantics

**Transformation by input type**:

| Input | Behaviour | Synchronous? |
|-------|-----------|-------------|
| `Array<T>` | Emits each element in order, then completes | ✅ Yes |
| `string` | Emits each character, then completes | ✅ Yes |
| `Set<T>` / `Map<K,V>` | Emits each entry (Map emits `[K,V]` pairs), then completes | ✅ Yes |
| `Iterable<T>` (generator) | Emits lazily from iterator, then completes | ✅ Yes (sync generators) |
| `Promise<T>` | Emits resolved value once, then completes. On rejection, errors. | ❌ Async |
| `AsyncIterable<T>` | Emits each yielded value, then completes | ❌ Async |
| `Observable<T>` | Mirror passthrough — subscribes and re-emits | Depends on source |

**Invariants**:
- **Completion guaranteed** for finite inputs (arrays, strings, Promises)
- **Synchronous for iterables**: array/string/Set/Map/sync-generator values emitted synchronously in the subscriber's call stack
- **Cold**: Each subscription creates a fresh iteration/consumption of the input
- **Promise rejection → Observable error**: `from(promise)` converts a rejected Promise to an erroring Observable

## Marble Diagram

```
from([1, 2, 3]):    (1)(2)(3)|    synchronous — parens = same tick

from(Promise.resolve(42)):   ----42|    async — arrives on microtask resolution

from('abc'):        (a)(b)(c)|   each character emitted synchronously

from(new Set([1,2,3])):  (1)(2)(3)|   insertion order
```

**Key observation**: `from` is the "adapter" that bridges the non-Observable world into RxJS. Use it whenever you have a value that isn't already an Observable but needs to enter a reactive pipeline.

## Behavioral Characteristics

**Arrays / Iterables / Strings**:
- Values emitted synchronously and sequentially in a single call stack frame
- Completion is synchronous after the last value
- `scheduler` parameter can make emission asynchronous (rarely needed)

**Promises**:
- Emits exactly one value (the resolved value) then completes
- Rejection maps to Observable error
- If Promise is already resolved: still emits asynchronously (microtask)
- `from(Promise.resolve(x))` is NOT synchronous — always async

**AsyncIterables** (async generators, `ReadableStream` web API):
- Emits values as the async iterator yields them
- Completes when the iterator returns (generator function finishes)

**Error handling**:
- Array/iterable: cannot error during iteration (unless iterator throws — propagated)
- Promise: rejection → Observable error
- AsyncIterable: rejection/thrown error → Observable error

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - inferred from the input type
 *
 * from([1, 2, 3])          → Observable<number>
 * from('hello')            → Observable<string>  (each char: string, not string[])
 * from(Promise<User>)      → Observable<User>
 * from(new Set<number>())  → Observable<number>
 * from(new Map<string, number>()) → Observable<[string, number]>  (tuple pairs)
 * from(Observable<T>)      → Observable<T>  (passthrough — same type)
 *
 * NOTE: from(promise) loses the specific resolved type if the Promise is untyped.
 * Always annotate: from(fetch(...) as Promise<User>) or use typed helper functions.
 */

import { from } from 'rxjs';

const nums$:  Observable<number>          = from([1, 2, 3]);
const chars$: Observable<string>          = from('hello');
const user$:  Observable<User>            = from(fetch('/api/me').then(r => r.json()) as Promise<User>);
const pairs$: Observable<[string, number]>= from(new Map([['a', 1], ['b', 2]]));
```

## Examples

### Basic Usage — Array, String, Set, Map
```typescript
import { from } from 'rxjs';

// Array — synchronous, one value per element
from([10, 20, 30]).subscribe(console.log);
// Output: 10, 20, 30

// String — each character
from('RxJS').subscribe(v => process.stdout.write(v + ' '));
// Output: R x J S

// Set — insertion order
from(new Set(['alpha', 'beta', 'gamma'])).subscribe(console.log);
// Output: alpha, beta, gamma

// Map — emits [key, value] tuples
from(new Map([['a', 1], ['b', 2]])).subscribe(([k, v]) => console.log(`${k}=${v}`));
// Output: a=1, b=2
```

### Common Pattern — Promise to Observable
```typescript
import { from } from 'rxjs';
import { switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

// Wrap a fetch() call
function getUser(id: number): Observable<User> {
  return from(
    fetch(`/api/users/${id}`).then(r => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json() as Promise<User>;
    })
  );
}

// Now it's composable with the full RxJS operator suite
getUser(42).pipe(
  catchError(err => of(null as User | null))
).subscribe(user => user ? renderProfile(user) : showNotFound());

// Within a pipe — converting per-value async operations
userIds$.pipe(
  switchMap(id => from(fetch(`/api/users/${id}`).then(r => r.json() as Promise<User>)))
).subscribe(console.log);
```

### Common Pattern — Array of HTTP Requests (flatMap pattern)
```typescript
import { from } from 'rxjs';
import { mergeMap, toArray } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const productIds = [1, 2, 3, 4, 5];

// Fetch all products concurrently, collect results
from(productIds).pipe(
  mergeMap(id => ajax.getJSON<Product>(`/api/products/${id}`)),
  toArray()
).subscribe(products => renderProductList(products));

// With concurrency limit (at most 2 requests at a time)
from(productIds).pipe(
  mergeMap(id => ajax.getJSON<Product>(`/api/products/${id}`), 2),
  toArray()
).subscribe(products => renderProductList(products));
```

### Common Pattern — Generator Functions for Lazy Sequences
```typescript
import { from } from 'rxjs';
import { map, take } from 'rxjs/operators';

// Infinite Fibonacci generator
function* fibonacci(): Generator<number> {
  let [a, b] = [0, 1];
  while (true) {
    yield a;
    [a, b] = [b, a + b];
  }
}

from(fibonacci()).pipe(take(8)).subscribe(console.log);
// Output: 0, 1, 1, 2, 3, 5, 8, 13

// Range generator (alternative to range() operator)
function* range(start: number, end: number, step = 1): Generator<number> {
  for (let i = start; i < end; i += step) yield i;
}

from(range(0, 10, 2)).subscribe(console.log);
// Output: 0, 2, 4, 6, 8
```

### Common Pattern — Async Generator / AsyncIterable
```typescript
import { from } from 'rxjs';

// Paginated API with async generator
async function* fetchAllPages(url: string): AsyncGenerator<Item[]> {
  let cursor: string | null = null;
  do {
    const response = await fetch(`${url}${cursor ? `?cursor=${cursor}` : ''}`);
    const { items, nextCursor }: { items: Item[]; nextCursor: string | null } = await response.json();
    yield items;
    cursor = nextCursor;
  } while (cursor !== null);
}

from(fetchAllPages('/api/items')).pipe(
  mergeMap(items => from(items)) // flatten pages to individual items
).subscribe(item => processItem(item));
```

### Edge Cases — Already-Observable, Empty Array, Rejected Promise
```typescript
import { from, of, EMPTY, throwError } from 'rxjs';

// Edge case 1: passing an Observable to from — passthrough (identity)
const source$ = of(1, 2, 3);
from(source$).subscribe(console.log);
// Output: 1, 2, 3 — from() mirrors the Observable

// Edge case 2: empty array — completes without emitting
from([]).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done')
});
// Output: done

// Edge case 3: rejected Promise — Observable error
from(Promise.reject(new Error('failed'))).subscribe({
  next:  v => console.log(v),
  error: e => console.log('error:', e.message)
});
// Output: error: failed

// Edge case 4: synchronous vs async — Promise is always async
let order: string[] = [];
from([1]).subscribe(v => order.push(`array:${v}`));
from(Promise.resolve(2)).subscribe(v => order.push(`promise:${v}`));
order.push('sync');
// After microtask: order = ['array:1', 'sync', 'promise:2']
// Array emits synchronously; Promise emits on microtask queue
```

## Common Pitfalls

### Anti-pattern: Confusing `from` and `of` for Single Values
```typescript
import { from, of } from 'rxjs';

// ❌ CONFUSING — from(singleValue) only works for ObservableInput types
from(42).subscribe(console.log);
// TypeError: 42 is not iterable and not a Promise/Observable

// ✅ CORRECT — of() for arbitrary static values
of(42).subscribe(console.log);
// Output: 42

// ✅ CORRECT — from() for arrays, Promises, iterables
from([42]).subscribe(console.log); // wraps 42 in an array first
// Output: 42

// from() vs of() for arrays:
from([1, 2, 3]).subscribe(console.log); // Output: 1, 2, 3 (each element)
of([1, 2, 3]).subscribe(console.log);   // Output: [1, 2, 3] (the array itself)
of(1, 2, 3).subscribe(console.log);     // Output: 1, 2, 3 (spread form)

// WHY: from() iterates its input and emits each item. of() emits its arguments
// as-is. from([1,2,3]) and of(1,2,3) produce the same output, but from([arr])
// and of(arr) are different — of(arr) emits the array as one value.
```

### Anti-pattern: Assuming `from(promise)` Is Synchronous
```typescript
import { from } from 'rxjs';

// ❌ WRONG ASSUMPTION — Promise resolution is always asynchronous
let result: number | null = null;

from(Promise.resolve(42)).subscribe(v => { result = v; });

console.log(result); // null — the Promise hasn't resolved yet!

// ✅ CORRECT — use the subscriber to access the value
from(Promise.resolve(42)).subscribe(v => {
  console.log('value:', v); // 42 — arrives asynchronously
  useValue(v);
});

// ✅ CORRECT — in async contexts, use lastValueFrom
import { lastValueFrom } from 'rxjs';
const value = await lastValueFrom(from(Promise.resolve(42)));
console.log(value); // 42

// WHY: Even Promise.resolve() (already-resolved Promises) emit asynchronously
// via the microtask queue. from(promise) never emits synchronously.
// Arrays, Sets, strings, and sync generators DO emit synchronously.
```

### Anti-pattern: Using `from` Inside a Pipe When the Operator Already Accepts ObservableInput
```typescript
import { from } from 'rxjs';
import { switchMap, mergeMap } from 'rxjs/operators';

// ❌ REDUNDANT — switchMap/mergeMap already accept ObservableInput as return values
source$.pipe(
  switchMap(v => from(fetch(`/api/data/${v}`).then(r => r.json())))
);

// ✅ CLEANER — return the Promise directly; switchMap wraps it automatically
source$.pipe(
  switchMap(v => fetch(`/api/data/${v}`).then(r => r.json()))
);

// ✅ ALSO REDUNDANT — from([items]) inside mergeMap
source$.pipe(
  mergeMap(items => from([...items]))
);
// ✅ CLEANER
source$.pipe(
  mergeMap(items => [...items]) // array is ObservableInput — accepted directly
);

// WHY: switchMap, mergeMap, concatMap, exhaustMap — all higher-order operators
// accept any ObservableInput as their return value. They internally call from()
// (or equivalent) on whatever you return. Wrapping in from() explicitly is
// redundant and adds noise without benefit.
```

## Related Operators

**Same Category (Creation)**:
- **`of(...values)`**: Emits arguments as-is (not iterated) — use for static known values; `of(arr)` emits the array as one value while `from(arr)` iterates it
- **`interval(n)`**: Repeating time-based source — use when values should arrive on a schedule
- **`timer(n)`**: One-shot delayed source
- **`defer(factory)`**: Lazy creation — use when the Observable should be freshly created on each subscription
- **`EMPTY`**: Completes immediately with no emissions — equivalent to `from([])`

**Commonly Used With**:
- **`mergeMap` / `switchMap`**: Convert each source value to an Observable (often wrapping a Promise)
- **`toArray`**: Collect all `from`-emitted items back into an array after processing
- **`take(1)` / `first()`**: Make a Promise-derived Observable usable in `forkJoin` (though Promises already complete after one value)

**Alternatives by Use Case**:

| Input Type | Instead of `from` | Use | Why |
|------------|-------------------|-----|-----|
| Single static value | `from([v])` | `of(v)` | Clearer intent |
| Multiple static values | `from([a, b, c])` | `of(a, b, c)` | No array wrapper needed |
| Promise | `from(promise)` | `from(promise)` | This IS the canonical form |
| HTTP response | `from(fetch(...))` | `ajax.getJSON(url)` | Handles errors, better TypeScript |
| Empty | `from([])` | `EMPTY` | Semantic constant |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/from](https://rxjs.dev/api/index/function/from)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/from.html](http://reactivex.io/documentation/operators/from.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/from.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/from.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Universal Observable Adapter
- **Cognitive Load**: 2/5 — The Promise async behavior, the from([arr]) vs of(arr) distinction, and the redundant-inside-operators pitfall are the teaching points
- **Usage Frequency**: 5/5 — One of the most-used creation functions; bridges Promise-based code into RxJS pipelines
- **Composability**: 5/5 — Universal entry point for any non-Observable value type

**Teaching Sequence**:
- **Prerequisites**: `of` (contrast needed), `Observable` subscription model
- **Teaches**: ObservableInput type system, sync vs async emission, Promise integration
- **Common with**: `mergeMap`, `switchMap`, `toArray`, `forkJoin`, Promises
