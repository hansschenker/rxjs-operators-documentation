# startWith

**Category**: Combination  
**Import**: `import { startWith } from 'rxjs';`

## Description

`startWith` returns an Observable that, at the moment of subscription, synchronously emits all values provided as arguments (in the order given) before subscribing to the source Observable and mirroring its emissions. The prepended values are emitted eagerly and synchronously — the subscriber receives them before any asynchronous work begins.

This operator is commonly used to provide an initial or default state for a stream, to seed `combineLatest` or `combineLatestWith` so they emit immediately without waiting for every source to produce a value, and to show loading or placeholder UI before data arrives.

## Signature

```typescript
function startWith<T, A extends readonly unknown[] = T[]>(
  ...values: A
): OperatorFunction<T, T | ValueFromArray<A>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| values | `...A` | One or more values to emit synchronously before the source is subscribed to. |

## Return Type

`OperatorFunction<T, T | ValueFromArray<A>>` — An Observable that emits the provided values first, then all values from the source.

## Marble Diagram

```
Source:     ----1----2----3--|
            startWith(0)
Output:     0---1----2----3--|
            ^-- emitted synchronously at subscription
```

## Examples

### Example 1: Providing an initial "loading" state before data arrives

```typescript
import { switchMap, startWith, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

interface State {
  loading: boolean;
  data: User[] | null;
  error: string | null;
}

const users$ = ajax.getJSON<User[]>('/api/users').pipe(
  map(data => ({ loading: false, data, error: null } as State)),
  startWith({ loading: true, data: null, error: null } as State)
);

users$.subscribe(state => {
  if (state.loading) {
    showSpinner();
  } else {
    hideSpinner();
    renderUsers(state.data!);
  }
});
```

### Example 2: Seeding combineLatestWith so it emits immediately

```typescript
import { BehaviorSubject, combineLatestWith, map, startWith } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const searchTerm$ = new BehaviorSubject<string>('');
const categoryFilter$ = new BehaviorSubject<string>('all');

// Without startWith, ajax would only fire once both BehaviorSubjects emit,
// but BehaviorSubjects emit on subscribe so this fires immediately.
// Using startWith on a regular Subject ensures the combination always has a seed.
const userTyping$ = new Subject<string>();

ajax.getJSON<Product[]>('/api/products').pipe(
  // Combine server data with an initial empty filter state
  combineLatestWith(
    userTyping$.pipe(startWith('')),      // seed the search term
    categoryFilter$
  ),
  map(([products, term, category]) =>
    products.filter(p =>
      (category === 'all' || p.category === category) &&
      p.name.toLowerCase().includes(term.toLowerCase())
    )
  )
).subscribe(filtered => renderProductList(filtered));
```

### Example 3: Announcing subscription to a long-running stream

```typescript
import { interval, map, take, startWith } from 'rxjs';

// A stock price ticker that announces itself before it starts streaming
interval(3000).pipe(
  take(5),
  map(i => ({ price: 100 + Math.random() * 10, tick: i })),
  startWith({ status: 'Connecting to price feed…' })
).subscribe(event => {
  if ('status' in event) {
    console.log(event.status);
  } else {
    console.log(`Price: $${event.price.toFixed(2)}`);
  }
});
// Connecting to price feed…
// Price: $104.32
// Price: $107.81
// ...
```

## Common Pitfalls

- **Synchronous emission**: The prepended values are emitted synchronously during subscription, before any asynchronous source values arrive. This is usually desired, but be aware that downstream operators or subscribers receive these values in the same call stack as `subscribe()`.
- **Type widening**: `startWith` widens the output type to `T | ValueFromArray<A>`. If you prepend a sentinel value of a different type (e.g. `null` or a status string), downstream code must handle the union type. Use discriminated unions or type guards to keep the code type-safe.
- **Does not replay values**: Unlike `BehaviorSubject`, `startWith` only provides the initial value at the time of subscription. Late subscribers get the `startWith` values but not any previously emitted source values. Use `shareReplay` or `BehaviorSubject` if replay semantics are needed.
- **Multiple values are emitted in order**: `startWith(a, b, c)` emits `a`, then `b`, then `c` before subscribing to the source. This differs from `of(a, b, c).pipe(concatWith(source$))` only in conciseness — the behaviour is the same.

## Related Operators

- `endWith` — appends values synchronously after the source completes
- `concatWith` — a more general way to prepend entire Observables (not just static values)
- `BehaviorSubject` — if you need late subscribers to also receive an initial value
- `finalize` — runs a callback when the source completes or errors, but does not emit values
