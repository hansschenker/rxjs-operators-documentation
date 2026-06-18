# combineLatestWith

**Category**: Combination  
**Import**: `import { combineLatestWith } from 'rxjs';`

## Description

`combineLatestWith` is the pipeable equivalent of the `combineLatest` creation operator. It subscribes to the source Observable and all provided Observable inputs simultaneously. Once every participating Observable has emitted at least one value, it emits an array containing the latest value from each source. After that, any time any source emits a new value, a new array of the latest values is emitted.

Unlike `withLatestFrom`, which only emits when the primary (source) Observable emits, `combineLatestWith` emits whenever any of the combined sources emit. This makes it suitable for eagerly recalculating derived state whenever any input changes.

## Signature

```typescript
function combineLatestWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, Cons<T, A>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| otherSources | `...ObservableInputTuple<A>` | One or more Observable inputs to combine with the source. |

## Return Type

`OperatorFunction<T, Cons<T, A>>` — Emits a tuple `[T, ...A]` containing the latest value from the source followed by the latest value from each additional source, in the order they were provided.

## Marble Diagram

```
Source A: --1-----3-----|
Source B: -----2-----4--|
          combineLatestWith(B)
Output:   -----[1,2]-[3,2]-[3,4]--|
          (first emit after both A and B have emitted at least once)
```

## Examples

### Example 1: Combining two form inputs to produce a live preview

```typescript
import { fromEvent, map, combineLatestWith } from 'rxjs';

const firstName = document.getElementById('first-name') as HTMLInputElement;
const lastName = document.getElementById('last-name') as HTMLInputElement;

const firstName$ = fromEvent(firstName, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);
const lastName$ = fromEvent(lastName, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);

firstName$.pipe(
  combineLatestWith(lastName$),
  map(([first, last]) => `${first} ${last}`.trim())
).subscribe(fullName => {
  document.getElementById('preview')!.textContent = fullName;
});
```

### Example 2: Recalculating a price whenever quantity or discount changes

```typescript
import { BehaviorSubject, combineLatestWith, map } from 'rxjs';

const quantity$ = new BehaviorSubject<number>(1);
const discountPercent$ = new BehaviorSubject<number>(0);
const unitPrice$ = new BehaviorSubject<number>(29.99);

unitPrice$.pipe(
  combineLatestWith(quantity$, discountPercent$),
  map(([price, qty, discount]) => {
    const subtotal = price * qty;
    return subtotal * (1 - discount / 100);
  })
).subscribe(total => {
  console.log(`Total: $${total.toFixed(2)}`);
});

// Any change to price, quantity, or discount recalculates the total
quantity$.next(3);       // Total: $89.97
discountPercent$.next(10); // Total: $80.97
```

### Example 3: Combining server data with local filter state

```typescript
import { interval, BehaviorSubject, combineLatestWith, map, switchMap } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const filterCategory$ = new BehaviorSubject<string>('all');
const refreshTick$ = interval(30_000); // Refresh data every 30 seconds

filterCategory$.pipe(
  combineLatestWith(refreshTick$),
  switchMap(([category]) =>
    ajax.getJSON<Product[]>(`/api/products?category=${category}`)
  )
).subscribe(products => renderProductList(products));

// Changing the filter immediately re-fetches with the new category
filterCategory$.next('electronics');
```

## Common Pitfalls

- **Waits for all sources to emit before producing output**: If any source never emits (e.g. a Subject that has had no values pushed to it yet), `combineLatestWith` will produce no output at all. Use `startWith` to give sources an initial value if needed.
- **Emits on every source change, not just the primary**: If you only want to emit when the primary (piped) source emits and merely want the latest value from other sources, use `withLatestFrom` instead.
- **Completed sources still contribute their last value**: If one of the combined sources completes, `combineLatestWith` continues to use its last emitted value for future combinations. Output only completes when all combined sources complete.
- **Tuple type grows with each additional source**: The emitted tuple has a type element for each source. For more than a handful of sources this can become unwieldy; consider structuring the combined state as a plain object with a `map` after the operator.

## Related Operators

- `withLatestFrom` — only emits when the source emits; other sources merely supply their latest cached value
- `combineLatest` — creation operator equivalent; takes a static array of Observables
- `combineLatestAll` — for dynamically produced sets of inner Observables from a higher-order source
- `zipWith` — combines values by index rather than by latest value
