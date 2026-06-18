# distinctUntilKeyChanged — Advanced Patterns

For fundamentals see the core [distinctUntilKeyChanged](./distinctUntilKeyChanged) doc. This page covers composite key strategies, deep equality comparators, form field change detection, and the comparison table with `distinctUntilChanged` and `distinct`.

---

## Mental Model

```typescript
import { distinctUntilKeyChanged } from 'rxjs/operators';

// distinctUntilKeyChanged(key) — suppress consecutive identical values of obj[key]:
users$.pipe(
  distinctUntilKeyChanged('status')
  // Emits only when user.status changes from previous emission
)

// distinctUntilKeyChanged(key, comparator) — custom equality for obj[key]:
users$.pipe(
  distinctUntilKeyChanged('role', (prev, curr) => prev.id === curr.id)
  // Emits when user.role.id changes (deep comparison)
)

// Equivalent to:
source$.pipe(
  distinctUntilChanged((prev, curr) => prev[key] === curr[key])
)
// distinctUntilKeyChanged is just syntactic sugar — same semantics
```

**Key distinction from `distinct`**: `distinctUntilKeyChanged` only suppresses *consecutive* repetitions of the same key value. The same key value can re-appear after a different value:

```typescript
of(
  { status: 'loading' },
  { status: 'loading' }, // ← suppressed
  { status: 'error'   },
  { status: 'loading' }  // ← emits (different from previous 'error')
).pipe(distinctUntilKeyChanged('status')).subscribe(console.log);
// { status: 'loading' }, { status: 'error' }, { status: 'loading' }
```

---

## Pattern 1: UI State Change Detection

Trigger re-renders only when specific state slices change:

```typescript
import { distinctUntilKeyChanged, map } from 'rxjs/operators';

interface AppState {
  user: { id: string; name: string; avatar: string };
  cart: { items: CartItem[]; total: number };
  theme: 'light' | 'dark';
  language: string;
}

const state$ = store.select<AppState>(state => state);

// Render user avatar only when avatar URL changes:
state$.pipe(
  map(s => s.user),
  distinctUntilKeyChanged('avatar')
).subscribe(user => updateAvatar(user.avatar));

// Update cart badge only when total changes:
state$.pipe(
  map(s => s.cart),
  distinctUntilKeyChanged('total')
).subscribe(cart => updateCartBadge(cart.total));

// Switch theme only when theme preference changes:
state$.pipe(
  distinctUntilKeyChanged('theme')
).subscribe(state => applyTheme(state.theme));
```

---

## Pattern 2: Form Field Dirty Detection

Track individual form field changes without re-processing unchanged fields:

```typescript
import { distinctUntilKeyChanged, pairwise, filter, map } from 'rxjs/operators';

interface OrderForm {
  customerId:  string;
  productId:   string;
  quantity:    number;
  discount:    number;
  shippingAddr: string;
}

const formValues$ = form.valueChanges as Observable<OrderForm>;

// Recalculate price only when pricing-relevant fields change:
formValues$.pipe(
  distinctUntilKeyChanged('productId')
).pipe(
  switchMap(form => priceService.getPrice(form.productId, form.quantity))
).subscribe(price => updatePriceDisplay(price));

// Validate shipping address only when it changes:
formValues$.pipe(
  distinctUntilKeyChanged('shippingAddr'),
  debounceTime(500)
).subscribe(form => validateShippingAddress(form.shippingAddr));

// Track which fields changed between consecutive values:
formValues$.pipe(
  pairwise(),
  map(([prev, curr]) => {
    const changedFields = (Object.keys(curr) as (keyof OrderForm)[])
      .filter(key => prev[key] !== curr[key]);
    return { prev, curr, changedFields };
  }),
  filter(({ changedFields }) => changedFields.length > 0)
).subscribe(({ changedFields }) => {
  console.log('Changed:', changedFields);
  markFieldsDirty(changedFields);
});
```

---

## Pattern 3: Custom Comparator for Deep Equality

Use a comparator when the key value is an object or requires semantic equality:

```typescript
import { distinctUntilKeyChanged } from 'rxjs/operators';

interface User { id: string; role: { id: string; name: string; permissions: string[] }; }

// Shallow === on objects always changes (different references):
// ❌ Without comparator — emits every time even if role content is the same:
users$.pipe(distinctUntilKeyChanged('role'))
// role is an object — reference changes on every emission even if content is identical

// ✅ Custom comparator for role ID equality:
users$.pipe(
  distinctUntilKeyChanged('role', (prev, curr) => prev.id === curr.id)
)

// Deep equality using JSON.stringify (safe for plain objects without methods):
users$.pipe(
  distinctUntilKeyChanged('permissions', (prev, curr) =>
    JSON.stringify([...prev].sort()) === JSON.stringify([...curr].sort())
  )
)

// Array-content equality:
users$.pipe(
  distinctUntilKeyChanged('tags', (prev: string[], curr: string[]) =>
    prev.length === curr.length &&
    prev.every((tag, i) => tag === curr[i])
  )
)
```

---

## Pattern 4: Stacked Key Filters (Multiple Key Change Detection)

Chain multiple `distinctUntilKeyChanged` calls or use `distinctUntilChanged` with multi-key logic:

```typescript
import { distinctUntilKeyChanged, distinctUntilChanged } from 'rxjs/operators';

interface QueryState {
  searchTerm: string;
  sortBy:     'name' | 'date' | 'relevance';
  pageSize:   number;
  filters:    string[];
}

// Option A: chain distinct calls (each key change triggers new emission):
query$.pipe(
  distinctUntilKeyChanged('searchTerm'),
  distinctUntilKeyChanged('sortBy')
  // Emits when either searchTerm OR sortBy changes
  // But loses changes to the OTHER key if one is unchanged
)
// ⚠️ Chaining distinct calls is rarely what you want — it filters to
//    changes that differ on EVERY chained key

// ✅ Option B: distinctUntilChanged with composite key:
query$.pipe(
  distinctUntilChanged((prev, curr) =>
    prev.searchTerm === curr.searchTerm &&
    prev.sortBy     === curr.sortBy     &&
    prev.pageSize   === curr.pageSize
    // filters not included — changes to filters don't re-query
  )
).subscribe(q => executeQuery(q));

// ✅ Option C: pick + distinctUntilChanged for selective tracking:
query$.pipe(
  map(q => ({ term: q.searchTerm, sort: q.sortBy })),
  distinctUntilChanged((prev, curr) =>
    prev.term === curr.term && prev.sort === curr.sort
  ),
  withLatestFrom(query$), // get full state for the query
  map(([, fullQuery]) => fullQuery)
).subscribe(q => executeQuery(q));
```

---

## Pattern 5: Route Parameter Change Detection

React to specific route parameter changes without full route re-initialization:

```typescript
import { ActivatedRoute } from '@angular/router';
import { distinctUntilKeyChanged, filter, switchMap } from 'rxjs/operators';

@Component({})
export class ProductDetailComponent {
  private route = inject(ActivatedRoute);

  product$ = this.route.params.pipe(
    distinctUntilKeyChanged('id'),          // only when product ID changes
    switchMap(params => this.productService.getProduct(params['id'])),
    shareReplay(1)
  );

  reviews$ = this.route.params.pipe(
    distinctUntilKeyChanged('id'),
    switchMap(params => this.reviewService.getReviews(params['id']))
  );

  // Tab changes (query params) don't reload product:
  activeTab$ = this.route.queryParams.pipe(
    distinctUntilKeyChanged('tab'),
    map(params => params['tab'] ?? 'overview')
  );
}
```

---

## Pattern 6: Sensor / Telemetry Deduplication

Suppress redundant sensor readings where only specific channels matter:

```typescript
import { distinctUntilKeyChanged, filter, map } from 'rxjs/operators';

interface SensorReading {
  deviceId:    string;
  temperature: number;
  humidity:    number;
  pressure:    number;
  timestamp:   number;
}

const sensorStream$ = webSocket$<SensorReading>('wss://sensors.internal/ws');

// Alert on temperature spikes — don't alert on humidity/pressure noise:
sensorStream$.pipe(
  distinctUntilKeyChanged('temperature'),
  filter(r => r.temperature > 80),
  throttleTime(60_000) // max 1 alert per minute per sensor
).subscribe(r => sendThermalAlert(r));

// Log humidity — only when it changes:
sensorStream$.pipe(
  distinctUntilKeyChanged('humidity', (prev, curr) =>
    Math.abs(prev - curr) < 2 // ignore sub-2% changes
  )
).subscribe(r => logHumidityChange(r));

// Dashboard update — suppress if all three values unchanged:
sensorStream$.pipe(
  distinctUntilChanged((prev, curr) =>
    Math.abs(prev.temperature - curr.temperature) < 0.5 &&
    Math.abs(prev.humidity    - curr.humidity)    < 1   &&
    Math.abs(prev.pressure    - curr.pressure)    < 0.1
  )
).subscribe(r => updateDashboard(r));
```

---

## `distinctUntilKeyChanged` vs `distinctUntilChanged` vs `distinct`

```typescript
// distinctUntilKeyChanged('key')
// — "suppress if obj.key === previous obj.key"
// — Most ergonomic for single-property state objects
// — Uses === by default; accepts custom comparator
users$.pipe(distinctUntilKeyChanged('status'))

// distinctUntilChanged((a, b) => a.status === b.status)
// — Identical behavior to above but more verbose
// — Useful for multi-key conditions or complex comparisons
users$.pipe(distinctUntilChanged((a, b) => a.status === b.status))

// distinctUntilChanged()
// — Uses === on the whole value (reference equality)
// — Works for primitives or immutable objects with stable references
statusStream$.pipe(distinctUntilChanged()) // only for string/number/boolean streams

// distinct(u => u.status)
// — "suppress if status has EVER appeared before in this stream"
// — Memory grows; requires flushes$ for long-running streams
// — Use for "process each unique status exactly once" not "suppress repeats"
users$.pipe(distinct(u => u.status))
```

---

## Common Pitfalls

### Using `distinctUntilKeyChanged` on Nested Objects

```typescript
interface State { config: { timeout: number; retries: number } }

// ❌ config is an object — === always false even if content identical:
state$.pipe(distinctUntilKeyChanged('config'))
// Every emission passes through because { timeout: 3 } !== { timeout: 3 }

// ✅ Use custom comparator:
state$.pipe(
  distinctUntilKeyChanged('config', (prev, curr) =>
    prev.timeout === curr.timeout &&
    prev.retries === curr.retries
  )
)
```

### Chaining for Unrelated Keys (AND logic, not OR)

```typescript
// ❌ Chaining gives AND semantics (both must be consecutive-same to suppress):
state$.pipe(
  distinctUntilKeyChanged('userId'),
  distinctUntilKeyChanged('role')
)
// Only suppresses when BOTH userId AND role are same as previous
// What you likely wanted: suppress when EITHER is same — use distinctUntilChanged with multi-key condition

// ✅ Use distinctUntilChanged for OR logic:
state$.pipe(
  distinctUntilChanged((prev, curr) =>
    prev.userId === curr.userId || prev.role === curr.role
  )
)
```

### Forgetting That `null` and `undefined` Are Valid Key Values

```typescript
interface Item { id: string; parentId: string | null }

// ❌ null comparison works with === so this is fine, but may surprise:
items$.pipe(distinctUntilKeyChanged('parentId'))
// null === null → suppressed (correct!)
// undefined === undefined → suppressed (correct!)
// null !== undefined → emits (be aware of this edge)

// ✅ Make the comparison explicit if null/undefined handling matters:
items$.pipe(
  distinctUntilKeyChanged('parentId', (prev, curr) =>
    (prev ?? null) === (curr ?? null) // treat null and undefined as equal
  )
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `distinctUntilKeyChanged` is `distinctUntilChanged` with ergonomic syntax for single-key property comparison. For object-valued keys, always provide a custom comparator — the default `===` will never suppress object values. For multi-key conditions, step up to `distinctUntilChanged` with an explicit predicate.
