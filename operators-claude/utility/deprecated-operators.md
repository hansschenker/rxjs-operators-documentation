# Deprecated Operators

These operators are deprecated in RxJS 7 and will be removed in RxJS 8. Each entry explains the reason and the correct modern replacement.

---

## `mapTo(value)` → `map(() => value)`

Maps every emission to the same constant value.

**Status**: Deprecated in RxJS 7.4. Removed in RxJS 8.

```typescript
// ❌ DEPRECATED
import { mapTo } from 'rxjs/operators';
clicks$.pipe(mapTo('click!'))

// ✅ REPLACEMENT
import { map } from 'rxjs/operators';
clicks$.pipe(map(() => 'click!'))
// WHY: map(() => value) is equally readable and avoids a separate import.
// The single-purpose mapTo() doesn't justify its API surface.
```

---

## `pluck(...keys)` → `map(obj => obj?.key?.nested)`

Extracts a nested property from each emission by key path.

**Status**: Deprecated in RxJS 7.4. Removed in RxJS 8.

```typescript
// ❌ DEPRECATED
import { pluck } from 'rxjs/operators';
users$.pipe(pluck('address', 'city'))

// ✅ REPLACEMENT
import { map } from 'rxjs/operators';
users$.pipe(map(user => user?.address?.city))
// WHY: pluck() is not type-safe — it returns Observable<unknown>.
// Optional chaining (user?.address?.city) is typed correctly and
// handles nullish values without special-casing.
```

---

## `publish()` → `connectable()` or `share()`

Creates a ConnectableObservable for multicasting.

**Status**: Deprecated in RxJS 7.0.

```typescript
// ❌ DEPRECATED
import { publish } from 'rxjs/operators';
const hot$ = source$.pipe(publish());
hot$.connect();

// ✅ REPLACEMENT
import { connectable } from 'rxjs';
const hot$ = connectable(source$);
hot$.connect();

// Or for ref-counted multicasting:
source$.pipe(share())
```

---

## `multicast(subject)` → `connectable()` or `connect()`

Multicasts through a provided Subject.

**Status**: Deprecated in RxJS 7.0.

```typescript
// ❌ DEPRECATED
import { multicast } from 'rxjs/operators';
source$.pipe(multicast(new Subject())).connect()

// ✅ REPLACEMENT — manual connect
import { connectable, Subject } from 'rxjs';
connectable(source$, { connector: () => new Subject() }).connect()

// ✅ REPLACEMENT — pipeable graph fork
import { connect } from 'rxjs/operators';
source$.pipe(
  connect(shared$ => merge(shared$.pipe(mapA), shared$.pipe(mapB)))
)
```

---

## `refCount()` → `share()`

Auto-connects when first subscriber arrives, disconnects on last unsubscribe.

**Status**: Deprecated in RxJS 7.0 (used after `publish()` or `multicast()`).

```typescript
// ❌ DEPRECATED
source$.pipe(publish(), refCount())

// ✅ REPLACEMENT
source$.pipe(share())
// share() = publish() + refCount() as a single composable operator
```

---

## `tap(nextFn, errorFn, completeFn)` — positional observer form

The three-argument signature of `tap` is deprecated.

**Status**: Deprecated in RxJS 7.4.

```typescript
// ❌ DEPRECATED — positional arguments
source$.pipe(
  tap(
    v   => console.log('next:', v),
    e   => console.error('error:', e),
    ()  => console.log('complete')
  )
)

// ✅ REPLACEMENT — observer object form
source$.pipe(
  tap({
    next:     v  => console.log('next:', v),
    error:    e  => console.error('error:', e),
    complete: () => console.log('complete')
  })
)
```

---

## `combineLatest([obs1, obs2])` — array overload

The array-argument form of `combineLatest` is deprecated.

**Status**: Deprecated in RxJS 7.0. Only the spread-args form remains.

```typescript
// ❌ DEPRECATED — array argument
combineLatest([a$, b$, c$])

// ✅ REPLACEMENT — spread or dictionary
combineLatest([a$, b$, c$])   // still works but prefer named form:
combineLatest({ a: a$, b: b$, c: c$ })
// WHY: dictionary form gives named keys in the result instead of index-based
// destructuring — far more readable with 3+ sources.
```

---

## `pairs(obj)` → `from(Object.entries(obj))`

Emits `[key, value]` pairs from an object.

**Status**: Deprecated in RxJS 7.0.

```typescript
// ❌ DEPRECATED
import { pairs } from 'rxjs';
pairs({ a: 1, b: 2 }).subscribe(([key, val]) => console.log(key, val));

// ✅ REPLACEMENT
import { from } from 'rxjs';
from(Object.entries({ a: 1, b: 2 }))
  .subscribe(([key, val]) => console.log(key, val));
```

---

## `toPromise()` → `firstValueFrom()` / `lastValueFrom()`

Converts an Observable to a Promise.

**Status**: Deprecated in RxJS 7.0.

```typescript
// ❌ DEPRECATED
await source$.toPromise() // returns last value or undefined on empty

// ✅ REPLACEMENT
import { firstValueFrom, lastValueFrom } from 'rxjs';
await firstValueFrom(source$) // throws EmptyError if no values
await lastValueFrom(source$)  // waits for completion, takes last
// WHY: toPromise() returns undefined on empty streams — a silent failure.
// firstValueFrom/lastValueFrom throw explicitly on empty, which is safer.
```

---

## Summary Table

| Deprecated | Replacement | Since |
|---|---|---|
| `mapTo(v)` | `map(() => v)` | 7.4 |
| `pluck('a', 'b')` | `map(x => x?.a?.b)` | 7.4 |
| `publish()` | `connectable()` | 7.0 |
| `multicast(subj)` | `connectable()` / `connect()` | 7.0 |
| `refCount()` | `share()` | 7.0 |
| `tap(fn, fn, fn)` | `tap({ next, error, complete })` | 7.4 |
| `pairs(obj)` | `from(Object.entries(obj))` | 7.0 |
| `toPromise()` | `firstValueFrom()` / `lastValueFrom()` | 7.0 |
