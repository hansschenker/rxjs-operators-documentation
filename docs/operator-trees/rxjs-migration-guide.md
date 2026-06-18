# RxJS Migration Guide

## v5 → v6: The `pipe()` Migration

RxJS 6 moved from chained operators (`observable.map(...).filter(...)`) to the pipeable `pipe()` API.

### Import Changes

```typescript
// ❌ RxJS 5 — operators imported from 'rxjs/add/operator/*'
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/filter';
observable.map(x => x * 2).filter(x => x > 5).subscribe();

// ✅ RxJS 6+ — standalone functions piped together
import { map, filter } from 'rxjs/operators';
observable.pipe(
  map(x => x * 2),
  filter(x => x > 5)
).subscribe();
```

### Creation Operator Imports

```typescript
// ❌ RxJS 5
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import 'rxjs/add/observable/of';
import 'rxjs/add/observable/from';

// ✅ RxJS 6+
import { Observable, Subject, of, from } from 'rxjs';
import { map, filter, mergeMap } from 'rxjs/operators';
```

### Automated Migration

The RxJS team provided a codemod for v5→v6:
```bash
npx rxjs-tslint-rules
# or
npx rxjs-5-to-6-migrate -p tsconfig.json
```

---

## v6 → v7: Key Breaking Changes and Deprecations

### 1. `toPromise()` → `firstValueFrom` / `lastValueFrom`

```typescript
// ❌ v6 — toPromise() returns undefined on empty streams
const value = await observable$.toPromise();

// ✅ v7 — explicit empty handling
import { firstValueFrom, lastValueFrom } from 'rxjs';
const value = await firstValueFrom(observable$);  // throws EmptyError if empty
const last  = await lastValueFrom(observable$);   // waits for completion
// With default: await firstValueFrom(obs$, { defaultValue: null })
```

### 2. `publish()` / `multicast()` → `connectable()` / `share()` / `connect()`

```typescript
// ❌ v6 deprecated
source$.pipe(publish()).connect()
source$.pipe(multicast(new Subject()), refCount())

// ✅ v7
import { connectable } from 'rxjs';
import { share, connect } from 'rxjs/operators';

connectable(source$).connect()          // manual connect
source$.pipe(share())                   // auto ref-count (replaces publish+refCount)
source$.pipe(connect(s$ => merge(...))) // pipeable branching
```

### 3. `shareReplay` Behavior Change

```typescript
// v6 — shareReplay(1) kept source subscribed with 0 subscribers (leak risk)
source$.pipe(shareReplay(1)) // refCount: false was the default

// v7 — shareReplay(1) now uses refCount: true by default (auto-disconnect)
source$.pipe(shareReplay(1)) // safe — disconnects when all unsubscribe

// To keep v6 behavior explicitly (permanent cache):
source$.pipe(shareReplay({ bufferSize: 1, refCount: false }))
```

### 4. `combineLatest([])` → Spread or Dictionary

```typescript
// ❌ v6 array form (still works but dictionary is preferred)
combineLatest([a$, b$, c$]).subscribe(([a, b, c]) => ...)

// ✅ v7 dictionary form (named keys, self-documenting)
combineLatest({ a: a$, b: b$, c: c$ }).subscribe(({ a, b, c }) => ...)
```

### 5. Operator Deprecations in v7.4

```typescript
// mapTo(value)  →  map(() => value)
// pluck('a', 'b')  →  map(x => x?.a?.b)
// tap(fn, fn, fn)  →  tap({ next, error, complete })
// pairs(obj)  →  from(Object.entries(obj))
```

### 6. Error Handling: `throwError` Signature

```typescript
// ❌ v6 — factory function not required
throwError(new Error('oops'))

// ✅ v7 — factory function required (avoids eager construction)
throwError(() => new Error('oops'))
```

### 7. `of(undefined)` Emits `undefined`

```typescript
// v6: of() with no args emitted nothing (complete immediately)
// v7: of() emits nothing; of(undefined) emits undefined then completes
// Behavior: of() → |, of(undefined) → (undefined|)
// Previously undefined was silently dropped in some cases — now explicit
```

### 8. `Subject` Error Propagation

```typescript
// v7: A Subject that has errored throws on new .next() calls
// v6: calling .next() on an errored Subject was silently ignored
const subject = new Subject();
subject.error(new Error('oops'));
subject.next(1); // v7: throws! v6: silently ignored
```

---

## Import Path Consolidation (v7)

RxJS 7 consolidated many previously separate import paths:

```typescript
// ✅ v7 — everything from root or 'rxjs/operators'
import {
  Observable, Subject, BehaviorSubject, ReplaySubject, AsyncSubject,
  of, from, fromEvent, interval, timer, EMPTY, NEVER,
  combineLatest, merge, concat, zip, race, forkJoin,
  firstValueFrom, lastValueFrom, connectable,
  asyncScheduler, queueScheduler, asapScheduler, animationFrameScheduler
} from 'rxjs';

import {
  map, filter, mergeMap, switchMap, concatMap, exhaustMap,
  tap, catchError, retry, timeout, share, shareReplay,
  debounceTime, throttleTime, distinctUntilChanged, takeUntil,
  // ... all pipeable operators
} from 'rxjs/operators';

// Specialized imports still use sub-paths:
import { webSocket }    from 'rxjs/webSocket';
import { ajax }         from 'rxjs/ajax';
import { TestScheduler } from 'rxjs/testing';
```

---

## v7 → v8: What to Expect (Preview)

RxJS 8 will remove operators deprecated in v7. Prepare by:

1. Replacing `mapTo` with `map(() => value)`
2. Replacing `pluck` with `map(x => x?.key)`
3. Replacing `publish()` with `connectable()` or `share()`
4. Replacing `multicast` with `connectable()` or `connect()`
5. Replacing `toPromise()` with `firstValueFrom`/`lastValueFrom`

Run this to find deprecated usage:
```bash
# Install rxjs-tslint-rules or use eslint-plugin-rxjs:
npx eslint --rule '{"rxjs/no-deprecated": "error"}' src/**/*.ts
```

---

## Common Migration Mistakes

### Forgetting `pipe()` Wrapping

```typescript
// ❌ v5 style — operators called directly on Observable
source$.map(x => x * 2).filter(x => x > 5)

// ✅ v6+ — all operators inside pipe()
source$.pipe(
  map(x => x * 2),
  filter(x => x > 5)
)
```

### Wrong Import Path for Operators

```typescript
// ❌ v5 style (operators from separate files):
import { map } from 'rxjs/operator/map';

// ❌ Incorrect v7:
import { map } from 'rxjs';  // map is NOT in root

// ✅ Correct v7:
import { map } from 'rxjs/operators';  // operators in 'rxjs/operators'
import { of }  from 'rxjs';            // creation in root
```

### `shareReplay` Default Behavior Difference

```typescript
// If upgrading from v6 and relying on permanent-cache behavior,
// you MUST explicitly set refCount: false in v7:
source$.pipe(shareReplay({ bufferSize: 1, refCount: false }))
// Otherwise v7's new default (refCount: true) will disconnect on 0 subscribers
```
