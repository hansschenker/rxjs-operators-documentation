# Preparing for RxJS 8: Migration Guide & Signal Interop

What's changing in RxJS 8, how to prepare your RxJS 7.x codebase today, and how to integrate with Angular Signals, React's evolving model, and the TC39 Observable proposal.

---

## What's Changing in RxJS 8

RxJS 8 focuses on three goals:
1. **Remove deprecated APIs** — operators removed in RxJS 7 with a deprecation warning are gone
2. **Improve tree-shaking** — smaller bundle sizes for apps that use few operators
3. **Better TypeScript** — stricter generics, improved inference, tighter error typing

**Not a rewrite** — the core Observable model, pipe(), and all non-deprecated operators are unchanged.

---

## Deprecated APIs to Remove Now

### `subscribe()` signature changes

```typescript
// ❌ Object-argument with error/complete positional args (RxJS 6 style):
source$.subscribe(
  value => console.log(value),
  error => console.error(error),
  () => console.log('complete')
);
// Still works in RxJS 7, gone in RxJS 8

// ✅ Observer object form:
source$.subscribe({
  next:     value => console.log(value),
  error:    error => console.error(error),
  complete: ()    => console.log('complete')
});
```

### `retryWhen` → `retry({ delay })`

```typescript
// ❌ retryWhen — removed in RxJS 8:
source$.pipe(
  retryWhen(errors =>
    errors.pipe(
      scan((n, err) => { if (n >= 3) throw err; return n + 1; }, 0),
      delayWhen(n => timer(1000 * Math.pow(2, n)))
    )
  )
)

// ✅ retry with config object (RxJS 7.4+):
source$.pipe(
  retry({
    count: 3,
    delay: (error, retryCount) => timer(1000 * Math.pow(2, retryCount))
  })
)
```

### `throwError(value)` → `throwError(() => value)`

```typescript
// ❌ throwError(error) — removed in RxJS 8:
throwError(new Error('Something went wrong'))

// ✅ throwError with factory function:
throwError(() => new Error('Something went wrong'))
// WHY: the factory form defers error creation until subscription time,
// preventing shared mutable error state
```

### `toPromise()` → `firstValueFrom` / `lastValueFrom`

```typescript
// ❌ toPromise() — removed in RxJS 8:
const value = await source$.toPromise();

// ✅ firstValueFrom or lastValueFrom:
import { firstValueFrom, lastValueFrom } from 'rxjs';

const first = await firstValueFrom(source$);
const last  = await lastValueFrom(finiteSource$);

// firstValueFrom errors on empty stream — use defaultValue option for safety:
const safe = await firstValueFrom(source$, { defaultValue: null });
```

### `combineLatest([a$, b$])` (array form stays, object form new)

```typescript
// Object form (RxJS 7+, no change in RxJS 8):
combineLatest({ users: users$, products: products$ }).subscribe(
  ({ users, products }) => render(users, products)
);
```

### `tap` side-effect observers

```typescript
// ❌ tap(nextFn, errorFn, completeFn) positional args — removed in RxJS 8:
source$.pipe(tap(v => log(v), err => logError(err), () => logDone()))

// ✅ tap with observer object:
source$.pipe(tap({
  next:     v   => log(v),
  error:    err => logError(err),
  complete: ()  => logDone()
}))
```

---

## Codemod: Automated Migration

Run the official RxJS codemod (when available) or use these manual search patterns:

```bash
# Find deprecated subscribe() positional args:
grep -rn "\.subscribe(" src/ | grep -v "subscribe({" | grep ","

# Find retryWhen usage:
grep -rn "retryWhen" src/

# Find throwError(new/:
grep -rn "throwError(" src/ | grep -v "throwError(() =>"

# Find toPromise():
grep -rn "\.toPromise()" src/
```

---

## Angular Signals + RxJS Interop

Angular 16+ introduces Signals as a synchronous reactive primitive. RxJS Observables and Signals are complementary — use both.

### `toSignal` — Observable → Signal

```typescript
import { toSignal } from '@angular/core/rxjs-interop';
import { Component, inject } from '@angular/core';

@Component({
  template: `
    <div *ngFor="let user of users()">{{ user.name }}</div>
    <span>{{ count() }}</span>
  `
})
export class UserListComponent {
  private userService = inject(UserService);

  // Observable → Signal (auto-unsubscribes with component lifecycle):
  users = toSignal(this.userService.getUsers(), { initialValue: [] });

  // With async pipe replacement:
  count = toSignal(
    this.userService.getUsers().pipe(map(u => u.length)),
    { initialValue: 0 }
  );
}
```

### `toObservable` — Signal → Observable

```typescript
import { toObservable } from '@angular/core/rxjs-interop';
import { signal, computed } from '@angular/core';

@Component({})
export class SearchComponent {
  searchQuery = signal('');

  // Signal → Observable (for debouncing, switching, etc.):
  results$ = toObservable(this.searchQuery).pipe(
    debounceTime(300),
    filter(q => q.length >= 2),
    switchMap(q => this.searchService.search(q)),
    shareReplay(1)
  );

  // Back to Signal for template:
  results = toSignal(this.results$, { initialValue: [] });
}
```

### When to Use Signals vs Observables

```typescript
// ✅ Use Signals for:
// - UI state (selected tab, toggle, form values)
// - Derived/computed values from other signals
// - Synchronous state that templates read directly
const activeTab = signal<'overview' | 'details'>('overview');
const tabTitle  = computed(() => activeTab() === 'overview' ? 'Overview' : 'Details');

// ✅ Use Observables for:
// - HTTP requests (async, one-shot or streaming)
// - WebSocket streams
// - Debounced/throttled input handling
// - Complex operator chains (switchMap, retryWhen, etc.)
// - Events that are "fire and forget" (don't need current value)
const searchResults$ = query$.pipe(debounceTime(300), switchMap(search));

// ✅ Bridge at the boundary:
// Signal for state → Observable for async effects → Signal back for template
const userId = signal<string | null>(null);

const userProfile = toSignal(
  toObservable(userId).pipe(
    filter((id): id is string => id !== null),
    switchMap(id => this.userService.getProfile(id))
  ),
  { initialValue: null }
);
```

---

## TC39 Observable Proposal

The TC39 Observable proposal (Stage 2 as of 2025) aims to standardize Observables as a built-in JavaScript type. Key differences from RxJS:

```typescript
// TC39 Observable (built-in, when available):
const obs = new Observable(subscriber => {
  subscriber.next(1);
  subscriber.next(2);
  subscriber.complete();
});

// Differences from RxJS Observable:
// 1. subscribe() returns a Subscription (same)
// 2. No pipe() method built-in — operators must be imported separately
// 3. Designed to interop with for-await-of (AsyncIterable)
// 4. Error handling via subscriber.error() (same)

// Interop: RxJS Observable ↔ TC39 Observable
// RxJS 8 will likely add Symbol.observable support for seamless interop
const rxjsObs = from(nativeObs); // wraps TC39 Observable as RxJS Observable
```

**For now (2025)**: Use RxJS. The TC39 proposal will not replace RxJS — RxJS adds the operator ecosystem that the primitive doesn't have.

---

## React: RxJS Without `async` Pipe

React has no equivalent of Angular's `async` pipe — you need custom hooks:

```typescript
import { useState, useEffect } from 'react';
import { Observable } from 'rxjs';

function useObservable<T>(obs$: Observable<T>, initial: T): T {
  const [value, setValue] = useState<T>(initial);

  useEffect(() => {
    const sub = obs$.subscribe(setValue);
    return () => sub.unsubscribe(); // cleanup on unmount
  }, [obs$]);

  return value;
}

// Usage:
function SearchResults({ query }: { query: string }) {
  const results = useObservable(
    useMemo(
      () => of(query).pipe(
        debounceTime(300),
        filter(q => q.length >= 2),
        switchMap(q => searchApi(q))
      ),
      [query]
    ),
    []
  );

  return <ul>{results.map(r => <li key={r.id}>{r.name}</li>)}</ul>;
}
```

React's upcoming Compiler (React Forget) and new concurrency model are not Observable-based — RxJS in React is a deliberate architectural choice, not the "React way."

---

## Bundle Size: RxJS 7 vs 8

RxJS 7 already has good tree-shaking via the `rxjs` entry point. RxJS 8 improves further:

```typescript
// ✅ Already optimal in RxJS 7 — import from 'rxjs' and 'rxjs/operators':
import { Observable, Subject, BehaviorSubject } from 'rxjs';
import { map, filter, switchMap } from 'rxjs/operators';

// ❌ Don't import from deep paths (only works in RxJS 6):
import { map } from 'rxjs/internal/operators/map'; // breaks in RxJS 7+
```

**Bundle size estimates** (gzipped):
- Full RxJS 7 import (everything): ~45KB
- Typical app using 20 operators: ~12KB
- Minimal app (from, map, switchMap): ~4KB

---

## Upgrade Checklist: RxJS 7 → 8

```typescript
// 1. Fix subscribe() positional args:
//    .subscribe(fn, fn, fn) → .subscribe({ next: fn, error: fn, complete: fn })

// 2. Fix throwError():
//    throwError(err) → throwError(() => err)

// 3. Replace retryWhen:
//    retryWhen(errors => errors.pipe(...)) → retry({ count, delay })

// 4. Replace toPromise():
//    .toPromise() → firstValueFrom() or lastValueFrom()

// 5. Check for removed operators (already removed in RxJS 7):
//    combineAll → combineLatestAll
//    mergeMapTo  → mergeMap(() => source)
//    switchMapTo → switchMap(() => source)
//    concatMapTo → concatMap(() => source)
//    pluck       → map(obj => obj.key)
//    partition (pipeable) → filter + filter (or partition creation)

// 6. Verify tap() usage:
//    tap(fn, fn, fn) → tap({ next: fn, error: fn, complete: fn })
```

---

## Pattern: `takeUntilDestroyed` (Angular 16+)

Replace the manual `takeUntil(destroy$)` pattern:

```typescript
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DestroyRef, inject } from '@angular/core';

// Old pattern (still valid):
@Component({})
class OldComponent implements OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    dataStream$.pipe(takeUntil(this.destroy$)).subscribe(render);
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

// New pattern (Angular 16+):
@Component({})
class NewComponent {
  constructor() {
    dataStream$.pipe(
      takeUntilDestroyed() // automatically uses injected DestroyRef
    ).subscribe(render);
  }
}

// Outside injection context (e.g., factory function):
@Injectable()
class MyService {
  private destroyRef = inject(DestroyRef);

  getData() {
    return dataStream$.pipe(
      takeUntilDestroyed(this.destroyRef) // explicit DestroyRef
    );
  }
}
```

---

## Common Pitfalls When Migrating

### `firstValueFrom` on Never-Completing Streams

```typescript
// ❌ firstValueFrom on a BehaviorSubject — works, but misleading:
const value = await firstValueFrom(behaviorSubject$);
// Resolves immediately with current value — but subscription is created+destroyed each call
// Use getValue() instead for synchronous access

// ✅ BehaviorSubject.getValue() for synchronous reads:
const value = behaviorSubject.getValue();

// ✅ firstValueFrom for streams that will eventually emit:
const user = await firstValueFrom(this.userService.getUser(id));
```

### Mixing Signal and Observable Subscriptions in the Same Component

```typescript
// ❌ Manual subscription + toSignal — creates two subscriptions:
@Component({})
class BadComponent {
  data = toSignal(this.data$, { initialValue: null });

  constructor() {
    this.data$.subscribe(d => this.sideEffect(d)); // ← second subscription!
  }
}

// ✅ Use one source, branch with tap:
@Component({})
class GoodComponent {
  private data$ = this.service.getData().pipe(
    tap(d => this.sideEffect(d)), // side-effect inline
    shareReplay(1)
  );
  data = toSignal(this.data$, { initialValue: null });
}
```
