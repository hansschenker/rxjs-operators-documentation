# Angular Signals ↔ RxJS Interop

Angular 16+ introduced Signals as a synchronous reactive primitive. `@angular/core/rxjs-interop` provides the bridge between Signals and Observables.

---

## The Two Worlds

| | RxJS Observable | Angular Signal |
|---|---|---|
| Push vs pull | Push (producer drives) | Pull (consumer reads) |
| Async capable | Yes | Synchronous only |
| Lazy | Yes (cold) | Always computed |
| Completion | Can complete or error | Never completes |
| Change detection | Manual (`async` pipe) | Automatic (fine-grained) |
| Operators | Full RxJS pipeline | `computed()`, `effect()` |

**When to use each**:
- **Observable**: async operations (HTTP, WebSocket, timers), complex transformations, existing RxJS infrastructure
- **Signal**: synchronous UI state, derived values, Angular component state

---

## `toSignal()` — Observable → Signal

```typescript
import { toSignal } from '@angular/core/rxjs-interop';
import { Component, inject } from '@angular/core';

@Component({
  template: `
    <!-- No async pipe needed — signal is synchronous -->
    <div>{{ user()?.name }}</div>
    <ul>
      <li *ngFor="let item of items()">{{ item.name }}</li>
    </ul>
  `
})
export class MyComponent {
  private userService = inject(UserService);

  // Observable → Signal (auto-unsubscribes on component destroy)
  user  = toSignal(this.userService.currentUser$);
  items = toSignal(this.userService.items$, { initialValue: [] });
  //                                          ^ required if Observable may not emit synchronously
}
```

### `toSignal` Options

```typescript
// initialValue — value before first emission
const count = toSignal(counter$, { initialValue: 0 });

// requireSync — throw if Observable doesn't emit synchronously
// (use for BehaviorSubject and synchronous sources)
const config = toSignal(behaviorSubject$, { requireSync: true });
// No undefined | T union — type is just T

// injector — use outside injection context
const injector = inject(Injector);
const data = toSignal(data$, { injector });
```

---

## `toObservable()` — Signal → Observable

```typescript
import { toObservable } from '@angular/core/rxjs-interop';
import { signal, computed } from '@angular/core';

@Component({ ... })
export class SearchComponent {
  searchQuery = signal('');
  minLength   = signal(2);

  // Signal → Observable (emits synchronously on subscribe, then on each change)
  searchQuery$ = toObservable(this.searchQuery);

  // Compose with RxJS operators:
  results$ = this.searchQuery$.pipe(
    filter(q => q.length >= this.minLength()),  // read other signal in pipe
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(q => this.api.search(q)),
    catchError(() => of([]))
  );

  // Back to signal for template:
  results = toSignal(this.results$, { initialValue: [] });
}
```

**Important**: `toObservable(signal)` emits:
1. The current signal value synchronously on subscription
2. Every subsequent signal change asynchronously (via microtask)

---

## Pattern 1: Form Input with Signal + Observable Pipeline

```typescript
import { signal } from '@angular/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';

@Component({
  template: `
    <input [value]="query()" (input)="query.set($event.target.value)" />
    <div *ngFor="let r of results()">{{ r.name }}</div>
  `
})
export class SearchComponent {
  query   = signal('');
  results = toSignal(
    toObservable(this.query).pipe(
      debounceTime(300),
      distinctUntilChanged(),
      filter(q => q.length > 1),
      switchMap(q => this.api.search(q).pipe(catchError(() => of([])))),
    ),
    { initialValue: [] }
  );
}
```

---

## Pattern 2: Route Params as Signal

```typescript
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute } from '@angular/router';
import { map } from 'rxjs/operators';

@Component({ ... })
export class ItemDetailComponent {
  private route = inject(ActivatedRoute);

  itemId = toSignal(
    this.route.paramMap.pipe(map(p => p.get('id'))),
    { requireSync: false }
  );

  item = toSignal(
    toObservable(this.itemId).pipe(
      filter(Boolean),
      distinctUntilChanged(),
      switchMap(id => this.api.getItem(id))
    ),
    { initialValue: null }
  );
}
```

---

## Pattern 3: Combining Signals and Observables

```typescript
import { signal, computed } from '@angular/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';

@Component({ ... })
export class DashboardComponent {
  // Pure signal state:
  selectedTab   = signal<'all' | 'active' | 'done'>('all');
  sortDirection = signal<'asc' | 'desc'>('asc');

  // Derived signal (synchronous, no Observable needed):
  isAscending = computed(() => this.sortDirection() === 'asc');

  // Observable for async data:
  private allItems$ = this.itemService.items$.pipe(shareReplay(1));

  // Combine signal filter with Observable data:
  filteredItems = toSignal(
    combineLatest({
      items: this.allItems$,
      tab:   toObservable(this.selectedTab),
      sort:  toObservable(this.sortDirection)
    }).pipe(
      map(({ items, tab, sort }) => {
        const filtered = tab === 'all' ? items : items.filter(i => i.status === tab);
        return sort === 'asc' ? filtered : [...filtered].reverse();
      })
    ),
    { initialValue: [] }
  );
}
```

---

## Pattern 4: `effect()` for Observable Side Effects

Angular's `effect()` runs whenever its dependencies change — use it to bridge signal changes to imperative APIs:

```typescript
import { effect, signal } from '@angular/core';

@Component({ ... })
export class ThemeComponent {
  theme = signal<'light' | 'dark'>('light');

  constructor() {
    // effect() runs on theme changes — bridges to DOM
    effect(() => {
      document.body.classList.toggle('dark', this.theme() === 'dark');
    });

    // effect() with Observable side effect:
    effect(() => {
      const currentTheme = this.theme();
      // Subscribing inside effect is discouraged — use toObservable instead:
    });
  }
}

// Better — use toObservable for Observable-based side effects:
toObservable(this.theme).pipe(
  takeUntilDestroyed()
).subscribe(theme => this.analytics.track('theme-change', { theme }));
```

---

## Migration: `async` Pipe → `toSignal`

```typescript
// Before (async pipe):
@Component({
  template: `
    <ng-container *ngIf="user$ | async as user">
      <h1>{{ user.name }}</h1>
    </ng-container>
  `
})
export class OldComponent {
  user$ = this.userService.getUser();
}

// After (toSignal — cleaner, no structural directive):
@Component({
  template: `
    @if (user()) {
      <h1>{{ user()!.name }}</h1>
    }
  `
})
export class NewComponent {
  user = toSignal(this.userService.getUser());
}
```

---

## When Not to Convert

- **Keep as Observable** when the stream is shared between multiple services or components via `shareReplay` — converting to Signal loses the sharing semantics.
- **Keep as Signal** when the value is purely synchronous UI state with no async dependencies.
- **Avoid `toSignal` in services** — Signals are scoped to the injection context; services may outlive components.

---

## Common Pitfalls

### Missing `initialValue` Causes `undefined` in Type

```typescript
// ❌ TYPE: Signal<User | undefined> — undefined before first emission
const user = toSignal(userStream$);
// user() is User | undefined — must handle undefined in template

// ✅ Provide initialValue for non-nullable type:
const user = toSignal(userStream$, { initialValue: DEFAULT_USER });
// user() is User — always defined

// ✅ Or use requireSync for BehaviorSubject (guaranteed sync emission):
const count = toSignal(behaviorSubject$, { requireSync: true });
// count() is number — no undefined
```

### `toObservable` Emits Current Value Synchronously

```typescript
const mySignal = signal(0);
const obs$ = toObservable(mySignal);

// obs$ emits 0 synchronously when subscribed
// then emits new values asynchronously (via microtask scheduler)
// This means operators like debounceTime(0) may skip the first value!

obs$.pipe(debounceTime(0)).subscribe(v => console.log(v));
mySignal.set(1);
// May only log: 1 (initial 0 debounced away)
```
