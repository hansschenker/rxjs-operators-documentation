# RxJS in Zoneless Angular

Angular's experimental zoneless mode (`provideExperimentalZonelessChangeDetection`) removes Zone.js entirely. Change detection becomes fully signal- and event-driven. This guide covers how RxJS streams integrate with that model.

---

## What Changes Without Zone.js

Zone.js works by monkey-patching async APIs (setTimeout, Promise, XHR) to trigger change detection after every async operation. In zoneless mode:

- **No automatic CD on subscription emissions** — Angular doesn't know a stream emitted
- **`async` pipe still works** — it calls `ChangeDetectorRef.markForCheck()` internally
- **Signals are the primary CD trigger** — `signal()`, `computed()`, `effect()`
- **Manual CD is explicit** — `ChangeDetectorRef.markForCheck()` or `detectChanges()`

```typescript
// Zone.js world: this automatically triggers CD
someObservable$.subscribe(value => {
  this.data = value; // Zone.js sees the async callback, marks view dirty
});

// Zoneless world: same code — NO CD triggered
someObservable$.subscribe(value => {
  this.data = value; // nothing updates the view
});
```

---

## Strategy 1: async Pipe (Recommended — Works Unchanged)

The `async` pipe calls `markForCheck()` on every emission — it's zoneless-safe out of the box.

```typescript
@Component({
  template: `
    <div *ngFor="let item of items$ | async">{{ item.name }}</div>
    <div>{{ (status$ | async)?.message }}</div>
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ItemListComponent {
  items$ = this.itemService.getItems();
  status$ = this.statusService.status$;

  constructor(private itemService: ItemService, private statusService: StatusService) {}
}
```

**This is the lowest-friction path.** If you already use `async` pipe + `OnPush`, zoneless migration is nearly zero-effort.

---

## Strategy 2: toSignal — Bridge Streams to Signals

`toSignal()` from `@angular/core/rxjs-interop` converts an Observable to a Signal. Signals are the native CD trigger in zoneless Angular.

```typescript
import { toSignal } from '@angular/core/rxjs-interop';
import { Component, inject } from '@angular/core';

@Component({
  template: `
    @if (user()) {
      <h1>{{ user()!.name }}</h1>
      <p>Balance: {{ balance() | currency }}</p>
    } @else {
      <app-skeleton />
    }
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class UserDashboardComponent {
  private userService = inject(UserService);

  // Observable → Signal: CD happens automatically on emission
  user = toSignal(this.userService.currentUser$, { initialValue: null });
  balance = toSignal(this.userService.balance$, { initialValue: 0 });
}
```

### toSignal Options

```typescript
import { toSignal } from '@angular/core/rxjs-interop';

// initialValue: avoid undefined during first render
const value = toSignal(source$, { initialValue: 0 });

// requireSync: for streams that emit synchronously on subscribe
const syncValue = toSignal(of(42), { requireSync: true });
// Signal<number> — no undefined, no initialValue needed

// injector: use outside injection context
const injector = inject(Injector);
const deferred = toSignal(source$, { injector });

// rejectErrors: throw errors in template (caught by ErrorHandler)
const safe = toSignal(source$.pipe(catchError(() => of(null))), { initialValue: null });
```

---

## Strategy 3: fromSignal — Drive Streams from Signals

`fromSignal()` / `toObservable()` converts a Signal to an Observable — bridging back when you need RxJS operators.

```typescript
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { signal, computed } from '@angular/core';

@Component({ /* ... */ })
export class SearchComponent {
  query = signal('');

  // Signal → Observable → RxJS pipeline → Signal
  results = toSignal(
    toObservable(this.query).pipe(
      debounceTime(300),
      distinctUntilChanged(),
      filter(q => q.length >= 2),
      switchMap(q => this.searchApi.search(q)),
      catchError(() => of([])),
    ),
    { initialValue: [] }
  );

  onInput(event: Event): void {
    this.query.set((event.target as HTMLInputElement).value);
  }
}
```

**Pattern**: Signal → `toObservable()` → RxJS operators → `toSignal()` → template. This is the canonical zoneless reactive pattern.

---

## Strategy 4: Manual markForCheck with inject(ChangeDetectorRef)

For cases where you must subscribe imperatively (e.g. third-party library integration):

```typescript
import { Component, inject, ChangeDetectorRef, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({ changeDetection: ChangeDetectionStrategy.OnPush })
export class ManualCdComponent implements OnDestroy {
  private cdr = inject(ChangeDetectorRef);
  private destroy$ = new Subject<void>();

  data: SomeData | null = null;

  constructor(private dataService: DataService) {
    dataService.data$.pipe(
      takeUntil(this.destroy$),
    ).subscribe(data => {
      this.data = data;
      this.cdr.markForCheck(); // explicit CD trigger
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

---

## Reactive State Pattern: Signals + Streams Combined

The recommended zoneless architecture combines signals for local state with streams for async data.

```typescript
import { Component, signal, computed, inject } from '@angular/core';
import { toSignal, toObservable } from '@angular/core/rxjs-interop';
import { switchMap, catchError, startWith } from 'rxjs/operators';

type LoadState<T> = { status: 'loading' } | { status: 'data'; data: T } | { status: 'error'; error: string };

@Component({
  template: `
    @switch (state().status) {
      @case ('loading') { <app-spinner /> }
      @case ('error')   { <app-error [message]="state().error" /> }
      @case ('data')    { <app-list [items]="state().data" /> }
    }
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ProductListComponent {
  private api = inject(ProductApiService);

  // Local filter signal — owned by this component
  categoryFilter = signal<string | null>(null);

  // Derived async state — reacts to filter changes
  state = toSignal(
    toObservable(this.categoryFilter).pipe(
      switchMap(category =>
        this.api.getProducts(category).pipe(
          startWith({ status: 'loading' } as LoadState<Product[]>),
          catchError(err => of({ status: 'error', error: err.message } as LoadState<Product[]>)),
        ).pipe(
          startWith({ status: 'loading' } as LoadState<Product[]>),
        )
      ),
    ),
    { initialValue: { status: 'loading' } as LoadState<Product[]> }
  );

  selectCategory(cat: string | null): void {
    this.categoryFilter.set(cat); // triggers switchMap automatically
  }
}
```

---

## Effect Pattern for Side Effects

`effect()` runs when signals change — use it for imperative side effects triggered by signal-backed streams.

```typescript
import { effect, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';

@Component({ /* ... */ })
export class NotificationComponent {
  private notificationService = inject(NotificationService);

  latestNotification = toSignal(
    this.notificationService.notifications$,
    { initialValue: null }
  );

  constructor() {
    effect(() => {
      const notification = this.latestNotification();
      if (notification?.priority === 'high') {
        // Runs reactively whenever signal changes
        this.showToast(notification.message);
      }
    });
  }

  private showToast(message: string): void { /* ... */ }
}
```

---

## Migration Checklist: Zone.js → Zoneless

```
□ Replace manual subscribe + property assignment with toSignal()
□ Audit all .subscribe() calls — add markForCheck() or migrate to async pipe
□ Add ChangeDetectionStrategy.OnPush to all components (required for zoneless)
□ Replace fromEvent (in components) with Angular event bindings or toObservable(signal)
□ Replace interval/timer in components with toSignal(interval(...))
□ Verify async pipe usage — it's zoneless-safe as-is
□ Test with provideExperimentalZonelessChangeDetection() in TestBed
□ Remove zone.js from polyfills.ts once migration is complete
```

---

## Related Guides

- **[Angular Signals + RxJS](./angular-signals-rxjs.md)** — deep interop reference
- **[Reactive Forms Advanced](./rxjs-reactive-forms-advanced.md)** — forms in zoneless context
- **[NgRx ComponentStore](./rxjs-ngrx-component-store.md)** — state management patterns
