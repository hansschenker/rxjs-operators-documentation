# Angular + RxJS Patterns

Practical patterns for using RxJS effectively in Angular applications.

---

## 1. Async Pipe — The Foundation

The `async` pipe subscribes to an Observable, renders the latest value, and unsubscribes automatically on component destroy. It is the preferred way to consume Observables in Angular templates.

```typescript
@Component({
  template: `
    <!-- Single subscription, auto-cleanup: -->
    <div *ngIf="user$ | async as user">
      <h1>{{ user.name }}</h1>
      <span>{{ user.email }}</span>
    </div>

    <!-- List rendering: -->
    <li *ngFor="let item of items$ | async">{{ item.name }}</li>

    <!-- Combining multiple streams — subscribe once with ngIf: -->
    <ng-container *ngIf="vm$ | async as vm">
      <h1>{{ vm.user.name }}</h1>
      <ul><li *ngFor="let item of vm.items">{{ item }}</li></ul>
    </ng-container>
  `
})
export class MyComponent {
  user$  = this.userService.getUser();
  items$ = this.itemService.getItems();

  // View model — combine streams into one object for single async pipe:
  vm$ = combineLatest({
    user:  this.user$,
    items: this.items$
  });
}
```

**Why view models**: Each `| async` pipe creates a separate subscription. A view model (`combineLatest({...})`) subscribes once, preventing duplicate requests and keeping templates clean.

---

## 2. Subscription Cleanup

### `takeUntilDestroyed` (Angular 16+) — Preferred

```typescript
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Component, inject, DestroyRef } from '@angular/core';

@Component({ ... })
export class MyComponent {
  // In class field (injection context — no DestroyRef needed):
  readonly count$ = interval(1000).pipe(takeUntilDestroyed());

  // In a method (outside injection context — inject DestroyRef explicitly):
  private destroyRef = inject(DestroyRef);

  ngOnInit() {
    this.dataService.poll().pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(this.handleData.bind(this));
  }
}
```

### `takeUntil(destroy$)` — Universal (pre-Angular 16)

```typescript
@Component({ ... })
export class MyComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(1000).pipe(takeUntil(this.destroy$)).subscribe(v => this.tick(v));
    fromEvent(window, 'resize').pipe(takeUntil(this.destroy$)).subscribe(() => this.resize());
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

---

## 3. Smart Services — `shareReplay` for HTTP

Services that expose data Observables should share them to prevent duplicate HTTP requests.

```typescript
@Injectable({ providedIn: 'root' })
export class UserService {
  private readonly user$ = this.http.get<User>('/api/me').pipe(
    shareReplay(1) // cache response; all consumers share one request
  );

  getUser(): Observable<User> { return this.user$; }
  getUserName(): Observable<string> { return this.user$.pipe(map(u => u.name)); }
  getUserRole(): Observable<string> { return this.user$.pipe(map(u => u.role)); }
  // All three share one HTTP request
}
```

---

## 4. Reactive Forms

```typescript
import { FormControl } from '@angular/forms';

@Component({ ... })
export class SearchComponent implements OnInit {
  searchControl = new FormControl('');

  results$ = this.searchControl.valueChanges.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    filter(q => (q ?? '').length >= 2),
    switchMap(q => this.api.search(q ?? '').pipe(
      catchError(() => of([]))  // don't kill stream on error
    )),
    shareReplay(1)
  );
}
```

### Form Validity Stream

```typescript
// React to form validity changes:
this.form.statusChanges.pipe(
  map(status => status === 'VALID'),
  distinctUntilChanged(),
  takeUntilDestroyed(this.destroyRef)
).subscribe(valid => this.submitBtn.disabled = !valid);
```

---

## 5. HTTP Loading State Pattern

```typescript
import { BehaviorSubject, combineLatest } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class DataService {
  private loading$ = new BehaviorSubject(false);
  private error$   = new BehaviorSubject<string | null>(null);

  readonly vm$ = combineLatest({
    loading: this.loading$,
    error:   this.error$,
  });

  fetchData(): Observable<Item[]> {
    this.loading$.next(true);
    this.error$.next(null);

    return this.http.get<Item[]>('/api/items').pipe(
      finalize(() => this.loading$.next(false)),
      catchError(err => {
        this.error$.next(err.message);
        return EMPTY;
      })
    );
  }
}
```

```html
<ng-container *ngIf="dataService.vm$ | async as vm">
  <app-spinner *ngIf="vm.loading"></app-spinner>
  <app-error   *ngIf="vm.error" [message]="vm.error"></app-error>
</ng-container>
```

---

## 6. Route Parameter Streams

```typescript
import { ActivatedRoute } from '@angular/router';

@Component({ ... })
export class ItemDetailComponent {
  item$ = this.route.paramMap.pipe(
    map(params => params.get('id')!),
    distinctUntilChanged(),          // don't reload if same ID
    switchMap(id => this.api.getItem(id).pipe(
      catchError(() => EMPTY)        // 404 → empty, no error thrown
    )),
    shareReplay(1)
  );

  constructor(private route: ActivatedRoute, private api: ApiService) {}
}
```

---

## 7. Angular Signals Interop (Angular 16+)

Angular 16 introduced `toObservable` and `toSignal` from `@angular/core/rxjs-interop`.

```typescript
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { signal, computed } from '@angular/core';

@Component({ ... })
export class MyComponent {
  // Signal → Observable (for use in RxJS pipelines):
  searchQuery = signal('');
  searchQuery$ = toObservable(this.searchQuery);

  results$ = this.searchQuery$.pipe(
    debounceTime(300),
    switchMap(q => this.api.search(q))
  );

  // Observable → Signal (for use in Angular templates without async pipe):
  results = toSignal(this.results$, { initialValue: [] });
  // Template: {{ results() }} — no async pipe needed
}
```

```typescript
// toSignal automatically unsubscribes on component destroy
// toObservable emits synchronously on first subscribe (current signal value)

// Combining signals and Observables:
const userId = signal(1);
const userId$ = toObservable(userId);

const user$ = userId$.pipe(
  switchMap(id => this.http.get<User>(`/api/users/${id}`))
);

const user = toSignal(user$); // Signal<User | undefined>
```

---

## 8. NgRx Store Patterns

```typescript
import { Store } from '@ngrx/store';

@Component({ ... })
export class MyComponent {
  // Selecting state — already an Observable<T>:
  items$     = this.store.select(selectItems);
  isLoading$ = this.store.select(selectLoading);

  // View model from store:
  vm$ = combineLatest({
    items:     this.items$,
    isLoading: this.isLoading$,
    user:      this.store.select(selectCurrentUser)
  });

  constructor(private store: Store) {}
}
```

### Dispatching on Observable Events

```typescript
// Dispatch action when route changes:
this.route.paramMap.pipe(
  map(p => p.get('id')!),
  distinctUntilChanged(),
  takeUntilDestroyed(this.destroyRef)
).subscribe(id => this.store.dispatch(loadItem({ id })));
```

---

## 9. WebSocket Pattern

```typescript
import { webSocket } from 'rxjs/webSocket';
import { retry, filter, share } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class WebSocketService {
  private ws$ = webSocket('wss://api.example.com/ws').pipe(
    retry({ delay: () => timer(3000) }), // reconnect after 3s on error
    share()                               // one connection, many consumers
  );

  // Type-filtered message streams:
  priceUpdates$ = this.ws$.pipe(filter((m: any) => m.type === 'price'));
  statusChanges$ = this.ws$.pipe(filter((m: any) => m.type === 'status'));
}
```

---

## 10. Optimistic UI Pattern

```typescript
@Component({ ... })
export class TodoComponent {
  private todos$ = new BehaviorSubject<Todo[]>([]);

  addTodo(text: string) {
    const optimisticItem: Todo = { id: tempId(), text, pending: true };

    // Immediately show in UI:
    this.todos$.next([...this.todos$.value, optimisticItem]);

    this.api.createTodo(text).subscribe({
      next: created => {
        // Replace optimistic with real item:
        this.todos$.next(
          this.todos$.value.map(t => t.id === optimisticItem.id ? created : t)
        );
      },
      error: () => {
        // Rollback on failure:
        this.todos$.next(
          this.todos$.value.filter(t => t.id !== optimisticItem.id)
        );
      }
    });
  }
}
```
