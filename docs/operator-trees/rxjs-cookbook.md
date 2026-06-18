# RxJS Cookbook

Common real-world patterns as ready-to-use recipes.

---

## 1. Auto-Save with Debounce

```typescript
import { debounceTime, switchMap, catchError, EMPTY } from 'rxjs';

formChanges$.pipe(
  debounceTime(1000),                    // wait 1s after last change
  switchMap(data =>                      // cancel in-flight save on new change
    this.api.save(data).pipe(
      catchError(err => {
        this.showError(err);
        return EMPTY;                    // don't kill stream on save failure
      })
    )
  )
).subscribe(() => this.showSaveIndicator());
```

---

## 2. Infinite Scroll / Pagination

```typescript
import { scan, switchMap, startWith, exhaustMap } from 'rxjs';

const loadMore$ = fromEvent(loadMoreBtn, 'click');

loadMore$.pipe(
  startWith(null),                       // load first page immediately
  scan(page => page + 1, 0),            // increment page on each click
  exhaustMap(page =>                     // ignore clicks while loading
    this.api.getPage(page).pipe(
      catchError(() => EMPTY)
    )
  )
).subscribe(items => this.items.push(...items));
```

---

## 3. Polling with Exponential Backoff

```typescript
import { timer, switchMap, retry, catchError, of } from 'rxjs';

function poll<T>(fn: () => Observable<T>, intervalMs = 5000): Observable<T> {
  return timer(0, intervalMs).pipe(
    switchMap(() => fn().pipe(
      retry({
        count: 3,
        delay: (_, attempt) => timer(500 * 2 ** attempt)
      })
    )),
    catchError(() => of(null as unknown as T)) // fall through on exhausted retries
  );
}

// Usage:
poll(() => this.api.getStatus(), 10_000)
  .pipe(takeUntilDestroyed(this.destroyRef))
  .subscribe(status => this.status = status);
```

---

## 4. WebSocket with Auto-Reconnect

```typescript
import { webSocket } from 'rxjs/webSocket';
import { retry, share, filter } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class RealtimeService {
  private connection$ = webSocket('wss://api.example.com/ws').pipe(
    retry({ delay: () => timer(3000) }),   // reconnect after 3s
    share()                                // one connection, many consumers
  );

  messages$<T>(type: string): Observable<T> {
    return this.connection$.pipe(
      filter((msg: any) => msg.type === type),
      map((msg: any) => msg.payload as T)
    );
  }
}
```

---

## 5. Type-ahead Search with Caching

```typescript
import { shareReplay, switchMap, debounceTime, distinctUntilChanged } from 'rxjs';

const cache = new Map<string, Observable<Result[]>>();

function search(query: string): Observable<Result[]> {
  if (!cache.has(query)) {
    cache.set(query,
      this.api.search(query).pipe(shareReplay(1)) // cache per query
    );
  }
  return cache.get(query)!;
}

searchInput$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  filter(q => q.length >= 2),
  switchMap(q => search(q))
).subscribe(renderResults);
```

---

## 6. Concurrent Request Batching

```typescript
import { bufferTime, mergeMap, filter } from 'rxjs';

// Batch IDs arriving within 50ms into a single bulk request
idRequests$.pipe(
  bufferTime(50),                         // collect IDs for 50ms
  filter(ids => ids.length > 0),          // skip empty batches
  mergeMap(ids => this.api.getBulk(ids))  // one request per batch
).subscribe(items => this.cache.setMany(items));
```

---

## 7. Drag and Drop Stream

```typescript
import { fromEvent, switchMap, takeUntil, map } from 'rxjs';

const mousedown$ = fromEvent<MouseEvent>(el, 'mousedown');
const mousemove$ = fromEvent<MouseEvent>(document, 'mousemove');
const mouseup$   = fromEvent<MouseEvent>(document, 'mouseup');

const drag$ = mousedown$.pipe(
  switchMap(start =>
    mousemove$.pipe(
      map(move => ({
        x: move.clientX - start.offsetX,
        y: move.clientY - start.offsetY
      })),
      takeUntil(mouseup$)
    )
  )
);

drag$.subscribe(({ x, y }) => {
  el.style.left = `${x}px`;
  el.style.top  = `${y}px`;
});
```

---

## 8. Retry Only on Specific Errors

```typescript
import { retry, catchError, throwError, timer } from 'rxjs';

source$.pipe(
  retry({
    count: 3,
    delay: (err, attempt) => {
      if (err.status === 429) return timer(2000 * attempt); // rate limit — retry
      if (err.status >= 500) return timer(1000);            // server error — retry
      return throwError(() => err);                         // 4xx client error — don't retry
    }
  }),
  catchError(err => {
    this.logger.error(err);
    return of(FALLBACK_VALUE);
  })
).subscribe(handler);
```

---

## 9. Parallel Requests with Combined Result

```typescript
import { forkJoin, combineLatest, zip } from 'rxjs';

// Wait for ALL to complete (forkJoin):
forkJoin({
  user:   this.http.get<User>('/api/me'),
  config: this.http.get<Config>('/api/config'),
  perms:  this.http.get<Perm[]>('/api/permissions')
}).subscribe(({ user, config, perms }) => this.init(user, config, perms));

// React to ANY completing (combineLatest — all must emit at least once):
combineLatest({
  searchResults: this.results$,
  filters:       this.activeFilters$,
  page:          this.currentPage$
}).subscribe(({ searchResults, filters, page }) => this.render(searchResults, filters, page));
```

---

## 10. Optimistic Delete with Rollback

```typescript
import { BehaviorSubject } from 'rxjs';

items$ = new BehaviorSubject<Item[]>(this.initialItems);

deleteItem(id: string) {
  const previous = this.items$.value;
  const optimistic = previous.filter(item => item.id !== id);

  this.items$.next(optimistic); // immediate UI update

  this.api.delete(id).subscribe({
    error: () => this.items$.next(previous) // rollback on failure
  });
}
```

---

## 11. Loading / Error / Data State Machine

```typescript
type State<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; error: string };

function toLoadingState<T>(source$: Observable<T>): Observable<State<T>> {
  return concat(
    of({ status: 'loading' } as State<T>),
    source$.pipe(
      map(data  => ({ status: 'success', data }  as State<T>)),
      catchError(err => of({ status: 'error', error: err.message } as State<T>))
    )
  );
}

// Usage:
this.state$ = toLoadingState(this.api.getData());
```

```html
<ng-container *ngIf="state$ | async as s">
  <app-spinner *ngIf="s.status === 'loading'"></app-spinner>
  <app-error   *ngIf="s.status === 'error'"   [message]="s.error"></app-error>
  <app-list    *ngIf="s.status === 'success'" [items]="s.data"></app-list>
</ng-container>
```

---

## 12. Event Bus / Message Passing

```typescript
import { Subject, filter, map } from 'rxjs';

interface AppEvent<T = unknown> { type: string; payload: T; }

@Injectable({ providedIn: 'root' })
export class EventBusService {
  private bus$ = new Subject<AppEvent>();

  emit<T>(type: string, payload: T) {
    this.bus$.next({ type, payload });
  }

  on<T>(type: string): Observable<T> {
    return this.bus$.pipe(
      filter(e => e.type === type),
      map(e => e.payload as T)
    );
  }
}

// Usage:
this.eventBus.emit('user:logout', { reason: 'session-expired' });
this.eventBus.on<LogoutEvent>('user:logout').pipe(
  takeUntilDestroyed(this.destroyRef)
).subscribe(({ reason }) => this.handleLogout(reason));
```
