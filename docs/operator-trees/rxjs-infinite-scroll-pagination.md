# Infinite Scroll & Pagination with RxJS

Page-based, cursor-based, and infinite scroll patterns — from simple arrays to real-time feeds.

---

## Pattern 1: Simple Page-Based Pagination

```typescript
import { BehaviorSubject, switchMap, combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class PaginationService<T> {
  private page$    = new BehaviorSubject(1);
  private perPage$ = new BehaviorSubject(20);

  readonly state$ = combineLatest({
    page:    this.page$,
    perPage: this.perPage$
  }).pipe(
    switchMap(({ page, perPage }) =>
      this.http.get<PagedResponse<T>>('/api/items', {
        params: { page: String(page), per_page: String(perPage) }
      })
    ),
    shareReplay(1)
  );

  nextPage()     { this.page$.next(this.page$.getValue() + 1); }
  prevPage()     { this.page$.next(Math.max(1, this.page$.getValue() - 1)); }
  goTo(n: number) { this.page$.next(n); }
  setPerPage(n: number) { this.perPage$.next(n); this.page$.next(1); }
}

interface PagedResponse<T> {
  data:       T[];
  total:      number;
  page:       number;
  totalPages: number;
}
```

---

## Pattern 2: Infinite Scroll — Accumulate Pages

```typescript
import { Subject, merge, EMPTY } from 'rxjs';
import { scan, switchMap, exhaustMap, startWith } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class InfiniteScrollService<T> {
  private loadMore$  = new Subject<void>();
  private reset$     = new Subject<void>();
  private cursor     = '';
  private hasMore    = true;

  readonly items$ = merge(
    this.reset$.pipe(
      tap(() => { this.cursor = ''; this.hasMore = true; }),
      map(() => 'reset' as const)
    ),
    this.loadMore$.pipe(
      filter(() => this.hasMore),
      map(() => 'load' as const)
    )
  ).pipe(
    startWith('reset' as const),
    exhaustMap(action => {
      if (action === 'reset') this.cursor = '';
      return this.http.get<CursorPageResponse<T>>('/api/items', {
        params: { cursor: this.cursor, limit: '20' }
      }).pipe(
        tap(res => {
          this.cursor  = res.nextCursor ?? '';
          this.hasMore = !!res.nextCursor;
        }),
        map(res => ({ items: res.data, action }))
      );
    }),
    scan(
      (acc, { items, action }) =>
        action === 'reset' ? items : [...acc, ...items],
      [] as T[]
    ),
    shareReplay(1)
  );

  loadMore() { this.loadMore$.next(); }
  reset()    { this.reset$.next(); }
}

interface CursorPageResponse<T> {
  data:       T[];
  nextCursor: string | null;
}
```

---

## Pattern 3: Scroll Sentinel (IntersectionObserver)

The most performant trigger for infinite scroll — no scroll event listener:

```typescript
import { Observable, Subject } from 'rxjs';
import { filter, exhaustMap, scan, takeUntil } from 'rxjs/operators';

// Reusable IntersectionObserver Observable:
function intersectionOf(
  element: Element,
  options?: IntersectionObserverInit
): Observable<IntersectionObserverEntry> {
  return new Observable(observer => {
    const io = new IntersectionObserver(
      entries => entries.forEach(e => observer.next(e)),
      options
    );
    io.observe(element);
    return () => io.disconnect();
  });
}

@Component({
  template: `
    <div *ngFor="let item of items$ | async">{{ item.name }}</div>
    <div #sentinel></div>
  `
})
export class InfiniteListComponent {
  @ViewChild('sentinel') sentinelRef!: ElementRef;
  private destroy$ = new Subject<void>();

  readonly items$: Observable<Item[]>;

  constructor(private api: ApiService) {
    // Defined in ngAfterViewInit — sentinel exists in DOM
  }

  ngAfterViewInit() {
    const trigger$ = intersectionOf(this.sentinelRef.nativeElement, {
      threshold: 0.1
    }).pipe(
      filter(entry => entry.isIntersecting)
    );

    this.items$ = trigger$.pipe(
      startWith(null),                      // load first page immediately
      exhaustMap((_, page) =>               // index = page number
        this.api.getPage(page)
      ),
      scan((acc, page) => [...acc, ...page], [] as Item[]),
      takeUntil(this.destroy$)
    );
  }

  ngOnDestroy() { this.destroy$.next(); }
}
```

---

## Pattern 4: Cursor-Based with URL Sync

Sync pagination state with the browser URL (deep-linkable):

```typescript
import { ActivatedRoute, Router } from '@angular/router';
import { switchMap, map, distinctUntilChanged } from 'rxjs/operators';

@Component({ ... })
export class SearchResultsComponent {
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private api: ApiService
  ) {}

  readonly results$ = this.route.queryParamMap.pipe(
    map(params => ({
      query:  params.get('q')    ?? '',
      cursor: params.get('cursor') ?? '',
    })),
    distinctUntilChanged(
      (a, b) => a.query === b.query && a.cursor === b.cursor
    ),
    switchMap(({ query, cursor }) =>
      this.api.search(query, cursor)
    )
  );

  loadNext(cursor: string) {
    this.router.navigate([], {
      queryParamsHandling: 'merge',
      queryParams: { cursor }
    });
  }

  loadPrev(cursor: string) {
    this.router.navigate([], {
      queryParamsHandling: 'merge',
      queryParams: { cursor }
    });
  }
}
```

---

## Pattern 5: Virtualized List — Only Load Visible Range

For very large lists, only load data for the visible viewport:

```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { switchMap, map, debounceTime, distinctUntilChanged } from 'rxjs/operators';

@Injectable()
export class VirtualListService<T> {
  private visibleRange$ = new BehaviorSubject({ start: 0, end: 50 });
  private ITEM_HEIGHT   = 50; // px
  private OVERSCAN      = 5;  // items to load beyond visible area

  readonly visibleItems$ = this.visibleRange$.pipe(
    debounceTime(50),             // debounce rapid scrolls
    distinctUntilChanged(
      (a, b) => a.start === b.start && a.end === b.end
    ),
    switchMap(({ start, end }) =>
      this.api.getRange(
        Math.max(0, start - this.OVERSCAN),
        end + this.OVERSCAN
      )
    )
  );

  onScroll(scrollTop: number, containerHeight: number): void {
    const start = Math.floor(scrollTop / this.ITEM_HEIGHT);
    const end   = start + Math.ceil(containerHeight / this.ITEM_HEIGHT);
    this.visibleRange$.next({ start, end });
  }
}
```

---

## Pattern 6: Prefetch Next Page

While user reads current page, fetch the next one in the background:

```typescript
import { tap, shareReplay, switchMap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class PrefetchingPaginationService<T> {
  private page$ = new BehaviorSubject(1);
  private prefetchCache = new Map<number, Observable<T[]>>();

  readonly currentPage$ = this.page$.pipe(
    switchMap(page => this.getPage(page)),
    tap((_, page) => this.prefetchPage(page + 1)), // prefetch next
    shareReplay(1)
  );

  private getPage(page: number): Observable<T[]> {
    if (!this.prefetchCache.has(page)) {
      this.prefetchCache.set(page,
        this.http.get<T[]>(`/api/items?page=${page}`).pipe(
          shareReplay(1)
        )
      );
    }
    return this.prefetchCache.get(page)!;
  }

  private prefetchPage(page: number): void {
    if (!this.prefetchCache.has(page)) {
      this.getPage(page).subscribe(); // warm the cache
    }
  }

  next() { this.page$.next(this.page$.getValue() + 1); }
}
```

---

## Pattern 7: Real-Time Feed with Pagination

Combine live updates with historical pagination:

```typescript
import { merge, BehaviorSubject } from 'rxjs';
import { scan, switchMap, shareReplay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class LiveFeedService {
  private loadMore$ = new Subject<void>();

  readonly feed$ = merge(
    // Historical: load pages on demand
    this.loadMore$.pipe(
      startWith(null),
      exhaustMap((_, page) =>
        this.api.getFeedPage(page).pipe(
          map(items => ({ type: 'historical' as const, items }))
        )
      )
    ),
    // Live: new items arrive via WebSocket
    this.ws.messages$.pipe(
      map(item => ({ type: 'live' as const, items: [item] }))
    )
  ).pipe(
    scan((acc, { type, items }) =>
      type === 'live'
        ? [items[0], ...acc]  // prepend live items
        : [...acc, ...items], // append historical
      [] as FeedItem[]
    ),
    shareReplay(1)
  );

  loadMore() { this.loadMore$.next(); }
}
```

---

## Strategy Decision Table

| Requirement | Strategy | Key operators |
|---|---|---|
| Simple page nav with URL | Page params + router | `BehaviorSubject`, `switchMap`, router |
| Infinite scroll (append) | Cursor + accumulate | `exhaustMap`, `scan`, IntersectionObserver |
| Large list performance | Virtual scroll | `debounceTime`, `distinctUntilChanged`, `switchMap` |
| Deep-linkable pages | URL-synced cursor | `ActivatedRoute`, `queryParamMap`, `switchMap` |
| Live feed + history | Merge live + historical | `merge`, `scan`, WebSocket |
| Prefetch next page | Page cache + `tap` | `shareReplay`, `tap`, `BehaviorSubject` |

---

## Common Pitfalls

### Using `switchMap` Instead of `exhaustMap` for Load More

```typescript
// ❌ switchMap — rapid clicks cancel the in-flight request:
loadMore$.pipe(
  switchMap(() => api.getNextPage()) // click twice fast → first request cancelled!
)

// ✅ exhaustMap — ignore clicks while request is in flight:
loadMore$.pipe(
  exhaustMap(() => api.getNextPage()) // second click dropped until first completes
)
```

### Resetting Accumulator on Filter Change

```typescript
// ❌ scan keeps growing even when query changes:
query$.pipe(
  switchMap(q => loadMore$.pipe(
    startWith(null),
    exhaustMap(() => api.search(q))
  )),
  scan((acc, items) => [...acc, ...items], []) // accumulates across queries!
)

// ✅ Reset scan when query changes by moving it inside switchMap:
query$.pipe(
  switchMap(q =>
    loadMore$.pipe(
      startWith(null),
      exhaustMap(() => api.search(q)),
      scan((acc, items) => [...acc, ...items], [] as Item[]) // resets per query
    )
  )
)
```
