# Virtual Scroll Patterns with RxJS

Rendering only visible rows, reactive viewport management, dynamic item heights, and integrating with CDK virtual scroll.

---

## The Core Idea

Virtual scroll renders only the items visible in the viewport, swapping DOM nodes as the user scrolls. The RxJS layer handles the reactive viewport state, smooth scrolling calculations, and data fetching.

```
Total items: 100,000
Visible:     20 rows (the "window")
DOM nodes:   20 + small overscan buffer

As user scrolls: update which 20 items are rendered, reuse DOM nodes
```

---

## Pattern 1: Basic Virtual Viewport State

```typescript
import { fromEvent, BehaviorSubject, combineLatest } from 'rxjs';
import { map, distinctUntilChanged, shareReplay, debounceTime } from 'rxjs/operators';

interface ViewportState {
  scrollTop:    number;
  clientHeight: number;
  itemHeight:   number;
  totalItems:   number;
}

interface VisibleRange {
  startIndex:  number;
  endIndex:    number;
  offsetTop:   number; // px offset for first visible item
  totalHeight: number; // px height of full virtual list
}

function computeVisibleRange(
  { scrollTop, clientHeight, itemHeight, totalItems }: ViewportState,
  overscan = 3
): VisibleRange {
  const startIndex  = Math.max(0, Math.floor(scrollTop / itemHeight) - overscan);
  const visibleRows = Math.ceil(clientHeight / itemHeight);
  const endIndex    = Math.min(totalItems - 1, startIndex + visibleRows + overscan * 2);

  return {
    startIndex,
    endIndex,
    offsetTop:   startIndex * itemHeight,
    totalHeight: totalItems * itemHeight
  };
}

class VirtualScrollController<T> {
  private scrollTop$    = new BehaviorSubject(0);
  private clientHeight$ = new BehaviorSubject(0);
  private allItems$     = new BehaviorSubject<T[]>([]);

  readonly visibleRange$ = combineLatest([
    this.scrollTop$,
    this.clientHeight$,
    this.allItems$.pipe(map(items => items.length))
  ]).pipe(
    map(([scrollTop, clientHeight, totalItems]) =>
      computeVisibleRange({ scrollTop, clientHeight, itemHeight: 48, totalItems })
    ),
    distinctUntilChanged((a, b) =>
      a.startIndex === b.startIndex && a.endIndex === b.endIndex
    ),
    shareReplay(1)
  );

  readonly visibleItems$ = combineLatest([
    this.visibleRange$,
    this.allItems$
  ]).pipe(
    map(([range, items]) => ({
      items:       items.slice(range.startIndex, range.endIndex + 1),
      startIndex:  range.startIndex,
      offsetTop:   range.offsetTop,
      totalHeight: range.totalHeight
    }))
  );

  onScroll(scrollTop: number):   void { this.scrollTop$.next(scrollTop); }
  onResize(height: number):       void { this.clientHeight$.next(height); }
  setItems(items: T[]):           void { this.allItems$.next(items); }
}
```

---

## Pattern 2: Infinite Scroll with Virtual Window

Combine virtual scroll with infinite loading:

```typescript
import { switchMap, scan, filter, map } from 'rxjs/operators';
import { Subject } from 'rxjs';

interface InfiniteVirtualState<T> {
  items:       T[];
  loading:     boolean;
  page:        number;
  hasMore:     boolean;
}

class InfiniteVirtualScroll<T> {
  private loadPage$ = new Subject<void>();
  private controller = new VirtualScrollController<T>();

  // Trigger load when user nears bottom of loaded items:
  private nearEnd$ = this.controller.visibleRange$.pipe(
    filter(({ endIndex }) => {
      const loaded = this.state$.getValue().items.length;
      return endIndex > loaded - 10; // trigger when 10 items from end
    }),
    debounceTime(100),
    distinctUntilChanged()
  );

  private state$ = new BehaviorSubject<InfiniteVirtualState<T>>({
    items: [], loading: false, page: 0, hasMore: true
  });

  constructor(private fetchPage: (page: number) => Observable<T[]>) {
    // Auto-load when near end:
    this.nearEnd$.pipe(
      filter(() => !this.state$.getValue().loading && this.state$.getValue().hasMore),
      switchMap(() => {
        const { page } = this.state$.getValue();
        this.state$.next({ ...this.state$.getValue(), loading: true });
        return this.fetchPage(page + 1).pipe(
          catchError(() => of([] as T[]))
        );
      }),
      takeUntilDestroyed()
    ).subscribe(newItems => {
      const prev = this.state$.getValue();
      const items = [...prev.items, ...newItems];
      this.state$.next({
        items,
        loading: false,
        page:    prev.page + 1,
        hasMore: newItems.length > 0
      });
      this.controller.setItems(items);
    });
  }

  readonly view$ = this.controller.visibleItems$;
  readonly loading$ = this.state$.pipe(map(s => s.loading));

  onScroll(scrollTop: number):  void { this.controller.onScroll(scrollTop); }
  onResize(height: number):      void { this.controller.onResize(height); }
}
```

---

## Pattern 3: Angular CDK Virtual Scroll + RxJS

```typescript
import { CdkVirtualScrollViewport } from '@angular/cdk/scrolling';
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged } from 'rxjs/operators';

@Component({
  template: `
    <cdk-virtual-scroll-viewport itemSize="48" style="height: 600px">
      <div *cdkVirtualFor="let item of items$ | async; trackBy: trackById"
           class="item-row">
        {{ item.name }}
      </div>
    </cdk-virtual-scroll-viewport>
  `
})
export class VirtualListComponent {
  @ViewChild(CdkVirtualScrollViewport) viewport!: CdkVirtualScrollViewport;

  // CDK handles rendering; RxJS handles data loading:
  private scrollIndex$ = new Subject<number>();

  readonly items$ = this.dataService.getAllItems().pipe(shareReplay(1));

  // Detect when user scrolls near end for prefetching:
  ngAfterViewInit() {
    this.viewport.scrolledIndexChange.pipe(
      debounceTime(100),
      withLatestFrom(this.items$),
      filter(([index, items]) => index > items.length - 20),
      takeUntilDestroyed()
    ).subscribe(() => this.dataService.prefetchNextPage());
  }

  trackById = (_: number, item: { id: string }) => item.id;
}
```

---

## Pattern 4: Dynamic Item Heights

When items have variable heights, track cumulative offsets:

```typescript
interface ItemLayout {
  index:  number;
  height: number;
  offset: number; // cumulative offset from top
}

class DynamicHeightVirtualScroll<T> {
  private measuredHeights = new Map<number, number>();
  private defaultHeight   = 48;
  private layouts$        = new BehaviorSubject<ItemLayout[]>([]);

  measureItem(index: number, height: number): void {
    this.measuredHeights.set(index, height);
    this.recomputeLayouts();
  }

  private recomputeLayouts(): void {
    const items  = this.allItems$.getValue();
    let   offset = 0;
    const layouts = items.map((_, index) => {
      const height = this.measuredHeights.get(index) ?? this.defaultHeight;
      const layout = { index, height, offset };
      offset += height;
      return layout;
    });
    this.layouts$.next(layouts);
  }

  // Binary search for visible range given scrollTop:
  private findStartIndex(scrollTop: number, layouts: ItemLayout[]): number {
    let lo = 0, hi = layouts.length - 1;
    while (lo < hi) {
      const mid = Math.floor((lo + hi) / 2);
      if (layouts[mid].offset < scrollTop) lo = mid + 1;
      else                                  hi = mid;
    }
    return Math.max(0, lo - 1);
  }

  readonly visibleItems$ = combineLatest([
    this.scrollTop$,
    this.clientHeight$,
    this.layouts$
  ]).pipe(
    map(([scrollTop, clientHeight, layouts]) => {
      const startIndex = this.findStartIndex(scrollTop, layouts);
      let   endIndex   = startIndex;
      while (endIndex < layouts.length - 1 &&
             layouts[endIndex].offset < scrollTop + clientHeight) {
        endIndex++;
      }
      return {
        items:      this.allItems$.getValue().slice(startIndex, endIndex + 1),
        startIndex,
        offsetTop:  layouts[startIndex]?.offset ?? 0,
        totalHeight: layouts.reduce((s, l) => s + l.height, 0)
      };
    })
  );
}
```

---

## Pattern 5: Search-Filtered Virtual List

Filter large dataset without re-rendering everything:

```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { map, debounceTime, distinctUntilChanged } from 'rxjs/operators';

class FilterableVirtualList<T> {
  private allItems$   = new BehaviorSubject<T[]>([]);
  private searchTerm$ = new BehaviorSubject('');
  private filterFn$   = new BehaviorSubject<(item: T) => boolean>(() => true);

  readonly filteredItems$ = combineLatest([
    this.allItems$,
    this.searchTerm$.pipe(debounceTime(200), distinctUntilChanged()),
    this.filterFn$
  ]).pipe(
    map(([items, term, filterFn]) => {
      const lowerTerm = term.toLowerCase();
      return items.filter(item =>
        filterFn(item) &&
        (term.length === 0 || JSON.stringify(item).toLowerCase().includes(lowerTerm))
      );
    }),
    shareReplay(1)
  );

  // Reset scroll on filter change:
  readonly filteredItemsWithReset$ = this.filteredItems$.pipe(
    tap(() => this.scrollController.onScroll(0)) // jump back to top
  );

  setSearch(term: string):              void { this.searchTerm$.next(term); }
  setFilter(fn: (item: T) => boolean):  void { this.filterFn$.next(fn); }
  setItems(items: T[]):                 void { this.allItems$.next(items); }
}
```

---

## Common Pitfalls

### Not Using `trackBy` — Full DOM Rebuild on Each Update

```typescript
// ❌ Without trackBy, Angular re-creates all visible DOM nodes on each scroll:
*ngFor="let item of visibleItems"

// ✅ Always provide trackBy to reuse existing DOM nodes:
*ngFor="let item of visibleItems; trackBy: trackById"
trackById = (_: number, item: { id: string }) => item.id;
```

### Scroll Jank from Synchronous Heavy Work

```typescript
// ❌ Filtering/sorting inside scroll handler — blocks main thread:
fromEvent(container, 'scroll').subscribe(e => {
  const filtered = hugeArray.filter(complexFilter); // sync, blocks paint
  this.visibleItems = computeVisible(filtered, scrollTop);
});

// ✅ Pre-compute filtered list; only derive visible slice on scroll:
const filteredItems$ = searchTerm$.pipe(
  map(term => hugeArray.filter(item => matches(item, term))), // pre-computed
  shareReplay(1)
);

fromEvent(container, 'scroll').pipe(
  withLatestFrom(filteredItems$),
  map(([e, items]) => computeVisibleSlice(items, (e.target as Element).scrollTop)),
  auditTime(0, animationFrameScheduler)
).subscribe(visible => this.visibleItems = visible);
```
