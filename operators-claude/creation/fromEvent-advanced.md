# fromEvent — Advanced Patterns

For `fromEvent` fundamentals see the core [fromEvent](./fromEvent) doc. This page covers event delegation, keyboard shortcuts, gesture detection, drag-and-drop, and pointer events.

---

## Pattern 1: Keyboard Shortcut System

```typescript
import { fromEvent, merge } from 'rxjs';
import { filter, map, distinctUntilChanged } from 'rxjs/operators';

interface Shortcut {
  key:   string;
  ctrl?: boolean;
  shift?: boolean;
  alt?:  boolean;
  meta?: boolean;
}

function shortcut(combo: Shortcut): Observable<KeyboardEvent> {
  return fromEvent<KeyboardEvent>(document, 'keydown').pipe(
    filter(e =>
      e.key.toLowerCase() === combo.key.toLowerCase() &&
      !!e.ctrlKey  === !!combo.ctrl  &&
      !!e.shiftKey === !!combo.shift &&
      !!e.altKey   === !!combo.alt   &&
      !!e.metaKey  === !!combo.meta
    ),
    tap(e => e.preventDefault())
  );
}

// Usage:
merge(
  shortcut({ key: 's', ctrl: true }).pipe(map(() => 'save')),
  shortcut({ key: 'z', ctrl: true }).pipe(map(() => 'undo')),
  shortcut({ key: 'z', ctrl: true, shift: true }).pipe(map(() => 'redo')),
  shortcut({ key: 'Escape' }).pipe(map(() => 'cancel'))
).subscribe(command => dispatch(command));
```

---

## Pattern 2: Event Delegation (Single Listener for Many Children)

```typescript
import { fromEvent } from 'rxjs';
import { filter, map } from 'rxjs/operators';

// One listener on the container, not one per list item:
const listContainer = document.querySelector('#todo-list')!;

const itemClicks$ = fromEvent<MouseEvent>(listContainer, 'click').pipe(
  map(e => e.target as HTMLElement),
  filter(el => el.matches('[data-todo-id]')),
  map(el => ({
    id:     el.dataset['todoId']!,
    action: el.dataset['action'] ?? 'select'
  }))
);

const deleteClicks$ = itemClicks$.pipe(filter(e => e.action === 'delete'));
const selectClicks$ = itemClicks$.pipe(filter(e => e.action === 'select'));

deleteClicks$.subscribe(({ id }) => removeTodo(id));
selectClicks$.subscribe(({ id }) => selectTodo(id));
```

---

## Pattern 3: Drag and Drop

```typescript
import { fromEvent, EMPTY } from 'rxjs';
import { switchMap, takeUntil, map, tap } from 'rxjs/operators';

function makeDraggable(element: HTMLElement): Observable<{ x: number; y: number }> {
  const mousedown$ = fromEvent<MouseEvent>(element, 'mousedown');
  const mousemove$ = fromEvent<MouseEvent>(document, 'mousemove');
  const mouseup$   = fromEvent<MouseEvent>(document, 'mouseup');

  return mousedown$.pipe(
    tap(e => e.preventDefault()),
    switchMap(start => {
      const offsetX = start.clientX - element.offsetLeft;
      const offsetY = start.clientY - element.offsetTop;

      return mousemove$.pipe(
        map(e => ({
          x: e.clientX - offsetX,
          y: e.clientY - offsetY
        })),
        takeUntil(mouseup$)
      );
    })
  );
}

// Usage:
const panel = document.querySelector('#panel') as HTMLElement;
makeDraggable(panel).subscribe(({ x, y }) => {
  panel.style.left = `${x}px`;
  panel.style.top  = `${y}px`;
});
```

---

## Pattern 4: Pointer Events (Touch + Mouse Unified)

```typescript
import { fromEvent, merge } from 'rxjs';
import { map } from 'rxjs/operators';

interface Point { x: number; y: number; }

// Unified pointer stream — works with mouse and touch:
function pointerEvents(el: Element) {
  const toPoint = (e: PointerEvent): Point => ({ x: e.clientX, y: e.clientY });

  return {
    down$: fromEvent<PointerEvent>(el, 'pointerdown').pipe(map(toPoint)),
    move$: fromEvent<PointerEvent>(el, 'pointermove').pipe(map(toPoint)),
    up$:   fromEvent<PointerEvent>(document, 'pointerup').pipe(map(toPoint)),
  };
}

// Pinch-to-zoom detection (two fingers):
function pinchDistance(e: TouchEvent): number {
  const [a, b] = Array.from(e.touches);
  return Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
}

const canvas = document.querySelector('canvas')!;
fromEvent<TouchEvent>(canvas, 'touchmove').pipe(
  filter(e => e.touches.length === 2),
  map(pinchDistance),
  pairwise(),
  map(([prev, curr]) => curr / prev)  // scale factor
).subscribe(scale => zoom(scale));
```

---

## Pattern 5: Long Press Detection

```typescript
import { fromEvent, timer, NEVER } from 'rxjs';
import { switchMap, takeUntil, filter } from 'rxjs/operators';

function longPress(element: Element, duration = 500): Observable<void> {
  const pointerdown$ = fromEvent(element, 'pointerdown');
  const pointerup$   = fromEvent(document, 'pointerup');
  const pointermove$ = fromEvent<PointerEvent>(document, 'pointermove').pipe(
    filter(e => Math.hypot(e.movementX, e.movementY) > 5) // cancel on movement
  );

  return pointerdown$.pipe(
    switchMap(() =>
      timer(duration).pipe(
        takeUntil(merge(pointerup$, pointermove$))
      )
    ),
    map(() => undefined as void)
  );
}

// Usage:
longPress(deleteButton, 800).subscribe(() => confirmDelete());
```

---

## Pattern 6: Double-Click vs Single-Click Disambiguation

```typescript
import { fromEvent, timer } from 'rxjs';
import { buffer, filter, map, debounceTime } from 'rxjs/operators';

const clicks$ = fromEvent(button, 'click');

// Buffer clicks within 300ms window, then classify:
const buffered$ = clicks$.pipe(
  buffer(clicks$.pipe(debounceTime(300))),
  map(clicks => clicks.length)
);

const singleClick$ = buffered$.pipe(filter(n => n === 1), map(() => 'single'));
const doubleClick$ = buffered$.pipe(filter(n => n >= 2), map(() => 'double'));

merge(singleClick$, doubleClick$).subscribe(type => {
  if (type === 'single') selectItem();
  if (type === 'double') editItem();
});
```

---

## Pattern 7: Scroll Position Stream with Intersection Observer

```typescript
import { fromEvent, Observable } from 'rxjs';
import { map, distinctUntilChanged, throttleTime } from 'rxjs/operators';

// Efficient infinite scroll trigger using IntersectionObserver:
function whenVisible(sentinel: Element): Observable<boolean> {
  return new Observable(observer => {
    const io = new IntersectionObserver(
      entries => observer.next(entries[0].isIntersecting),
      { threshold: 0.1 }
    );
    io.observe(sentinel);
    return () => io.disconnect();
  });
}

// Usage:
const loadMoreSentinel = document.querySelector('#load-more-sentinel')!;

whenVisible(loadMoreSentinel).pipe(
  filter(Boolean),              // only when becomes visible
  exhaustMap(() => loadNextPage()),
  takeUntilDestroyed()
).subscribe(appendItems);
```

---

## Pattern 8: Global vs Local Event Listener Cleanup

```typescript
// ❌ Global listener never cleaned up — memory leak:
fromEvent(document, 'click').subscribe(handleClick);

// ✅ Tied to component lifetime with takeUntil:
@Component({ ... })
export class MyComponent {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    fromEvent<KeyboardEvent>(document, 'keydown').pipe(
      filter(e => e.key === 'Escape'),
      takeUntil(this.destroy$)
    ).subscribe(() => this.close());
  }

  ngOnDestroy() { this.destroy$.next(); }
}

// ✅ Angular 16+ — takeUntilDestroyed():
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.key === 'Escape'),
  takeUntilDestroyed(this.destroyRef)
).subscribe(() => this.close());
```

---

## `fromEvent` vs `fromEventPattern` vs Manual `Observable`

```typescript
// fromEvent — standard DOM/Node EventEmitter:
fromEvent(button, 'click')
// ✓ Cleanest. Works for DOM elements, EventEmitter, jQuery.

// fromEventPattern — non-standard add/remove API:
fromEventPattern(
  handler => thirdPartyLib.on('data', handler),
  handler => thirdPartyLib.off('data', handler)
)
// ✓ For libraries with custom add/remove APIs.

// new Observable — full control:
new Observable(observer => {
  const handler = (e: Event) => observer.next(e);
  element.addEventListener('click', handler, { passive: true });
  return () => element.removeEventListener('click', handler);
})
// ✓ For { passive: true }, { once: true }, or capture phase options.
```

---

## Common Pitfalls

### Forgetting `{ passive: true }` on Scroll/Touch Listeners

```typescript
// ❌ fromEvent doesn't support EventListenerOptions:
fromEvent(window, 'scroll')  // adds non-passive listener — blocks scrolling!

// ✅ Use Observable constructor for passive listeners:
new Observable<Event>(observer => {
  const handler = (e: Event) => observer.next(e);
  window.addEventListener('scroll', handler, { passive: true });
  return () => window.removeEventListener('scroll', handler);
})
```

### Attaching to a Non-Existent Element

```typescript
// ❌ Element not yet in DOM — fromEvent gets null:
fromEvent(document.querySelector('#modal'), 'click')  // null!

// ✅ Defer until element exists, or use event delegation:
// Option 1 — defer:
defer(() => of(document.querySelector('#modal'))).pipe(
  filter(Boolean),
  switchMap(el => fromEvent(el, 'click'))
)
// Option 2 — delegate on existing ancestor (see Pattern 2)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**Key insight**: `fromEvent` is RxJS's bridge to the imperative event world. Its power multiplies with `switchMap` (drag-and-drop, keyboard combos), `buffer`+`debounceTime` (click disambiguation), and `merge` (unifying multiple event types). Always pair with `takeUntil` or `takeUntilDestroyed` on global listeners to prevent memory leaks.
