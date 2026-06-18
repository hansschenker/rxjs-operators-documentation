# fromEvent

## Identity

- **Name**: fromEvent
- **Category**: Creation Operators
- **Type**: DOM/Node event adapter — wraps an event target's event listener in an Observable
- **Import**:
  ```typescript
  import { fromEvent } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function fromEvent<T>(
    target: FromEventTarget<T>,
    eventName: string,
    options?: EventListenerOptions | ((...args: any[]) => T),
    resultSelector?: (...args: any[]) => T
  ): Observable<T>

  // FromEventTarget<T> includes:
  // - DOM EventTarget (Element, Document, Window)
  // - Node.js EventEmitter
  // - jQuery-like objects with on/off
  // - Objects with addEventListener/removeEventListener
  ```

## Functional Specification

**Concept**: Creates a cold Observable that, on subscription, calls `addEventListener(eventName, handler)` on the target. Each event fires a `next()` notification. On unsubscription, calls `removeEventListener(eventName, handler)` — automatic cleanup.

**Key properties**:
- **Lazy**: event listener is added only when subscribed, removed when unsubscribed
- **Cold**: each subscriber gets its own listener registration
- **Long-lived**: does not complete on its own — runs until unsubscribed
- **Type safety**: `fromEvent<MouseEvent>(el, 'click')` narrows the event type
- Works with any object following the `addEventListener`/`removeEventListener` pattern

**`options` parameter**: Passed directly to `addEventListener` — supports `{ capture: true }`, `{ passive: true }`, etc.

## Marble Diagram

```
fromEvent(button, 'click'):

User clicks:   -----click----click---------click---...
Result:        -----e1-------e2------------e3------...
               (MouseEvent)

No completion — stream runs indefinitely until unsubscribed.

Unsubscription:
.subscribe() → addEventListener called
.unsubscribe() → removeEventListener called automatically
```

## Type System Integration

```typescript
import { fromEvent } from 'rxjs';

// DOM events — infer from EventMap when element type is known
const button = document.querySelector('button')!;
fromEvent(button, 'click')           // Observable<Event>
fromEvent<MouseEvent>(button, 'click') // Observable<MouseEvent>

// More specific — HTMLElement events
const input = document.querySelector<HTMLInputElement>('input')!;
fromEvent<InputEvent>(input, 'input').subscribe(e => {
  console.log((e.target as HTMLInputElement).value);
});

// Node.js EventEmitter
import { EventEmitter } from 'events';
const emitter = new EventEmitter();
fromEvent<string>(emitter, 'data').subscribe(console.log);

// KeyboardEvent with capture options
fromEvent<KeyboardEvent>(document, 'keydown', { capture: true })
  .subscribe(e => console.log(e.key));
```

## Examples

### Basic Usage
```typescript
import { fromEvent } from 'rxjs';
import { map } from 'rxjs/operators';

// Button clicks
fromEvent(document.getElementById('btn')!, 'click').pipe(
  map((e: MouseEvent) => ({ x: e.clientX, y: e.clientY }))
).subscribe(pos => console.log('click at:', pos));

// Input changes
fromEvent<InputEvent>(document.querySelector('input')!, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
).subscribe(value => console.log('input:', value));

// Window resize
fromEvent(window, 'resize').pipe(
  map(() => ({ width: window.innerWidth, height: window.innerHeight }))
).subscribe(console.log);
```

### Common Pattern — Debounced Search
```typescript
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const searchInput = document.querySelector<HTMLInputElement>('#search')!;

fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query =>
    query.length >= 2
      ? ajax.getJSON<Result[]>(`/api/search?q=${encodeURIComponent(query)}`)
      : of([])
  )
).subscribe(results => renderResults(results));
```

### Common Pattern — Angular Component Teardown
```typescript
import { fromEvent, Subject } from 'rxjs';
import { takeUntil, throttleTime } from 'rxjs/operators';

@Component({ selector: 'app-scroll', template: '...' })
export class ScrollComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    fromEvent(window, 'scroll').pipe(
      throttleTime(16),   // ~60fps
      takeUntil(this.destroy$)
    ).subscribe(() => this.onScroll());
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    // fromEvent automatically removes the event listener via takeUntil teardown
  }

  private onScroll() { /* ... */ }
}
```

### Common Pattern — Keyboard Shortcut Handler
```typescript
import { fromEvent } from 'rxjs';
import { filter, map } from 'rxjs/operators';

// Ctrl+S save shortcut
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.ctrlKey && e.key === 's'),
  map(e => { e.preventDefault(); return e; })
).subscribe(() => saveDocument());

// Escape key to close modals
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.key === 'Escape')
).subscribe(() => closeModal());
```

### Common Pattern — Drag and Drop
```typescript
import { fromEvent, EMPTY } from 'rxjs';
import { switchMap, map, takeUntil } from 'rxjs/operators';

const draggable = document.querySelector('#draggable')!;

const mousedown$ = fromEvent<MouseEvent>(draggable, 'mousedown');
const mousemove$ = fromEvent<MouseEvent>(document, 'mousemove');
const mouseup$   = fromEvent<MouseEvent>(document, 'mouseup');

mousedown$.pipe(
  switchMap(start =>
    mousemove$.pipe(
      map(move => ({
        dx: move.clientX - start.clientX,
        dy: move.clientY - start.clientY
      })),
      takeUntil(mouseup$)
    )
  )
).subscribe(({ dx, dy }) => {
  draggable.style.transform = `translate(${dx}px, ${dy}px)`;
});
```

## Common Pitfalls

### Anti-pattern: Not Unsubscribing (Memory / Listener Leak)
```typescript
import { fromEvent } from 'rxjs';

// ❌ LEAK — event listener never removed
fromEvent(window, 'resize').subscribe(handleResize);
// addEventListener registered, subscription kept alive forever
// handleResize called on every resize for the lifetime of the page

// ✅ CORRECT — unsubscribe when done
import { takeUntil } from 'rxjs/operators';
const destroy$ = new Subject<void>();

fromEvent(window, 'resize').pipe(
  takeUntil(destroy$)
).subscribe(handleResize);

// When component/feature is torn down:
destroy$.next();
destroy$.complete(); // removeEventListener called automatically

// WHY: fromEvent.subscribe() calls addEventListener — each subscription is a
// live DOM listener. Without unsubscription (via takeUntil, take(n), or manual
// .unsubscribe()), the listener and its closure remain in memory and fire
// indefinitely. In SPAs and component frameworks, always clean up with
// takeUntil(destroy$) or the framework's equivalent.
```

### Anti-pattern: `fromEvent` Inside Component Logic Without Cleanup
```typescript
import { fromEvent } from 'rxjs';

class SearchComponent {
  // ❌ NO CLEANUP — listener leaks when component is destroyed and re-created
  setupSearch() {
    fromEvent(this.input, 'input').pipe(
      debounceTime(300)
    ).subscribe(this.search.bind(this));
    // If setupSearch() is called multiple times or component is destroyed,
    // orphaned listeners accumulate
  }
}

// ✅ CORRECT — track subscription and clean up
class SearchComponent {
  private subscription?: Subscription;

  setupSearch() {
    this.subscription?.unsubscribe(); // cancel any prior subscription
    this.subscription = fromEvent(this.input, 'input').pipe(
      debounceTime(300)
    ).subscribe(this.search.bind(this));
  }

  destroy() {
    this.subscription?.unsubscribe();
  }
}
```

## Related Operators

- **`fromEventPattern(addHandler, removeHandler)`**: For non-standard event APIs that don't follow `addEventListener`/`removeEventListener`
- **`interval`**: Creates a timer-based Observable (no event target)
- **`Subject`**: Manual event bridge — use when you need to push events imperatively rather than listen to a DOM target
- **`takeUntil`**: The standard Angular/React companion for `fromEvent` cleanup
- **`throttleTime` / `debounceTime`**: Almost always paired with `fromEvent` for rate-limiting DOM events

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/fromEvent](https://rxjs.dev/api/index/function/fromEvent)

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key teaching point**: `fromEvent` automatically removes the event listener on unsubscription — but only if you actually unsubscribe. Always pair with `takeUntil(destroy$)` or another completion mechanism in component contexts.
