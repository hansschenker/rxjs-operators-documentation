# Accessibility Patterns with RxJS

Keyboard navigation, focus management, screen reader announcements, and accessible interactive widgets — all built with RxJS event streams.

---

## Pattern 1: Keyboard Navigation (Arrow Keys)

Roving tabindex navigation for lists and menus:

```typescript
import { fromEvent, merge } from 'rxjs';
import { filter, map, scan, distinctUntilChanged } from 'rxjs/operators';

interface NavState { index: number; total: number; }

function arrowKeyNav(
  container: HTMLElement,
  itemSelector: string,
  orientation: 'vertical' | 'horizontal' | 'both' = 'vertical'
): Observable<number> {
  const items = () => Array.from(container.querySelectorAll<HTMLElement>(itemSelector));

  const KEYS = {
    vertical:   { prev: 'ArrowUp',   next: 'ArrowDown'  },
    horizontal: { prev: 'ArrowLeft', next: 'ArrowRight' },
    both:       { prev: 'ArrowUp',   next: 'ArrowDown',
                  prevH: 'ArrowLeft', nextH: 'ArrowRight' }
  }[orientation];

  const keydown$ = fromEvent<KeyboardEvent>(container, 'keydown');

  const move$ = keydown$.pipe(
    filter(e => Object.values(KEYS).includes(e.key)),
    map(e => {
      e.preventDefault();
      return [KEYS.prev, (KEYS as any).prevH].includes(e.key) ? -1 : 1;
    })
  );

  const home$ = keydown$.pipe(
    filter(e => e.key === 'Home'),
    map(() => 'HOME' as const)
  );

  const end$ = keydown$.pipe(
    filter(e => e.key === 'End'),
    map(() => 'END' as const)
  );

  return merge(move$, home$, end$).pipe(
    scan((state: NavState, action) => {
      const total = items().length;
      if (action === 'HOME') return { index: 0, total };
      if (action === 'END')  return { index: total - 1, total };
      const next = (state.index + action + total) % total; // wrap around
      return { index: next, total };
    }, { index: 0, total: items().length }),
    map(s => s.index),
    distinctUntilChanged(),
    tap(index => {
      const els = items();
      els.forEach((el, i) => {
        el.setAttribute('tabindex', i === index ? '0' : '-1');
        if (i === index) el.focus();
      });
    })
  );
}

// Usage — accessible dropdown menu:
arrowKeyNav(menuContainer, '[role="menuitem"]').pipe(
  takeUntilDestroyed()
).subscribe(index => highlightMenuItem(index));
```

---

## Pattern 2: Focus Trap (Modal / Dialog)

Keep focus within a modal while it's open:

```typescript
import { fromEvent, merge } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

function trapFocus(modal: HTMLElement): Observable<void> {
  const FOCUSABLE = 'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])';

  const getFocusable = () =>
    Array.from(modal.querySelectorAll<HTMLElement>(FOCUSABLE))
         .filter(el => !el.hasAttribute('disabled'));

  return fromEvent<KeyboardEvent>(document, 'keydown').pipe(
    filter(e => e.key === 'Tab'),
    tap(e => {
      const focusable = getFocusable();
      if (focusable.length === 0) { e.preventDefault(); return; }

      const first = focusable[0];
      const last  = focusable[focusable.length - 1];

      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    }),
    map(() => void 0)
  );
}

// Usage with modal open/close lifecycle:
const modalClose$ = new Subject<void>();

openModal$.pipe(
  switchMap(modal => {
    const previousFocus = document.activeElement as HTMLElement;

    // Focus first focusable element in modal:
    const firstFocusable = modal.querySelector<HTMLElement>(
      'button, [href], input, [tabindex]:not([tabindex="-1"])'
    );
    firstFocusable?.focus();

    return trapFocus(modal).pipe(
      takeUntil(modalClose$),
      finalize(() => previousFocus?.focus()) // restore focus on close
    );
  })
).subscribe();
```

---

## Pattern 3: Screen Reader Live Region Announcements

Announce dynamic content changes to screen readers:

```typescript
class LiveRegionAnnouncer {
  private polite$   = new Subject<string>();
  private assertive$ = new Subject<string>();

  private region: HTMLElement;

  constructor() {
    this.region = document.createElement('div');
    this.region.setAttribute('aria-live', 'polite');
    this.region.setAttribute('aria-atomic', 'true');
    this.region.style.cssText = 'position:absolute;left:-9999px;width:1px;height:1px;overflow:hidden';
    document.body.appendChild(this.region);

    // Polite: brief pause allows screen reader to finish current sentence:
    this.polite$.pipe(
      debounceTime(100),
      tap(msg => {
        this.region.textContent = '';
        setTimeout(() => { this.region.textContent = msg; }, 50);
      })
    ).subscribe();

    // Assertive: interrupt immediately:
    this.assertive$.pipe(
      tap(msg => {
        this.region.setAttribute('aria-live', 'assertive');
        this.region.textContent = msg;
        setTimeout(() => this.region.setAttribute('aria-live', 'polite'), 1000);
      })
    ).subscribe();
  }

  announce(message: string, priority: 'polite' | 'assertive' = 'polite'): void {
    priority === 'assertive'
      ? this.assertive$.next(message)
      : this.polite$.next(message);
  }
}

const announcer = new LiveRegionAnnouncer();

// Announce search results:
searchResults$.pipe(
  map(results => `${results.length} results found`),
  distinctUntilChanged(),
  takeUntilDestroyed()
).subscribe(msg => announcer.announce(msg));

// Announce errors urgently:
formErrors$.pipe(
  filter(errors => Object.keys(errors).length > 0),
  map(errors => `Form error: ${Object.values(errors)[0]}`),
  takeUntilDestroyed()
).subscribe(msg => announcer.announce(msg, 'assertive'));
```

---

## Pattern 4: Keyboard Shortcut System

```typescript
import { fromEvent } from 'rxjs';
import { filter, map, share } from 'rxjs/operators';

interface Shortcut {
  key:      string;
  ctrl?:    boolean;
  shift?:   boolean;
  alt?:     boolean;
  meta?:    boolean;
  prevent?: boolean;
}

function shortcut$(shortcut: Shortcut): Observable<KeyboardEvent> {
  return fromEvent<KeyboardEvent>(document, 'keydown').pipe(
    filter(e =>
      e.key.toLowerCase() === shortcut.key.toLowerCase() &&
      !!e.ctrlKey  === !!shortcut.ctrl  &&
      !!e.shiftKey === !!shortcut.shift &&
      !!e.altKey   === !!shortcut.alt   &&
      !!e.metaKey  === !!shortcut.meta
    ),
    filter(e => {
      // Don't fire inside text inputs:
      const tag = (e.target as HTMLElement).tagName;
      return !['INPUT', 'TEXTAREA', 'SELECT'].includes(tag);
    }),
    tap(e => { if (shortcut.prevent) e.preventDefault(); }),
    share()
  );
}

// Register application shortcuts:
shortcut$({ key: '/', prevent: true }).pipe(
  takeUntilDestroyed()
).subscribe(() => searchBox.focus());

shortcut$({ key: 'Escape' }).pipe(
  takeUntilDestroyed()
).subscribe(() => closeActiveModal());

shortcut$({ key: 'k', ctrl: true, prevent: true }).pipe(
  takeUntilDestroyed()
).subscribe(() => openCommandPalette());

shortcut$({ key: '?', shift: true, prevent: true }).pipe(
  takeUntilDestroyed()
).subscribe(() => openKeyboardShortcutsHelp());
```

---

## Pattern 5: Accessible Combobox (Autocomplete)

```typescript
@Component({
  template: `
    <div role="combobox" [attr.aria-expanded]="(open$ | async)"
         aria-haspopup="listbox">
      <input #input
        [attr.aria-activedescendant]="(activeId$ | async)"
        aria-autocomplete="list"
        (input)="query$.next($event.target.value)"
        (keydown)="keydown$.next($event)" />
      <ul role="listbox" *ngIf="open$ | async">
        <li *ngFor="let opt of options$ | async; let i = index"
            [id]="'opt-' + i"
            role="option"
            [attr.aria-selected]="(activeIndex$ | async) === i"
            (click)="select(opt)">
          {{ opt.label }}
        </li>
      </ul>
    </div>
  `
})
export class AccessibleComboboxComponent {
  query$      = new Subject<string>();
  keydown$    = new Subject<KeyboardEvent>();

  options$    = this.query$.pipe(
    debounceTime(200),
    switchMap(q => q.length < 2 ? of([]) : this.api.search(q)),
    shareReplay(1)
  );

  open$       = this.options$.pipe(map(opts => opts.length > 0));

  activeIndex$ = this.keydown$.pipe(
    withLatestFrom(this.options$),
    scan((idx, [event, options]) => {
      if (event.key === 'ArrowDown') { event.preventDefault(); return Math.min(idx + 1, options.length - 1); }
      if (event.key === 'ArrowUp')   { event.preventDefault(); return Math.max(idx - 1, 0); }
      if (event.key === 'Escape')    { return -1; }
      return idx;
    }, -1),
    startWith(-1),
    distinctUntilChanged()
  );

  activeId$ = combineLatest([this.activeIndex$, this.options$]).pipe(
    map(([idx]) => idx >= 0 ? `opt-${idx}` : null)
  );

  select(option: Option): void { /* ... */ }
}
```

---

## Pattern 6: Reduced Motion Preference

Respect `prefers-reduced-motion` reactively:

```typescript
function prefersReducedMotion$(): Observable<boolean> {
  const mq = window.matchMedia('(prefers-reduced-motion: reduce)');

  return new Observable<boolean>(subscriber => {
    subscriber.next(mq.matches);
    const handler = (e: MediaQueryListEvent) => subscriber.next(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }).pipe(
    distinctUntilChanged(),
    shareReplay(1)
  );
}

// Conditionally animate:
combineLatest([animationTrigger$, prefersReducedMotion$()]).pipe(
  map(([trigger, reduced]) => reduced ? 'instant' : 'animated'),
  takeUntilDestroyed()
).subscribe(mode => applyTransition(mode));
```

---

## Common Pitfalls

### Keyboard Events Firing Inside Text Inputs

```typescript
// ❌ Global shortcut fires while user types in a search box:
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.key === '/')
).subscribe(() => focusSearch()); // fires while typing "/" in input!

// ✅ Exclude focused input elements:
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.key === '/'),
  filter(e => {
    const tag = (e.target as HTMLElement).tagName;
    return !['INPUT', 'TEXTAREA', 'SELECT'].includes(tag) &&
           !(e.target as HTMLElement).isContentEditable;
  })
)
```
