# Drag, Drop & Animation Patterns with RxJS

From basic drag-and-drop to spring physics and canvas animation — RxJS's event composition model excels here.

---

## Why RxJS for Animation and Interaction?

Animation and drag are inherently about **streams of events over time**:
- Drag = `pointerdown` → continuous `pointermove` → `pointerup`
- Animation = frames ticking via `requestAnimationFrame`
- Spring = position evolving over time from physics equations

RxJS models all of these naturally. The `switchMap(startEvent => moveEvents.pipe(takeUntil(endEvent)))` pattern is the canonical drag foundation.

---

## Pattern 1: Drag and Drop (Full Production)

```typescript
import { fromEvent, merge } from 'rxjs';
import { switchMap, takeUntil, map, tap, startWith } from 'rxjs/operators';

interface DragState {
  dragging: boolean;
  x: number;
  y: number;
  startX: number;
  startY: number;
}

function draggable(
  element: HTMLElement,
  container?: HTMLElement
): Observable<DragState> {
  const bound = container?.getBoundingClientRect();

  const pointerdown$ = fromEvent<PointerEvent>(element, 'pointerdown');
  const pointermove$ = fromEvent<PointerEvent>(document, 'pointermove');
  const pointerup$   = fromEvent<PointerEvent>(document, 'pointerup');

  return pointerdown$.pipe(
    tap(e => {
      e.preventDefault();
      element.setPointerCapture(e.pointerId);
    }),
    switchMap(start => {
      const offsetX = start.clientX - element.offsetLeft;
      const offsetY = start.clientY - element.offsetTop;

      return pointermove$.pipe(
        map(e => {
          let x = e.clientX - offsetX;
          let y = e.clientY - offsetY;

          // Constrain to container:
          if (bound) {
            x = Math.max(bound.left, Math.min(bound.right  - element.offsetWidth,  x));
            y = Math.max(bound.top,  Math.min(bound.bottom - element.offsetHeight, y));
          }

          return { dragging: true, x, y, startX: start.clientX, startY: start.clientY };
        }),
        takeUntil(pointerup$),
        startWith({ dragging: true, x: element.offsetLeft, y: element.offsetTop,
                    startX: start.clientX, startY: start.clientY }),
        finalize(() => {
          element.releasePointerCapture(start.pointerId);
        })
      );
    }),
    startWith({ dragging: false, x: element.offsetLeft, y: element.offsetTop,
                startX: 0, startY: 0 })
  );
}

// Usage:
draggable(panel, document.querySelector('#workspace')!).subscribe(state => {
  if (state.dragging) {
    panel.style.left = `${state.x}px`;
    panel.style.top  = `${state.y}px`;
  }
});
```

---

## Pattern 2: Drop Zones

```typescript
import { fromEvent, merge, EMPTY } from 'rxjs';
import { switchMap, takeUntil, map, distinctUntilChanged } from 'rxjs/operators';

function dropZones(
  draggable: HTMLElement,
  zones: HTMLElement[]
): Observable<{ zone: HTMLElement | null; dropped: boolean }> {
  const pointerdown$ = fromEvent<PointerEvent>(draggable, 'pointerdown');
  const pointermove$ = fromEvent<PointerEvent>(document, 'pointermove');
  const pointerup$   = fromEvent<PointerEvent>(document, 'pointerup');

  const getZoneUnderPointer = (x: number, y: number) =>
    zones.find(z => {
      const r = z.getBoundingClientRect();
      return x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
    }) ?? null;

  return pointerdown$.pipe(
    switchMap(() =>
      merge(
        pointermove$.pipe(
          map(e => ({
            zone:    getZoneUnderPointer(e.clientX, e.clientY),
            dropped: false
          })),
          distinctUntilChanged((a, b) => a.zone === b.zone),
          tap(({ zone }) => {
            zones.forEach(z => z.classList.remove('drop-target-active'));
            zone?.classList.add('drop-target-active');
          }),
          takeUntil(pointerup$)
        ),
        pointerup$.pipe(
          map(e => ({
            zone:    getZoneUnderPointer(e.clientX, e.clientY),
            dropped: true
          })),
          tap(() => zones.forEach(z => z.classList.remove('drop-target-active')))
        )
      )
    )
  );
}
```

---

## Pattern 3: Smooth Animation with `animationFrames`

```typescript
import { animationFrames } from 'rxjs';
import { map, takeWhile, pairwise } from 'rxjs/operators';

// Animate a value from `from` to `to` over `durationMs`:
function animateTo(
  from: number,
  to:   number,
  durationMs: number,
  easing = (t: number) => t  // linear by default
): Observable<number> {
  return animationFrames().pipe(
    map(({ elapsed }) => Math.min(elapsed / durationMs, 1)),
    takeWhile(t => t < 1, true),
    map(t => from + (to - from) * easing(t))
  );
}

// Easing functions:
const easeInOut = (t: number) => t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
const easeOut   = (t: number) => t * (2 - t);
const easeIn    = (t: number) => t * t;

// Usage:
animateTo(0, 300, 500, easeInOut).subscribe(x => {
  element.style.transform = `translateX(${x}px)`;
});
```

---

## Pattern 4: Spring Physics Animation

```typescript
import { animationFrames } from 'rxjs';
import { pairwise, scan, map, takeWhile } from 'rxjs/operators';

interface SpringState {
  position: number;
  velocity: number;
  target:   number;
}

function spring(
  from:      number,
  to:        number,
  stiffness  = 0.1,
  damping    = 0.8
): Observable<number> {
  const initial: SpringState = { position: from, velocity: 0, target: to };

  return animationFrames().pipe(
    pairwise(),
    map(([prev, curr]) => (curr.timestamp - prev.timestamp) / 16), // normalized delta
    scan((state, dt): SpringState => {
      const force    = (state.target - state.position) * stiffness;
      const velocity = (state.velocity + force) * damping;
      const position = state.position + velocity * dt;
      return { position, velocity, target: state.target };
    }, initial),
    map(s => s.position),
    takeWhile(pos => Math.abs(pos - to) > 0.01, true) // stop when settled
  );
}

// Usage:
spring(0, 300, 0.08, 0.75).subscribe(x => {
  element.style.transform = `translateX(${x}px)`;
});
```

---

## Pattern 5: Parallax Scroll

```typescript
import { fromEvent } from 'rxjs';
import { map, throttleTime, animationFrameScheduler, distinctUntilChanged } from 'rxjs/operators';

const layers = document.querySelectorAll<HTMLElement>('[data-parallax-speed]');

fromEvent(window, 'scroll').pipe(
  throttleTime(0, animationFrameScheduler),   // sync with rAF
  map(() => window.scrollY),
  distinctUntilChanged()
).subscribe(scrollY => {
  layers.forEach(layer => {
    const speed = parseFloat(layer.dataset['parallaxSpeed'] ?? '0.5');
    layer.style.transform = `translateY(${scrollY * speed}px)`;
  });
});
```

---

## Pattern 6: Canvas Particle System

```typescript
import { animationFrames, fromEvent } from 'rxjs';
import { pairwise, map, withLatestFrom, scan, startWith } from 'rxjs/operators';

interface Particle { x: number; y: number; vx: number; vy: number; life: number; }

const canvas  = document.querySelector<HTMLCanvasElement>('canvas')!;
const ctx     = canvas.getContext('2d')!;

const mouse$ = fromEvent<MouseEvent>(canvas, 'mousemove').pipe(
  map(e => ({ x: e.offsetX, y: e.offsetY })),
  startWith({ x: 0, y: 0 })
);

const particles$ = animationFrames().pipe(
  pairwise(),
  map(([a, b]) => (b.timestamp - a.timestamp) / 1000), // dt in seconds
  withLatestFrom(mouse$),
  scan((particles: Particle[], [dt, mouse]) => {
    // Spawn new particle at mouse:
    const newParticle: Particle = {
      x: mouse.x, y: mouse.y,
      vx: (Math.random() - 0.5) * 100,
      vy: -Math.random() * 150,
      life: 1.0
    };

    // Update + filter alive:
    return [...particles, newParticle]
      .map(p => ({
        ...p,
        x:    p.x + p.vx * dt,
        y:    p.y + p.vy * dt,
        vy:   p.vy + 200 * dt, // gravity
        life: p.life - dt
      }))
      .filter(p => p.life > 0);
  }, [])
);

particles$.subscribe(particles => {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  particles.forEach(p => {
    ctx.globalAlpha = p.life;
    ctx.fillStyle   = `hsl(${p.life * 60}, 80%, 60%)`;
    ctx.fillRect(p.x, p.y, 4, 4);
  });
});
```

---

## Pattern 7: Gesture Velocity (Fling/Swipe)

Calculate velocity on pointer release to implement fling gestures:

```typescript
import { fromEvent } from 'rxjs';
import { switchMap, takeUntil, pairwise, last, map, timestamp } from 'rxjs/operators';

const pointerdown$ = fromEvent<PointerEvent>(element, 'pointerdown');
const pointermove$ = fromEvent<PointerEvent>(document, 'pointermove');
const pointerup$   = fromEvent<PointerEvent>(document, 'pointerup');

const fling$ = pointerdown$.pipe(
  switchMap(() =>
    pointermove$.pipe(
      timestamp(),
      pairwise(),
      map(([prev, curr]) => ({
        vx: (curr.value.clientX - prev.value.clientX) / (curr.timestamp - prev.timestamp),
        vy: (curr.value.clientY - prev.value.clientY) / (curr.timestamp - prev.timestamp)
      })),
      takeUntil(pointerup$),
      last()    // take only the velocity at release
    )
  )
);

fling$.subscribe(({ vx, vy }) => {
  if (Math.abs(vx) > 0.5) swipeHorizontal(vx > 0 ? 'right' : 'left');
  if (Math.abs(vy) > 0.5) swipeVertical(vy > 0 ? 'down' : 'up');
});
```

---

## Key Operator Combinations for Interaction

| Pattern | Core operators |
|---|---|
| Basic drag | `switchMap`, `takeUntil`, `map` |
| Constrained drag | `switchMap`, `takeUntil`, `map` (with bounds clamp) |
| Drop zone hover | `distinctUntilChanged`, `tap` for class toggle |
| Linear animation | `animationFrames`, `takeWhile(inclusive)`, `map` |
| Spring physics | `animationFrames`, `pairwise`, `scan` |
| Parallax | `throttleTime(0, animationFrameScheduler)`, `distinctUntilChanged` |
| Swipe velocity | `pairwise`, `timestamp`, `last` |

---

## Common Pitfalls

### Not Using `setPointerCapture`

```typescript
// ❌ Without pointer capture, fast mouse moves lose the element:
pointerdown$.pipe(
  switchMap(() => pointermove$.pipe(takeUntil(pointerup$)))
)
// Mouse moves faster than element → pointermove fires on other elements → drag breaks

// ✅ Capture pointer to element on start:
pointerdown$.pipe(
  tap(e => element.setPointerCapture(e.pointerId)),
  switchMap(() => pointermove$.pipe(
    takeUntil(pointerup$),
    finalize(() => element.releasePointerCapture(...))
  ))
)
```

### Using `mousemove` Instead of `pointermove`

```typescript
// ❌ mousemove doesn't work for touch:
fromEvent(document, 'mousemove')

// ✅ pointermove unifies mouse, touch, and stylus:
fromEvent<PointerEvent>(document, 'pointermove')
// Also: add touch-action: none CSS to prevent scroll-interference on touch devices
```
