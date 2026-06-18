# animationFrames — Advanced Patterns

For `animationFrames` fundamentals see the core [animationFrames](./animationFrames) doc. This page covers game loops, physics simulations, canvas rendering, spring animations, and the full `animationFrameScheduler` toolkit.

---

## What `animationFrames` Provides

```typescript
import { animationFrames } from 'rxjs';

animationFrames().subscribe(({ timestamp, elapsed }) => {
  // timestamp: DOMHighResTimeStamp from requestAnimationFrame
  // elapsed:   ms since first subscription
  render(elapsed);
});
```

`animationFrames()` emits on every browser paint frame (~60fps). It uses `requestAnimationFrame` under the hood and automatically unsubscribes (cancels rAF) when the Observable is torn down.

---

## Pattern 1: Fixed-Step Game Loop

```typescript
import { animationFrames } from 'rxjs';
import { pairwise, map, scan, takeUntilDestroyed } from 'rxjs/operators';

interface GameState {
  entities: Entity[];
  score:    number;
  running:  boolean;
}

// Compute dt (delta time) between frames for physics stability:
animationFrames().pipe(
  pairwise(),
  map(([prev, curr]) => ({
    dt:        Math.min((curr.timestamp - prev.timestamp) / 1000, 0.05), // cap at 50ms
    timestamp: curr.timestamp
  })),
  scan((state: GameState, { dt }) => {
    if (!state.running) return state;
    return {
      ...state,
      entities: state.entities.map(e => updateEntity(e, dt)),
      score:    state.score + detectCollisions(state.entities)
    };
  }, initialGameState),
  takeUntilDestroyed()
).subscribe(state => {
  clearCanvas(ctx);
  state.entities.forEach(e => drawEntity(ctx, e));
  drawHUD(ctx, state.score);
});
```

---

## Pattern 2: Spring Physics Animation

Smooth spring animation using Hooke's law with damping:

```typescript
import { animationFrames } from 'rxjs';
import { scan, map, takeWhile, shareReplay } from 'rxjs/operators';

interface SpringState {
  position:   number;
  velocity:   number;
  target:     number;
}

function springAnimation(
  from:    number,
  to:      number,
  stiffness = 170,
  damping   = 26
): Observable<number> {
  const MASS = 1;

  return animationFrames().pipe(
    pairwise(),
    map(([prev, curr]) => Math.min((curr.timestamp - prev.timestamp) / 1000, 0.05)),
    scan((state: SpringState, dt) => {
      const force      = -stiffness * (state.position - state.target);
      const damper     = -damping * state.velocity;
      const acceleration = (force + damper) / MASS;
      const velocity   = state.velocity + acceleration * dt;
      const position   = state.position + velocity * dt;
      return { position, velocity, target: state.target };
    }, { position: from, velocity: 0, target: to }),
    map(s => s.position),
    takeWhile(
      (pos, i) => {
        // Stop when settled (close to target, low velocity):
        return i < 5 || Math.abs(pos - to) > 0.01;
      },
      true // inclusive — emit the final settled value
    )
  );
}

// Usage — animate panel sliding in:
const panel = document.querySelector('.panel') as HTMLElement;

springAnimation(300, 0, 180, 28).pipe(
  takeUntilDestroyed()
).subscribe(x => {
  panel.style.transform = `translateX(${x}px)`;
});
```

---

## Pattern 3: Particle System

```typescript
import { animationFrames, combineLatest, BehaviorSubject } from 'rxjs';
import { scan, map, pairwise, takeUntilDestroyed } from 'rxjs/operators';

interface Particle {
  x: number; y: number;
  vx: number; vy: number;
  life: number; // 0–1, decreases over time
  color: string;
}

const emitter$ = new BehaviorSubject<{ x: number; y: number } | null>(null);

animationFrames().pipe(
  pairwise(),
  map(([prev, curr]) => ({
    dt:       Math.min((curr.timestamp - prev.timestamp) / 1000, 0.05),
    emitPos:  emitter$.getValue()
  })),
  scan((particles: Particle[], { dt, emitPos }) => {
    // Spawn new particles from emitter:
    const spawned: Particle[] = emitPos ? Array.from({ length: 3 }, () => ({
      x:     emitPos.x,
      y:     emitPos.y,
      vx:    (Math.random() - 0.5) * 200,
      vy:    -Math.random() * 300 - 100,
      life:  1,
      color: `hsl(${Math.random() * 60 + 20}, 100%, 60%)`
    })) : [];

    // Update existing + filter dead:
    const updated = particles
      .map(p => ({
        ...p,
        x:    p.x + p.vx * dt,
        y:    p.y + p.vy * dt + 0.5 * 400 * dt * dt, // gravity
        vy:   p.vy + 400 * dt,
        life: p.life - dt * 1.5
      }))
      .filter(p => p.life > 0);

    return [...updated, ...spawned].slice(0, 500); // cap particle count
  }, []),
  takeUntilDestroyed()
).subscribe(particles => {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  particles.forEach(p => {
    ctx.globalAlpha = p.life;
    ctx.fillStyle   = p.color;
    ctx.fillRect(p.x - 2, p.y - 2, 4, 4);
  });
  ctx.globalAlpha = 1;
});

// Move emitter with pointer:
fromEvent<PointerEvent>(canvas, 'pointermove').subscribe(e => {
  emitter$.next({ x: e.offsetX, y: e.offsetY });
});
```

---

## Pattern 4: Canvas Animation with Off-Screen Rendering

```typescript
import { animationFrames, Subject } from 'rxjs';
import { switchMap, takeUntilDestroyed, map } from 'rxjs/operators';

class CanvasRenderer {
  private offscreen = new OffscreenCanvas(800, 600);
  private ctx       = this.offscreen.getContext('2d')!;
  private drawCmds$ = new Subject<DrawCommand[]>();

  readonly frame$ = animationFrames().pipe(
    // Render latest draw commands each frame:
    withLatestFrom(this.drawCmds$.pipe(startWith([]))),
    map(([, cmds]) => cmds),
    takeUntilDestroyed()
  );

  start(canvas: HTMLCanvasElement): Subscription {
    const mainCtx = canvas.getContext('2d')!;

    return this.frame$.subscribe(cmds => {
      // Render to offscreen buffer:
      this.ctx.clearRect(0, 0, 800, 600);
      cmds.forEach(cmd => executeCommand(this.ctx, cmd));

      // Blit to visible canvas:
      mainCtx.clearRect(0, 0, canvas.width, canvas.height);
      mainCtx.drawImage(this.offscreen, 0, 0);
    });
  }

  update(cmds: DrawCommand[]): void {
    this.drawCmds$.next(cmds);
  }
}
```

---

## Pattern 5: `animationFrameScheduler` for DOM Updates

Use `animationFrameScheduler` to batch Observable-driven DOM updates to paint frames:

```typescript
import { animationFrameScheduler } from 'rxjs';
import { observeOn, auditTime } from 'rxjs/operators';

// Batch rapid state changes to one DOM update per frame:
stateChanges$.pipe(
  auditTime(0, animationFrameScheduler), // coalesce to next rAF
  takeUntilDestroyed()
).subscribe(state => updateDOM(state));

// Or use observeOn to schedule delivery at next animation frame:
highFrequencyData$.pipe(
  observeOn(animationFrameScheduler),
  takeUntilDestroyed()
).subscribe(data => {
  // This subscriber callback runs inside requestAnimationFrame
  renderChart(data);
});
```

---

## Pattern 6: Pause/Resume Animation

```typescript
import { animationFrames, BehaviorSubject, EMPTY } from 'rxjs';
import { switchMap, scan, pairwise, map } from 'rxjs/operators';

const paused$ = new BehaviorSubject(false);

const animation$ = paused$.pipe(
  switchMap(paused =>
    paused
      ? EMPTY
      : animationFrames().pipe(
          pairwise(),
          map(([prev, curr]) => curr.timestamp - prev.timestamp)
        )
  )
);

let accumulatedTime = 0;

animation$.pipe(
  scan((elapsed, dt) => elapsed + dt, 0),
  takeUntilDestroyed()
).subscribe(elapsed => renderFrame(elapsed));

// Toggle pause:
pauseButton$.subscribe(() => paused$.next(!paused$.getValue()));
```

---

## `animationFrames()` vs `interval(0, animationFrameScheduler)` vs `defer`

```typescript
// animationFrames() — built-in, provides timestamp and elapsed:
animationFrames().subscribe(({ timestamp, elapsed }) => ...)
// ✓ Provides elapsed time since subscription
// ✓ Auto-teardown cancels rAF
// ✓ Most ergonomic for animation

// interval(0, animationFrameScheduler) — fires on each frame, no metadata:
interval(0, animationFrameScheduler).subscribe(frameIndex => ...)
// ✓ Works with existing interval-based code
// ✗ No timestamp, no elapsed time

// defer(() => ...) inside rAF — manual, full control:
new Observable(subscriber => {
  let rafId: number;
  const loop = (ts: number) => {
    subscriber.next(ts);
    rafId = requestAnimationFrame(loop);
  };
  rafId = requestAnimationFrame(loop);
  return () => cancelAnimationFrame(rafId);
})
// ✓ Maximum control
// ✗ Verbose; reinvents animationFrames()
```

---

## Common Pitfalls

### Not Capping Delta Time (Spiral of Death)

```typescript
// ❌ Using raw dt — tab backgrounded for 10s causes dt = 10000ms:
animationFrames().pipe(
  pairwise(),
  map(([a, b]) => b.timestamp - a.timestamp), // dt could be huge
  scan((state, dt) => physicsUpdate(state, dt), init)
)
// Physics explodes with large dt — entities teleport across screen

// ✅ Always cap dt:
map(([a, b]) => Math.min((b.timestamp - a.timestamp) / 1000, 0.05)) // max 50ms
```

### Running Heavy Logic Inside the Frame Callback

```typescript
// ❌ Expensive computation inside rAF callback — causes frame drops:
animationFrames().subscribe(({ elapsed }) => {
  const result = heavyComputation(elapsed); // blocks rAF budget
  render(result);
});

// ✅ Compute in Web Worker, animate with result:
animationFrames().pipe(
  withLatestFrom(workerResult$) // always use latest computed result
).subscribe(([, result]) => render(result));
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 5/5
**Key insight**: `animationFrames()` + `pairwise()` + `map(dt)` + `scan(physics)` is the complete recipe for any RxJS-driven animation or game loop. The composability with `takeWhile`, `BehaviorSubject` gates, and `withLatestFrom` makes it far more flexible than imperative `requestAnimationFrame` loops.
