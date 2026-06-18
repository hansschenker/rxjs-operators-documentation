# animationFrames

## Identity

- **Name**: animationFrames
- **Category**: Creation Operators
- **Type**: Animation frame stream — emits on every `requestAnimationFrame` tick with elapsed time
- **Import**:
  ```typescript
  import { animationFrames } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function animationFrames(
    timestampProvider?: TimestampProvider
  ): Observable<{ timestamp: number; elapsed: number }>
  ```
- **Added**: RxJS 7.0

## Functional Specification

`animationFrames()` returns a cold Observable that emits on every animation frame. Each emission carries:
- `timestamp` — the DOMHighResTimeStamp from `requestAnimationFrame` (ms since page load)
- `elapsed` — ms since the Observable was subscribed to

The Observable runs indefinitely — use `takeUntil`, `take`, or `takeWhile` to stop it. Unsubscribing calls `cancelAnimationFrame` automatically.

**Why `animationFrames` over `interval(0, animationFrameScheduler)`**:

| | `animationFrames()` | `interval(0, animationFrameScheduler)` |
|---|---|---|
| Timing data | `timestamp` + `elapsed` included | No timing data |
| Cancel on unsub | Yes | Yes |
| Pauses when tab hidden | Yes (browser throttles rAF) | Depends on scheduler |
| Use when | Need elapsed time for animation | Just need frame-rate ticks |

## Marble Diagram

```
subscribe at t=0:
  frame 1 (t=16ms):  emit { timestamp: 16,  elapsed: 16  }
  frame 2 (t=33ms):  emit { timestamp: 33,  elapsed: 33  }
  frame 3 (t=50ms):  emit { timestamp: 50,  elapsed: 50  }
  ...
  unsubscribe → cancelAnimationFrame called
```

## Examples

### Basic Usage — Smooth Counter Animation
```typescript
import { animationFrames } from 'rxjs';
import { map, takeWhile } from 'rxjs/operators';

function animateCounter(from: number, to: number, durationMs: number) {
  animationFrames().pipe(
    map(({ elapsed }) => elapsed / durationMs),   // 0 → 1 progress
    takeWhile(progress => progress < 1, true),     // inclusive: emit final frame
    map(progress => Math.round(from + (to - from) * Math.min(progress, 1)))
  ).subscribe(value => {
    counterEl.textContent = String(value);
  });
}

animateCounter(0, 1000, 2000); // count from 0 to 1000 over 2 seconds
```

### Common Pattern — CSS Transition via JS
```typescript
import { animationFrames } from 'rxjs';
import { map, takeWhile } from 'rxjs/operators';

function fadeIn(el: HTMLElement, durationMs = 300) {
  el.style.opacity = '0';
  el.style.display = 'block';

  animationFrames().pipe(
    map(({ elapsed }) => Math.min(elapsed / durationMs, 1)),
    takeWhile(opacity => opacity < 1, true) // inclusive final frame
  ).subscribe({
    next:     opacity  => { el.style.opacity = String(opacity); },
    complete: ()       => { el.style.opacity = '1'; }
  });
}
```

### Common Pattern — Game Loop
```typescript
import { animationFrames } from 'rxjs';
import { withLatestFrom, map } from 'rxjs/operators';

const keys$ = new BehaviorSubject<Set<string>>(new Set());

// Game loop: runs at display refresh rate
animationFrames().pipe(
  withLatestFrom(keys$),
  map(([{ elapsed, timestamp }, keys]) => ({
    dt: elapsed,  // delta time for physics
    keys,
    timestamp
  }))
).subscribe(({ dt, keys }) => {
  updatePhysics(dt);
  renderFrame(keys);
});
```

### Common Pattern — Easing Functions
```typescript
import { animationFrames } from 'rxjs';
import { map, takeWhile } from 'rxjs/operators';

// Ease-in-out cubic
function easeInOut(t: number): number {
  return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
}

function animatePosition(el: HTMLElement, from: number, to: number, ms = 500) {
  animationFrames().pipe(
    map(({ elapsed }) => easeInOut(Math.min(elapsed / ms, 1))),
    takeWhile(t => t < 1, true)
  ).subscribe(t => {
    el.style.transform = `translateX(${from + (to - from) * t}px)`;
  });
}
```

## Common Pitfalls

### Anti-pattern: Not Unsubscribing
```typescript
import { animationFrames } from 'rxjs';

// ❌ MEMORY LEAK — animationFrames runs forever
animationFrames().subscribe(({ elapsed }) => render(elapsed));
// runs until the page is closed — no cleanup

// ✅ CORRECT — always bound with takeUntil, take, or takeWhile
import { takeUntil, Subject } from 'rxjs';
const destroy$ = new Subject<void>();

animationFrames().pipe(
  takeUntil(destroy$)
).subscribe(({ elapsed }) => render(elapsed));

// On component destroy:
destroy$.next();
destroy$.complete();

// WHY: animationFrames never completes. Without a termination condition,
// it holds a rAF loop open indefinitely, preventing GC of the subscriber.
```

## Related Operators

- **`interval(0, animationFrameScheduler)`**: Frame-rate ticks without timing data
- **`observeOn(animationFrameScheduler)`**: Move downstream work to animation frame context
- **`timer(0, animationFrameScheduler)`**: Single deferred emission on next frame

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/animationFrames](https://rxjs.dev/api/index/function/animationFrames)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching points**:
1. Always terminate — `animationFrames` never completes on its own
2. `elapsed` is ms since subscription — use it for progress-based animations (`elapsed / duration → 0..1`)
3. Browser throttles rAF when tab is hidden — animations pause automatically
