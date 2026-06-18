# pairwise — Advanced Patterns

For `pairwise` fundamentals see the core [pairwise](./pairwise) doc. This page covers delta detection, velocity, trend analysis, and comparison pipelines.

---

## What `pairwise` Does

`pairwise` emits pairs of consecutive values `[previous, current]`. The first value is swallowed — no pair until the second emission.

```
Source:   --1--2--3--4--5--|
pairwise: ----[1,2]--[2,3]--[3,4]--[4,5]--|
```

This is the operator for "what changed between now and the last time?"

---

## Pattern 1: Delta / Diff Calculation

```typescript
import { pairwise } from 'rxjs/operators';

// Price change:
priceStream$.pipe(
  pairwise(),
  map(([prev, curr]) => ({
    price:  curr,
    delta:  curr - prev,
    pct:    ((curr - prev) / prev * 100).toFixed(2) + '%',
    trend:  curr > prev ? 'up' : curr < prev ? 'down' : 'flat'
  }))
).subscribe(renderPriceTicker);
```

---

## Pattern 2: Velocity (Rate of Change)

When each emission carries a timestamp, calculate rate:

```typescript
import { pairwise, map } from 'rxjs/operators';
import { timestamp } from 'rxjs/operators';

interface Measurement { value: number; time: number; }

measurements$.pipe(
  pairwise(),
  map(([a, b]: [Measurement, Measurement]) => ({
    velocity:     (b.value - a.value) / ((b.time - a.time) / 1000), // units/sec
    acceleration: undefined  // would need tripletwise for this
  }))
).subscribe(console.log);

// Without explicit timestamps — use timestamp() to add them:
sensorValues$.pipe(
  timestamp(),                                      // adds { value, timestamp }
  pairwise(),
  map(([a, b]) => (b.value - a.value) / (b.timestamp - a.timestamp) * 1000)
).subscribe(logVelocity);
```

---

## Pattern 3: Detecting Direction Change

```typescript
import { pairwise, filter, map } from 'rxjs/operators';

type Direction = 'up' | 'down' | 'flat';

function getDirection(prev: number, curr: number): Direction {
  if (curr > prev) return 'up';
  if (curr < prev) return 'down';
  return 'flat';
}

// Only emit when direction changes (trend reversal detection):
priceStream$.pipe(
  pairwise(),
  map(([prev, curr]) => getDirection(prev, curr)),
  distinctUntilChanged(),    // suppress same direction repeated
  pairwise(),                // now pair directions to detect change
  filter(([prev, curr]) => prev !== 'flat' && curr !== 'flat' && prev !== curr),
  map(([prev, curr]) => `Trend reversal: ${prev} → ${curr}`)
).subscribe(alertTrader);
```

---

## Pattern 4: Object Property Change Detection

```typescript
import { pairwise, map, filter } from 'rxjs/operators';

interface UserProfile { name: string; email: string; role: string; }

// Emit only changed fields:
profileState$.pipe(
  pairwise(),
  map(([prev, curr]) =>
    Object.keys(curr).filter(
      key => curr[key as keyof UserProfile] !== prev[key as keyof UserProfile]
    ) as (keyof UserProfile)[]
  ),
  filter(changed => changed.length > 0)
).subscribe(changed => auditLog(`Profile changed: ${changed.join(', ')}`));

// Full diff object:
profileState$.pipe(
  pairwise(),
  map(([prev, curr]) =>
    Object.fromEntries(
      Object.entries(curr)
        .filter(([k, v]) => v !== prev[k as keyof UserProfile])
        .map(([k, v]) => [k, { from: prev[k as keyof UserProfile], to: v }])
    )
  ),
  filter(diff => Object.keys(diff).length > 0)
).subscribe(auditDiff);
```

---

## Pattern 5: Scroll Direction Detection

```typescript
import { fromEvent } from 'rxjs';
import { pairwise, map, distinctUntilChanged } from 'rxjs/operators';

const scrollY$ = fromEvent(window, 'scroll').pipe(
  map(() => window.scrollY)
);

const scrollDirection$ = scrollY$.pipe(
  pairwise(),
  map(([prev, curr]) => curr > prev ? 'down' : 'up'),
  distinctUntilChanged()  // only emit on direction change
);

// Hide header on scroll down, show on scroll up:
scrollDirection$.subscribe(dir => {
  header.style.transform = dir === 'down'
    ? 'translateY(-100%)'
    : 'translateY(0)';
});
```

---

## Pattern 6: Animation Frame Delta

```typescript
import { animationFrames } from 'rxjs';
import { pairwise, map } from 'rxjs/operators';

// Calculate time between frames for smooth animation:
const frameDelta$ = animationFrames().pipe(
  pairwise(),
  map(([prev, curr]) => ({
    elapsed:  curr.elapsed - prev.elapsed,   // ms since last frame
    timestamp: curr.timestamp
  }))
);

frameDelta$.subscribe(({ elapsed }) => {
  const speed = 200; // pixels per second
  position += speed * (elapsed / 1000);
  sprite.style.left = `${position}px`;
});
```

---

## Pattern 7: Sequential Form Validation State Machine

Detect when validation transitions from invalid to valid (ready to submit):

```typescript
import { pairwise, filter, map } from 'rxjs/operators';

type ValidationState = 'pristine' | 'invalid' | 'valid';

formValidity$.pipe(
  pairwise(),
  filter(([prev, curr]) => prev !== 'valid' && curr === 'valid')
  // Only emits when form becomes valid for the first time
).subscribe(() => submitButton.disabled = false);

formValidity$.pipe(
  pairwise(),
  filter(([prev, curr]) => prev === 'valid' && curr !== 'valid')
  // Re-disable if user breaks a field after making it valid
).subscribe(() => submitButton.disabled = true);
```

---

## `pairwise` vs `bufferCount(2, 1)` vs `scan`

```typescript
// pairwise — [prev, curr], drops first value:
source$.pipe(pairwise())
// Emits: [1,2], [2,3], [3,4]

// bufferCount(2, 1) — same as pairwise but emits first value as single-element:
source$.pipe(bufferCount(2, 1))
// Emits: [1], [1,2], [2,3], [3,4]   ← includes the first item alone

// scan — rolling window of any size, keep previous N values:
source$.pipe(
  scan<number, number[]>((acc, curr) => [...acc.slice(-2), curr], [])
)
// Emits: [1], [1,2], [1,2,3], [2,3,4]   ← rolling window of 3
```

Use `pairwise` when you need exactly the last 2 values and don't want the incomplete first pair.

---

## `pairwise` for Undo/Redo Detection

```typescript
import { pairwise, filter } from 'rxjs/operators';

editorState$.pipe(
  pairwise(),
  filter(([prev, curr]) =>
    prev.content !== curr.content ||
    prev.cursor  !== curr.cursor
  ),
  map(([prev]) => prev)             // emit the state we left
).subscribe(history => undoStack.push(history));
```

---

## Common Pitfalls

### Expecting Emission on First Value

```typescript
// ❌ Misconception: pairwise emits immediately on first value
// Reality: first value is held; emission starts on second value

source$.pipe(pairwise()).subscribe(console.log);
// source emits 1 → nothing emitted
// source emits 2 → [1, 2] emitted
// source emits 3 → [2, 3] emitted

// ✅ If you need first value too, use startWith to seed a "previous":
source$.pipe(
  startWith(null),
  pairwise()           // now emits [null, firstValue] immediately
)
```

### Using `pairwise` on a Stream That Completes After One Value

```typescript
// ❌ http.get completes after one value — pairwise never emits:
this.http.get('/api/value').pipe(
  pairwise() // never emits! Only one value, needs two
)

// ✅ pairwise needs a continuous stream:
// Use on BehaviorSubject state, timer, fromEvent, etc.
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key pattern**: `pairwise()` followed by `map(([prev, curr]) => ...)` is the idiom for "what changed." Combine with `distinctUntilChanged()` after to suppress repeated identical deltas. Its main limitation is swallowing the first value — seed with `startWith(seed)` if you need the first emission.
