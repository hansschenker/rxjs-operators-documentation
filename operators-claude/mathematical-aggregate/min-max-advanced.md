# min / max — Advanced Patterns

For fundamentals see the core [min / max](./min-max) doc. This page covers custom comparators, running extremes with `scan`, multi-field comparisons, and stream analytics.

---

## Built-In Behaviour

```typescript
import { min, max } from 'rxjs/operators';

// Primitives — emits on completion:
of(3, 1, 4, 1, 5, 9).pipe(max()).subscribe(v => console.log(v)); // 9
of(3, 1, 4, 1, 5, 9).pipe(min()).subscribe(v => console.log(v)); // 1

// Custom comparator:
of({ score: 3 }, { score: 1 }, { score: 9 }).pipe(
  max((a, b) => a.score - b.score)
).subscribe(v => console.log(v)); // { score: 9 }
```

Both operators **wait for completion** before emitting. For running min/max use `scan`.

---

## Pattern 1: Running Min / Max with `scan`

Emit the current extreme after each value — works on infinite streams:

```typescript
import { scan, map, distinctUntilChanged } from 'rxjs/operators';

function runningMax<T>(compare: (a: T, b: T) => number = (a: any, b: any) => a - b) {
  return (source$: Observable<T>) =>
    source$.pipe(
      scan((currentMax, value) =>
        currentMax === undefined || compare(value, currentMax) > 0
          ? value
          : currentMax
      )
    );
}

function runningMin<T>(compare: (a: T, b: T) => number = (a: any, b: any) => a - b) {
  return (source$: Observable<T>) =>
    source$.pipe(
      scan((currentMin, value) =>
        currentMin === undefined || compare(value, currentMin) < 0
          ? value
          : currentMin
      )
    );
}

// Live price high/low tracker:
priceStream$.pipe(
  runningMax(),
  distinctUntilChanged(),
  takeUntilDestroyed()
).subscribe(high => updateDayHigh(high));

priceStream$.pipe(
  runningMin(),
  distinctUntilChanged(),
  takeUntilDestroyed()
).subscribe(low => updateDayLow(low));
```

---

## Pattern 2: Running Min AND Max Together

Track both extremes with a single `scan`:

```typescript
interface Range { min: number; max: number; }

const priceRange$ = priceStream$.pipe(
  scan(
    (range: Range | null, price) => ({
      min: range ? Math.min(range.min, price) : price,
      max: range ? Math.max(range.max, price) : price
    }),
    null as Range | null
  ),
  filter((r): r is Range => r !== null),
  shareReplay(1)
);

priceRange$.subscribe(({ min, max }) => {
  updateRangeBar(min, max);
  updateSpread(max - min);
});
```

---

## Pattern 3: Top-N Values (Leaderboard)

Maintain the N highest values seen so far:

```typescript
function topN<T>(
  n:       number,
  compare: (a: T, b: T) => number = (a: any, b: any) => a - b
): OperatorFunction<T, T[]> {
  return source$ => source$.pipe(
    scan((top: T[], value) => {
      const updated = [...top, value].sort((a, b) => compare(b, a)); // descending
      return updated.slice(0, n);
    }, []),
    distinctUntilChanged((a, b) =>
      a.length === b.length && a.every((v, i) => v === b[i])
    )
  );
}

// Live top-5 scores:
scoreEvents$.pipe(
  map(e => e.score),
  topN(5),
  takeUntilDestroyed()
).subscribe(top5 => renderLeaderboard(top5));
```

---

## Pattern 4: Windowed Min/Max (Sliding Window)

Compute min/max over a rolling window of recent values:

```typescript
import { bufferCount, map } from 'rxjs/operators';

function windowedStats(windowSize: number): OperatorFunction<number, { min: number; max: number; avg: number }> {
  return source$ => source$.pipe(
    scan((buf: number[], v) => [...buf, v].slice(-windowSize), []),
    filter(buf => buf.length > 0),
    map(buf => ({
      min: Math.min(...buf),
      max: Math.max(...buf),
      avg: buf.reduce((s, v) => s + v, 0) / buf.length
    }))
  );
}

// Sensor stats over last 100 readings:
sensorStream$.pipe(
  windowedStats(100),
  auditTime(1000), // update display once per second
  takeUntilDestroyed()
).subscribe(stats => updateStatsPanel(stats));
```

---

## Pattern 5: Multi-Field Comparison

Find the "best" item by a composite score:

```typescript
interface Player {
  name:     string;
  kills:    number;
  deaths:   number;
  assists:  number;
}

function kda(p: Player): number {
  return (p.kills + p.assists * 0.5) / Math.max(p.deaths, 1);
}

// Best KDA player after each update:
playerUpdates$.pipe(
  scan((players: Map<string, Player>, update) => {
    const next = new Map(players);
    next.set(update.name, update);
    return next;
  }, new Map<string, Player>()),
  map(players => {
    let best: Player | null = null;
    for (const player of players.values()) {
      if (!best || kda(player) > kda(best)) best = player;
    }
    return best;
  }),
  filter((p): p is Player => p !== null),
  distinctUntilChanged((a, b) => a.name === b.name),
  takeUntilDestroyed()
).subscribe(mvp => highlightMVP(mvp));
```

---

## `min` / `max` vs `reduce` vs `scan`

```typescript
// min / max — built-in, waits for completion:
source$.pipe(max())         // single value on complete
// ✓ Concise; ✗ requires finite stream

// reduce — same as min/max but arbitrary fold:
source$.pipe(reduce((acc, v) => v > acc ? v : acc, -Infinity))
// ✓ Custom logic; ✗ requires completion

// scan — running result, works on infinite streams:
source$.pipe(scan((acc, v) => v > acc ? v : acc))
// ✓ Live updates; ✓ works on infinite streams
```

**Rule**: Use `min`/`max` for finite collections (HTTP responses, `from(array)`). Use `scan` for live streams.

---

## Common Pitfalls

### `max()` / `min()` Never Emit on Infinite Streams

```typescript
// ❌ interval() never completes — max() never emits:
interval(100).pipe(max()).subscribe(v => console.log(v)); // never fires

// ✅ Bound the stream first:
interval(100).pipe(take(50), max()).subscribe(v => console.log(v));

// Or use running max for live streams:
interval(100).pipe(
  scan((acc, v) => Math.max(acc, v), -Infinity),
  distinctUntilChanged()
).subscribe(v => console.log(v));
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: `min`/`max` are convenience operators for finite streams. In most real-world reactive code you want the `scan`-based running equivalent instead, since data arrives as a live stream rather than a completed sequence.
