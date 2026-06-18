# Real-Time Charts and Data Visualization with RxJS

Streaming data into charts, windowed aggregation, live dashboards, sparklines, and performance-safe rendering pipelines.

---

## The Core Pattern: Data → Transform → Render

```typescript
// 1. Source: raw data stream (WebSocket, polling, user events)
// 2. Transform: aggregate, window, downsample
// 3. Rate-limit: auditTime to frame rate
// 4. Render: update chart library

rawData$.pipe(
  windowedAggregate(60_000),        // 1-minute buckets
  auditTime(0, animationFrameScheduler), // sync to display refresh
  takeUntilDestroyed()
).subscribe(data => chart.update(data));
```

---

## Pattern 1: Live Line Chart (Chart.js / ApexCharts)

```typescript
import { animationFrameScheduler } from 'rxjs';
import { scan, auditTime, map } from 'rxjs/operators';

interface DataPoint { timestamp: number; value: number; }

const MAX_POINTS = 100;

// Maintain a rolling window of the last N points:
const chartData$ = priceStream$.pipe(
  map(price => ({ timestamp: Date.now(), value: price })),
  scan((points: DataPoint[], point) =>
    [...points, point].slice(-MAX_POINTS),
    []
  ),
  auditTime(0, animationFrameScheduler), // one update per frame max
  takeUntilDestroyed()
);

// Chart.js integration:
chartData$.subscribe(points => {
  chart.data.labels       = points.map(p => formatTime(p.timestamp));
  chart.data.datasets[0].data = points.map(p => p.value);
  chart.update('none'); // 'none' = no animation on update
});
```

---

## Pattern 2: Windowed OHLC (Candlestick) Aggregation

Aggregate tick data into OHLC candles for financial charts:

```typescript
import { windowTime, mergeMap, toArray, filter, map } from 'rxjs/operators';

interface Tick  { price: number; volume: number; ts: number; }
interface Candle { open: number; high: number; low: number; close: number; volume: number; ts: number; }

function toOHLC(periodMs: number): OperatorFunction<Tick, Candle> {
  return source$ => source$.pipe(
    windowTime(periodMs),
    mergeMap(window$ =>
      window$.pipe(
        toArray(),
        filter(ticks => ticks.length > 0),
        map(ticks => ({
          open:   ticks[0].price,
          high:   Math.max(...ticks.map(t => t.price)),
          low:    Math.min(...ticks.map(t => t.price)),
          close:  ticks[ticks.length - 1].price,
          volume: ticks.reduce((s, t) => s + t.volume, 0),
          ts:     ticks[0].ts
        }))
      )
    )
  );
}

// 1-minute candles:
tickStream$.pipe(
  toOHLC(60_000),
  scan((candles: Candle[], candle) => [...candles.slice(-200), candle], []),
  auditTime(0, animationFrameScheduler),
  takeUntilDestroyed()
).subscribe(candles => candlestickChart.setData(candles));
```

---

## Pattern 3: Multi-Metric Dashboard

Combine multiple streams into a single dashboard state update:

```typescript
import { combineLatest, timer } from 'rxjs';
import { switchMap, map, shareReplay, distinctUntilChanged } from 'rxjs/operators';

interface DashboardState {
  cpu:       number;
  memory:    number;
  requests:  number;
  errorRate: number;
  latencyP95: number;
}

// Each metric streams independently:
const cpu$      = poll$(fetchCPU,     2000).pipe(shareReplay(1));
const memory$   = poll$(fetchMemory,  5000).pipe(shareReplay(1));
const requests$ = wsMetrics$.pipe(map(m => m.requestsPerSec), shareReplay(1));
const errors$   = wsMetrics$.pipe(map(m => m.errorRate), shareReplay(1));
const latency$  = poll$(fetchLatency, 3000).pipe(shareReplay(1));

const dashboard$ = combineLatest({
  cpu:       cpu$,
  memory:    memory$,
  requests:  requests$,
  errorRate: errors$,
  latencyP95: latency$
}).pipe(
  auditTime(0, animationFrameScheduler),
  distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
  takeUntilDestroyed()
);

dashboard$.subscribe(state => {
  updateGauge('cpu',      state.cpu);
  updateGauge('memory',   state.memory);
  updateCounter('rps',    state.requests);
  updateAlert('errors',   state.errorRate > 0.05);
  updateLatencyChart(     state.latencyP95);
});
```

---

## Pattern 4: Sparkline with Rolling Statistics

Compact chart with real-time P50/P95 overlay:

```typescript
import { scan, map, shareReplay } from 'rxjs/operators';

interface WindowedStats {
  values:  number[];
  p50:     number;
  p95:     number;
  p99:     number;
  min:     number;
  max:     number;
  mean:    number;
}

function rollingStats(windowSize: number): OperatorFunction<number, WindowedStats> {
  return source$ => source$.pipe(
    scan((values: number[], v) => [...values, v].slice(-windowSize), []),
    map(values => {
      const sorted = [...values].sort((a, b) => a - b);
      const p = (pct: number) => sorted[Math.floor(sorted.length * pct)] ?? 0;
      const mean = values.reduce((s, v) => s + v, 0) / (values.length || 1);
      return {
        values,
        p50:  p(0.50),
        p95:  p(0.95),
        p99:  p(0.99),
        min:  sorted[0] ?? 0,
        max:  sorted[sorted.length - 1] ?? 0,
        mean: Math.round(mean * 10) / 10
      };
    })
  );
}

latencyStream$.pipe(
  rollingStats(500),
  auditTime(500), // update stats every 500ms
  takeUntilDestroyed()
).subscribe(stats => {
  updateSparkline(stats.values);
  updateStatLabels(stats);
  if (stats.p95 > 2000) highlightSLABreach(stats.p95);
});
```

---

## Pattern 5: Zoom/Pan State with Observable Data Queries

Reactive chart viewport — query data when user pans or zooms:

```typescript
import { BehaviorSubject } from 'rxjs';
import { switchMap, debounceTime, distinctUntilChanged } from 'rxjs/operators';

interface ViewPort { from: number; to: number; resolution: 'minute' | 'hour' | 'day'; }

const viewport$ = new BehaviorSubject<ViewPort>({
  from:       Date.now() - 3600_000,
  to:         Date.now(),
  resolution: 'minute'
});

// Fetch data whenever viewport changes (debounced):
const visibleData$ = viewport$.pipe(
  debounceTime(200),
  distinctUntilChanged((a, b) =>
    a.from === b.from && a.to === b.to && a.resolution === b.resolution
  ),
  switchMap(vp =>
    this.api.getMetrics(vp.from, vp.to, vp.resolution).pipe(
      catchError(() => of([]))
    )
  ),
  shareReplay(1)
);

// Wire chart pan/zoom events to viewport:
fromEvent(chart, 'pan').pipe(
  map(event => computeNewViewport(viewport$.getValue(), event)),
  debounceTime(50)
).subscribe(vp => viewport$.next(vp));

fromEvent(chart, 'zoom').pipe(
  map(event => computeZoomedViewport(viewport$.getValue(), event))
).subscribe(vp => viewport$.next(vp));

visibleData$.subscribe(data => chart.setData(data));
```

---

## Pattern 6: WebGL / Canvas High-Performance Rendering

For >10k data points, use canvas with typed arrays instead of DOM:

```typescript
import { animationFrames } from 'rxjs';
import { withLatestFrom, map } from 'rxjs/operators';

class CanvasLineChart {
  private canvas: HTMLCanvasElement;
  private ctx:    CanvasRenderingContext2D;
  private data$   = new BehaviorSubject<Float32Array>(new Float32Array(0));

  constructor(canvas: HTMLCanvasElement) {
    this.canvas = canvas;
    this.ctx    = canvas.getContext('2d')!;

    // Render loop — draws latest data each frame:
    animationFrames().pipe(
      withLatestFrom(this.data$),
      map(([, data]) => data),
      takeUntilDestroyed()
    ).subscribe(data => this.render(data));
  }

  updateData(values: Float32Array): void {
    this.data$.next(values);
  }

  private render(data: Float32Array): void {
    const { width, height } = this.canvas;
    this.ctx.clearRect(0, 0, width, height);
    if (data.length === 0) return;

    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;

    this.ctx.beginPath();
    for (let i = 0; i < data.length; i++) {
      const x = (i / (data.length - 1)) * width;
      const y = height - ((data[i] - min) / range) * height;
      i === 0 ? this.ctx.moveTo(x, y) : this.ctx.lineTo(x, y);
    }
    this.ctx.stroke();
  }
}

// Feed streaming data as typed array for performance:
rawDataStream$.pipe(
  scan((buf: number[], v) => [...buf, v].slice(-10_000), []),
  map(arr => new Float32Array(arr)),
  auditTime(0, animationFrameScheduler)
).subscribe(data => chart.updateData(data));
```

---

## Common Pitfalls

### Calling `chart.update()` Too Frequently

```typescript
// ❌ Chart library called on every emission — causes frame drops:
priceStream$.subscribe(price => {
  chart.data.datasets[0].data.push(price);
  chart.update(); // may fire 100+ times/second
});

// ✅ Always throttle chart updates to frame rate:
priceStream$.pipe(
  scan((data, price) => [...data, price].slice(-100), []),
  auditTime(0, animationFrameScheduler) // one update per rAF max
).subscribe(data => {
  chart.data.datasets[0].data = data;
  chart.update('none');
});
```

### Memory Leak from Unbounded Data Accumulation

```typescript
// ❌ Accumulating all data forever — grows without bound:
const allData$ = source$.pipe(
  scan((all, v) => [...all, v], []) // never pruned
);
// After 1 hour at 10/s → 36,000 points in memory

// ✅ Keep only the visible window:
const windowedData$ = source$.pipe(
  scan((all, v) => [...all, v].slice(-MAX_POINTS), [])
);
```
