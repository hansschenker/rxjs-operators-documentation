# timeInterval / timestamp — Advanced Patterns

For fundamentals see the core [timeInterval / timestamp](./timeInterval-timestamp) doc. This page covers performance measurement, timing diagnostics, SLA monitoring, and animation timing patterns.

---

## What They Produce

```typescript
import { timeInterval, timestamp } from 'rxjs/operators';

// timestamp() wraps each value in { value, timestamp: number (ms since epoch) }
source$.pipe(timestamp())
// → { value: 'A', timestamp: 1718700000000 }

// timeInterval() wraps in { value, interval: number (ms since last emission) }
source$.pipe(timeInterval())
// → { value: 'A', interval: 342 }  // 342ms since previous emission
```

---

## Pattern 1: API Response Time Measurement

Track response latency per endpoint:

```typescript
import { timestamp, map, tap } from 'rxjs/operators';

interface TimedResponse<T> {
  data:      T;
  latencyMs: number;
  endpoint:  string;
}

function timedRequest<T>(
  source$:  Observable<T>,
  endpoint: string
): Observable<TimedResponse<T>> {
  const requestStart = Date.now();

  return source$.pipe(
    timestamp(),
    map(({ value, timestamp }) => ({
      data:      value,
      latencyMs: timestamp - requestStart,
      endpoint
    })),
    tap(({ latencyMs, endpoint }) => {
      if (latencyMs > 2000) {
        console.warn(`[SLOW] ${endpoint}: ${latencyMs}ms`);
        metrics.record('slow_request', { endpoint, latencyMs });
      }
    })
  );
}

// Usage:
timedRequest(
  this.http.get<User[]>('/api/users'),
  'GET /api/users'
).subscribe(({ data, latencyMs }) => {
  console.log(`Users loaded in ${latencyMs}ms`);
  renderUsers(data);
});
```

---

## Pattern 2: Emission Rate Monitoring (Events Per Second)

```typescript
import { timeInterval, scan, map, distinctUntilChanged } from 'rxjs/operators';
import { timer } from 'rxjs';

function emissionRate<T>(
  source$: Observable<T>,
  windowMs = 1000
): Observable<number> {
  return source$.pipe(
    timeInterval(),
    scan(
      (state, { interval }) => {
        const now = Date.now();
        // Slide the window — remove intervals older than windowMs:
        const recent = [...state.intervals, interval].filter(
          (_, i, arr) => arr.slice(0, i + 1).reduce((s, v) => s + v, 0) <= windowMs
        );
        return { intervals: recent, windowMs };
      },
      { intervals: [] as number[], windowMs }
    ),
    map(({ intervals, windowMs }) =>
      intervals.length === 0
        ? 0
        : (intervals.length / windowMs) * 1000 // events per second
    ),
    distinctUntilChanged()
  );
}

// Monitor message bus throughput:
emissionRate(messageBus$).subscribe(eps => {
  updateThroughputGauge(eps);
  if (eps > 1000) alertHighLoad(eps);
});
```

---

## Pattern 3: Stale Data Detection

Flag values that haven't updated recently:

```typescript
import { timestamp, combineLatest, map, timer } from 'rxjs/operators';

function withStaleness<T>(
  source$:     Observable<T>,
  staleAfterMs: number
): Observable<{ value: T; isStale: boolean; ageMs: number }> {
  return combineLatest([
    source$.pipe(timestamp()),
    timer(0, 1000) // tick every second
  ]).pipe(
    map(([{ value, timestamp: ts }]) => {
      const ageMs  = Date.now() - ts;
      const isStale = ageMs > staleAfterMs;
      return { value, isStale, ageMs };
    })
  );
}

// Usage — show "data is stale" warning in UI:
withStaleness(priceStream$, 10_000).pipe(
  takeUntilDestroyed()
).subscribe(({ value, isStale, ageMs }) => {
  updatePrice(value);
  if (isStale) showStaleWarning(`Price data is ${Math.round(ageMs / 1000)}s old`);
  else         hideStaleWarning();
});
```

---

## Pattern 4: SLA Breach Detection

Monitor operation durations against Service Level Agreements:

```typescript
import { timestamp, pairwise, map, filter } from 'rxjs/operators';

interface SLAResult {
  operationId: string;
  durationMs:  number;
  breach:      boolean;
  slaMs:       number;
}

function slaMonitor<T extends { id: string }>(
  completions$: Observable<T>,
  submissions$: Observable<T>,
  slaMs:        number
): Observable<SLAResult> {
  const inFlight = new Map<string, number>(); // id → start timestamp

  return merge(
    submissions$.pipe(
      timestamp(),
      tap(({ value, timestamp }) => inFlight.set(value.id, timestamp)),
      ignoreElements()
    ),
    completions$.pipe(
      timestamp(),
      map(({ value, timestamp }) => {
        const startTime = inFlight.get(value.id);
        if (!startTime) return null;
        inFlight.delete(value.id);
        const durationMs = timestamp - startTime;
        return {
          operationId: value.id,
          durationMs,
          breach:      durationMs > slaMs,
          slaMs
        };
      }),
      filter((r): r is SLAResult => r !== null)
    )
  );
}

// Track API call SLAs:
slaMonitor(apiResponses$, apiRequests$, 3000).pipe(
  filter(r => r.breach),
  takeUntilDestroyed()
).subscribe(breach => {
  alerting.raise(`SLA breach: ${breach.operationId} took ${breach.durationMs}ms (SLA: ${breach.slaMs}ms)`);
  metrics.increment('sla_breach', { operation: breach.operationId });
});
```

---

## Pattern 5: Timing Histogram (Distribution of Intervals)

```typescript
import { timeInterval, scan } from 'rxjs/operators';

interface Histogram {
  buckets: Map<string, number>;
  p50: number;
  p95: number;
  p99: number;
}

function timingHistogram<T>(source$: Observable<T>): Observable<Histogram> {
  const BUCKETS = [10, 50, 100, 250, 500, 1000, 2000, 5000]; // ms boundaries

  return source$.pipe(
    timeInterval(),
    scan((acc, { interval }) => {
      const all     = [...acc.all, interval].sort((a, b) => a - b);
      const buckets = new Map<string, number>();
      for (const limit of BUCKETS) {
        buckets.set(`≤${limit}ms`, all.filter(v => v <= limit).length);
      }
      buckets.set(`>${BUCKETS[BUCKETS.length - 1]}ms`, all.filter(v => v > BUCKETS[BUCKETS.length - 1]).length);

      const p = (pct: number) => all[Math.floor(all.length * pct / 100)] ?? 0;
      return { all, buckets, p50: p(50), p95: p(95), p99: p(99) };
    }, { all: [] as number[], buckets: new Map(), p50: 0, p95: 0, p99: 0 }),
    map(({ buckets, p50, p95, p99 }) => ({ buckets, p50, p95, p99 }))
  );
}

// Monitor event processing latency distribution:
timingHistogram(processedEvents$).pipe(
  throttleTime(5000) // log histogram every 5 seconds
).subscribe(({ p50, p95, p99 }) => {
  console.log(`Latency — p50: ${p50}ms, p95: ${p95}ms, p99: ${p99}ms`);
});
```

---

## Pattern 6: Idle/Active Detection

Detect when a stream has gone quiet:

```typescript
import { timeInterval, scan, map } from 'rxjs/operators';
import { timer, merge } from 'rxjs';

function activityState<T>(
  source$:     Observable<T>,
  idleAfterMs: number
): Observable<'active' | 'idle'> {
  return merge(
    source$.pipe(map(() => 'active' as const)),
    source$.pipe(
      switchMap(() => timer(idleAfterMs).pipe(map(() => 'idle' as const)))
    )
  ).pipe(
    distinctUntilChanged()
  );
}

// Usage — connection health indicator:
activityState(wsMessages$, 30_000).subscribe(state => {
  updateConnectionIndicator(state === 'active' ? 'live' : 'stale');
});
```

---

## `timeInterval` vs `timestamp` vs `pairwise`

```typescript
// timestamp() — absolute time of each emission:
source$.pipe(timestamp())
// → { value: 'A', timestamp: 1718700000342 }
// Use: correlating with external logs, stale-data checks

// timeInterval() — relative time BETWEEN emissions:
source$.pipe(timeInterval())
// → { value: 'A', interval: 342 }
// Use: rate monitoring, inter-event duration

// pairwise() — two consecutive values (no time info):
source$.pipe(pairwise())
// → ['previous', 'current']
// Use: delta calculation, direction change — no timing

// timestamp() + pairwise() — both delta AND time:
source$.pipe(
  timestamp(),
  pairwise(),
  map(([prev, curr]) => ({
    value:      curr.value,
    durationMs: curr.timestamp - prev.timestamp
  }))
)
```

---

## Common Pitfalls

### `timeInterval` Measures Wall-Clock Time, Not Processing Time

```typescript
// ❌ Assuming timeInterval reflects processing delay of the operator:
source$.pipe(
  timeInterval(),
  tap(({ interval }) => console.log(`Operator took ${interval}ms`))
)
// Measures time between source emissions, not operator execution time

// ✅ To measure operator processing time, sandwich with timestamp():
source$.pipe(
  timestamp(),
  map(({ value, timestamp: start }) => {
    const result = heavyTransform(value);
    return { result, durationMs: Date.now() - start };
  })
)
```

### First `timeInterval` Emission Is Time Since Subscription

```typescript
// ❌ Expecting uniform intervals from the start:
source$.pipe(timeInterval()).subscribe(({ interval }) => {
  // First emission: interval = time since subscribe(), not since prev emission
  // Subsequent: interval = time since prev emission
});

// ✅ Skip or handle the first interval separately if it's an outlier:
source$.pipe(
  timeInterval(),
  skip(1) // skip the "time to first value" measurement
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**When to reach for these**: Any time you need to measure, log, or react to the *timing* of events rather than their *content*. `timestamp()` gives you an absolute anchor; `timeInterval()` gives you relative rhythm. The most common production use is latency measurement and stale-data detection.
