# windowTime — Advanced Patterns

For `windowTime` fundamentals see the core [windowTime](./windowTime) doc. This page covers overlapping windows, session detection, event rate measurement, and the comparison with `bufferTime`.

---

## `windowTime` vs `bufferTime` — The Key Distinction

Both group events into time-based batches, but they differ in what you receive:

```typescript
// bufferTime — emits an ARRAY of collected values after each window closes:
source$.pipe(
  bufferTime(1000)
).subscribe(arr => console.log('Buffer:', arr)); // Buffer: [1, 2, 3]

// windowTime — emits an OBSERVABLE for each window (higher-order):
source$.pipe(
  windowTime(1000)
).subscribe(window$ => {
  window$.pipe(toArray()).subscribe(arr => console.log('Window:', arr)); // Window: [1, 2, 3]
  // BUT: you can also apply operators inside each window before collecting:
  window$.pipe(max()).subscribe(m => console.log('Max in window:', m));
});
```

Use `windowTime` when you need to **apply operators to values within the window** (e.g., `max()`, `scan()`, `first()`). Use `bufferTime` when you just need the array.

---

## Pattern 1: Sliding Window (Overlapping)

`windowTime(windowDuration, interval)` creates overlapping windows:

```typescript
import { windowTime, mergeMap, toArray, map } from 'rxjs/operators';

// Every 500ms, emit an array of events from the last 2s (overlapping):
events$.pipe(
  windowTime(2000, 500),                    // 2s window, new window every 500ms
  mergeMap(window$ => window$.pipe(toArray())),
  map(events => ({ count: events.length, events }))
).subscribe(({ count }) => {
  updateRateDisplay(`${count} events in last 2s`);
});
```

---

## Pattern 2: Session Detection

Detect user sessions (activity followed by inactivity timeout):

```typescript
import { windowTime, mergeMap, filter, toArray, map } from 'rxjs/operators';
import { Subject, merge } from 'rxjs';

const userActivity$ = merge(
  fromEvent(document, 'mousemove'),
  fromEvent(document, 'keydown'),
  fromEvent(document, 'click')
).pipe(map(() => Date.now()));

// Group activity into sessions separated by 30s of inactivity:
// Note: windowTime groups by fixed time, not inactivity — use bufferWhen for inactivity:
import { bufferWhen, debounceTime } from 'rxjs/operators';
import { timer } from 'rxjs';

const sessionEnd$ = userActivity$.pipe(
  debounceTime(30_000) // 30s of inactivity = session end
);

const sessions$ = userActivity$.pipe(
  bufferWhen(() => sessionEnd$), // collect until session ends
  filter(events => events.length > 0), // skip empty sessions
  map(events => ({
    start:    events[0],
    end:      events[events.length - 1],
    duration: events[events.length - 1] - events[0],
    events:   events.length
  }))
);

sessions$.subscribe(session => {
  analytics.trackSession(session);
});
```

---

## Pattern 3: Event Rate Monitoring

Measure events per second using a sliding window:

```typescript
import { windowTime, mergeMap, count, map } from 'rxjs/operators';
import { timer, combineLatest } from 'rxjs';

// Real-time events-per-second counter:
const eventsPerSecond$ = events$.pipe(
  windowTime(1000, 200),              // 1s window, sampled every 200ms
  mergeMap(window$ => window$.pipe(count())),
  map(count => count)                 // events in the last 1000ms
);

eventsPerSecond$.subscribe(rate => updateSparkline(rate));
```

---

## Pattern 4: Fixed-Size Windows with `windowCount`

When you need windows by **count** rather than time:

```typescript
import { windowCount, mergeMap, toArray, map } from 'rxjs/operators';

// Process in batches of 50, emit each batch:
stream$.pipe(
  windowCount(50),                  // new window every 50 items
  mergeMap(window$ =>
    window$.pipe(
      toArray(),
      map(batch => processBatch(batch))
    )
  )
).subscribe(batchResult => storeBatchResult(batchResult));
```

---

## Pattern 5: First Event in Window (Throttle via Window)

```typescript
import { windowTime, mergeMap, first, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// Only process the first click in each 500ms window:
clicks$.pipe(
  windowTime(500),
  mergeMap(window$ =>
    window$.pipe(
      first(),
      catchError(() => EMPTY) // empty window — ignore
    )
  )
).subscribe(handleClick);
// Equivalent to throttleTime(500) but using window operators
```

---

## Pattern 6: Statistical Aggregates Over Time

```typescript
import { windowTime, mergeMap, map } from 'rxjs/operators';
import { combineLatest, forkJoin } from 'rxjs';
import { min, max, count, reduce } from 'rxjs/operators';

// Compute min/max/avg/count for each 5-second window:
sensorData$.pipe(
  windowTime(5000),
  mergeMap(window$ => {
    const values$ = window$.pipe(share()); // share one subscription across operators
    return forkJoin({
      min:   values$.pipe(min()),
      max:   values$.pipe(max()),
      count: values$.pipe(count()),
      sum:   values$.pipe(reduce((acc, v) => acc + v, 0))
    }).pipe(
      map(({ min, max, count, sum }) => ({
        min, max, count,
        avg: count > 0 ? sum / count : 0
      }))
    );
  })
).subscribe(stats => updateDashboard(stats));
```

---

## Pattern 7: Real-Time Log Analysis

Group log entries by time window for rate anomaly detection:

```typescript
import { windowTime, mergeMap, filter, count, map } from 'rxjs/operators';

interface LogEntry { level: 'INFO' | 'WARN' | 'ERROR'; message: string }

const logStream$: Observable<LogEntry> = /* ... */;

// Alert if more than 10 errors in any 1-minute window:
logStream$.pipe(
  windowTime(60_000),          // 1-minute windows
  mergeMap(window$ =>
    window$.pipe(
      filter(entry => entry.level === 'ERROR'),
      count()
    )
  ),
  filter(errorCount => errorCount > 10)
).subscribe(errorCount => {
  alerting.trigger(`High error rate: ${errorCount} errors in 1 minute`);
});
```

---

## `windowTime` vs `bufferTime` vs `throttleTime` vs `debounceTime`

| Operator | Collects | Emits | Use for |
|---|---|---|---|
| `bufferTime(ms)` | Events into array | Array every `ms` | Simple batch processing |
| `windowTime(ms)` | Events into Observable | Observable every `ms` | Statistical analysis within window |
| `throttleTime(ms)` | First event | Single events | Rate limiting, UI interactions |
| `debounceTime(ms)` | Last event | Single events | Search input, form validation |
| `auditTime(ms)` | Last event | Single events | Smooth high-frequency streams |

---

## Common Pitfalls

### Forgetting to Subscribe to Inner Windows

```typescript
// ❌ Window Observables are cold — subscribing to the outer stream creates windows,
//    but each window$.subscribe() is required to process values:
events$.pipe(
  windowTime(1000)
).subscribe(window$ => {
  // window$ exists but nobody is subscribed — values are LOST
  console.log('Got a window'); // this logs, but values inside window are lost
});

// ✅ Always subscribe to or pipe inner window$:
events$.pipe(
  windowTime(1000),
  mergeMap(window$ => window$.pipe(toArray())) // subscribe via mergeMap
).subscribe(arr => console.log(arr));
```

### Using `windowTime` When `bufferTime` Suffices

```typescript
// ❌ Overly complex — windowTime + toArray when bufferTime is simpler:
events$.pipe(
  windowTime(1000),
  mergeMap(w$ => w$.pipe(toArray()))
).subscribe(process);

// ✅ bufferTime is the right choice when you just need the array:
events$.pipe(
  bufferTime(1000)
).subscribe(process);
// WHY: windowTime is only needed when applying operators INSIDE the window
// (min, max, scan, first, etc.). For raw arrays, bufferTime is simpler.
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key rule**: Use `bufferTime` for arrays, `windowTime` for operator application inside windows. The power of `windowTime` is that each window is itself an Observable — you can apply `min()`, `max()`, `scan()`, or `first()` to it before collecting results.
