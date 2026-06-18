# timeInterval / timestamp

Two utility operators that annotate stream emissions with timing metadata.

---

## `timeInterval`

### Identity
- **Import**: `import { timeInterval } from 'rxjs/operators'`
- **Signature**: `timeInterval<T>(scheduler?: SchedulerLike): OperatorFunction<T, TimeInterval<T>>`
- **Category**: Utility — wraps each emission with the elapsed time since the previous emission

```typescript
interface TimeInterval<T> {
  value:    T       // the original emitted value
  interval: number  // milliseconds since the previous emission (or subscription)
}
```

### Functional Specification

Wraps each source emission with an object containing the original `value` and the `interval` — the number of milliseconds elapsed since the **previous** emission (or since subscription for the first value).

**Use cases**:
- Measure inter-emission gaps (typing speed, event rate, latency)
- Detect when a source is "slow" (interval exceeds a threshold)
- Log emission cadence for debugging

### Marble Diagram

```
Source (real time):  ---a(100ms)---b(50ms)-c(200ms)--|

timeInterval():
Result:  ---{value:'a', interval:100}---{value:'b', interval:50}-{value:'c', interval:200}--|

First emission interval = time from subscription to first value.
```

### Examples

```typescript
import { fromEvent, interval } from 'rxjs';
import { timeInterval, map, filter } from 'rxjs/operators';

// Measure typing speed (ms between keystrokes)
fromEvent(document, 'keydown').pipe(
  timeInterval(),
  map(({ value, interval }) => ({
    key:      (value as KeyboardEvent).key,
    pausedMs: interval
  }))
).subscribe(({ key, pausedMs }) => {
  console.log(`"${key}" typed after ${pausedMs}ms`);
});

// Detect slow emissions (> 500ms gap)
someStream$.pipe(
  timeInterval(),
  filter(({ interval }) => interval > 500)
).subscribe(({ value, interval }) => {
  console.warn(`Slow emission: ${value} arrived after ${interval}ms`);
});

// Measure actual interval vs expected (drift detection)
interval(1000).pipe(
  timeInterval()
).subscribe(({ value, interval }) => {
  const drift = interval - 1000;
  if (Math.abs(drift) > 50) console.warn(`Timer drift: ${drift}ms`);
});
```

### Common Pattern — Performance Monitoring
```typescript
import { tap, timeInterval, scan } from 'rxjs/operators';

// Rolling average inter-emission time
apiResponses$.pipe(
  timeInterval(),
  scan(
    (acc, { interval }) => ({
      total: acc.total + interval,
      count: acc.count + 1,
      avg:   (acc.total + interval) / (acc.count + 1)
    }),
    { total: 0, count: 0, avg: 0 }
  )
).subscribe(({ avg }) => updateLatencyDisplay(avg));
```

---

## `timestamp`

### Identity
- **Import**: `import { timestamp } from 'rxjs/operators'`
- **Signature**: `timestamp<T>(timestampProvider?: TimestampProvider): OperatorFunction<T, Timestamp<T>>`
- **Category**: Utility — wraps each emission with the absolute wall-clock time it was emitted

```typescript
interface Timestamp<T> {
  value:     T       // the original emitted value
  timestamp: number  // Date.now() at time of emission (ms since Unix epoch)
}
```

### Functional Specification

Wraps each source emission with an object containing the original `value` and `timestamp` — the absolute timestamp (milliseconds since epoch, equivalent to `Date.now()`) at the moment the emission occurred.

**Use cases**:
- Record when events occurred for logging or replay
- Calculate absolute age of an emission (`Date.now() - timestamp`)
- Build time-ordered event logs
- Correlate events across streams by wall-clock time

### Marble Diagram

```
Source (emits at real times):
  --a(t=1000)--b(t=1050)--c(t=1300)--|

timestamp():
Result: --{value:'a', timestamp:1000}--{value:'b', timestamp:1050}--{value:'c', timestamp:1300}--|
```

### Examples

```typescript
import { fromEvent, interval } from 'rxjs';
import { timestamp, map } from 'rxjs/operators';

// Log events with wall-clock timestamps
fromEvent(document, 'click').pipe(
  timestamp(),
  map(({ value, timestamp }) => ({
    type: 'click',
    at:   new Date(timestamp).toISOString(),
    x:    (value as MouseEvent).clientX,
    y:    (value as MouseEvent).clientY
  }))
).subscribe(event => auditLog.push(event));

// Calculate emission age
const eventAge$ = someEvent$.pipe(
  timestamp(),
  map(({ value, timestamp }) => ({
    ...value,
    ageMs: Date.now() - timestamp // ms since emission
  }))
);

// Event correlation — join two streams by time proximity
import { bufferTime, filter } from 'rxjs/operators';

const clicks$ = fromEvent(document, 'click').pipe(timestamp());
const keys$   = fromEvent(document, 'keydown').pipe(timestamp());

// Find keystrokes within 100ms of a click
clicks$.pipe(
  bufferTime(100),
  map(clicks => clicks.map(c => c.timestamp)),
  // ... correlate with keys$ timestamps
);
```

---

## `timeInterval` vs `timestamp`

| | `timeInterval` | `timestamp` |
|---|---|---|
| Measures | **Relative** — gap since previous | **Absolute** — wall-clock when emitted |
| Value type | `{ value, interval: number }` | `{ value, timestamp: number }` |
| Use when | How fast is the source? | When did this happen? |
| Drift detection | Yes | Indirectly |
| Event logging | No | Yes |
| Works with `Date` | Compute: `prev + interval` | Direct: `new Date(timestamp)` |

## Common Pitfall

```typescript
import { timeInterval, timestamp } from 'rxjs/operators';

// ❌ CONFUSION — using timestamp to measure gaps
source$.pipe(
  timestamp(),
  pairwise(),
  map(([prev, curr]) => curr.timestamp - prev.timestamp) // works but verbose
).subscribe(gap => console.log(`gap: ${gap}ms`));

// ✅ SIMPLER — timeInterval is purpose-built for gaps
source$.pipe(
  timeInterval(),
  map(({ interval }) => interval)
).subscribe(gap => console.log(`gap: ${gap}ms`));

// WHY: timeInterval does the subtraction for you. Use timestamp when you
// need the absolute time; use timeInterval when you need relative gaps.
```

## Related Operators

- **`delay(ms)`**: Shifts emissions by a fixed duration — distinct from measuring timing
- **`timeout`**: Errors if no emission within a time window — uses timing to enforce SLAs
- **`tap`**: Observe emissions without metadata wrapping — use for ad-hoc timing with `Date.now()`

## References
- [timeInterval](https://rxjs.dev/api/operators/timeInterval)
- [timestamp](https://rxjs.dev/api/operators/timestamp)

---

**`timeInterval`** — Cognitive Load: 1/5 | Usage: 2/5 | Relative gap measurement — use for latency monitoring and emission-rate analysis.
**`timestamp`** — Cognitive Load: 1/5 | Usage: 2/5 | Absolute wall-clock annotation — use for event logging and time-correlation.
