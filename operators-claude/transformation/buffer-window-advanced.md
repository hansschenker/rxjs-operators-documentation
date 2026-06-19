# buffer / window — Advanced Patterns

> **Cognitive Load**: 4/5 | **Usage Frequency**: 3/5 | **Composability**: 5/5
> **Teaching Sequence**: After `bufferTime`/`bufferCount` — introduces reactive, signal-driven collection boundaries

---

## Advanced Behavioral Model

`buffer(notifier$)` and `window(notifier$)` share the same boundary logic but differ in what they emit:

| | `buffer` | `window` |
|---|---|---|
| Emits | `T[]` arrays | `Observable<T>` inner streams |
| Values available | After boundary closes | As they arrive (streaming) |
| Memory | Accumulates all values | Lazy — only what downstream subscribes to |
| Best for | Batch processing, analytics | Streaming transformation, higher-order ops |

**Critical boundary behaviors:**
```
Source:   --1--2--3--4--5--6--|
Notifier: ----x--------x------|

buffer(notifier):
          ----[1,2]----[3,4,5,6]--[]|
                                    ^
                       final partial buffer on source complete

window(notifier):
          At subscribe: open window W1
          At first x:   W1 completes, emit W1=[1,2]; open W2
          At second x:  W2 completes, emit W2=[3,4,5,6]; open W3
          At complete:  W3 completes, emit W3=[]
```

**Empty buffer behavior** — unlike `bufferCount`, `buffer` can emit `[]`:
```
Source:   --1----------|
Notifier: --x--x--x---|

buffer:   --[]--[]--[1]|
               ^^
          Two empty buffers before the value arrives
```

---

## Type System Integration

```typescript
import { buffer, window } from 'rxjs/operators';
import { Subject, interval } from 'rxjs';

// buffer: OperatorFunction<T, T[]>
const batched$: Observable<number[]> = clicks$.pipe(
  buffer(interval(1000))  // Observable<number[]>
);

// window: OperatorFunction<T, Observable<T>>
const windowed$: Observable<Observable<number>> = source$.pipe(
  window(interval(1000))  // Observable<Observable<number>>
);

// Window inner observables must be subscribed to
windowed$.pipe(
  mergeMap(win$ => win$.pipe(toArray()))
).subscribe(batch => process(batch));

// Subject as a manual boundary trigger
const boundary$ = new Subject<void>();
const onDemandBuffer$ = source$.pipe(buffer(boundary$));
boundary$.next(); // flush current buffer
```

---

## Advanced Patterns

### 1. Double-Click Detection with buffer

Collect clicks into buffers closed by a silence period, then filter for double-clicks.

```typescript
import { fromEvent, interval } from 'rxjs';
import { buffer, filter, debounceTime } from 'rxjs/operators';

const clicks$ = fromEvent(document, 'click');

// Close buffer on 250ms of silence = one click "gesture" complete
const clickGroups$ = clicks$.pipe(
  buffer(clicks$.pipe(debounceTime(250))),
  filter(clicks => clicks.length >= 2),  // double-click or more
);

clickGroups$.subscribe(clicks => {
  console.log(`${clicks.length}-click detected`);
});

// Marble:
// Clicks: --c--c-----------c--c--c--|
// buffer: -----------[c,c]---------[c,c,c]|
//                    ^ 250ms silence closes buffer
```

### 2. Sliding Window Analytics with window + scan

`window` enables streaming analytics where each window is processed as it fills — no need to wait for the boundary.

```typescript
import { interval, zip } from 'rxjs';
import { window, mergeMap, scan, map, take } from 'rxjs/operators';

interface Metric { value: number; timestamp: number }

// Rolling 5-second windows, reporting average of each
const metrics$: Observable<Metric> = sensorStream$;

const rollingAverage$ = metrics$.pipe(
  window(interval(5000)),
  mergeMap((win$, index) =>
    win$.pipe(
      scan(
        (acc, m) => ({ sum: acc.sum + m.value, count: acc.count + 1 }),
        { sum: 0, count: 0 }
      ),
      map(({ sum, count }) => ({
        window: index,
        average: count > 0 ? sum / count : 0,
        sampleCount: count,
      })),
    )
  ),
);

rollingAverage$.subscribe(report => dashboard.update(report));
// Each window streams partial averages as data arrives,
// not just the final average at window close.
```

### 3. Batching API Calls with buffer + concatMap

Collect individual item requests into batches, then fire a single bulk API call per batch.

```typescript
import { Subject, timer } from 'rxjs';
import { buffer, filter, concatMap, from } from 'rxjs/operators';

interface ItemRequest { id: string }

const requestQueue$ = new Subject<ItemRequest>();

// Batch: flush every 100ms OR when 50 requests accumulate
const batchBoundary$ = merge(
  timer(0, 100),              // time-based flush
  requestQueue$.pipe(         // count-based flush
    bufferCount(50),
    map(() => void 0),
  )
);

const batchedRequests$ = requestQueue$.pipe(
  buffer(batchBoundary$),
  filter(batch => batch.length > 0),  // skip empty flushes
  concatMap(batch =>
    from(api.bulkFetch(batch.map(r => r.id)))
  ),
);

batchedRequests$.subscribe(results => cacheResults(results));

// Usage: fire requests freely — they auto-batch
requestQueue$.next({ id: 'user-1' });
requestQueue$.next({ id: 'user-2' });
// ... 100ms later → single api.bulkFetch(['user-1', 'user-2'])
```

### 4. window + switchMap for Cancellable Processing

`window` shines over `buffer` when each window's processing should be cancellable by the next window.

```typescript
import { interval, fromEvent } from 'rxjs';
import { window, switchMap, toArray, filter } from 'rxjs/operators';

// Process each 2-second window of user actions,
// but cancel processing if a new window starts
const userActions$ = fromEvent<KeyboardEvent>(document, 'keydown');

const processedWindows$ = userActions$.pipe(
  window(interval(2000)),
  switchMap(win$ =>
    win$.pipe(
      toArray(),
      filter(actions => actions.length > 0),
      // switchMap cancels heavy processing if next window opens
    )
  ),
);

processedWindows$.subscribe(actionBatch => {
  analyzeUserBehavior(actionBatch);
});
```

### 5. Manual Flush Pattern with Subject Boundary

Expose flush control to external code while keeping the reactive pipeline internal.

```typescript
import { Subject, merge, NEVER } from 'rxjs';
import { buffer, takeUntil, share } from 'rxjs/operators';

class ReactiveBuffer<T> {
  private flush$ = new Subject<void>();
  private destroy$ = new Subject<void>();
  private buffered$: Observable<T[]>;

  constructor(source$: Observable<T>, autoFlushMs?: number) {
    const boundary$ = merge(
      this.flush$,
      autoFlushMs ? interval(autoFlushMs) : NEVER,
    );

    this.buffered$ = source$.pipe(
      buffer(boundary$),
      takeUntil(this.destroy$),
      share(),
    );
  }

  get data$(): Observable<T[]> { return this.buffered$; }

  flush(): void { this.flush$.next(); }

  destroy(): void { this.destroy$.complete(); }
}

const buf = new ReactiveBuffer(sensorReadings$, 5000);
buf.data$.subscribe(batch => saveBatch(batch));

// Manual flush on user action
saveButton.addEventListener('click', () => buf.flush());
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — forgetting to subscribe to window inner Observables
source$.pipe(
  window(interval(1000))
).subscribe(win$ => {
  // win$ is an Observable — never subscribed, values are lost
  console.log('got window');
});

// ✅ CORRECT — flatten inner Observables with mergeMap/concatMap/switchMap
source$.pipe(
  window(interval(1000)),
  mergeMap(win$ => win$.pipe(toArray()))
).subscribe(batch => process(batch));
// WHY: window emits Observable<T>, not T[]. You must subscribe to the
// inner Observable to consume values.


// ❌ INCORRECT — using buffer when empty buffers cause downstream issues
clickStream$.pipe(
  buffer(interval(500)),
  map(clicks => clicks[0].target)  // TypeError if clicks is []
).subscribe(target => highlight(target));

// ✅ CORRECT — filter empty buffers first
clickStream$.pipe(
  buffer(interval(500)),
  filter(clicks => clicks.length > 0),
  map(clicks => clicks[0].target)
).subscribe(target => highlight(target));
// WHY: buffer emits [] when the notifier fires with no buffered values.
// Always guard against empty buffers unless [] is meaningful downstream.


// ❌ INCORRECT — using the source itself as the notifier (infinite loop)
source$.pipe(
  buffer(source$)  // closes on every source emission → single-element buffers
).subscribe(console.log);
// Output: [1], [2], [3] — not useful batching

// ✅ CORRECT — use an independent boundary signal
source$.pipe(
  buffer(interval(1000))  // independent timer
).subscribe(console.log);
// WHY: buffer(source$) closes a buffer on every value, which defeats batching.
// The notifier should be independent of the source.
```

---

## buffer vs window Decision Guide

```
Need batched arrays (T[])?          → buffer
Need to stream values as they arrive?  → window
Processing is cancellable per batch?   → window + switchMap
Need partial progress before close?    → window + scan
Simple collection for later processing? → buffer
```

---

## Related Operators

- **`bufferTime`** / **`bufferCount`** — fixed-interval and fixed-count buffering
- **`bufferWhen`** / **`windowWhen`** — factory-function boundaries (boundary per subscription)
- **`bufferToggle`** / **`windowToggle`** — open/close boundaries independently
- **`groupBy`** — partition by key rather than time/count boundaries
- **`toArray`** — collect all values into one array on completion (not a moving window)
- **`scan`** — rolling accumulation without discrete boundaries
