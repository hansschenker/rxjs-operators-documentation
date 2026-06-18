# bufferWhen / windowWhen — Advanced Patterns

For fundamentals see [bufferWhen / windowWhen](./bufferWhen-windowWhen). This page covers dynamic window sizing, adaptive buffering, protocol framing, and comparison with `bufferToggle`.

---

## Mental Model: Factory-Driven Window Boundaries

```typescript
import { bufferWhen, windowWhen } from 'rxjs/operators';

// bufferWhen(closingSelector)
// - Opens immediately on subscription
// - Calls closingSelector() when the current window closes
// - closingSelector returns an Observable → when that emits, close window and reopen
// - ONE window open at a time (unlike bufferToggle which can have multiple)

// Example — buffer until a signal fires, then restart:
const flush$ = fromEvent(flushButton, 'click');

events$.pipe(
  bufferWhen(() => flush$)  // collect until button click, emit array, restart
).subscribe(batch => processBatch(batch));

// windowWhen — same semantics but emits Observable<T> instead of T[]
events$.pipe(
  windowWhen(() => flush$),
  mergeMap(window$ => window$.pipe(toArray()))
) // streaming equivalent
```

Key distinction from `bufferToggle`: `bufferWhen` has **exactly one window open at all times** — it closes and immediately reopens. `bufferToggle` can have **multiple concurrent windows**.

---

## Pattern 1: Adaptive Buffer Size

Adjust the flush interval based on observed emission rate:

```typescript
import { bufferWhen, interval, timer } from 'rxjs/operators';

// Adaptive buffering — flush when either N items OR T milliseconds have elapsed:
function adaptiveBuffer$<T>(
  source$:   Observable<T>,
  maxItems:  number,
  maxWaitMs: number
): Observable<T[]> {
  let buffer: T[] = [];

  return source$.pipe(
    bufferWhen(() =>
      // Create a race: either count threshold or time threshold
      race(
        source$.pipe(
          skip(maxItems - 1), // fires when buffer reaches maxItems
          take(1)
        ),
        timer(maxWaitMs)       // fires after maxWaitMs regardless of count
      )
    ),
    filter(batch => batch.length > 0) // skip empty batches (timer fired with nothing)
  );
}

// Usage — flush every 10 items or every 2 seconds, whichever comes first:
clickEvents$.pipe(
  adaptiveBuffer$(10, 2000)
).subscribe(batch => saveClickBatch(batch));

// Simpler version using bufferTime with max:
clickEvents$.pipe(
  bufferTime(2000, null, 10) // 2s OR 10 items max — built-in parameter
).subscribe(batch => saveClickBatch(batch));
// Note: bufferTime(ms, startMs, maxBufferSize) achieves the same result
// bufferWhen is more flexible when the close condition is complex/custom
```

---

## Pattern 2: Protocol Framing — Buffer Until Delimiter

Collect stream data until a frame delimiter is received:

```typescript
import { bufferWhen, filter, map } from 'rxjs/operators';

interface Frame { data: string; isEnd: boolean }

// Buffer protocol frames until the end-of-frame marker:
rawFrames$.pipe(
  bufferWhen(() =>
    rawFrames$.pipe(
      filter(frame => frame.isEnd), // close window when end-of-frame arrives
      take(1)
    )
  ),
  map(frames => assembleMessage(frames))
).subscribe(message => handleCompleteMessage(message));

// Binary protocol — buffer until stop byte (0xFF):
rawBytes$.pipe(
  bufferWhen(() =>
    rawBytes$.pipe(
      filter(byte => byte === 0xFF), // stop byte as delimiter
      take(1)
    )
  ),
  map(bytes => new Uint8Array(bytes))
).subscribe(packet => processPacket(packet));

// HTTP chunked transfer simulation — buffer until empty chunk:
chunks$.pipe(
  bufferWhen(() =>
    chunks$.pipe(
      filter(chunk => chunk.length === 0), // empty chunk = end of body
      take(1)
    )
  ),
  map(chunks => chunks.join(''))
).subscribe(body => parseHttpBody(body));
```

---

## Pattern 3: Dynamic Flush Interval Based on Stream Behavior

Change how often windows close based on observed traffic:

```typescript
import { bufferWhen, interval, BehaviorSubject } from 'rxjs';

@Injectable()
class AdaptiveFlushService {
  private readonly flushInterval$ = new BehaviorSubject<number>(1000); // 1s default

  setFlushInterval(ms: number) {
    this.flushInterval$.next(ms);
  }

  buffer$<T>(source$: Observable<T>): Observable<T[]> {
    return source$.pipe(
      bufferWhen(() =>
        // Closing selector re-reads the current interval on each window:
        this.flushInterval$.pipe(
          switchMap(ms => interval(ms)),
          take(1) // close after one interval fires
        )
      ),
      filter(batch => batch.length > 0)
    );
  }
}

// Usage — slow down buffering under load:
const service = inject(AdaptiveFlushService);
const buffered$ = service.buffer$(telemetryStream$);

// When server reports high load, slow the flush rate:
serverLoadUpdates$.pipe(
  distinctUntilChanged()
).subscribe(load => {
  service.setFlushInterval(load === 'high' ? 5000 : 1000);
});
```

---

## Pattern 4: Transaction-Boundary Buffering

Collect all events within a logical transaction before processing:

```typescript
import { bufferWhen, filter, mergeMap } from 'rxjs/operators';

interface DbEvent
  | { type: 'BEGIN_TX';    txId: string }
  | { type: 'OPERATION';   txId: string; op: string }
  | { type: 'COMMIT';      txId: string }
  | { type: 'ROLLBACK';    txId: string }

// Buffer all operations between BEGIN and COMMIT/ROLLBACK:
dbEvents$.pipe(
  // Split into per-transaction streams first:
  groupBy(e => e.txId),
  mergeMap(tx$ =>
    tx$.pipe(
      bufferWhen(() =>
        tx$.pipe(
          filter(e => e.type === 'COMMIT' || e.type === 'ROLLBACK'),
          take(1)
        )
      ),
      take(1) // one buffer per transaction
    )
  ),
  map(events => {
    const txId     = events[0]?.txId;
    const ops      = events.filter(e => e.type === 'OPERATION');
    const committed = events.some(e => e.type === 'COMMIT');
    return { txId, ops, committed };
  })
).subscribe(tx => {
  if (tx.committed) applyTransaction(tx.ops);
  else rollbackTransaction(tx.txId);
});
```

---

## `bufferWhen` vs `bufferToggle` vs `bufferUntil`

```typescript
// bufferWhen(factory) — one window at a time, factory called on each reopen
// Use when: sequential windows, closing condition is complex or dynamic

source$.pipe(
  bufferWhen(() => someSignal$.pipe(take(1)))
)
// t=0 → window opens
// t=signal → window closes, emits buffer, immediately reopens
// t=next-signal → window closes, emits buffer, reopens again
// ...sequential, no overlap

// bufferToggle(open$, closeSelector) — multiple concurrent windows
// Use when: windows should overlap, each start event creates its own window

source$.pipe(
  bufferToggle(open$, () => close$.pipe(take(1)))
)
// open event 1 → window 1 opens
// open event 2 → window 2 opens (window 1 still open!)
// close → window 1 closes, emits its buffer
// close → window 2 closes, emits its buffer

// bufferTime(ms) / bufferCount(N) — uniform windows
// Use when: closing condition is purely time-based or count-based
source$.pipe(bufferTime(1000))

// Decision tree:
// Fixed time →        bufferTime(ms)
// Fixed count →       bufferCount(N)
// Time OR count →     bufferTime(ms, null, N)
// Event-triggered →   bufferWhen(() => event$.pipe(take(1)))
// Overlapping →       bufferToggle(open$, () => close$)
// Open/close events → bufferToggle
```

---

## Common Pitfalls

### Closing Selector Observable That Completes Without Emitting

```typescript
// ❌ If closing selector completes immediately, window closes immediately:
source$.pipe(
  bufferWhen(() => EMPTY)  // EMPTY completes with no emission
).subscribe(console.log);
// []: every single emission gets its own empty-buffer — not useful!

// ✅ Ensure the closing selector emits at least once before completing:
source$.pipe(
  bufferWhen(() => timer(1000))  // timer(1000) emits after 1s, then completes ✓
).subscribe(console.log);
```

### Reusing the Same Observable as Closing Selector Without `take(1)`

```typescript
// ❌ Closing selector subscribes to source$ again — may interact unexpectedly:
source$.pipe(
  bufferWhen(() => source$.pipe(skip(4)))
  // creates a SECOND subscription to source$ for the closing logic
  // can cause double-processing or unexpected timing
)

// ✅ Use a dedicated signal, or a timer, not the source$ itself:
const close$ = subject$.asObservable(); // separate signal

source$.pipe(
  bufferWhen(() => close$.pipe(take(1))) // dedicated close signal
)
```

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: `bufferWhen` shines for **protocol-level buffering** — when the window boundary is driven by the data itself (a delimiter byte, an end-of-frame marker, a COMMIT event). Its closing selector is called fresh on each new window, making it ideal for dynamic or state-dependent flush intervals. The key mental model: `bufferWhen` = one sequential window at a time, `bufferToggle` = multiple concurrent windows. For uniform time/count windows, the simpler `bufferTime(ms, null, maxSize)` is almost always cleaner.
