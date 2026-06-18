# bufferToggle / windowToggle — Advanced Patterns

For fundamentals see [bufferToggle / windowToggle](./bufferToggle-windowToggle). This page covers event-gated recording, recording sessions, activity windows, audio/video buffering concepts, and comparison with `bufferWhen`.

---

## Mental Model: Event-Gated Windows

```typescript
import { bufferToggle, windowToggle } from 'rxjs/operators';

// bufferToggle(openings$, closingSelector)
// - openings$ emits → open a new buffer
// - closingSelector(openValue) returns an Observable → when IT emits, close that buffer
// - Multiple buffers can be open simultaneously (overlapping windows)

// Example — buffer between start/stop signals:
const start$ = fromEvent(startBtn, 'click');
const stop$  = fromEvent(stopBtn, 'click');

source$.pipe(
  bufferToggle(start$, () => stop$)
).subscribe(buffer => console.log('Collected:', buffer));
// Starts collecting on start-click, emits array on stop-click
// Multiple clicks = multiple overlapping buffers

// windowToggle — same semantics, emits Observable<T> instead of T[]
source$.pipe(
  windowToggle(start$, () => stop$),
  mergeMap(window$ => window$.pipe(toArray()))
) // equivalent to bufferToggle but streaming
```

---

## Pattern 1: Recording/Session Windows

Capture user activity between "session start" and "session end" events:

```typescript
import { bufferToggle, Subject, merge } from 'rxjs';

// User activity recorder — capture events between login/logout:
const userLogin$  = this.authService.events$.pipe(filter(e => e.type === 'LOGIN'));
const userLogout$ = this.authService.events$.pipe(filter(e => e.type === 'LOGOUT'));

const sessionActivity$ = userInteractions$.pipe(
  bufferToggle(
    userLogin$,
    loginEvent => userLogout$.pipe(
      // Close the window on the FIRST logout after this login:
      first()
    )
  )
);

sessionActivity$.subscribe(session => {
  analyticsService.saveSession({
    events:    session,
    duration:  session.length,
    userId:    session[0]?.userId
  });
});

// Capture form changes within an edit session:
const editStart$ = fromEvent(editButton, 'click');
const editStop$  = merge(
  fromEvent(saveButton,   'click'),
  fromEvent(cancelButton, 'click')
);

formValueChanges$.pipe(
  bufferToggle(editStart$, () => editStop$.pipe(first()))
).subscribe(changes => {
  console.log(`${changes.length} changes during edit session`);
});
```

---

## Pattern 2: Overlapping Time Windows for Analysis

`bufferToggle` can open multiple windows simultaneously — unlike `bufferTime` which uses non-overlapping tumbling windows:

```typescript
import { bufferToggle, interval, timer } from 'rxjs';

// Sliding 5-second windows, opening every 1 second:
const windowOpen$ = interval(1000);

sensorData$.pipe(
  bufferToggle(
    windowOpen$,
    () => timer(5000)  // each window stays open for 5s
  ),
  map(window => ({
    samples: window.length,
    average: window.reduce((a, b) => a + b, 0) / (window.length || 1),
    min:     Math.min(...window),
    max:     Math.max(...window)
  }))
).subscribe(stats => updateAnalyticsDashboard(stats));

// This creates overlapping 5-second windows starting each second:
// t=0: window1 opens (closes t=5)
// t=1: window2 opens (closes t=6)
// t=2: window3 opens (closes t=7)
// ...each window independently captures all emissions during its 5s span
```

---

## Pattern 3: Keyboard/Gamepad Input Windows

Capture sequences of inputs within a time limit (combo detection):

```typescript
import { bufferToggle, fromEvent, timer, merge, EMPTY } from 'rxjs';
import { filter, map } from 'rxjs/operators';

// Capture all key presses within 1 second of the first key in a sequence:
const keyDown$ = fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  map(e => e.key)
);

// Open a window on any key press, close after 1000ms:
const keyCombos$ = keyDown$.pipe(
  bufferToggle(
    keyDown$,            // open on every key press
    () => timer(1000)    // close 1 second later
  ),
  map(keys => keys.join('+')),
  filter(combo => combo.length > 1) // skip single keystrokes
);

keyCombos$.subscribe(combo => {
  if (combo === 'ArrowUp+ArrowUp+ArrowDown+ArrowDown') {
    activateKonamiCode();
  }
});

// Game combo detection — only track combos starting with a specific key:
const comboStarter$ = keyDown$.pipe(filter(k => k === 'Shift'));

const shiftCombos$ = keyDown$.pipe(
  bufferToggle(
    comboStarter$,
    () => timer(500) // 500ms window after Shift
  )
);
```

---

## Pattern 4: Network Request Batching by Context

Buffer requests that arrive while a "batch window" is open:

```typescript
import { bufferToggle, Subject, switchMap, concatMap } from 'rxjs';

// Batch all API requests that arrive while a "batch window" is open:
const batchWindowOpen$  = new Subject<void>();
const batchWindowClose$ = new Subject<void>();

// User can manually open/close batch mode:
const pendingRequests$ = apiRequests$.pipe(
  bufferToggle(
    batchWindowOpen$,
    () => batchWindowClose$.pipe(first())
  )
);

pendingRequests$.pipe(
  concatMap(batch =>
    batch.length > 0
      ? this.api.bulkRequest$(batch)
      : EMPTY
  )
).subscribe(results => processResults(results));

// Alternative: auto-batch with a debounce-style window:
// Open window on first request, close after 50ms of silence
const firstRequest$ = apiRequests$.pipe(take(1));

apiRequests$.pipe(
  bufferToggle(
    firstRequest$,
    () => apiRequests$.pipe(debounceTime(50), first())
  ),
  repeat() // reopen after each batch completes
).subscribe(batch => processBatch(batch));
```

---

## `bufferToggle` vs `bufferWhen` vs `bufferTime`

```typescript
// bufferTime(N) — tumbling windows (non-overlapping, time-driven)
// Use when: you want every N milliseconds, no overlap, no event trigger
source$.pipe(bufferTime(1000))
// Opens every 1s, closes every 1s (sequential, non-overlapping)

// bufferWhen(closingSelector) — single buffer, factory provides close signal
// Opens immediately when subscribed, closes when factory Observable emits, then re-opens
// Use when: you have a single stream, timing is dynamic, no simultaneous windows
source$.pipe(bufferWhen(() => interval(randomDelay$())))

// bufferToggle(openings$, closingSelector) — multiple simultaneous buffers
// Opens when openings$ emits, each gets its own closing selector
// Use when: events trigger windows, multiple windows can overlap, start/stop semantics
source$.pipe(bufferToggle(opens$, open => closes$(open)))

// Overlap capability:
// bufferTime(N, M) — overlapping time windows (N=window size, M=step)
// bufferToggle      — full control over when each window opens/closes
// bufferWhen        — one window at a time, no overlap

// Decision:
// Multiple concurrent windows triggered by events → bufferToggle
// Single window with dynamic close trigger → bufferWhen
// Time-driven non-overlapping windows → bufferTime(N)
// Time-driven overlapping windows → bufferTime(N, step)
```

---

## Common Pitfalls

### Closing Selector That Never Completes — Window Never Closes

```typescript
// ❌ Closing selector returns a Subject that never emits — buffer grows forever:
const close$ = new Subject<void>();

source$.pipe(
  bufferToggle(
    opens$,
    () => close$  // if close$.next() is never called, buffer never emits
  )
).subscribe(console.log); // nothing ever emits, memory grows

// ✅ Always ensure the closing Observable emits or completes:
source$.pipe(
  bufferToggle(
    opens$,
    () => merge(
      close$,
      timer(30_000)  // safety timeout — close after 30s no matter what
    ).pipe(first())
  )
).subscribe(console.log);
```

### Multiple Open Signals Without Awareness of Overlap

```typescript
// ❌ Each click opens a new buffer — clicking rapidly creates many concurrent buffers:
fromEvent(button, 'click').pipe(
  bufferToggle(
    fromEvent(button, 'click'), // every click opens a new buffer
    () => timer(5000)
  )
).subscribe(buf => console.log(`Buffer of ${buf.length}`));
// 5 rapid clicks = 5 overlapping 5-second buffers — probably not intended

// ✅ Use take(1) or exhaustMap to prevent concurrent windows if overlap is undesired:
const singleWindow$ = exhaustMap(() =>
  source$.pipe(
    bufferWhen(() => timer(5000).pipe(first())),
    take(1) // one buffer per click
  )
);

fromEvent(button, 'click').pipe(
  exhaustMap(() => source$.pipe(
    bufferWhen(() => timer(5000)),
    take(1)
  ))
).subscribe(buf => console.log(`Buffer of ${buf.length}`));
```

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: `bufferToggle` is the operator for **event-gated recording** — collect emissions between a start signal and a stop signal. Its distinguishing power over `bufferTime` is that it can run multiple simultaneous buffers (each `open$` emission creates a new independent buffer), making it ideal for overlapping analysis windows and concurrent session recording. Always include a safety timeout in the closing selector (`merge(close$, timer(maxDuration)).pipe(first())`) — a closing Observable that never emits is an unbounded memory leak.
