# debounceTime

## Identity
- **Name**: debounceTime
- **Category**: Filtering Operators / Rate Limiting Operators
- **Type**: Time-based silence filter (emits after idle period)
- **Import**:
  ```typescript
  import { debounceTime } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { debounceTime } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function debounceTime<T>(
    dueTime: number,
    scheduler?: SchedulerLike
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable, typically one that bursts rapidly

**Output**: `Observable<T>` — an Observable that emits only the last value of each burst, after the source has been silent for `dueTime` milliseconds

**Transformation**: Each source emission starts (or restarts) a timer of `dueTime` ms. If no new emission arrives before the timer expires, the most recent value is forwarded. If a new emission arrives before the timer expires, the previous value is discarded and the timer resets. Only the final value of a rapid burst ever reaches downstream.

**Mathematical representation**:
```
Let S be the source Observable with emissions at times t₁, t₂, t₃, ...
Let D = dueTime (ms)

An emission vᵢ at time tᵢ is forwarded if and only if:
  tᵢ₊₁ - tᵢ > D  (next emission is more than D ms away)
  OR vᵢ is the last emission before source completes

All other emissions are silently discarded.
```

**Invariants**:
- **Last-value semantics**: Of any burst of emissions within `dueTime`, only the last is forwarded — intermediate values are permanently discarded
- **Minimum silence**: Every forwarded value is preceded by at least `dueTime` ms of source silence
- **Asynchronous**: Forwarding always happens asynchronously, at least `dueTime` ms after the triggering emission
- **Completion flushes**: If the source completes while a debounce timer is pending, the buffered value is emitted immediately before the completion signal
- **Error bypasses timer**: Source errors propagate immediately, discarding any pending buffered value

## Marble Diagram

```
Source:   --a-bc--------d-e-f--------|
          debounceTime(3--)
Result:   --------c-----------f------|

Legend:
  - : time unit (10ms here; 3-- = 30ms dueTime)
  a,b,c,d,e,f : emitted values
  | : completion
  a is discarded (b arrives before timer expires)
  b is discarded (c arrives before timer expires)
  c is emitted  (30ms of silence after c)
  d is discarded (e arrives before timer expires)
  e is discarded (f arrives before timer expires)
  f is emitted  (30ms of silence after f)
```

**Completion flushes pending value**:
```
Source:   --a--b--c--|
          debounceTime(3--)
Result:   --------c-|

c is pending in the debounce buffer when | arrives.
The buffer is flushed synchronously before | propagates.
```

**Error discards pending value**:
```
Source:   --a--b--c--#
          debounceTime(3--)
Result:   -----------#

The pending c is discarded; # propagates immediately.
```

**Key observation**: `debounceTime` answers the question "what was the user's *final intent* after a burst of input?" — it waits for silence before acting.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily on output subscription
- Internally holds at most one pending value at a time — each new emission replaces the previous pending value
- Uses `asyncScheduler` by default (macrotask-based); injectable via the `scheduler` parameter for testing

**Completion semantics**:
- If the source completes with no pending value, completion propagates immediately
- If the source completes with a pending buffered value, that value is emitted synchronously, then completion follows
- This flush-on-complete behaviour is unique to debounceTime among rate-limiting operators

**Error handling**:
- Source errors propagate immediately, bypassing the debounce timer
- Any value currently waiting in the debounce buffer is silently discarded on error
- The `debounceTime` operator itself does not throw

**Backpressure**:
- Buffers exactly one value (the latest) — memory is O(1) regardless of source emission rate
- High-frequency sources are rate-limited to at most one emission per `dueTime` ms
- Acts as a natural shield against overwhelming downstream with rapid bursts

**Hot vs. Cold**:
- Works identically with hot and cold sources
- With hot sources (e.g. `fromEvent`), all intermediate values between the last emission and the timer expiry are permanently lost — there is no replay

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type emitted by both source and result (unchanged — MonoTypeOperatorFunction)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * Type Narrowing:
 *   - None — debounceTime is a MonoTypeOperatorFunction; T in = T out
 *
 * Type Safety:
 *   - dueTime is typed as number (milliseconds)
 *   - scheduler is typed as SchedulerLike (optional; use VirtualTimeScheduler in tests)
 *   - The emitted value type T is fully preserved
 */

import { fromEvent } from 'rxjs';
import { debounceTime, map } from 'rxjs/operators';

const input = document.querySelector('input')!;

// T = Event, preserved through debounceTime
const debounced$ = fromEvent<InputEvent>(input, 'input').pipe(
  debounceTime(300),
  map(e => (e.target as HTMLInputElement).value)
);
// debounced$: Observable<string>

// Testing with VirtualTimeScheduler
import { TestScheduler } from 'rxjs/testing';

const scheduler = new TestScheduler((actual, expected) => {
  expect(actual).toEqual(expected);
});

scheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('a-b--c------|');
  const result$ = source$.pipe(debounceTime(3, scheduler));
  expectObservable(result$).toBe('--------c---|');
  // c is emitted 3 frames after c (the last in the burst)
  // then | flushes immediately (no pending value)
});
```

## Examples

### Basic Usage — Search Input Debounce
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, map, distinctUntilChanged } from 'rxjs/operators';

const searchInput = document.getElementById('search') as HTMLInputElement;

const search$ = fromEvent(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounceTime(300),          // wait 300ms of silence
  distinctUntilChanged()      // skip if value unchanged
);

search$.subscribe(query => {
  console.log('Searching for:', query);
  performSearch(query);
});

// User types "rxjs" quickly:
// 'r' at 0ms   → timer starts, reset at 50ms
// 'rx' at 50ms  → timer reset, reset at 100ms
// 'rxj' at 100ms → timer reset, reset at 150ms
// 'rxjs' at 150ms → timer starts
// 450ms (150 + 300): Searching for: rxjs
// Only one search request, not four!
```

### Common Pattern — Form Field Validation
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, map, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const usernameInput = document.getElementById('username') as HTMLInputElement;
const feedback     = document.getElementById('feedback')!;

interface AvailabilityResult { available: boolean; username: string; }

fromEvent(usernameInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(500),           // validate only after typing pauses
  switchMap(username =>
    ajax.getJSON<AvailabilityResult>(`/api/check-username?name=${username}`)
  )
).subscribe(({ available, username }) => {
  feedback.textContent = available
    ? `✓ "${username}" is available`
    : `✗ "${username}" is taken`;
  feedback.className = available ? 'valid' : 'invalid';
});

// Without debounceTime: one API call per keypress
// With debounceTime(500): one call per pause in typing — much cheaper
```

### Common Pattern — Window Resize Handler
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, map, startWith } from 'rxjs/operators';

interface WindowSize { width: number; height: number; }

const size$ = fromEvent(window, 'resize').pipe(
  debounceTime(150),           // skip intermediate resize events
  map((): WindowSize => ({
    width:  window.innerWidth,
    height: window.innerHeight,
  })),
  startWith({                  // emit immediately with current size
    width:  window.innerWidth,
    height: window.innerHeight,
  })
);

size$.subscribe(({ width, height }) => {
  console.log(`Resized to ${width}×${height}`);
  recalculateLayout(width, height);
});

// Without debounceTime: recalculateLayout called ~60 times/s during drag
// With debounceTime(150): called once, 150ms after resize stops
```

### Edge Cases — Completion Flush and Rapid Bursts
```typescript
import { Subject, of } from 'rxjs';
import { debounceTime, delay, concatMap } from 'rxjs/operators';

// Edge case 1: completion flushes pending value
const subject$ = new Subject<string>();

subject$.pipe(debounceTime(200)).subscribe({
  next:     v => console.log('next:', v),
  complete: () => console.log('complete'),
});

subject$.next('a');
subject$.next('b');
subject$.next('c'); // pending in buffer
subject$.complete(); // flushes 'c' immediately, then completes

// Output (synchronous):
// next: c
// complete

// Edge case 2: all values discarded (source completes before any silence)
of('x', 'y', 'z').pipe(
  concatMap(v => of(v).pipe(delay(10))), // emit every 10ms
  debounceTime(50)                        // 50ms silence needed
).subscribe({
  next:     v => console.log('next:', v),
  complete: () => console.log('complete'),
});
// Output:
// next: z     ← flushed by completion
// complete

// Edge case 3: error discards pending value
const errSubject$ = new Subject<number>();

errSubject$.pipe(debounceTime(200)).subscribe({
  next:  v   => console.log('next:', v),
  error: err => console.log('error:', err.message),
});

errSubject$.next(42); // pending in buffer
errSubject$.error(new Error('stream failed')); // discards 42

// Output:
// error: stream failed
// (42 is never emitted)
```

### Advanced Pattern — Debounced Search with Loading State
```typescript
import { fromEvent, merge, of } from 'rxjs';
import { debounceTime, map, switchMap, catchError,
         distinctUntilChanged, tap, startWith } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface SearchState {
  loading: boolean;
  results: string[];
  error: string | null;
  query: string;
}

const searchInput = document.getElementById('search') as HTMLInputElement;

const query$ = fromEvent(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  startWith(''),
  distinctUntilChanged(),
  debounceTime(300)
);

const state$ = query$.pipe(
  tap(() => setLoading(true)),
  switchMap(query =>
    query.length < 2
      ? of({ loading: false, results: [], error: null, query })
      : ajax.getJSON<string[]>(`/api/search?q=${query}`).pipe(
          map(results => ({ loading: false, results, error: null, query })),
          catchError(err  => of({ loading: false, results: [], error: err.message, query }))
        )
  )
);

state$.subscribe(({ loading, results, error, query }) => {
  setLoading(loading);
  if (error)           renderError(error);
  else if (!query)     clearResults();
  else                 renderResults(results);
});
```

## Common Pitfalls

### Anti-pattern: Confusing `debounceTime` with `throttleTime`
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, throttleTime } from 'rxjs/operators';

const clicks$ = fromEvent(document, 'click');

// ❌ WRONG for "respond to first click, ignore rapid re-clicks"
clicks$.pipe(
  debounceTime(1000)    // waits for 1s of silence — misses the first click
).subscribe(handleClick);

// ✅ CORRECT: throttleTime emits the FIRST value, then silences
clicks$.pipe(
  throttleTime(1000)    // emits first click, ignores for 1s
).subscribe(handleClick);

// ❌ WRONG for "search after user stops typing"
clicks$.pipe(
  throttleTime(300)     // emits FIRST keystroke, ignores rest — incomplete query
);

// ✅ CORRECT: debounceTime emits the LAST value after silence
fromEvent(input, 'input').pipe(
  debounceTime(300)     // waits for pause, emits final value
).subscribe(search);

// WHY: debounceTime = "wait for silence, emit last" (trailing edge)
//      throttleTime = "emit first, then silence" (leading edge)
// Choosing wrong means either missing user intent or acting on partial input.
```

### Anti-pattern: Intermediate Values Are Gone
```typescript
import { Subject } from 'rxjs';
import { debounceTime } from 'rxjs/operators';

// ❌ INCORRECT assumption: that debounceTime queues and replays values
const prices$ = new Subject<number>();

prices$.pipe(debounceTime(500)).subscribe(p => {
  updateChart(p); // Only last price per 500ms window — OK for display
  recordToAuditLog(p); // WRONG: intermediate prices are silently dropped!
});

prices$.next(100);
prices$.next(101);
prices$.next(99);
// Only 99 reaches the subscriber — 100 and 101 are lost forever

// ✅ CORRECT: use debounceTime only for display/trigger, log all values separately
prices$.subscribe(p => recordToAuditLog(p)); // tap all values
prices$.pipe(debounceTime(500)).subscribe(p => updateChart(p)); // display only

// WHY: debounceTime permanently discards non-final values.
// Never use it when every emission must be processed (audit logs, billing, etc.)
```

### Anti-pattern: `debounceTime` Before Expensive Setup
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ INEFFICIENT: debounceTime after switchMap — search fires on every keystroke
fromEvent(input, 'input').pipe(
  map(e => e.target.value),
  switchMap(q => ajax.getJSON(`/api/search?q=${q}`)), // fires immediately per keystroke
  debounceTime(300)  // only debounces the RESULTS, not the requests
).subscribe(renderResults);

// ✅ CORRECT: debounceTime before switchMap — search fires only after pause
fromEvent(input, 'input').pipe(
  map(e => e.target.value),
  debounceTime(300),                                  // wait for typing pause
  switchMap(q => ajax.getJSON(`/api/search?q=${q}`)) // then fire request
).subscribe(renderResults);

// WHY: Operator order matters. debounceTime must gate the source BEFORE
// expensive operations like HTTP requests or heavy computation.
```

### Anti-pattern: Too Short a Debounce on Slow Connections
```typescript
// ❌ FRAGILE: 100ms may not be enough if the user is on a mobile keyboard
fromEvent(input, 'input').pipe(
  debounceTime(100), // triggers on pauses between autocomplete suggestions
  switchMap(q => search(q))
).subscribe(render);

// ✅ BETTER: adapt to input method and use 300–500ms for text search
fromEvent(input, 'input').pipe(
  debounceTime(300), // accommodates hunt-and-peck typists and autocomplete
  distinctUntilChanged(),
  switchMap(q => search(q))
).subscribe(render);

// Rule of thumb:
// - Search / validation: 300–500ms
// - Window resize / scroll: 100–150ms
// - Button debounce (prevent double-click): 200–300ms
// - Autosave: 1000–2000ms

// WHY: A debounce too short still fires on intermediate values; too long
// feels sluggish. 300ms is the widely-accepted baseline for text input.
```

### Performance: Memory and Timer Cleanup
**When this matters**:
Long-lived subscriptions (dashboard components that never unmount) leak timer handles if the subscription is not cleaned up.

**What to do**:
```typescript
import { fromEvent, Subject } from 'rxjs';
import { debounceTime, takeUntil } from 'rxjs/operators';

// Angular component example
class SearchComponent implements OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    fromEvent(this.inputEl, 'input').pipe(
      debounceTime(300),
      takeUntil(this.destroy$) // cancel pending timer on destroy
    ).subscribe(this.search.bind(this));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

// Without takeUntil: if a value is buffered when the component is destroyed,
// the timer fires and the callback runs on a destroyed component.
```

## Related Operators

**Same Category (Rate Limiting)**:
- **`throttleTime`**: Emits the *first* value then silences for `dueTime` — use when you want leading-edge behaviour (act immediately, ignore subsequent rapid events)
- **`auditTime`**: Emits the *latest* value at fixed periodic intervals — similar to debounceTime but timer-driven rather than silence-driven; always fires if source is active
- **`sampleTime`**: Emits the latest value at fixed intervals regardless of source activity — use for polling/periodic snapshots
- **`debounce`**: Like `debounceTime` but with a dynamic per-emission Observable duration — use when the silence period should adapt based on the emitted value

**Complementary Operators**:
- **`distinctUntilChanged`**: Always pair after `debounceTime` for search inputs — prevents re-triggering if the user pauses on the same value twice
- **`switchMap`**: The natural downstream partner — cancels in-flight requests when a new debounced value arrives
- **`startWith`**: Use before `debounceTime` to emit an initial value immediately without waiting for the first source emission
- **`takeUntil`**: Use to cancel pending debounce timers on component/stream teardown

**Alternatives by Use Case**:

| Use Case | Instead of `debounceTime` | Use This | Why |
|----------|--------------------------|----------|-----|
| Act on first input, ignore rapid follow-ups | `debounceTime(ms)` | `throttleTime(ms)` | Leading edge — immediate response |
| Periodic snapshot of latest value | `debounceTime(ms)` | `auditTime(ms)` | Fixed-interval, not silence-based |
| Dynamic silence duration | `debounceTime(ms)` | `debounce(v => timer(fn(v)))` | Per-value timing |
| All values must be processed | `debounceTime(ms)` | `bufferTime(ms)` or no rate limit | No values discarded |
| Deduplicate same value | `debounceTime(ms)` | `distinctUntilChanged()` | No timing needed |

**Comparison — Rate Limiting Operators**:

| Operator | Emits | When | Discards |
|----------|-------|------|----------|
| `debounceTime(t)` | Last in burst | After `t`ms silence | All but last per burst |
| `throttleTime(t)` | First in burst | Immediately | All within `t`ms after first |
| `auditTime(t)` | Latest | Every `t`ms (if active) | All but latest per interval |
| `sampleTime(t)` | Latest | Every `t`ms (always) | All but latest per interval |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/debounceTime](https://rxjs.dev/api/operators/debounceTime)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/debounce.html](http://reactivex.io/documentation/operators/debounce.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/debounceTime.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/debounceTime.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Silence-Gate Strategy (Trailing-Edge Rate Limiting)
- **Cognitive Load**: 2/5 — The "wait for silence" metaphor is intuitive; the main subtlety is understanding that intermediate values are discarded and that completion flushes the buffer
- **Usage Frequency**: 5/5 — The canonical solution for search-as-you-type, form validation, and resize handlers; in virtually every UI-heavy application
- **Composability**: 4/5 — Pairs naturally with `switchMap`, `distinctUntilChanged`, and `takeUntil`; order-sensitive (must come before expensive operations)

**Problem Domain**:
Preventing over-triggering of expensive or disruptive operations (API calls, DOM reflows, analytics events) when the source emits in rapid bursts. The operator answers "what did the user *intend* after finishing their input?" rather than "what is the user *doing right now*?"

**When to Teach**:
Teach immediately after `filter` and `map` when introducing real-world UI patterns, since search-as-you-type is the canonical first async example.

- **Prerequisites**: `map`, `filter`, `switchMap` (for the canonical search pipeline), understanding of asynchronous operators
- **Teaches**: Rate limiting concepts, the silence metaphor, the distinction between leading-edge (throttle) and trailing-edge (debounce) strategies
- **Leads to**: `throttleTime` (contrast), `auditTime` (contrast), `debounce` (dynamic variant), scheduler injection for testing
- **Common with**: `switchMap`, `distinctUntilChanged`, `catchError`, `takeUntil`

**Common Misconceptions**:
1. **"debounceTime queues values and replays them"** — it discards all but the last; use `bufferTime` if every value must be processed
2. **"debounceTime and throttleTime do the same thing"** — debounce waits for silence (trailing edge); throttle acts immediately and then silences (leading edge)
3. **"completion always waits for the timer"** — completion *flushes* a pending value immediately, it does not wait for the timer to expire
4. **"the timer runs from the last emission regardless of source completion"** — completion interrupts the timer and forces an immediate flush
