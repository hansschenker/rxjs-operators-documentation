# skipUntil / skipWhile — Advanced Patterns

For fundamentals see the core [skipUntil / skipWhile](./skipUntil-skipWhile) doc. This page covers startup gates, deferred emission, initialization sequencing, and the key differences from `filter`, `takeUntil`, and `takeWhile`.

---

## Mental Model

```
skipUntil(signal$):  skip emissions until signal$ emits, then pass everything through
skipWhile(predicate): skip while predicate is true, then pass everything through (forever)
```

Both are **one-way gates**: once open, they never close. If you need a re-closeable gate, use `filter` with a `BehaviorSubject` or `combineLatest`.

---

## Pattern 1: App Initialization Gate

Hold all user actions until the app has fully initialized:

```typescript
import { skipUntil, shareReplay } from 'rxjs/operators';
import { Subject, forkJoin } from 'rxjs';

class AppGate {
  private ready$ = new Subject<void>();
  readonly open$ = this.ready$.pipe(shareReplay(1));

  markReady(): void {
    this.ready$.next();
    this.ready$.complete();
  }
}

const gate = new AppGate();

// Initialize async resources:
forkJoin({
  config: configService.load(),
  auth:   authService.restore(),
  i18n:   i18nService.load()
}).subscribe(() => gate.markReady());

// All user interactions are buffered until init completes:
userActions$.pipe(
  skipUntil(gate.open$),
  takeUntilDestroyed()
).subscribe(handleAction);

// API calls gated on auth:
apiRequests$.pipe(
  skipUntil(authService.authenticated$),
  takeUntilDestroyed()
).subscribe(executeRequest);
```

---

## Pattern 2: Skip Initial Loading State

Ignore the `null`/`undefined` placeholder before data arrives:

```typescript
import { skipWhile, distinctUntilChanged } from 'rxjs/operators';

interface LoadingState<T> { data: T | null; loading: boolean; }

// Skip until we have real data:
appState$.pipe(
  map(s => s.userData),
  skipWhile(user => user === null), // skip nulls at startup
  distinctUntilChanged()
).subscribe(user => renderUserProfile(user!));

// Skip until loading is complete:
appState$.pipe(
  skipWhile(s => s.loading),
  map(s => s.data)
).subscribe(renderContent);
```

---

## Pattern 3: Skip Until User Interaction (Lazy Activation)

Don't start expensive work until the user actually engages:

```typescript
import { skipUntil, share, merge } from 'rxjs/operators';
import { fromEvent } from 'rxjs';

const firstInteraction$ = merge(
  fromEvent(document, 'click'),
  fromEvent(document, 'keydown'),
  fromEvent(document, 'touchstart')
).pipe(
  take(1),
  shareReplay(1)
);

// Don't start expensive WebSocket until user interacts:
liveDataStream$.pipe(
  skipUntil(firstInteraction$),
  takeUntilDestroyed()
).subscribe(updateLiveUI);

// Don't prefetch secondary data until user scrolls past fold:
const belowFold$ = fromEvent(window, 'scroll').pipe(
  filter(() => window.scrollY > window.innerHeight),
  take(1)
);

secondaryContent$.pipe(
  skipUntil(belowFold$)
).subscribe(renderSecondaryContent);
```

---

## Pattern 4: Skip While Rate-Limited

Skip emissions while a cooldown is active:

```typescript
import { skipWhile, tap, switchMap } from 'rxjs/operators';
import { BehaviorSubject, timer } from 'rxjs';

// Simple cooldown flag:
class RateLimitedEmitter {
  private cooldown$ = new BehaviorSubject(false);

  wrap<T>(source$: Observable<T>, cooldownMs: number): Observable<T> {
    return source$.pipe(
      skipWhile(() => this.cooldown$.getValue()),
      tap(() => {
        this.cooldown$.next(true);
        timer(cooldownMs).subscribe(() => this.cooldown$.next(false));
      })
    );
  }
}

const limiter = new RateLimitedEmitter();

// Skip button clicks while cooldown active:
limiter.wrap(buttonClicks$, 2000).pipe(
  switchMap(click => submitForm())
).subscribe(handleResult);
```

---

## Pattern 5: Skip Until Authentication Token Valid

Gate all requests on a valid, non-expired auth token:

```typescript
import { skipUntil, filter, map } from 'rxjs/operators';

interface AuthToken { token: string; expiresAt: number; }

const validToken$ = authToken$.pipe(
  filter((t): t is AuthToken => t !== null && Date.now() < t.expiresAt),
  take(1)
);

// HTTP requests skip until we have a valid token:
pendingRequests$.pipe(
  skipUntil(validToken$),
  withLatestFrom(authToken$),
  map(([req, token]) => ({ ...req, headers: { Authorization: `Bearer ${token!.token}` } }))
).subscribe(executeRequest);
```

---

## Pattern 6: `skipWhile` for Startup Jitter

Skip erratic initial values from hardware/sensors before they stabilize:

```typescript
import { skipWhile, pairwise, filter } from 'rxjs/operators';

// GPS: skip until accuracy is below 20 meters:
gpsPosition$.pipe(
  skipWhile(pos => pos.accuracy > 20),
  takeUntilDestroyed()
).subscribe(updateMapPosition);

// Sensor: skip first 5 readings (sensor warm-up period):
sensorReadings$.pipe(
  skipWhile((_, index) => index < 5), // Note: skipWhile doesn't receive index
  // Alternative with index:
).subscribe(processSensorData);

// Alternative using scan for index-based skip:
sensorReadings$.pipe(
  scan((acc, val) => ({ count: acc.count + 1, val }), { count: 0, val: null }),
  filter(({ count }) => count > 5),
  map(({ val }) => val!)
).subscribe(processSensorData);
```

---

## `skipUntil` vs `skipWhile` vs `filter` vs `takeUntil`

```typescript
// skipUntil(signal$) — gate: skip until an Observable emits (one-way)
source$.pipe(skipUntil(ready$))
// ✓ Event-driven gate; works with any Observable as trigger
// ✗ Once open, never re-closes

// skipWhile(predicate) — condition gate: skip while condition holds (one-way)
source$.pipe(skipWhile(v => v < 10))
// ✓ Inline predicate; doesn't require a separate Observable
// ✗ Once open, never re-closes; predicate not re-evaluated after opening

// filter(predicate) — permanent conditional: drop non-matching values
source$.pipe(filter(v => v > 10))
// ✓ Re-evaluated on EVERY emission; gate can open and close
// ✓ Works on infinite streams
// ✗ More CPU if condition is heavy

// takeUntil(signal$) — mirror of skipUntil but CLOSES instead of opens:
source$.pipe(takeUntil(stop$))
// Passes through until signal$, then stops completely
```

**Decision**: Use `skipUntil` / `skipWhile` when you have a one-time startup condition. Use `filter` when the condition can fluctuate. Use `takeUntil` to *end* a stream on a signal.

---

## Common Pitfalls

### `skipWhile` Predicate Is Never Re-Evaluated After First Pass

```typescript
// ❌ Expecting skipWhile to re-apply after it has opened:
const flag$ = new BehaviorSubject(true);

source$.pipe(
  skipWhile(() => flag$.getValue()) // checked on EVERY value until condition is false
).subscribe(v => {
  // Once flag$.getValue() returns false for one value,
  // ALL subsequent values pass through — even if flag$ goes back to true!
  console.log(v);
});

flag$.next(false); // opens the gate
flag$.next(true);  // too late — gate is permanently open

// ✅ Use filter for a re-closeable gate:
source$.pipe(
  filter(() => !flag$.getValue()) // re-evaluated on every emission
)
```

### `skipUntil` Subscribes to the Notifier Immediately

```typescript
// ❌ Notifier that completes before source starts emitting:
const signal$ = timer(1000); // emits once at 1s, then completes

source$.pipe(
  skipUntil(signal$) // subscribes to timer(1000) immediately
)
// If source$ doesn't start emitting within 1s, the gate opens too early
// and ALL source values (even delayed ones) pass through — expected behavior,
// but surprising if you expected the gate to wait for source to be active first.

// ✅ Understand: skipUntil opens the gate when notifier emits, regardless of source timing.
// This is usually what you want for init gates.
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Primary use case**: `skipUntil` is the startup gate operator — hold back any stream until an async initialization completes. It's the RxJS equivalent of "wait for the app to boot before processing events." `skipWhile` is best for filtering out invalid initial values (loading states, null placeholders, sensor warm-up).
