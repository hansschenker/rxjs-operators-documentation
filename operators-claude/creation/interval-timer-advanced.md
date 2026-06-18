# interval / timer — Advanced Patterns

For `interval` and `timer` fundamentals see the core docs. This page covers heartbeats, complex scheduling, coordinated timers, and production timing patterns.

---

## `timer` vs `interval` — Choosing the Right One

```typescript
import { timer, interval } from 'rxjs';

// timer(delay) — emit once after delay, then complete:
timer(3000).subscribe(() => console.log('fired once after 3s'));

// timer(delay, period) — emit at delay, then every period:
timer(1000, 5000).subscribe(i => console.log(`tick ${i}`));
// tick 0 at 1s, tick 1 at 6s, tick 2 at 11s...

// interval(period) — emit every period, starting immediately:
interval(5000).subscribe(i => console.log(`tick ${i}`));
// tick 0 at 5s, tick 1 at 10s, tick 2 at 15s...

// timer(0, period) ≡ emit immediately, then every period:
timer(0, 5000).subscribe(i => console.log(`tick ${i}`));
// tick 0 at 0s (immediate!), tick 1 at 5s, tick 2 at 10s...
// This is usually better than interval() — no initial delay
```

---

## Pattern 1: Heartbeat / Keep-Alive

```typescript
import { timer, switchMap, catchError, of } from 'rxjs';

// Send keep-alive ping every 30s; reconnect if ping fails:
const heartbeat$ = timer(0, 30_000).pipe(
  switchMap(() =>
    this.ws.ping().pipe(
      catchError(() => {
        this.ws.reconnect();
        return of(null);
      })
    )
  ),
  takeUntilDestroyed(this.destroyRef)
);

heartbeat$.subscribe();
```

---

## Pattern 2: Session Timeout with Activity Reset

```typescript
import { timer, Subject, merge, switchMap } from 'rxjs';
import { map } from 'rxjs/operators';

const activity$ = merge(
  fromEvent(document, 'mousemove'),
  fromEvent(document, 'keydown'),
  fromEvent(document, 'click')
).pipe(map(() => 'active'));

// Each activity resets the 30-minute timeout:
const sessionTimeout$ = activity$.pipe(
  startWith('init'),
  switchMap(() => timer(30 * 60 * 1000)) // new 30m timer on every activity
);

sessionTimeout$.subscribe(() => {
  this.auth.logout();
  this.router.navigate(['/session-expired']);
});
```

---

## Pattern 3: Countdown Timer

```typescript
import { timer, map, takeWhile, finalize } from 'rxjs/operators';

function countdown(seconds: number): Observable<number> {
  return timer(0, 1000).pipe(
    map(i => seconds - i),         // count DOWN from seconds
    takeWhile(n => n >= 0, true),   // inclusive: emit 0 before completing
    finalize(() => console.log('Countdown complete'))
  );
}

countdown(10).subscribe({
  next:     n  => updateDisplay(`${n}s`),
  complete: () => submitForm()
});
```

---

## Pattern 4: Staggered / Cascading Start

Stagger the start of multiple operations:

```typescript
import { timer, merge, map } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

const items = ['A', 'B', 'C', 'D', 'E'];

// Start each item 200ms after the previous:
merge(
  ...items.map((item, i) =>
    timer(i * 200).pipe(map(() => item)) // A at 0ms, B at 200ms, C at 400ms...
  )
).pipe(
  mergeMap(item => animateIn(item))
).subscribe();
```

---

## Pattern 5: Exponential Backoff Scheduler

Use `timer` to schedule retries with exponential delay:

```typescript
import { timer, defer, retry } from 'rxjs';

// Retry with exponential backoff using timer:
function withExponentialBackoff<T>(
  source$: Observable<T>,
  maxRetries = 5,
  baseMs = 1000
): Observable<T> {
  return source$.pipe(
    retry({
      count: maxRetries,
      delay: (_, attempt) => timer(baseMs * Math.pow(2, attempt - 1))
      // 1s, 2s, 4s, 8s, 16s
    })
  );
}
```

---

## Pattern 6: Scheduled Cleanup / TTL Cache

Expire cached entries after a time-to-live:

```typescript
import { timer, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

class TtlCache<K, V> {
  private cache = new Map<K, { value: V; expire$: Subject<void> }>();

  set(key: K, value: V, ttlMs: number): void {
    this.delete(key); // clear any existing entry

    const expire$ = new Subject<void>();
    this.cache.set(key, { value, expire$ });

    timer(ttlMs).pipe(takeUntil(expire$)).subscribe(() => {
      this.cache.delete(key);
    });
  }

  get(key: K): V | undefined {
    return this.cache.get(key)?.value;
  }

  delete(key: K): void {
    const entry = this.cache.get(key);
    if (entry) {
      entry.expire$.next();
      entry.expire$.complete();
      this.cache.delete(key);
    }
  }
}
```

---

## Pattern 7: Coordinated Multi-Timer

Run multiple timers with different periods and coordinate their output:

```typescript
import { timer, combineLatest } from 'rxjs';
import { map, startWith } from 'rxjs/operators';

// Slow clock (seconds) + fast clock (tenths) displayed together:
const seconds$ = timer(0, 1000).pipe(map(i => i));
const tenths$  = timer(0, 100).pipe(map(i => i % 10));

combineLatest({ seconds: seconds$, tenths: tenths$ }).pipe(
  map(({ seconds, tenths }) => `${seconds}.${tenths}s`)
).subscribe(updateStopwatch);
```

---

## Pattern 8: Debounce Using `timer` Directly

```typescript
import { timer, Subject, switchMap } from 'rxjs';

// Manual debounce using switchMap + timer (illustrative):
const input$ = new Subject<string>();

input$.pipe(
  switchMap(value => timer(300).pipe(map(() => value)))
  // timer(300) is cancelled and restarted on each input
).subscribe(search);

// This is exactly what debounceTime(300) does internally
```

---

## `timer` vs `setTimeout` vs `setInterval`

| | `setTimeout` | `setInterval` | `timer` / `interval` |
|---|---|---|---|
| Cancellable | `clearTimeout(id)` | `clearInterval(id)` | `subscription.unsubscribe()` |
| Composable | No | No | Yes — pipe, combine, transform |
| Multiple consumers | No | No | Yes — share() |
| Testable | Requires mocking | Requires mocking | TestScheduler |
| Value | None | None | Incremental index |

---

## Common Pitfalls

### `interval` vs `timer(0, period)` — The Initial Delay Trap

```typescript
// ❌ interval starts after one period — first emission at 5s, not 0s:
interval(5000).subscribe(fetchData); // first fetch at 5s!

// ✅ timer(0, period) emits immediately, then every period:
timer(0, 5000).subscribe(() => fetchData()); // fetch at 0s, 5s, 10s...
// WHY: For polling, you almost always want an immediate first emission.
// Use interval only when you intentionally want to skip the first period.
```

### Not Unsubscribing from `interval`

```typescript
// ❌ MEMORY LEAK — interval runs forever even after component unmount:
ngOnInit() {
  interval(1000).subscribe(updateClock); // never stopped!
}

// ✅ Unsubscribe on destroy:
ngOnInit() {
  interval(1000).pipe(
    takeUntilDestroyed(this.destroyRef)
  ).subscribe(updateClock);
}
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**Key rule**: Prefer `timer(0, period)` over `interval(period)` for polling — `timer` emits immediately while `interval` waits one full period before the first emission. Use `timer(delay)` (without period) for one-shot delays.
