# Polling Patterns with RxJS

Fixed-interval, adaptive, conditional, long-polling, and server-sent event patterns.

---

## Pattern 1: Simple Fixed-Interval Poll

```typescript
import { timer } from 'rxjs';
import { switchMap, shareReplay } from 'rxjs/operators';

// Poll every 30 seconds:
const status$ = timer(0, 30_000).pipe(
  switchMap(() => this.api.getStatus()),
  shareReplay(1)
);

// Or with retry for transient failures:
const status$ = timer(0, 30_000).pipe(
  switchMap(() =>
    this.api.getStatus().pipe(
      retry({ count: 3, delay: 1000 })
    )
  ),
  shareReplay(1)
);
```

---

## Pattern 2: Poll Until Condition Met

```typescript
import { timer } from 'rxjs';
import { switchMap, takeWhile, tap } from 'rxjs/operators';

function pollUntilDone(jobId: string): Observable<JobStatus> {
  return timer(0, 2000).pipe(
    switchMap(() => this.api.getJobStatus(jobId)),
    tap(s => updateProgressBar(s.progress)),
    takeWhile(s => s.state === 'running', true) // inclusive: emit terminal state
  );
}

pollUntilDone('job-123').subscribe({
  next:     s => { if (s.state === 'done') showResult(s.result); },
  complete: () => hideProgressBar()
});
```

---

## Pattern 3: Adaptive Polling (Backoff When No Change)

```typescript
import { BehaviorSubject, timer, defer } from 'rxjs';
import { switchMap, distinctUntilChanged, pairwise, map } from 'rxjs/operators';

function adaptivePoll<T>(
  fetch: () => Observable<T>,
  minInterval = 1000,
  maxInterval = 30_000
): Observable<T> {
  const interval$ = new BehaviorSubject(minInterval);

  return interval$.pipe(
    switchMap(ms => timer(ms).pipe(switchMap(fetch))),
    pairwise(),
    tap(([prev, curr]) => {
      const changed = JSON.stringify(prev) !== JSON.stringify(curr);
      const current = interval$.getValue();
      interval$.next(
        changed
          ? minInterval                          // data changed → poll faster
          : Math.min(current * 2, maxInterval)   // no change → back off
      );
    }),
    map(([, curr]) => curr),
    distinctUntilChanged()
  );
}
```

---

## Pattern 4: Poll Only When Tab Is Visible

```typescript
import { fromEvent, merge, of } from 'rxjs';
import { switchMap, filter, startWith, timer } from 'rxjs/operators';

const visible$ = merge(
  of(document.visibilityState === 'visible'),
  fromEvent(document, 'visibilitychange').pipe(
    map(() => document.visibilityState === 'visible')
  )
);

// Poll only when tab is in focus:
visible$.pipe(
  switchMap(visible =>
    visible
      ? timer(0, 30_000).pipe(switchMap(() => this.api.getData()))
      : EMPTY  // pause polling when tab hidden
  )
).subscribe(render);
```

---

## Pattern 5: Long Polling

```typescript
import { expand, EMPTY } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

// Long poll: server holds response until data available or timeout:
function longPoll<T>(url: string): Observable<T> {
  return this.http.get<T>(url, { params: { timeout: '30' } }).pipe(
    expand(result =>
      result
        ? this.http.get<T>(url, { params: { timeout: '30' } }) // immediately poll again
        : timer(1000).pipe(switchMap(() =>                      // brief pause if empty
            this.http.get<T>(url, { params: { timeout: '30' } })
          ))
    ),
    filter(result => result !== null)
  );
}
```

---

## Pattern 6: Conditional Polling (Poll More When Active)

```typescript
import { combineLatest, timer } from 'rxjs';
import { switchMap, distinctUntilChanged } from 'rxjs/operators';

const userActive$ = merge(
  fromEvent(document, 'mousemove'),
  fromEvent(document, 'keydown')
).pipe(
  map(() => true),
  debounceTime(5000, asyncScheduler), // after 5s inactivity → false
  startWith(true),
  distinctUntilChanged()
);

// 5s when active, 60s when idle:
userActive$.pipe(
  switchMap(active => timer(0, active ? 5_000 : 60_000)),
  switchMap(() => this.api.getUpdates())
).subscribe(renderUpdates);
```

---

## Pattern 7: Manual Refresh + Auto-Poll

```typescript
import { merge, Subject, timer } from 'rxjs';
import { exhaustMap, shareReplay } from 'rxjs/operators';

const manualRefresh$ = new Subject<void>();

const data$ = merge(
  timer(0, 60_000),   // auto-poll every 60s
  manualRefresh$      // or on manual trigger
).pipe(
  exhaustMap(() =>    // prevent concurrent fetches
    this.api.getData().pipe(catchError(() => EMPTY))
  ),
  shareReplay(1)
);

refreshBtn.addEventListener('click', () => manualRefresh$.next());
```

---

## Polling Strategy Decision Table

| Requirement | Strategy | Key operators |
|---|---|---|
| Fixed heartbeat | Fixed interval | `timer(0, ms)`, `switchMap` |
| Wait for job completion | Poll until condition | `takeWhile(pred, true)` |
| Reduce load when idle | Adaptive backoff | `pairwise`, `BehaviorSubject` interval |
| Save bandwidth in background | Tab visibility gate | `visibilitychange`, `switchMap` |
| Server push simulation | Long poll | `expand`, `filter` |
| Activity-based rate | Conditional interval | `debounceTime`, `switchMap` |
| User-triggered + auto | Combined trigger | `merge`, `exhaustMap` |

---

## Common Pitfalls

### Using `mergeMap` Instead of `switchMap` for Polls

```typescript
// ❌ mergeMap — overlapping responses if API is slow:
timer(0, 5000).pipe(
  mergeMap(() => this.api.getData()) // t=0 starts, t=5 starts, t=0 response arrives late!
)

// ✅ switchMap — cancels in-flight request when next tick arrives:
timer(0, 5000).pipe(
  switchMap(() => this.api.getData())
)
```

### Not Pausing Poll When Component Destroys

```typescript
// ❌ Poll continues after navigation — memory leak + unnecessary API calls:
timer(0, 10_000).pipe(
  switchMap(() => this.api.getData())
).subscribe(render);

// ✅ Tie to component lifetime:
timer(0, 10_000).pipe(
  switchMap(() => this.api.getData()),
  takeUntilDestroyed(this.destroyRef)
).subscribe(render);
```
