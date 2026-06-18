# merge — Advanced Patterns

For `merge` fundamentals see the core [merge](./merge) doc. This page covers error isolation, dynamic source registration, priority merging, and concurrency control.

---

## The Core Problem `merge` Solves

`merge` subscribes to all sources simultaneously and emits values from whichever fires. By default, **one error kills the whole merged stream** — the advanced patterns below address when that's not what you want.

---

## Pattern 1: Error Isolation — Survive Individual Source Failures

```typescript
import { merge, EMPTY } from 'rxjs';
import { catchError, materialize, filter, dematerialize } from 'rxjs/operators';

// ❌ Default: one error terminates the entire merge:
merge(userStream$, orderStream$, notificationStream$)
// If notificationStream$ errors, all three stop

// ✅ Isolate each source — log errors, continue from others:
function isolate<T>(source$: Observable<T>, label: string): Observable<T> {
  return source$.pipe(
    catchError(err => {
      logger.error(`${label} failed:`, err);
      return EMPTY; // this source ends silently, others continue
    })
  );
}

merge(
  isolate(userStream$,         'users'),
  isolate(orderStream$,        'orders'),
  isolate(notificationStream$, 'notifications')
).subscribe(renderDashboard);
```

---

## Pattern 2: Error Isolation with Restart

When a source errors, reconnect it after a delay:

```typescript
import { merge, timer, defer } from 'rxjs';
import { catchError, switchMap, retry } from 'rxjs/operators';

function withRestart<T>(
  factory: () => Observable<T>,
  restartDelay = 2000
): Observable<T> {
  return defer(factory).pipe(
    catchError(err => {
      logger.warn('Source error, restarting:', err);
      return timer(restartDelay).pipe(
        switchMap(() => withRestart(factory, restartDelay))
      );
    })
  );
}

merge(
  withRestart(() => webSocket('/feed/prices')),
  withRestart(() => webSocket('/feed/news')),
  withRestart(() => webSocket('/feed/alerts'))
).subscribe(handleEvent);
```

---

## Pattern 3: Dynamic Source Registration

Add and remove sources at runtime:

```typescript
import { Subject, merge, Observable } from 'rxjs';
import { mergeAll, tap } from 'rxjs/operators';

class DynamicMerge<T> {
  private sources$ = new Subject<Observable<T>>();

  readonly output$ = this.sources$.pipe(mergeAll());

  add(source$: Observable<T>): void {
    this.sources$.next(source$);
  }

  complete(): void {
    this.sources$.complete();
  }
}

// Usage:
const feeds = new DynamicMerge<MarketEvent>();
feeds.output$.subscribe(renderEvent);

// Add streams dynamically:
feeds.add(subscribeToTicker('AAPL'));
feeds.add(subscribeToTicker('GOOGL'));
// Later:
feeds.add(subscribeToTicker('MSFT'));
```

---

## Pattern 4: Priority Merging (High-Priority Items First)

When sources have priority, use `concat` to drain higher-priority first, but `merge` for concurrent processing:

```typescript
import { merge, Subject } from 'rxjs';
import { map } from 'rxjs/operators';

const critical$ = new Subject<Task>();
const normal$   = new Subject<Task>();
const low$      = new Subject<Task>();

// Tag with priority, process all concurrently, sort downstream:
merge(
  critical$.pipe(map(t => ({ ...t, priority: 3 }))),
  normal$.pipe(map(t => ({ ...t, priority: 2 }))),
  low$.pipe(map(t => ({ ...t, priority: 1 })))
).pipe(
  bufferTime(100),                             // collect 100ms window
  filter(batch => batch.length > 0),
  map(batch => batch.sort((a, b) => b.priority - a.priority)) // sort by priority
).subscribe(processTaskBatch);
```

---

## Pattern 5: Concurrency-Limited Merge

`mergeMap` with concurrency limit is the operator-level solution. For a source-level version:

```typescript
import { merge, from } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Merge N sources with at most K active at a time:
function mergeWithConcurrency<T>(
  sources: Observable<T>[],
  concurrency: number
): Observable<T> {
  return from(sources).pipe(
    mergeMap(source => source, concurrency) // mergeMap handles the concurrency cap
  );
}

// Run 10 data streams but no more than 3 active at once:
const streams = dataIds.map(id => this.api.stream(id));
mergeWithConcurrency(streams, 3).subscribe(handleData);
```

---

## Pattern 6: Merge as Event Bus

```typescript
import { Subject, merge } from 'rxjs';
import { filter, map } from 'rxjs/operators';

type AppEvent =
  | { type: 'USER_ACTION'; payload: UserAction }
  | { type: 'API_RESPONSE'; payload: ApiResponse }
  | { type: 'SYSTEM_EVENT'; payload: SystemEvent };

class EventBus {
  private userActions$  = new Subject<UserAction>();
  private apiResponses$ = new Subject<ApiResponse>();
  private systemEvents$ = new Subject<SystemEvent>();

  readonly events$: Observable<AppEvent> = merge(
    this.userActions$.pipe(map(p => ({ type: 'USER_ACTION' as const,  payload: p }))),
    this.apiResponses$.pipe(map(p => ({ type: 'API_RESPONSE' as const, payload: p }))),
    this.systemEvents$.pipe(map(p => ({ type: 'SYSTEM_EVENT' as const, payload: p })))
  );

  // Type-safe subscription:
  on<K extends AppEvent['type']>(
    type: K
  ): Observable<Extract<AppEvent, { type: K }>['payload']> {
    return this.events$.pipe(
      filter((e): e is Extract<AppEvent, { type: K }> => e.type === type),
      map(e => e.payload as Extract<AppEvent, { type: K }>['payload'])
    );
  }

  emitUserAction(action: UserAction)   { this.userActions$.next(action); }
  emitApiResponse(res: ApiResponse)    { this.apiResponses$.next(res); }
  emitSystemEvent(event: SystemEvent)  { this.systemEvents$.next(event); }
}
```

---

## Pattern 7: Merge vs `combineLatest` vs `race`

```typescript
// merge — emit from ANY source as values arrive:
merge(a$, b$, c$)
// a emits 1 → 1
// b emits 2 → 2
// c emits 3 → 3
// All values from all sources, independent timing
// Completes when ALL sources complete

// combineLatest — emit latest combo when ANY source updates:
combineLatest([a$, b$, c$])
// Waits for all to emit at least once, then emits [a, b, c] on any change
// Use for "latest values from all" (form fields, filters, settings)

// race — subscribe to all, keep only the FIRST to emit, cancel others:
race([a$, b$, c$])
// Whichever emits first wins; others unsubscribed immediately
// Use for "fastest response wins" (timeout fallback, fastest API)
```

---

## `merge` Completion Semantics

```typescript
// merge completes when ALL sources complete:
merge(
  of(1, 2, 3),        // completes immediately after 3 values
  interval(1000)       // never completes
)
// → never completes (interval never completes)

// merge completes when FIRST error (unless isolated):
merge(
  interval(1000),
  throwError(() => new Error('oops'))
)
// → errors immediately

// To complete when FIRST source completes, use race:
race([longProcess$, timer(30_000).pipe(map(() => TIMEOUT))])
```

---

## Common Pitfalls

### Expecting `merge` to Complete When One Source Completes

```typescript
// ❌ Misconception: merge ends when one source ends
// Reality: merge ends when ALL sources end (or one errors)

merge(
  of(1, 2, 3),     // completes after 3 items
  timer(0, 1000)   // never completes
)
// This never completes — timer keeps emitting

// ✅ Use takeUntil to bound lifetime externally:
const stop$ = new Subject<void>();
merge(source1$, source2$, source3$).pipe(
  takeUntil(stop$)
).subscribe(handle);
```

### Not Isolating Errors in Production Merge Streams

```typescript
// ❌ One flaky WebSocket kills the dashboard:
merge(
  this.ws.prices$,
  this.ws.news$,
  this.http.poll('/health', 30_000)
).subscribe(renderDashboard);

// ✅ Wrap each in catchError or retry:
merge(
  this.ws.prices$.pipe(retry({ count: 3, delay: 1000 })),
  this.ws.news$.pipe(retry({ count: 3, delay: 1000 })),
  this.http.poll('/health', 30_000).pipe(catchError(() => EMPTY))
).subscribe(renderDashboard);
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**Key insight**: `merge` is deceptively simple — the complexity is in error handling. In any merge of multiple real-world sources (WebSockets, polls, user events), always decide whether each source should be independently error-tolerant. `catchError(() => EMPTY)` makes a source optional; `retry(...)` makes it resilient; leaving it bare means one failure kills all sources.
