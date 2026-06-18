# distinct — Advanced Patterns

For fundamentals see the core [distinct](./distinct) doc. This page covers key-based deduplication, flush strategies, set-based dedup, and the differences from `distinctUntilChanged`.

---

## Mental Model

```typescript
import { distinct } from 'rxjs/operators';

// distinct() — emits a value only if it has NEVER been seen before (across the entire stream)
// Uses a Set internally; memory grows with the number of unique values

of(1, 2, 1, 3, 2, 4).pipe(distinct()).subscribe(console.log);
// 1, 2, 3, 4

// distinct(keySelector) — deduplicate by a derived key:
users$.pipe(distinct(u => u.id)).subscribe(renderUser);
// Each user emitted at most once, keyed by id

// distinct(keySelector, flushes$) — clear the seen-set when flushes$ emits:
users$.pipe(distinct(u => u.id, pageChange$)).subscribe(renderUser);
// Resets on each page navigation — users can appear again on new pages
```

---

## `distinct` vs `distinctUntilChanged`

```typescript
// distinct — global dedup: value must never have been seen before
of(1, 2, 1, 3).pipe(distinct())             // 1, 2, 3   (1 suppressed second time)
of(1, 2, 1, 3).pipe(distinctUntilChanged()) // 1, 2, 1, 3 (1 re-emits after 2)

// Use distinct for:       eliminating duplicates anywhere in the stream history
// Use distinctUntilChanged: suppressing consecutive repeats only
```

---

## Pattern 1: Event Deduplication (Process Each Event Once)

```typescript
import { distinct, mergeMap } from 'rxjs/operators';

// WebSocket events may be re-delivered — process each ID exactly once:
wsEvents$.pipe(
  distinct(event => event.id),
  mergeMap(event => processEvent(event))
).subscribe(handleResult);

// DOM mutation observer — skip duplicate element insertions:
mutationObserver$.pipe(
  mergeMap(mutations => from(mutations.addedNodes)),
  distinct(node => (node as HTMLElement).dataset?.id),
  takeUntilDestroyed()
).subscribe(node => initComponent(node as HTMLElement));
```

---

## Pattern 2: Flush on Reset (Paginated Content)

Reset the seen-set when context changes:

```typescript
import { distinct, Subject } from 'rxjs/operators';

const pageChange$ = new Subject<void>();
const categoryChange$ = new Subject<void>();

// Products appear once per page session, reset when user navigates:
productStream$.pipe(
  distinct(
    p => p.id,
    merge(pageChange$, categoryChange$) // flush on either event
  ),
  takeUntilDestroyed()
).subscribe(renderProductCard);

// Search results — each query starts fresh:
const newQuery$ = searchQuery$.pipe(
  distinctUntilChanged(),
  skip(1) // not on initial value
);

searchResults$.pipe(
  distinct(result => result.id, newQuery$),
  takeUntilDestroyed()
).subscribe(appendResult);
```

---

## Pattern 3: Notification Deduplication

Show each unique notification type at most once per session:

```typescript
import { distinct, filter, map } from 'rxjs/operators';
import { merge } from 'rxjs';

interface Notification { id: string; type: string; message: string; priority: 'low' | 'high'; }

// High-priority: always show (no dedup)
// Low-priority: show each type once per session
const notifications$ = notificationStream$.pipe(share());

const highPriority$ = notifications$.pipe(
  filter(n => n.priority === 'high')
);

const lowPriority$ = notifications$.pipe(
  filter(n => n.priority === 'low'),
  distinct(n => n.type) // only first occurrence of each type
);

merge(highPriority$, lowPriority$).pipe(
  takeUntilDestroyed()
).subscribe(showNotification);
```

---

## Pattern 4: Dedup with Time-Based Flush

Reset dedup state every N minutes — allows repeats after a cooldown:

```typescript
import { distinct, interval, map } from 'rxjs/operators';

const FLUSH_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

// Re-allow the same metric alert after 5 minutes:
alertStream$.pipe(
  distinct(
    alert => `${alert.metric}:${alert.threshold}`,
    interval(FLUSH_INTERVAL_MS)
  ),
  takeUntilDestroyed()
).subscribe(triggerAlert);
```

---

## Pattern 5: Bounded Dedup (LRU-Style Cache)

`distinct` grows unboundedly. For long-lived streams, use a bounded seen-set:

```typescript
import { OperatorFunction } from 'rxjs';

function distinctBounded<T, K = T>(
  keySelector: (v: T) => K = v => v as unknown as K,
  maxSize = 1000
): OperatorFunction<T, T> {
  return source$ =>
    new Observable<T>(subscriber => {
      const seen: K[] = []; // use array for LRU eviction

      return source$.subscribe({
        next: value => {
          const key = keySelector(value);
          if (seen.includes(key)) return; // already seen
          seen.push(key);
          if (seen.length > maxSize) seen.shift(); // evict oldest
          subscriber.next(value);
        },
        error:    err => subscriber.error(err),
        complete: ()  => subscriber.complete()
      });
    });
}

// Long-running stream — deduplicate with bounded memory:
liveEventStream$.pipe(
  distinctBounded(e => e.correlationId, 5000),
  takeUntilDestroyed()
).subscribe(processEvent);
```

---

## Pattern 6: Dedup Across Multiple Fields

```typescript
import { distinct, map } from 'rxjs/operators';

interface Trade { symbol: string; price: number; exchange: string; ts: number; }

// Suppress identical trades (same symbol + exchange + price):
tradeStream$.pipe(
  distinct(t => `${t.symbol}|${t.exchange}|${t.price}`)
).subscribe(recordTrade);

// Or use a hash of the whole object:
function hashObject(obj: unknown): string {
  return JSON.stringify(obj, Object.keys(obj as object).sort());
}

eventStream$.pipe(
  distinct(hashObject)
).subscribe(processUniqueEvent);
```

---

## Common Pitfalls

### Memory Leak on Long-Running Streams Without `flushes$`

```typescript
// ❌ distinct() on an infinite stream — seen-Set grows forever:
liveStream$.pipe(
  distinct(item => item.id) // Set never cleared — memory leak over hours
).subscribe(process);

// ✅ Add a periodic flush or use distinctBounded:
liveStream$.pipe(
  distinct(item => item.id, interval(60_000)) // flush every minute
).subscribe(process);
```

### Confusing `distinct` with `distinctUntilChanged` for State

```typescript
// ❌ Using distinct for "don't re-render same state":
stateStream$.pipe(distinct(s => s.status)) // never re-renders 'loading' after first time!
// If status goes idle→loading→error→loading, second 'loading' is suppressed

// ✅ Use distinctUntilChanged for consecutive-repeat suppression:
stateStream$.pipe(distinctUntilChanged((a, b) => a.status === b.status))
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: `distinct` is for "process each item exactly once" — not for "suppress consecutive repeats." The `flushes$` parameter is essential for long-running streams; without it the internal Set is a memory leak. For most state-management deduplication, `distinctUntilChanged` is the right tool.
