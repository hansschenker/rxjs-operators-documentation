# race — Advanced Patterns

For `race` fundamentals see the core [race](./race) doc. This page covers timeout-race patterns, fastest-API-wins strategies, feature flag switching, and the comparison with `merge` and `combineLatest`.

---

## What `race` Does

`race` subscribes to all sources simultaneously and forwards emissions exclusively from whichever source emits **first**. All other sources are unsubscribed immediately.

```
A: ----1----2----3---|
B: ---------a----b--|  (B emits first)
C: --------x----y---|

race(A, B, C):
   ---------a----b--|   (C and A silently unsubscribed after B wins)
```

---

## Pattern 1: Timeout with Fallback

The most common `race` pattern — race a data source against a timeout:

```typescript
import { race, timer, of } from 'rxjs';
import { map } from 'rxjs/operators';

// Race: real data vs 3-second timeout fallback:
race(
  this.api.getPrices(),
  timer(3000).pipe(map(() => CACHED_PRICES))
).subscribe(prices => renderPrices(prices));
// If API responds in < 3s: use real data
// If 3s passes first: use cached prices
```

This is cleaner than `timeout` + `catchError` when you have a meaningful fallback — `race` makes the "two competing strategies" intent explicit.

---

## Pattern 2: Fastest API Wins

Query multiple data sources simultaneously, use the fastest response:

```typescript
import { race } from 'rxjs';

// Try primary, secondary, and cache in parallel — use whichever responds first:
race(
  primaryApi.getUser(id),
  secondaryApi.getUser(id),   // CDN replica — may be faster
  localCache.getUser(id)      // memory cache — always fastest if hit
).subscribe(renderUser);
// Whichever resolves first wins; the others are cancelled
```

---

## Pattern 3: Feature Flag / A-B Strategy Selection

Switch between two implementations based on which one initializes first:

```typescript
import { race } from 'rxjs';
import { filter, map } from 'rxjs/operators';

// Use new implementation if feature flag resolves quickly, else fallback:
race(
  this.featureFlags.isEnabled('new-checkout').pipe(
    filter(enabled => enabled),
    map(() => 'new')
  ),
  timer(500).pipe(map(() => 'legacy')) // give new impl 500ms to confirm
).pipe(
  take(1)
).subscribe(strategy => {
  if (strategy === 'new') this.router.navigate(['/checkout-v2']);
  else                    this.router.navigate(['/checkout']);
});
```

---

## Pattern 4: Race for User Interaction

Race a timeout against user interaction — do something different if the user acts quickly vs slowly:

```typescript
import { race, fromEvent, timer } from 'rxjs';
import { map, take } from 'rxjs/operators';

// Show a hint if user doesn't interact within 5 seconds:
race(
  fromEvent(document, 'mousemove').pipe(map(() => 'active')),
  fromEvent(document, 'keydown').pipe(map(() => 'active')),
  timer(5000).pipe(map(() => 'idle'))
).pipe(take(1)).subscribe(state => {
  if (state === 'idle') showOnboardingHint();
});
```

---

## Pattern 5: Cancel Long-Running Operation on User Action

```typescript
import { race, Subject } from 'rxjs';
import { tap, map } from 'rxjs/operators';

const cancel$ = new Subject<void>();

// Long operation that can be cancelled:
race(
  this.api.runLongExport(params).pipe(
    map(result => ({ type: 'success', result } as const))
  ),
  cancel$.pipe(
    map(() => ({ type: 'cancelled' } as const))
  )
).pipe(take(1)).subscribe(outcome => {
  if (outcome.type === 'success')   saveFile(outcome.result);
  if (outcome.type === 'cancelled') showCancelledMessage();
});

// User can cancel:
cancelButton.addEventListener('click', () => cancel$.next());
```

---

## Pattern 6: First Successful from Multiple Sources

`race` uses first-to-emit, not first-to-succeed. For first-to-succeed, combine with `catchError`:

```typescript
import { race, NEVER } from 'rxjs';
import { catchError } from 'rxjs/operators';

// First source that SUCCEEDS (not just emits):
function firstSuccess<T>(sources: Observable<T>[]): Observable<T> {
  return race(
    sources.map(source =>
      source.pipe(
        catchError(() => NEVER) // errors become silence — don't win the race
      )
    )
  );
}

// Usage:
firstSuccess([
  primaryApi.getData(),   // if this errors, secondary gets a chance
  secondaryApi.getData(),
  tertiaryApi.getData()
]).subscribe(renderData);
```

---

## `race` vs `merge` vs `combineLatest`

```typescript
// race — ONE winner, rest cancelled. Winner takes all.
race(a$, b$, c$)
// Subscribes to all; the first to emit wins; others cancelled

// merge — ALL sources, no cancellation. Every emission forwarded.
merge(a$, b$, c$)
// Subscribes to all; every emission from every source is forwarded

// combineLatest — ALL sources contribute, latest from each, fires on any change.
combineLatest([a$, b$, c$])
// Subscribes to all; emits when any changes, combining latest values
```

**Choose `race`** when you want one winner and the others should stop.  
**Choose `merge`** when you want every emission from every source.  
**Choose `combineLatest`** when you want to combine the current state from all sources.

---

## Pattern 7: Race with Retry

Race combined with retry for resilient first-success:

```typescript
import { race, timer } from 'rxjs';
import { retry, map, catchError, of } from 'rxjs/operators';

race(
  this.api.getFreshData().pipe(
    retry({ count: 2, delay: 500 }), // up to 3 attempts to get fresh data
    map(data => ({ source: 'fresh', data }))
  ),
  timer(4000).pipe(                   // after 4s total, give up on fresh data
    map(() => ({ source: 'cache', data: this.cache.getData() }))
  )
).subscribe(({ source, data }) => {
  console.log(`Using ${source} data`);
  render(data);
});
```

---

## Common Pitfalls

### `race` with Only One Source

```typescript
// ❌ Pointless — race with one source is just the source
race(this.api.getData()).subscribe(render);

// ✅ race needs 2+ competing sources to be meaningful
race(
  this.api.getData(),
  timer(3000).pipe(map(() => FALLBACK))
).subscribe(render);
```

### `race` Does NOT Wait for First to Complete — Only First to EMIT

```typescript
// ❌ Misconception: "race with the first to FINISH"
// race fires as soon as ANY source emits ANY value — even intermediate ones

// Example where this matters:
race(
  longRequest$.pipe(take(1)),  // emits after 5s
  shortRequest$               // emits at 1s, 2s, 3s...
).subscribe(render);
// shortRequest$ wins immediately at 1s (not after it completes)
// longRequest$ is cancelled at 1s

// ✅ For "first to complete," combine with last():
race(
  longRequest$,
  shortRequest$
) // already correct — HTTP Observables emit once then complete
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Primary use cases**: timeout with fallback, fastest-API-wins, cancellable long operations. The "first to emit wins" semantic is exactly right for these patterns — use `merge` when all sources should contribute, not just the winner.
