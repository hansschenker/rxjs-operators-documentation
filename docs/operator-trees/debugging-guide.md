# Debugging RxJS Streams

Practical techniques for diagnosing silent failures, unexpected behavior, and memory leaks in Observable pipelines.

---

## The Core Debugging Tool: `tap`

`tap` lets you observe values at any point in a pipeline without modifying them:

```typescript
import { tap } from 'rxjs/operators';

source$.pipe(
  tap(v => console.log('Before map:', v)),
  map(x => x * 2),
  tap(v => console.log('After map:', v)),
  filter(x => x > 4),
  tap({
    next:     v   => console.log('After filter:', v),
    error:    err => console.error('Error:', err),
    complete: ()  => console.log('Completed')
  })
).subscribe(render);
```

---

## The Debug Operator

A reusable labeled debug operator for development:

```typescript
import { tap } from 'rxjs/operators';
import { MonoTypeOperatorFunction } from 'rxjs';

function debug<T>(label: string): MonoTypeOperatorFunction<T> {
  return tap({
    next:       v   => console.log(`[${label}] next:`, v),
    error:      err => console.error(`[${label}] error:`, err),
    complete:   ()  => console.log(`[${label}] complete`),
    subscribe:  ()  => console.log(`[${label}] subscribed`),
    unsubscribe:()  => console.log(`[${label}] unsubscribed`)
  });
}

// Usage — label each stage:
userInput$.pipe(
  debug('input'),
  debounceTime(300),
  debug('debounced'),
  switchMap(q => search(q)),
  debug('results')
).subscribe(render);
```

---

## Diagnosing "Nothing Emits"

Work through this checklist when a stream produces no output:

### 1. Is it subscribed?

```typescript
// ❌ Cold Observable — never subscribed, nothing runs
const stream$ = of(1, 2, 3).pipe(map(x => x * 2));
// No subscribe call!

// ✅ Subscribe to trigger execution:
stream$.subscribe(console.log);
```

### 2. Is a source never emitting?

```typescript
// combineLatest waits for ALL sources — if one never emits, nothing emits
combineLatest({ a: a$, b: neverEmits$ }).pipe(
  tap(() => console.log('vm emitted')) // never logs!
)

// Fix: add startWith to optional sources:
combineLatest({ a: a$, b: neverEmits$.pipe(startWith(null)) })
```

### 3. Is `filter` too restrictive?

```typescript
source$.pipe(
  tap(v => console.log('before filter:', v)), // check values ARE arriving
  filter(x => x.active && x.score > 100),    // might be filtering everything
  tap(v => console.log('after filter:', v))   // check if anything passes
).subscribe(render);
```

### 4. Is an error being swallowed?

```typescript
source$.pipe(
  catchError(err => {
    console.error('Caught error:', err); // add this temporarily
    return EMPTY; // EMPTY silently completes — was this the issue?
  })
)
```

### 5. Is the source completing before emitting?

```typescript
source$.pipe(
  tap({
    complete: () => console.log('Completed — no more values')
  })
)
```

---

## Diagnosing "Stream Errors Immediately"

```typescript
source$.pipe(
  // Temporarily catch ALL errors to inspect them:
  catchError(err => {
    console.error('Full error object:', err);
    console.error('Stack:', err.stack);
    return EMPTY; // prevent crash while debugging
  })
).subscribe();
```

Common causes:
- `JSON.parse` on malformed data inside a `map`
- `undefined` property access: `map(x => x.user.name)` when `user` is null
- Incorrect `switchMap` projector throwing synchronously
- `combineLatest` with an Observable that immediately errors

---

## Diagnosing Memory Leaks

### Check for Unsubscribed Long-Lived Subscriptions

```typescript
import { tap } from 'rxjs/operators';

function trackSubscription<T>(label: string): MonoTypeOperatorFunction<T> {
  return tap({
    subscribe:   () => console.log(`[${label}] SUBSCRIBED (total: ${++count})`),
    unsubscribe: () => console.log(`[${label}] UNSUBSCRIBED (total: ${--count})`)
  });
}

let count = 0;
interval(1000).pipe(
  trackSubscription('interval'),
  takeUntilDestroyed(this.destroyRef)
).subscribe();
// If 'UNSUBSCRIBED' never logs → memory leak
```

### Find Subscriptions That Outlive Components (Angular)

```typescript
// In ngOnDestroy, check that all subscriptions are cleaned up:
ngOnDestroy() {
  console.log('Remaining subscriptions:', this.subscriptionCount);
  this.destroy$.next();
  this.destroy$.complete();
}
```

---

## Diagnosing Unexpected Emissions

### "Why is this emitting too many times?"

```typescript
// combineLatest fires on ANY source change — may be unexpected:
combineLatest({ a: a$, b: b$, c: c$, d: d$ }).pipe(
  tap(() => console.count('vm emit')) // count how often it fires
)

// Fix: debounce rapid cascading changes:
combineLatest({ a: a$, b: b$, c: c$, d: d$ }).pipe(
  debounceTime(0), // batch synchronous changes into one emission
  tap(() => console.count('vm emit after debounce'))
)
```

### "Why is this emitting duplicate values?"

```typescript
// Add distinctUntilChanged and log when it blocks:
source$.pipe(
  tap(v => console.log('RAW:', JSON.stringify(v))),
  distinctUntilChanged((a, b) => {
    const same = JSON.stringify(a) === JSON.stringify(b);
    if (same) console.log('DUPLICATE blocked');
    return same;
  })
)
```

---

## Diagnosing `switchMap` Cancellation

Understand which inner subscriptions are being cancelled:

```typescript
import { switchMap, tap } from 'rxjs/operators';

outer$.pipe(
  tap(v => console.log(`Outer emitted: ${v} — cancelling any in-flight request`)),
  switchMap(id =>
    this.api.get(id).pipe(
      tap({
        subscribe:   () => console.log(`Request ${id} started`),
        unsubscribe: () => console.log(`Request ${id} CANCELLED (switchMap)`),
        complete:    () => console.log(`Request ${id} completed`)
      })
    )
  )
).subscribe(console.log);
```

---

## Diagnosing Hot vs Cold Issues

```typescript
// Symptom: every subscriber gets a new HTTP request instead of sharing one
// Cause: cold Observable (HTTP) without shareReplay

const shared$ = this.http.get('/api/data');

// ❌ Two HTTP requests:
shared$.subscribe(A);
shared$.subscribe(B);

// ✅ One HTTP request:
const cached$ = shared$.pipe(shareReplay(1));
cached$.subscribe(A);
cached$.subscribe(B);

// Debug: count subscriptions to confirm sharing:
const traced$ = this.http.get('/api/data').pipe(
  tap({ subscribe: () => console.log('HTTP REQUEST MADE') }),
  shareReplay(1)
);
```

---

## The `materialize` Debug Technique

Convert all Observable events (next, error, complete) to values for inspection:

```typescript
import { materialize } from 'rxjs/operators';

source$.pipe(
  materialize() // every event becomes a Notification object
).subscribe(notification => {
  console.log('kind:', notification.kind);          // 'N', 'E', or 'C'
  console.log('value:', notification.value);        // for 'N'
  console.log('error:', notification.error);        // for 'E'
});
// Useful for testing: all events land in next(), none in error()
```

---

## Common Error Messages and Causes

| Error | Likely cause |
|---|---|
| `You provided an invalid object where a stream was expected` | Passing a Promise or non-Observable to an operator expecting Observable |
| `Cannot read properties of undefined` inside a pipe | `map` receiving null/undefined — add `filter(Boolean)` before |
| `EmptyError: no elements in sequence` | `firstValueFrom`/`lastValueFrom` on an Observable that completes without emitting — add default value |
| Stream completes without emitting | `EMPTY` returned from `catchError`, or `filter` blocks everything, or source completes early |
| `Maximum call stack size exceeded` | `expand` without a termination condition, or recursive Observable creation |
| Subscription fires after component destroyed | `takeUntil` placed before `switchMap` instead of after, or missing cleanup |

---

## Quick Debug Checklist

```
□ Is the Observable subscribed?
□ Does every combineLatest source have startWith?
□ Is switchMap/mergeMap projector returning an Observable (not a value)?
□ Is catchError returning an Observable (not throwing synchronously)?
□ Is takeUntil the LAST operator before subscribe?
□ Are long-lived streams (interval, fromEvent, WebSocket) using share/shareReplay?
□ Are HTTP requests using shareReplay(1) if used by multiple components?
□ Are subscriptions unsubscribed in component destroy?
```
