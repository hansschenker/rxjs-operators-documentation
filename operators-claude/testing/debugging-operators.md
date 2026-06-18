# Debugging RxJS Streams

A practical guide to understanding what's happening in an Observable pipeline.

---

## `tap` for Inspection

`tap` is the primary debugging tool — it lets you observe emissions, errors, and completions without affecting the stream.

```typescript
import { tap } from 'rxjs/operators';

// Log at every stage of a pipeline
source$.pipe(
  tap(v => console.log('after source:', v)),
  debounceTime(300),
  tap(v => console.log('after debounce:', v)),
  switchMap(v => ajax.getJSON(`/api?q=${v}`)),
  tap({
    next:     v => console.log('response:', v),
    error:    e => console.error('error:', e),
    complete: () => console.log('complete')
  })
).subscribe(renderResults);

// Labeled helper for pipelines:
function debug<T>(label: string) {
  return tap<T>({
    next:     v => console.log(`[${label}] next:`,     v),
    error:    e => console.error(`[${label}] error:`,  e),
    complete: () => console.log(`[${label}] complete`)
  });
}

source$.pipe(
  debug('raw'),
  debounceTime(300),
  debug('debounced'),
  switchMap(query => ajax.getJSON(`/api/search?q=${query}`)),
  debug('response')
).subscribe(renderResults);
```

---

## `materialize` for Full Notification Capture

When you need to capture a stream's complete notification history (including errors as values):

```typescript
import { materialize } from 'rxjs/operators';

const history: Notification<any>[] = [];

source$.pipe(
  materialize()
).subscribe(n => history.push(n));

// Inspect after the fact:
history.forEach(n => {
  if (n.kind === 'N') console.log('value:', n.value);
  if (n.kind === 'E') console.error('error:', n.error);
  if (n.kind === 'C') console.log('complete');
});
```

---

## Diagnosing Common Problems

### "Nothing is emitting"

```typescript
// 1. Check subscription — is anyone subscribed?
const sub = source$.subscribe(v => console.log('got:', v));
// If nothing logs, the source isn't emitting

// 2. Check for cold Observables not subscribed
const obs$ = ajax.getJSON('/api/data'); // cold — no request yet!
obs$.subscribe(console.log); // NOW the request fires

// 3. Check combineLatest seeding
combineLatest([a$, b$]).subscribe(console.log);
// Won't emit until BOTH a$ and b$ have emitted at least once
a$.next(1); // b$ still hasn't emitted → nothing

// 4. Check for operator that requires completion (toArray, reduce, last, takeLast)
source$.pipe(toArray()).subscribe(console.log);
// Won't emit until source completes
```

### "I'm getting duplicate emissions"

```typescript
// Multiple subscriptions to a cold Observable create independent executions
const cold$ = ajax.getJSON('/api/data');
cold$.subscribe(handlerA); // request 1
cold$.subscribe(handlerB); // request 2 — separate HTTP call!

// Fix: shareReplay for caching, or share for live streams
const shared$ = cold$.pipe(shareReplay(1));
shared$.subscribe(handlerA); // one request
shared$.subscribe(handlerB); // shares the same response
```

### "My subscription never completes"

```typescript
// Likely causes:
// 1. mergeMap with infinite inner — source completes but inner doesn't
source$.pipe(mergeMap(() => interval(1000))).subscribe(); // never completes

// 2. combineLatest — one source never completes
combineLatest([finiteSource$, interval(1000)]).subscribe(); // never completes

// 3. BehaviorSubject — never completes unless you call .complete()
const subject = new BehaviorSubject(0);
subject.subscribe(console.log); // never completes until subject.complete()
```

### "My error isn't being caught"

```typescript
// catchError only catches synchronous and Observable errors, not Promise rejections
// that escape the Observable chain:

source$.pipe(
  switchMap(async v => {
    throw new Error('async error'); // throws inside async function
  }),
  catchError(err => { console.log('caught'); return EMPTY; }) // NOT caught!
).subscribe();

// Fix: wrap the async in an Observable properly
source$.pipe(
  switchMap(v =>
    from(asyncOperation(v)).pipe( // from() wraps the Promise
      catchError(err => EMPTY)    // now catchError works
    )
  )
).subscribe();
```

---

## `finalize` for Teardown Logging

```typescript
import { finalize } from 'rxjs/operators';

// finalize runs on complete, error, OR unsubscription
source$.pipe(
  finalize(() => console.log('stream ended — reason unknown'))
).subscribe();

// Combine with tap to distinguish reason:
let completed = false;
source$.pipe(
  tap({ complete: () => (completed = true) }),
  finalize(() => console.log(completed ? 'completed' : 'unsubscribed or errored'))
).subscribe();
```

---

## Subscription Lifecycle Checklist

| Symptom | Check |
|---------|-------|
| No emissions at all | Is the Observable subscribed? Is it cold? |
| Missing first emission | Is it combineLatest with an unseeded source? |
| Duplicate HTTP requests | Is a cold Observable subscribed multiple times? |
| Stream never completes | Infinite inner in mergeMap? combineLatest with infinite source? |
| Error not caught | Is the throw inside an async function outside the Observable chain? |
| Memory leak | Are hot or infinite Observables unsubscribed on component destroy? |

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 5/5
**Key tools**: `tap` for live inspection, `materialize` for capture, `finalize` for teardown, `TestScheduler` for time-based operators.
