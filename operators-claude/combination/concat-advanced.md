# concat — Advanced Patterns

For `concat` fundamentals see the core [concat](./concat) doc. This page covers sequential loading sequences, conditional chaining, queue-based processing, and `concat` vs `concatMap` tradeoffs.

---

## What Makes `concat` Different

`concat` subscribes to sources **one at a time**, in order, waiting for each to complete before starting the next. This guarantees ordering — something `merge` cannot provide.

```
concat(A$, B$, C$):

A$: --1--2--|
B$:          --3--4--|
C$:                   --5--|

Result: --1--2--3--4--5--|
        (sequential, ordered, no overlap)
```

---

## Pattern 1: Loading Sequence (Show Skeleton → Cache → Live)

```typescript
import { concat, of } from 'rxjs';
import { delay, tap } from 'rxjs/operators';

// Three-phase load: skeleton → cached → live
function loadWithFallback<T>(
  cached:  T | null,
  fetch$:  Observable<T>
): Observable<T | 'loading'> {
  return concat(
    of('loading' as const),                              // 1. Show skeleton immediately
    cached ? of(cached).pipe(delay(0)) : EMPTY,         // 2. Show cache if available
    fetch$.pipe(tap(data => saveToCache(data)))          // 3. Fetch live data
  );
}

loadWithFallback(this.cache.get('users'), this.api.getUsers()).subscribe(data => {
  if (data === 'loading') showSkeleton();
  else                    renderUsers(data);
});
```

---

## Pattern 2: Sequential Dependent Requests

When each step needs the result of the previous:

```typescript
import { concat, defer } from 'rxjs';
import { tap, switchMap } from 'rxjs/operators';

// Step 1: login → Step 2: load profile → Step 3: load permissions
let token: string;
let userId: string;

concat(
  this.auth.login(credentials).pipe(
    tap(res => { token = res.token; userId = res.userId; })
  ),
  defer(() => this.api.getProfile(userId, token)).pipe(
    tap(profile => setProfile(profile))
  ),
  defer(() => this.api.getPermissions(userId, token))
).subscribe({
  next:     data => processStep(data),
  complete: () => navigateToDashboard(),
  error:    err => showLoginError(err)
});
```

`defer` is essential here — it lazily reads the `token`/`userId` values set by the previous step.

---

## Pattern 3: App Initialization Gate

Run initialization tasks in order before showing the app:

```typescript
import { concat, forkJoin } from 'rxjs';
import { tap, switchMap } from 'rxjs/operators';

// Sequential: auth check → then parallel config load:
const init$ = concat(
  this.auth.checkSession().pipe(
    tap(user => this.store.dispatch(setUser(user)))
  ),
  forkJoin({                              // after auth, load these in parallel
    config:       this.config.load(),
    translations: this.i18n.load('en'),
    featureFlags: this.flags.load()
  }).pipe(
    tap(({ config, translations, featureFlags }) => {
      this.store.dispatch(setConfig(config));
      this.store.dispatch(setTranslations(translations));
      this.store.dispatch(setFlags(featureFlags));
    })
  )
);

init$.subscribe({
  complete: () => this.appReady$.next(true),
  error:    err => this.router.navigate(['/error'])
});
```

---

## Pattern 4: Retry Each Source Independently

```typescript
import { concat } from 'rxjs';
import { retry, catchError, of } from 'rxjs/operators';

// Each source retries independently — failure of one doesn't cancel the queue:
concat(
  step1$.pipe(retry(2), catchError(err => of({ step: 1, error: err }))),
  step2$.pipe(retry(2), catchError(err => of({ step: 2, error: err }))),
  step3$.pipe(retry(2), catchError(err => of({ step: 3, error: err })))
).subscribe(result => {
  if ('error' in result) handleStepError(result);
  else                   handleStepSuccess(result);
});
```

---

## Pattern 5: Dynamic Queue Processing

Build a queue where items process one at a time in arrival order:

```typescript
import { Subject, concat, defer } from 'rxjs';
import { concatMap } from 'rxjs/operators';

class TaskQueue<T, R> {
  private queue$ = new Subject<Observable<R>>();

  readonly results$ = this.queue$.pipe(
    concatMap(task$ => task$)  // process one at a time, in order
  );

  enqueue(task: () => Observable<R>): void {
    this.queue$.next(defer(task));
  }

  complete(): void { this.queue$.complete(); }
}

// Usage:
const queue = new TaskQueue<void, SaveResult>();
queue.results$.subscribe(result => updateUI(result));

// Items processed in order, each waits for the previous:
saveButtons.forEach(btn =>
  fromEvent(btn, 'click').subscribe(() =>
    queue.enqueue(() => this.api.save(getData(btn)))
  )
);
```

---

## Pattern 6: Animation Sequence

Run CSS animations in order:

```typescript
import { concat, fromEvent } from 'rxjs';
import { take, tap } from 'rxjs/operators';

function animateTo(element: Element, classes: string): Observable<void> {
  return new Observable(observer => {
    element.classList.add(...classes.split(' '));
    const handler = () => {
      observer.next();
      observer.complete();
    };
    element.addEventListener('animationend', handler, { once: true });
    return () => element.removeEventListener('animationend', handler);
  });
}

// Run animations sequentially:
concat(
  animateTo(overlay, 'fade-in'),
  animateTo(modal,   'slide-up'),
  animateTo(content, 'appear')
).subscribe({
  complete: () => modal.focus()
});
```

---

## `concat` vs `concatMap` vs `forkJoin`

```typescript
// concat — sequential sources, known at call time:
concat(step1$, step2$, step3$)
// ✓ Reads like a sequence of steps
// ✗ Sources must be known upfront

// concatMap — sequential, driven by an upstream source:
steps$.pipe(concatMap(step => executeStep(step)))
// ✓ Dynamic: steps can arrive over time
// ✓ Each step gets the upstream value

// forkJoin — parallel, all at once:
forkJoin([step1$, step2$, step3$])
// ✓ Fastest: all run in parallel
// ✗ No ordering guarantee, no inter-step dependencies
```

---

## `concat` Completion Semantics

```typescript
// concat waits for each source to complete — NEVER sources hang:
concat(
  of(1, 2, 3),      // completes immediately
  interval(1000)    // never completes!
)
// interval blocks all subsequent sources — none after it will ever run

// ✅ Bound potentially-infinite inner sources:
concat(
  of(1, 2, 3),
  interval(1000).pipe(take(5)) // completes after 5 values
)
```

---

## Common Pitfalls

### Using `concat` When Sources Share State (Use `defer`)

```typescript
// ❌ Variable captured at subscription time, not step execution time:
const token = getToken(); // might be stale by step 2

concat(
  login$,
  this.api.getProfile(token)  // token evaluated NOW, not after login
)

// ✅ defer evaluates lazily when the step starts:
concat(
  login$.pipe(tap(t => savedToken = t)),
  defer(() => this.api.getProfile(savedToken))  // reads savedToken after login
)
```

### `concat` with a Source That Never Completes

```typescript
// ❌ interval never completes — hangs the queue:
concat(
  interval(1000),     // this never completes!
  of('will never run')
)

// ✅ Always complete inner streams when using concat:
concat(
  interval(1000).pipe(take(5)),   // completes after 5 emissions
  of('runs after 5 seconds')
)
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `concat` is the sequential pipeline — use it when steps must happen in order and each step must complete before the next begins. The most important advanced pattern is combining `concat` with `defer` for dependent steps, and `concat` with `forkJoin` for "sequential phases, parallel within each phase" initialization sequences.
