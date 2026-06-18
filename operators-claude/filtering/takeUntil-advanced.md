# takeUntil — Advanced Patterns

For `takeUntil` fundamentals see the core [takeUntil](./takeUntil) doc. This page covers complex subscription lifecycle management, multi-source teardown, and common memory leak patterns.

---

## The Fundamental Contract

`takeUntil(notifier$)` completes the source when `notifier$` emits **or completes**. It does not error on notifier completion — it simply completes the main stream.

```typescript
import { Subject, interval } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

const destroy$ = new Subject<void>();

interval(1000).pipe(
  takeUntil(destroy$)
).subscribe(console.log);

// Later, to unsubscribe:
destroy$.next();
destroy$.complete(); // also signals that destroy$ itself is done
```

---

## Pattern 1: Component Destroy (Angular pre-v16)

The classic Angular pattern before `takeUntilDestroyed`:

```typescript
import { Component, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({ ... })
export class MyComponent implements OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    this.userService.user$.pipe(
      takeUntil(this.destroy$)
    ).subscribe(user => this.user = user);

    this.eventService.events$.pipe(
      takeUntil(this.destroy$)
    ).subscribe(event => this.handleEvent(event));
    // One destroy$ signal unsubscribes ALL of the above
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

---

## Pattern 2: Angular v16+ `takeUntilDestroyed`

The modern Angular approach — no manual `Subject` needed:

```typescript
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DestroyRef, inject, Component } from '@angular/core';

@Component({ ... })
export class MyComponent {
  private destroyRef = inject(DestroyRef);

  ngOnInit() {
    this.data$.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(renderData);

    // Can also use without argument inside injection context:
    this.events$.pipe(
      takeUntilDestroyed() // injects DestroyRef automatically
    ).subscribe(handleEvent);
  }
}
```

---

## Pattern 3: Route Navigation Teardown

Unsubscribe when the user navigates away:

```typescript
import { Router, NavigationStart } from '@angular/router';
import { filter, takeUntil } from 'rxjs/operators';

@Component({ ... })
export class PageComponent implements OnInit {
  private navigateAway$ = this.router.events.pipe(
    filter(e => e instanceof NavigationStart),
    // We only need the first navigation event — take(1) makes it self-completing
  );

  ngOnInit() {
    this.liveData$.pipe(
      takeUntil(this.navigateAway$)
    ).subscribe(renderData);
  }
}
```

---

## Pattern 4: Conditional Teardown — Multiple Termination Sources

Stop a stream on any of several conditions:

```typescript
import { merge, Subject, fromEvent } from 'rxjs';
import { takeUntil, filter } from 'rxjs/operators';

const destroy$    = new Subject<void>();
const cancel$     = new Subject<void>();
const timeout$    = timer(30_000);
const navigate$   = this.router.events.pipe(filter(e => e instanceof NavigationStart));

// Stop on ANY of these:
const stop$ = merge(destroy$, cancel$, timeout$, navigate$);

longRunningOperation$.pipe(
  takeUntil(stop$)
).subscribe({
  next:     result   => renderResult(result),
  complete: ()       => clearProgressBar()
});

// User clicks cancel button:
cancelButton.addEventListener('click', () => cancel$.next());
```

---

## Pattern 5: Auto-Restart After Stop

Combine `takeUntil` with `repeat` for streams that restart on some signal:

```typescript
import { defer } from 'rxjs';
import { takeUntil, repeat, retry } from 'rxjs/operators';

// Stop polling when paused, restart when resumed:
const paused$  = this.ui.paused$;  // emits true/false
const resume$  = paused$.pipe(filter(p => !p));
const pause$   = paused$.pipe(filter(p => p));

defer(() => this.api.getUpdates()).pipe(
  takeUntil(pause$),         // stop when paused
  repeat({ delay: () => resume$ }), // wait for resume before re-subscribing
  retry({ delay: 1000 })     // retry on errors
).subscribe(handleUpdate);
```

---

## Pattern 6: takeUntil for Request Cancellation

Cancel an HTTP request when a new one should supersede it — but with explicit control:

```typescript
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

// In practice, switchMap is simpler for this. Use explicit cancel$ when
// you need to cancel from outside the stream (e.g., user clicks "cancel"):
class UploadService {
  private cancel$ = new Subject<void>();

  upload(file: File): Observable<UploadProgress> {
    return this.http.request(
      new HttpRequest('POST', '/api/upload', file, { reportProgress: true })
    ).pipe(
      takeUntil(this.cancel$)
    );
  }

  cancelUpload() {
    this.cancel$.next();
  }
}
```

---

## Pattern 7: Scoped Subscription Groups

Group subscriptions by feature and tear down the whole group at once:

```typescript
@Injectable()
export class DashboardService {
  private featureDestroy = new Map<string, Subject<void>>();

  subscribeToFeature(feature: string, source$: Observable<unknown>): void {
    const destroy$ = new Subject<void>();
    this.featureDestroy.set(feature, destroy$);

    source$.pipe(
      takeUntil(destroy$)
    ).subscribe();
  }

  unsubscribeFeature(feature: string): void {
    const destroy$ = this.featureDestroy.get(feature);
    if (destroy$) {
      destroy$.next();
      destroy$.complete();
      this.featureDestroy.delete(feature);
    }
  }

  unsubscribeAll(): void {
    this.featureDestroy.forEach(d => { d.next(); d.complete(); });
    this.featureDestroy.clear();
  }
}
```

---

## Critical Placement Rule: `takeUntil` Must Be Last

```typescript
// ❌ MEMORY LEAK — takeUntil before switchMap may not unsubscribe inner Observables
source$.pipe(
  takeUntil(destroy$),   // ← wrong position
  switchMap(id => this.api.get(id)) // inner subscription leaks!
).subscribe();

// ✅ takeUntil AFTER flattening operators:
source$.pipe(
  switchMap(id => this.api.get(id)),
  takeUntil(destroy$)    // ← correct: last operator before subscribe
).subscribe();
// WHY: When takeUntil completes the stream, RxJS unsubscribes in reverse
// order. If takeUntil is before switchMap, it completes the outer stream
// but the switchMap's current inner subscription may continue running.
```

---

## Common Pitfalls

### `destroy$.next()` Without `complete()`

```typescript
// ❌ Subject stays open — if accidentally subscribed to again, it
//    won't auto-complete on the next destroy cycle
ngOnDestroy() {
  this.destroy$.next(); // triggers takeUntil
  // forgot: this.destroy$.complete();
}

// ✅ Always complete the Subject on destroy:
ngOnDestroy() {
  this.destroy$.next();
  this.destroy$.complete(); // marks Subject as done, releases resources
}
```

### Using `takeUntil` for One-Shot Requests

```typescript
// ❌ OVERKILL — for one-shot HTTP requests use firstValueFrom or take(1)
this.http.get('/api/data').pipe(
  takeUntil(this.destroy$) // HTTP completes itself after one emission
).subscribe();

// ✅ HTTP requests auto-complete — takeUntil is only for long-lived streams:
// Streams that need takeUntil: interval, fromEvent, WebSocket, Subject, BehaviorSubject
// Streams that don't:          HTTP requests, of(), from([...])
```

### `takeUntil` on the `destroy$` Subject Itself

```typescript
// ❌ INFINITE LOOP — destroy$ taking itself until itself never terminates
this.destroy$.pipe(
  takeUntil(this.destroy$) // always fires before the notifier can fire
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key rule**: `takeUntil(destroy$)` should be the LAST operator in every long-lived pipe. For Angular v16+, replace the manual `destroy$` Subject pattern with `takeUntilDestroyed(this.destroyRef)`.
