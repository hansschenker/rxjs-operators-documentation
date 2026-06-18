# Subscription Management Guide

The most common RxJS mistake is forgetting to unsubscribe. An active subscription holds a reference to everything in its closure — components, DOM nodes, HTTP connections. This guide covers every unsubscription pattern.

---

## The Problem

```typescript
// ❌ MEMORY LEAK — subscription lives forever
@Component({ ... })
export class MyComponent implements OnInit {
  ngOnInit() {
    interval(1000).subscribe(v => this.count = v);
    // interval runs forever — component is never GC'd
  }
}
```

---

## Pattern 1: `takeUntil(destroy$)` — Subject-Based Lifecycle

The most flexible approach. A `Subject` fires once on destroy, completing all streams that use `takeUntil`.

```typescript
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({ ... })
export class MyComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(1000).pipe(
      takeUntil(this.destroy$)
    ).subscribe(v => this.count = v);

    fromEvent(window, 'resize').pipe(
      takeUntil(this.destroy$)  // same destroy$ handles all subscriptions
    ).subscribe(() => this.recalculate());
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

**Why `complete()` after `next()`**: Calling `complete()` prevents the Subject itself from leaking. Without it, the Subject remains subscribed to by any `takeUntil` operator that hasn't fired yet.

---

## Pattern 2: `takeUntilDestroyed()` — Angular 16+ (Recommended)

Angular 16 added `takeUntilDestroyed` from `@angular/core/rxjs-interop`. It integrates with Angular's `DestroyRef` automatically.

```typescript
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Component, inject, DestroyRef } from '@angular/core';

@Component({ ... })
export class MyComponent {
  // Inside injection context (constructor/field initializer):
  data$ = this.http.get('/api/data').pipe(
    takeUntilDestroyed()  // no ngOnDestroy needed
  );

  // Outside injection context — inject DestroyRef explicitly:
  private destroyRef = inject(DestroyRef);

  someMethod() {
    interval(1000).pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(v => this.count = v);
  }
}
```

---

## Pattern 3: `Subscription` Object — Manual Tracking

For imperative code or non-Angular environments:

```typescript
import { Subscription } from 'rxjs';

class MyService {
  private subs = new Subscription();

  start() {
    // add() aggregates multiple subscriptions
    this.subs.add(
      interval(1000).subscribe(v => this.tick(v))
    );
    this.subs.add(
      fromEvent(window, 'resize').subscribe(() => this.resize())
    );
  }

  destroy() {
    this.subs.unsubscribe(); // unsubscribes all at once
  }
}
```

`Subscription.add()` is composable — `parent.add(child)` means unsubscribing the parent also unsubscribes the child.

---

## Pattern 4: `async` Pipe — Template-Managed (Angular)

The cleanest approach for Angular: the `async` pipe subscribes and unsubscribes automatically with the component.

```typescript
@Component({
  template: `
    <div *ngIf="user$ | async as user">
      {{ user.name }}
    </div>
    <li *ngFor="let item of items$ | async">{{ item }}</li>
  `
})
export class MyComponent {
  user$  = this.userService.getCurrentUser();
  items$ = this.itemService.getItems();
  // No subscribe(), no ngOnDestroy needed
}
```

**Limitation**: Each `async` pipe creates a separate subscription. Use `*ngIf="(data$ | async) as data"` to subscribe once and share the value in the template.

---

## Pattern 5: `take(1)` / `first()` — Self-Completing Streams

For one-shot Observables that should complete after the first value:

```typescript
import { take } from 'rxjs/operators';

// HTTP calls complete naturally — no unsubscription needed
this.http.get('/api/user').subscribe(user => this.user = user);

// But for hot streams, take(1) ensures cleanup:
userLoaded$.pipe(take(1)).subscribe(user => this.initializeWith(user));
// Stream completes after first emission — automatically unsubscribed
```

---

## Pattern 6: `takeWhile` — Condition-Based Completion

```typescript
import { takeWhile } from 'rxjs/operators';

// Emit while component is active:
interval(1000).pipe(
  takeWhile(() => this.isActive)
).subscribe(v => this.tick(v));

// Self-referential condition:
statusPolling$.pipe(
  takeWhile(status => status !== 'complete', true) // inclusive: emit final 'complete'
).subscribe(updateProgressBar);
```

---

## When Each Pattern Fits

| Pattern | Best for |
|---|---|
| `takeUntil(destroy$)` | Multiple subscriptions, any framework |
| `takeUntilDestroyed()` | Angular 16+ components/services |
| `Subscription.add()` | Imperative lifecycle, non-Angular |
| `async` pipe | Angular template bindings |
| `take(1)` / `first()` | One-shot reactions to hot streams |
| `takeWhile(cond)` | Condition-driven, self-contained logic |

---

## Diagnosing Subscription Leaks

### Symptom: Component method called after it's destroyed
```typescript
// ❌ Causes "Cannot set property of null" after navigation
interval(1000).subscribe(v => {
  this.count = v; // 'this' may be destroyed
});
```

### Symptom: Memory grows over time in a SPA
Check for:
1. `subscribe()` calls in `ngOnInit` without corresponding `ngOnDestroy`
2. Subscriptions to hot Observables (`fromEvent`, `interval`, `webSocket`) without a terminator
3. `BehaviorSubject` / `ReplaySubject` subscriptions without cleanup

### Detection — Count active subscriptions
```typescript
import { tap } from 'rxjs/operators';

let activeCount = 0;

function trackSubscription<T>(): MonoTypeOperatorFunction<T> {
  return tap({
    subscribe:   () => console.log(`active: ${++activeCount}`),
    unsubscribe: () => console.log(`active: ${--activeCount}`)
  });
}

// Add to any suspicious stream:
myStream$.pipe(trackSubscription()).subscribe(handler);
```

---

## The `finalize` Safety Net

`finalize` runs on complete, error, **or** unsubscribe — use it to assert cleanup happened:

```typescript
import { finalize } from 'rxjs/operators';

interval(1000).pipe(
  takeUntil(destroy$),
  finalize(() => console.log('cleaned up')) // confirm takeUntil fired
).subscribe(v => this.tick(v));
```

---

## Common Mistakes

### Putting `takeUntil` Before Other Operators That Create Inner Subscriptions

```typescript
// ❌ switchMap's inner Observables escape takeUntil
source$.pipe(
  takeUntil(destroy$),    // outer stream stops here...
  switchMap(v => inner$)  // ...but inner$ was already subscribed and keeps running
)

// ✅ takeUntil LAST — applies to the whole chain
source$.pipe(
  switchMap(v => inner$), // inner subscriptions managed by switchMap
  takeUntil(destroy$)     // kills the whole chain including current inner
)
// WHY: takeUntil unsubscribes from its upstream source. If it's before
// switchMap, switchMap never runs — so placing it last is correct.
```

### Calling `unsubscribe()` Inside `subscribe()`

```typescript
// ❌ INCORRECT — subscription variable not yet assigned when first value arrives synchronously
const sub = of(1, 2, 3).subscribe(v => {
  if (v === 2) sub.unsubscribe(); // 'sub' may be undefined for sync source
});

// ✅ Use take/takeWhile instead:
of(1, 2, 3).pipe(
  takeWhile(v => v < 2, true)
).subscribe(console.log);
```

### Forgetting That HTTP Observables Are Safe

```typescript
// ✓ SAFE — HttpClient.get() completes after response, no cleanup needed
this.http.get('/api/data').subscribe(data => this.data = data);

// Only needs cleanup if cancelled mid-flight:
const sub = this.http.get('/api/data').subscribe(data => this.data = data);
// If navigation happens before response: sub.unsubscribe() cancels the request
```
