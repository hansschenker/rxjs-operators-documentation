# Notification & Toast Patterns with RxJS

Managing toast queues, notification systems, and alert stacks — sequential display, deduplication, priority ordering.

---

## Why RxJS for Notifications?

Notifications have inherently stream-like requirements:
- **Queue**: toasts must not overlap — show one, wait for dismiss, show next
- **Deduplication**: don't show the same error message three times
- **Priority**: errors should jump the queue ahead of info messages
- **Timeout**: auto-dismiss after N seconds
- **Cancellation**: user dismisses early → skip remaining display time

---

## Pattern 1: Sequential Toast Queue

Show one toast at a time, queue the rest:

```typescript
import { Subject, timer } from 'rxjs';
import { concatMap, tap } from 'rxjs/operators';

export interface Toast {
  id:       string;
  type:    'info' | 'success' | 'warning' | 'error';
  message:  string;
  duration: number; // ms to display
}

@Injectable({ providedIn: 'root' })
export class ToastService {
  private queue$ = new Subject<Toast>();

  readonly activeToast$ = this.queue$.pipe(
    concatMap(toast =>
      new Observable<Toast>(observer => {
        observer.next(toast);                  // show the toast
        const sub = timer(toast.duration)      // auto-dismiss timer
          .subscribe(() => {
            observer.next({ ...toast, id: '' }); // clear
            observer.complete();
          });
        return () => sub.unsubscribe();
      })
    )
  );

  show(toast: Omit<Toast, 'id'>): void {
    this.queue$.next({
      ...toast,
      id: crypto.randomUUID()
    });
  }

  success(message: string) { this.show({ type: 'success', message, duration: 3000 }); }
  error(message: string)   { this.show({ type: 'error',   message, duration: 5000 }); }
  info(message: string)    { this.show({ type: 'info',    message, duration: 3000 }); }
  warning(message: string) { this.show({ type: 'warning', message, duration: 4000 }); }
}
```

---

## Pattern 2: Early Dismiss + Queue Advance

Allow user to dismiss a toast early, advancing the queue:

```typescript
import { Subject, race, timer } from 'rxjs';
import { concatMap, map, tap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class DismissableToastService {
  private queue$   = new Subject<Toast>();
  private dismiss$ = new Subject<void>();

  readonly activeToast$ = this.queue$.pipe(
    concatMap(toast =>
      new Observable<Toast | null>(observer => {
        observer.next(toast);

        const done$ = race(
          timer(toast.duration),          // auto-dismiss
          this.dismiss$                   // user dismiss
        );

        const sub = done$.pipe(take(1)).subscribe(() => {
          observer.next(null);            // clear display
          observer.complete();            // advance queue
        });

        return () => sub.unsubscribe();
      })
    )
  );

  show(toast: Omit<Toast, 'id'>): void {
    this.queue$.next({ ...toast, id: crypto.randomUUID() });
  }

  dismiss(): void { this.dismiss$.next(); }
}
```

---

## Pattern 3: Priority Queue

Errors jump ahead of info messages:

```typescript
import { Subject, merge, of } from 'rxjs';
import { concatMap, groupBy, mergeMap, scan, map, filter } from 'rxjs/operators';

type Priority = 'low' | 'normal' | 'high' | 'critical';
const PRIORITY_ORDER: Record<Priority, number> = {
  low: 1, normal: 2, high: 3, critical: 4
};

@Injectable({ providedIn: 'root' })
export class PriorityToastService {
  private incoming$ = new Subject<Toast & { priority: Priority }>();

  readonly activeToast$ = this.incoming$.pipe(
    // Collect toasts into a priority-sorted buffer:
    scan((queue, toast) => {
      const sorted = [...queue, toast].sort(
        (a, b) => PRIORITY_ORDER[b.priority] - PRIORITY_ORDER[a.priority]
      );
      return sorted;
    }, [] as (Toast & { priority: Priority })[]),

    // Only advance when the last toast was consumed (simplified — use concatMap in practice):
    concatMap(queue => {
      const [next, ...rest] = queue;
      return next ? of(next) : EMPTY;
    })
  );
}
```

---

## Pattern 4: Deduplication — Suppress Repeated Messages

```typescript
import { Subject, timer } from 'rxjs';
import { concatMap, distinctUntilChanged, bufferTime, filter, map } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class DeduplicatedToastService {
  private queue$ = new Subject<Toast>();
  private recentMessages = new Map<string, number>(); // message → expiry time

  show(toast: Omit<Toast, 'id'>): void {
    const key = `${toast.type}:${toast.message}`;
    const now  = Date.now();

    // Suppress if same message shown within the last 5 seconds:
    if ((this.recentMessages.get(key) ?? 0) > now) {
      return;
    }

    this.recentMessages.set(key, now + 5000);
    this.queue$.next({ ...toast, id: crypto.randomUUID() });
  }

  // Alternatively — deduplicate in the stream:
  readonly uniqueToasts$ = this.queue$.pipe(
    bufferTime(100),                          // collect bursts
    filter(batch => batch.length > 0),
    map(batch => {
      const seen = new Set<string>();
      return batch.filter(t => {
        const key = `${t.type}:${t.message}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }),
    concatMap(batch => from(batch))           // flatten deduped batch
  );
}
```

---

## Pattern 5: Stacked Notifications (Multiple Visible)

Show up to N notifications simultaneously, queue the rest:

```typescript
import { BehaviorSubject, Subject } from 'rxjs';
import { scan, map } from 'rxjs/operators';

const MAX_VISIBLE = 3;

@Injectable({ providedIn: 'root' })
export class StackedNotificationService {
  private add$    = new Subject<Notification>();
  private remove$ = new Subject<string>();           // by id

  readonly notifications$ = merge(
    this.add$.pipe(map(n => ({ type: 'add' as const, notification: n }))),
    this.remove$.pipe(map(id => ({ type: 'remove' as const, id })))
  ).pipe(
    scan((state: { visible: Notification[]; queue: Notification[] }, action) => {
      if (action.type === 'add') {
        if (state.visible.length < MAX_VISIBLE) {
          return { ...state, visible: [...state.visible, action.notification] };
        } else {
          return { ...state, queue: [...state.queue, action.notification] };
        }
      } else {
        const visible = state.visible.filter(n => n.id !== action.id);
        // Promote from queue:
        const promoted = state.queue[0];
        return {
          visible: promoted ? [...visible, promoted] : visible,
          queue:   promoted ? state.queue.slice(1) : state.queue
        };
      }
    }, { visible: [], queue: [] }),
    shareReplay(1)
  );

  show(notification: Omit<Notification, 'id'>): void {
    this.add$.next({ ...notification, id: crypto.randomUUID() });
  }

  dismiss(id: string): void { this.remove$.next(id); }
}
```

---

## Pattern 6: Auto-Dismiss with Progress Bar

```typescript
import { timer, animationFrames } from 'rxjs';
import { map, takeWhile, switchMap } from 'rxjs/operators';

function toastWithProgress(
  message: string,
  durationMs: number
): Observable<{ message: string; progress: number; done: boolean }> {
  const start = Date.now();

  return animationFrames().pipe(
    map(() => {
      const elapsed  = Date.now() - start;
      const progress = Math.min(elapsed / durationMs, 1);
      return { message, progress, done: progress >= 1 };
    }),
    takeWhile(state => !state.done, true)
  );
}

// In component:
activeToast$.pipe(
  switchMap(toast =>
    toast
      ? toastWithProgress(toast.message, toast.duration)
      : EMPTY
  )
).subscribe(({ message, progress, done }) => {
  if (done) hideToast();
  else      renderToast(message, progress);
});
```

---

## Pattern 7: Global Error → Notification Bridge

Automatically route uncaught errors to the toast system:

```typescript
import { fromEvent } from 'rxjs';
import { map, filter } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class GlobalErrorNotificationService {
  constructor(private toasts: ToastService) {
    // Browser uncaught errors:
    fromEvent<ErrorEvent>(window, 'error').pipe(
      map(e => e.message),
      filter(msg => !msg.includes('ResizeObserver'))  // filter noise
    ).subscribe(msg => this.toasts.error(msg));

    // Unhandled Promise rejections:
    fromEvent<PromiseRejectionEvent>(window, 'unhandledrejection').pipe(
      map(e => String(e.reason?.message ?? e.reason ?? 'Unknown error'))
    ).subscribe(msg => this.toasts.error(msg));
  }
}
```

---

## Decision Table

| Requirement | Pattern | Key operators |
|---|---|---|
| One at a time | Sequential queue | `concatMap`, `timer` |
| User can dismiss early | Dismiss + advance | `race`, `concatMap` |
| Errors before info | Priority queue | `scan` (priority sort), `concatMap` |
| No duplicates | Deduplication | `Map` cache or `bufferTime`+dedup |
| Multiple visible | Stacked | `scan` (visible/queue state) |
| Progress bar | Animation | `animationFrames`, `takeWhile(inclusive)` |
| Global error bridge | Error → toast | `fromEvent(window, 'error')` |

---

## Common Pitfalls

### Using `switchMap` for Toast Queue

```typescript
// ❌ switchMap cancels previous toast when new one arrives:
toastQueue$.pipe(
  switchMap(toast => timer(toast.duration).pipe(map(() => toast)))
)
// Second toast cancels first — first toast disappears immediately!

// ✅ concatMap queues toasts — each waits for the previous:
toastQueue$.pipe(
  concatMap(toast => showToastFor(toast.duration))
)
```

### Toast Service Without Cleanup

```typescript
// ❌ Timer-based toasts never cleaned up if component destroys mid-toast:
timer(3000).subscribe(() => dismissToast());

// ✅ Use async pipe or takeUntilDestroyed for Angular component:
this.toastService.activeToast$.pipe(
  takeUntilDestroyed(this.destroyRef)
).subscribe(renderToast);
```
