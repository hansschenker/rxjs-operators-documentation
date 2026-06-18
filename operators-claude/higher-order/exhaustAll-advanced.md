# exhaustAll / exhaustMap — Advanced Patterns

For fundamentals see the core [exhaustAll / exhaustMap](./exhaustAll) doc. This page covers form submit protection, double-click prevention, optimistic UI with rollback, and the full higher-order operator decision matrix.

---

## Mental Model

```typescript
import { exhaustMap, exhaustAll } from 'rxjs/operators';

// exhaustMap — ignore new outer values while an inner Observable is active:
submit$.pipe(
  exhaustMap(data => saveToServer(data))
)
// If user clicks submit 3 times rapidly:
//   - First click: inner Observable starts
//   - Second click: IGNORED (inner still active)
//   - Third click: IGNORED (inner still active)
//   - Once save completes: next click will be accepted

// exhaustAll — higher-order equivalent:
// Observable-of-Observables → exhaustAll() ignores new inner Observables while one is running
clicks$.pipe(
  map(() => httpRequest$),
  exhaustAll()           // same as exhaustMap(() => httpRequest$)
)
```

**The key invariant**: once an inner subscription is active, all outer emissions are silently dropped until it completes. This is the "busy lock" operator.

---

## Pattern 1: Form Submit Protection

The primary use case — prevent double-submits without disabling the button:

```typescript
import { exhaustMap, tap, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// Submit form exactly once per successful completion:
fromEvent(submitButton, 'click').pipe(
  map(() => getFormValues()),
  exhaustMap(formData =>
    this.api.submitOrder(formData).pipe(
      tap(() => showSuccessToast('Order placed!')),
      catchError(err => {
        showErrorToast(err.message);
        return EMPTY; // EMPTY completes inner → next submit accepted
      })
    )
  ),
  takeUntilDestroyed()
).subscribe();

// Angular reactive form version:
@Component({})
export class CheckoutComponent {
  submitForm$ = new Subject<OrderFormData>();

  readonly submission$ = this.submitForm$.pipe(
    exhaustMap(data =>
      this.orderService.placeOrder(data).pipe(
        tap(order => this.router.navigate(['/order-confirmed', order.id])),
        catchError(err => {
          this.errorMessage = err.message;
          return EMPTY;
        })
      )
    ),
    takeUntilDestroyed()
  );
}
```

---

## Pattern 2: Loading State Without Button Disable

Track loading state alongside exhaustMap — no need to disable the button:

```typescript
import { exhaustMap, map, startWith, catchError } from 'rxjs/operators';
import { merge, Subject, of } from 'rxjs';

interface SubmitState { loading: boolean; error: string | null; success: boolean; }

const submit$ = new Subject<FormData>();

const state$: Observable<SubmitState> = submit$.pipe(
  exhaustMap(data =>
    this.api.save(data).pipe(
      map(() => ({ loading: false, error: null, success: true })),
      startWith({ loading: true, error: null, success: false }),
      catchError(err => of({ loading: false, error: err.message, success: false }))
    )
  ),
  startWith({ loading: false, error: null, success: false })
);

// Template-driven state — no imperative loading flags:
state$.pipe(takeUntilDestroyed()).subscribe(state => {
  loadingSpinner.hidden     = !state.loading;
  submitButton.textContent  = state.loading ? 'Saving…' : 'Save';
  errorMessage.textContent  = state.error ?? '';
});
```

---

## Pattern 3: Double-Click / Keyboard Shortcut Protection

Prevent rapid keyboard shortcuts from triggering parallel operations:

```typescript
import { fromEvent, exhaustMap } from 'rxjs';
import { filter, switchMap } from 'rxjs/operators';

// Ctrl+S shortcut — save, but never overlap saves:
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.ctrlKey && e.key === 's'),
  tap(e => e.preventDefault()),
  exhaustMap(() => this.documentService.save(this.doc).pipe(
    tap(() => this.lastSaved = new Date()),
    catchError(err => {
      console.error('Save failed:', err);
      return EMPTY;
    })
  )),
  takeUntilDestroyed()
).subscribe();

// Double-click protection on action buttons:
const actionButton = document.querySelector('#delete-btn')!;

fromEvent(actionButton, 'click').pipe(
  exhaustMap(() =>
    // Confirm dialog as Observable (resolves when dialog closes):
    this.dialog.open(ConfirmDialog).afterClosed().pipe(
      filter(confirmed => confirmed),
      switchMap(() => this.api.deleteItem(this.itemId))
    )
  ),
  takeUntilDestroyed()
).subscribe(() => this.router.navigate(['/items']));
```

---

## Pattern 4: Optimistic Update with `exhaustMap` + Rollback

Use `exhaustMap` to prevent concurrent mutations on the same resource:

```typescript
import { BehaviorSubject, exhaustMap, catchError } from 'rxjs';

interface ItemState { items: Item[]; saving: boolean; }

const state$ = new BehaviorSubject<ItemState>({ items: [], saving: false });
const toggleItem$ = new Subject<string>(); // item ID

// Optimistic toggle — exhaustMap prevents concurrent toggles:
toggleItem$.pipe(
  exhaustMap(id => {
    const current = state$.getValue();

    // Optimistic update immediately:
    state$.next({
      items: current.items.map(item =>
        item.id === id ? { ...item, done: !item.done } : item
      ),
      saving: true
    });

    return this.api.toggleItem(id).pipe(
      tap(updated => {
        state$.next({
          items: state$.getValue().items.map(item =>
            item.id === id ? updated : item
          ),
          saving: false
        });
      }),
      catchError(() => {
        // Rollback:
        state$.next({ ...current, saving: false });
        return EMPTY;
      })
    );
  }),
  takeUntilDestroyed()
).subscribe();
```

---

## Pattern 5: Guarded Navigation (Route-Level)

Prevent navigation while a background save is in progress:

```typescript
import { exhaustMap, first } from 'rxjs/operators';
import { CanDeactivate } from '@angular/router';

@Injectable()
export class UnsavedChangesGuard implements CanDeactivate<EditComponent> {
  canDeactivate(component: EditComponent): Observable<boolean> {
    if (!component.hasUnsavedChanges()) return of(true);

    // Prompt user — use exhaustMap to prevent concurrent dialog opens:
    return fromEvent<MouseEvent>(document, 'click').pipe(
      startWith(null),
      exhaustMap(() =>
        component.showSavePrompt().pipe(
          switchMap(action => {
            if (action === 'save')    return component.save().pipe(map(() => true));
            if (action === 'discard') return of(true);
            return of(false); // cancel
          })
        )
      ),
      first()
    );
  }
}
```

---

## Pattern 6: Token Refresh — Exhaustive (No Parallel Refreshes)

Use `exhaustMap` to deduplicate concurrent token refresh requests:

```typescript
import { exhaustMap, share, filter, switchMap } from 'rxjs/operators';

// Singleton refresh stream — multiple 401s trigger only one refresh:
const tokenRefresh$ = new Subject<void>();

const refreshToken$ = tokenRefresh$.pipe(
  exhaustMap(() =>
    this.authService.refreshToken().pipe(
      catchError(err => {
        this.authService.logout();
        return EMPTY;
      })
    )
  ),
  share() // multicast to all waiting requests
);

// HTTP interceptor:
intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
  return next.handle(req).pipe(
    catchError(err => {
      if (err.status !== 401) return throwError(() => err);

      tokenRefresh$.next(); // trigger refresh (exhaustMap deduplicates)

      return refreshToken$.pipe(
        first(),
        switchMap(() => next.handle(this.addToken(req))) // retry with new token
      );
    })
  );
}
```

---

## `exhaustMap` vs Other Flattening Operators

```typescript
// THE DECISION MATRIX for inner Observables:

// switchMap — cancel previous, take latest
// Use for: search autocomplete, route data, anything where "latest wins"
searchQuery$.pipe(switchMap(q => search(q)))

// concatMap — queue, preserve order, no cancellation
// Use for: sequential writes, migrations, animations that must complete
actions$.pipe(concatMap(action => process(action)))

// mergeMap — all concurrent, no cancellation
// Use for: parallel independent operations (file uploads, independent API calls)
files$.pipe(mergeMap(file => upload(file)))

// exhaustMap — ignore new while busy, no cancellation
// Use for: form submits, keyboard shortcuts, one-at-a-time operations
submit$.pipe(exhaustMap(data => save(data)))
```

---

## `exhaustAll` for Dynamic Source Registration

```typescript
import { Subject, exhaustAll } from 'rxjs';

// Higher-order: each outer emission is an Observable to flatten with exhaustAll:
const requestTrigger$ = new Subject<Observable<ApiResponse>>();

requestTrigger$.pipe(
  exhaustAll() // if a request is in-flight, new requests are ignored
).subscribe(handleResponse);

// Trigger a request (will be ignored if one is already running):
onButtonClick(() => requestTrigger$.next(this.api.fetchData()));
```

---

## Common Pitfalls

### Catching Error Wrongly Locks `exhaustMap` Forever

```typescript
// ❌ Catching error in outer pipe — exhaustMap never receives completion:
submit$.pipe(
  exhaustMap(data => this.api.save(data)),
  catchError(err => {
    showError(err);
    return EMPTY; // EMPTY completes the OUTER stream, not inner!
    // Result: no more submits accepted after first error
  })
)

// ✅ Catch error INSIDE the inner Observable:
submit$.pipe(
  exhaustMap(data =>
    this.api.save(data).pipe(
      catchError(err => {
        showError(err);
        return EMPTY; // completes inner → exhaustMap unlocks for next submit
      })
    )
  )
)
```

### Using `exhaustMap` When Queuing Is Required

```typescript
// ❌ exhaustMap for sequential data writes — some writes dropped:
userEdits$.pipe(
  exhaustMap(edit => this.db.write(edit))
  // If user types quickly, intermediate edits are LOST
)

// ✅ concatMap to queue all writes:
userEdits$.pipe(
  concatMap(edit => this.db.write(edit))
  // All writes queued and executed in order
)
```

### Forgetting That `exhaustAll` Drops Observables (Not Values)

```typescript
// ❌ Confusing exhaustMap with throttle/debounce:
// exhaustMap doesn't "slow down" a stream — it DROPS entire inner Observables
// If a 5-second API call is in-flight and 10 clicks arrive, all 10 are dropped.
// This is the desired behavior for submit protection, but not for throttling.

// ✅ Use throttleTime for rate-limiting without losing all intermediate values:
events$.pipe(throttleTime(500)) // still gets values, just rate-limited
events$.pipe(exhaustMap(e => process(e))) // only one active at a time, rest dropped
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `exhaustMap` is the "busy lock" — when one inner Observable is active, outer emissions are silently discarded. This makes it uniquely suited for form submit protection, keyboard shortcut deduplication, and token refresh serialization. The critical rule: catch errors INSIDE the inner Observable or `exhaustMap` will permanently stop accepting new values after the first error.
