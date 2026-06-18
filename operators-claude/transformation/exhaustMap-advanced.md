# exhaustMap — Advanced Patterns

For `exhaustMap` fundamentals see the core [exhaustMap](./exhaustMap) doc. This page covers form submission, upload management, login protection, double-click prevention, and the subtle differences from `switchMap` and `concatMap`.

---

## The Core Guarantee

`exhaustMap` says: **"If I'm already working, ignore new requests."**

```
Input:   --A-----B--C--------D--|
                B ignored (A in flight)
                   C ignored (A still in flight)
Inner A: -----a1--|
Inner D:            --------d1--|

Result:  ----------a1--------d1--|
```

This is the only flattening operator that **silently drops** inputs rather than queuing or cancelling them.

---

## Pattern 1: Form Submission (The Classic Use Case)

```typescript
import { exhaustMap } from 'rxjs/operators';
import { fromEvent } from 'rxjs';

const submitBtn = document.querySelector<HTMLButtonElement>('#submit')!;

fromEvent(submitBtn, 'click').pipe(
  exhaustMap(() =>
    this.api.submitForm(this.form.value).pipe(
      tap({ subscribe: () => submitBtn.disabled = true }),
      finalize(() => submitBtn.disabled = false)
    )
  )
).subscribe({
  next:  res => showSuccess(res),
  error: err => showError(err)
});
// Second click during in-flight request: silently ignored
// Third click after completion: processed normally
```

---

## Pattern 2: Login / Authentication

Prevent double login attempts:

```typescript
import { exhaustMap, tap, catchError } from 'rxjs/operators';

loginButton$.pipe(
  withLatestFrom(loginForm.valueChanges.pipe(startWith(loginForm.value))),
  map(([, formValue]) => formValue),
  exhaustMap(credentials =>
    this.auth.login(credentials).pipe(
      tap(() => this.router.navigate(['/dashboard'])),
      catchError(err => {
        this.errorMessage$.next(err.message);
        return EMPTY;  // catchError inside exhaustMap — don't kill the outer stream
      })
    )
  )
).subscribe();
```

---

## Pattern 3: File Upload with Progress

Only one upload at a time — queue others via user feedback:

```typescript
import { exhaustMap, map } from 'rxjs/operators';
import { Subject } from 'rxjs';

interface UploadState {
  progress: number;
  status: 'idle' | 'uploading' | 'done' | 'error';
}

const fileSelected$ = new Subject<File>();
const uploadState$  = new BehaviorSubject<UploadState>({ progress: 0, status: 'idle' });

fileSelected$.pipe(
  exhaustMap(file => {
    if (uploadState$.getValue().status === 'uploading') {
      showToast('Upload already in progress — please wait');
      return EMPTY; // explicit feedback before dropping
    }

    return this.uploadService.upload(file).pipe(
      tap(state => uploadState$.next(state)),
      catchError(err => {
        uploadState$.next({ progress: 0, status: 'error' });
        return EMPTY;
      }),
      finalize(() => {
        if (uploadState$.getValue().status !== 'error') {
          uploadState$.next({ progress: 100, status: 'done' });
        }
      })
    );
  })
).subscribe();
```

---

## Pattern 4: Rate-Limited Action (User Cannot Spam)

```typescript
import { exhaustMap, timer } from 'rxjs/operators';

// Like-button: enforce 2s cooldown between likes:
likeButton$.pipe(
  exhaustMap(() =>
    this.api.like(postId).pipe(
      switchMap(() => timer(2000)),  // hold for 2s cooldown after success
      catchError(() => timer(500))   // still cooldown on error
    )
  )
).subscribe();

// The `exhaustMap` drops clicks during the 2-second timer — no double-likes
```

---

## Pattern 5: Keyboard-Triggered Expensive Operation

Prevent rapid Enter-key presses from firing multiple heavy operations:

```typescript
import { fromEvent, exhaustMap } from 'rxjs';
import { filter } from 'rxjs/operators';

fromEvent<KeyboardEvent>(searchInput, 'keydown').pipe(
  filter(e => e.key === 'Enter'),
  exhaustMap(() =>
    this.api.runExpensiveSearch(searchInput.value).pipe(
      catchError(() => EMPTY)
    )
  )
).subscribe(renderResults);
```

---

## Pattern 6: Polling with Manual Refresh

Allow manual refresh only when no poll is in flight:

```typescript
import { merge, timer, Subject } from 'rxjs';
import { exhaustMap, switchMap, shareReplay } from 'rxjs/operators';

const manualRefresh$ = new Subject<void>();

// Auto-poll every 30s + manual refresh button:
const refresh$ = merge(
  timer(0, 30_000),
  manualRefresh$
);

const data$ = refresh$.pipe(
  exhaustMap(() =>  // drop overlap between auto and manual
    this.api.getData().pipe(
      catchError(() => EMPTY)
    )
  ),
  shareReplay(1)
);

// Manual refresh only fires if not already fetching:
refreshButton.addEventListener('click', () => manualRefresh$.next());
```

---

## `exhaustMap` vs `switchMap` vs `concatMap`

The key question: **what should happen when a new trigger arrives while the previous operation is still running?**

```typescript
// switchMap — Cancel old, start new (latest wins):
trigger$.pipe(switchMap(() => operation$))
// Use for: search, route data, live filters
// Risk: cancels potentially important operations

// exhaustMap — Drop new, keep old (first wins):
trigger$.pipe(exhaustMap(() => operation$))
// Use for: form submit, login, uploads
// Risk: user gets no feedback that their action was dropped

// concatMap — Queue new, process in order (everyone wins, eventually):
trigger$.pipe(concatMap(() => operation$))
// Use for: audit log, ordered saves, sequential animations
// Risk: queue grows unboundedly if triggers fire faster than operations complete
```

---

## When `exhaustMap` Silently Drops — Add User Feedback

`exhaustMap` drops inputs without any notification. For critical user actions, add explicit feedback:

```typescript
import { exhaustMap, tap } from 'rxjs/operators';
import { BehaviorSubject } from 'rxjs';

const inFlight$ = new BehaviorSubject(false);

trigger$.pipe(
  tap(trigger => {
    if (inFlight$.getValue()) {
      // Explicit feedback — user knows why their action was dropped:
      showToast('Request already in progress…', { duration: 1500 });
    }
  }),
  exhaustMap(trigger => {
    inFlight$.next(true);
    return this.api.process(trigger).pipe(
      finalize(() => inFlight$.next(false))
    );
  })
).subscribe(result => renderResult(result));

// Disable button while in-flight:
inFlight$.subscribe(busy => submitBtn.disabled = busy);
```

---

## Common Pitfalls

### Confusing `exhaustMap` with `switchMap` for Saves

```typescript
// ❌ switchMap for saves — cancels an in-flight save!
saveEvents$.pipe(
  switchMap(data => this.api.save(data))
)
// User clicks Save, starts request, clicks Save again
// → First save request CANCELLED. Data may not be persisted!

// ✅ exhaustMap — second click ignored, first save completes safely:
saveEvents$.pipe(
  exhaustMap(data => this.api.save(data))
)
// Or concatMap if you need to save all changes in order:
saveEvents$.pipe(
  concatMap(data => this.api.save(data))
)
```

### No Feedback When Action Is Dropped

```typescript
// ❌ User clicks three times rapidly — only the first works, no feedback:
clicks$.pipe(
  exhaustMap(() => this.api.submit(form))
)
// User: "Did it work? Why is nothing happening?"

// ✅ Indicate busy state:
const busy$ = new BehaviorSubject(false);
clicks$.pipe(
  exhaustMap(() => {
    busy$.next(true);
    return this.api.submit(form).pipe(finalize(() => busy$.next(false)));
  })
).subscribe();
busy$.subscribe(busy => submitBtn.textContent = busy ? 'Saving…' : 'Save');
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Decision rule**: Use `exhaustMap` when triggering the same operation twice would be harmful (double submit, double login, double upload). Always pair with visible busy state — silent drops confuse users. If you need ALL triggers processed in order, use `concatMap` instead.
