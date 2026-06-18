# Form Validation with RxJS

Reactive form validation patterns: real-time feedback, async validation, cross-field validation, and form submission control.

---

## Core Operators for Form Validation

| Operator | Role |
|---|---|
| `debounceTime` | Delay validation until user stops typing |
| `distinctUntilChanged` | Skip validation if value hasn't changed |
| `switchMap` | Cancel pending validation when value changes |
| `combineLatest` | Cross-field validation |
| `merge` | Combine validation streams |
| `startWith` | Provide initial validation state |
| `shareReplay(1)` | Share validation result between error display and submit button |

---

## Pattern 1: Basic Async Validation

Validate a username's availability with a network request:

```typescript
import { debounceTime, distinctUntilChanged, filter, switchMap, catchError, map } from 'rxjs/operators';
import { of } from 'rxjs';

// Angular Reactive Forms async validator:
function usernameAvailabilityValidator(authService: AuthService): AsyncValidatorFn {
  return (control: AbstractControl): Observable<ValidationErrors | null> => {
    if (!control.value || control.value.length < 3) return of(null);

    return of(control.value).pipe(
      debounceTime(400),
      distinctUntilChanged(),
      switchMap(username => authService.checkUsername(username)),
      map(available => available ? null : { usernameTaken: true }),
      catchError(() => of(null)) // network error = pass validation
    );
  };
}

// Register it:
this.form = this.fb.group({
  username: ['', [Validators.required, Validators.minLength(3)], [usernameAvailabilityValidator(this.auth)]]
});
```

---

## Pattern 2: Cross-Field Validation

Validate fields against each other (password confirmation, date ranges):

```typescript
import { combineLatest, debounceTime, distinctUntilChanged } from 'rxjs';

const password$ = this.form.get('password')!.valueChanges;
const confirm$  = this.form.get('confirmPassword')!.valueChanges;

combineLatest({ password: password$, confirm: confirm$ }).pipe(
  debounceTime(200),
  distinctUntilChanged((a, b) =>
    a.password === b.password && a.confirm === b.confirm
  )
).subscribe(({ password, confirm }) => {
  const confirmControl = this.form.get('confirmPassword')!;
  if (password && confirm && password !== confirm) {
    confirmControl.setErrors({ ...confirmControl.errors, passwordMismatch: true });
  } else {
    const errors = { ...confirmControl.errors };
    delete errors['passwordMismatch'];
    confirmControl.setErrors(Object.keys(errors).length ? errors : null);
  }
});
```

---

## Pattern 3: Form-Level Async Validation

Validate the entire form as a unit (e.g. check if a date range is available):

```typescript
import { combineLatest, switchMap, catchError, of } from 'rxjs';
import { debounceTime, distinctUntilChanged, shareReplay } from 'rxjs/operators';

const startDate$ = this.form.get('startDate')!.valueChanges;
const endDate$   = this.form.get('endDate')!.valueChanges;

readonly availability$ = combineLatest({ start: startDate$, end: endDate$ }).pipe(
  filter(({ start, end }) => start && end && end > start),
  debounceTime(600),
  distinctUntilChanged((a, b) => a.start === b.start && a.end === b.end),
  switchMap(({ start, end }) =>
    this.bookingApi.checkAvailability(start, end).pipe(
      catchError(() => of({ available: true })) // fail open
    )
  ),
  shareReplay(1) // share between error display and submit button
);

readonly canSubmit$ = combineLatest({
  formValid:    of(this.form.statusChanges).pipe(startWith(this.form.status)),
  availability: this.availability$
}).pipe(
  map(({ formValid, availability }) =>
    formValid === 'VALID' && availability.available
  )
);
```

---

## Pattern 4: Real-Time Validation Status

Track validation state (valid / pending / invalid) reactively:

```typescript
import { merge, of } from 'rxjs';
import { debounceTime, switchMap, map, startWith, catchError } from 'rxjs/operators';

type FieldState =
  | { status: 'idle' }
  | { status: 'validating' }
  | { status: 'valid' }
  | { status: 'invalid'; message: string };

function createFieldValidator<T>(
  value$: Observable<T>,
  validate: (v: T) => Observable<string | null>,
  debounceDue = 400
): Observable<FieldState> {
  return value$.pipe(
    map(() => ({ status: 'validating' } as FieldState)), // immediate feedback
    switchMap(v =>
      of(v).pipe(
        debounceTime(debounceDue),
        switchMap(val => validate(val as T)),
        map(error =>
          error === null
            ? ({ status: 'valid' } as FieldState)
            : ({ status: 'invalid', message: error } as FieldState)
        ),
        startWith({ status: 'validating' } as FieldState),
        catchError(() => of({ status: 'valid' } as FieldState))
      )
    ),
    startWith({ status: 'idle' } as FieldState)
  );
}

// Usage:
readonly emailState$ = createFieldValidator(
  this.emailControl.valueChanges,
  email => this.auth.checkEmail(email).pipe(
    map(exists => exists ? 'Email already registered' : null)
  )
);
```

---

## Pattern 5: Debounced Submit with Duplicate Prevention

Prevent double-submissions and show loading state:

```typescript
import { Subject, BehaviorSubject } from 'rxjs';
import { exhaustMap, tap, catchError, finalize } from 'rxjs/operators';

@Component({ ... })
export class CheckoutComponent {
  readonly submit$   = new Subject<void>();
  readonly loading$  = new BehaviorSubject(false);
  readonly error$    = new BehaviorSubject<string | null>(null);

  constructor() {
    this.submit$.pipe(
      exhaustMap(() => { // ignore new submits while one is in flight
        if (this.form.invalid) return EMPTY;
        this.loading$.next(true);
        this.error$.next(null);

        return this.api.submitOrder(this.form.value).pipe(
          tap(result => this.router.navigate(['/confirmation', result.id])),
          catchError(err => {
            this.error$.next(err.message);
            return EMPTY;
          }),
          finalize(() => this.loading$.next(false))
        );
      }),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe();
  }

  onSubmit() { this.submit$.next(); }
}
```

---

## Pattern 6: Dependent Field Validation

Validation that changes based on another field's value:

```typescript
import { switchMap, distinctUntilChanged, combineLatest } from 'rxjs';

const country$ = this.form.get('country')!.valueChanges;
const zipCode$ = this.form.get('zipCode')!.valueChanges;

// Validate zip code format based on selected country:
combineLatest({ country: country$, zip: zipCode$ }).pipe(
  debounceTime(300),
  distinctUntilChanged((a, b) => a.country === b.country && a.zip === b.zip),
  switchMap(({ country, zip }) => {
    if (!zip) return of(null);
    return this.addressService.validatePostalCode(zip, country).pipe(
      map(valid => valid ? null : { invalidPostalCode: true }),
      catchError(() => of(null))
    );
  })
).subscribe(error => {
  this.form.get('zipCode')!.setErrors(error);
});
```

---

## Pattern 7: Validation with Feedback Delay

Show "checking…" message for a minimum visible duration (avoid flicker):

```typescript
import { timer, merge, of, combineLatest } from 'rxjs';
import { switchMap, map, share } from 'rxjs/operators';

function withMinDuration<T>(minMs: number) {
  return (source$: Observable<T>): Observable<T> =>
    combineLatest([
      source$,
      timer(minMs) // minimum duration
    ]).pipe(map(([value]) => value));
}

const validation$ = emailInput$.pipe(
  debounceTime(300),
  switchMap(email =>
    this.auth.checkEmail(email).pipe(
      withMinDuration(500), // show "checking" for at least 500ms
      catchError(() => of(null))
    )
  ),
  share()
);
```

---

## Complete Angular Reactive Form Example

```typescript
import { Component, inject, DestroyRef } from '@angular/core';
import { FormBuilder, Validators } from '@angular/forms';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { debounceTime, distinctUntilChanged, switchMap, catchError, map } from 'rxjs/operators';
import { combineLatest, of } from 'rxjs';

@Component({
  template: `
    <form [formGroup]="form" (ngSubmit)="submit$.next()">
      <input formControlName="username" placeholder="Username" />
      <span *ngIf="usernameState() === 'validating'">Checking availability…</span>
      <span *ngIf="usernameState() === 'taken'" class="error">Username taken</span>

      <input formControlName="email" type="email" placeholder="Email" />

      <button [disabled]="(canSubmit$ | async) === false" type="submit">
        {{ (loading$ | async) ? 'Saving…' : 'Register' }}
      </button>
    </form>
  `
})
export class RegisterComponent {
  private fb          = inject(FormBuilder);
  private authService = inject(AuthService);
  private destroyRef  = inject(DestroyRef);

  form = this.fb.group({
    username: ['', [Validators.required, Validators.minLength(3)]],
    email:    ['', [Validators.required, Validators.email]],
    password: ['', [Validators.required, Validators.minLength(8)]],
  });

  submit$  = new Subject<void>();
  loading$ = new BehaviorSubject(false);

  canSubmit$ = this.form.statusChanges.pipe(
    startWith(this.form.status),
    map(status => status === 'VALID'),
    combineLatestWith(this.loading$),
    map(([valid, loading]) => valid && !loading)
  );

  usernameState = toSignal(
    this.form.get('username')!.valueChanges.pipe(
      debounceTime(400),
      distinctUntilChanged(),
      filter(v => v && v.length >= 3),
      switchMap(username =>
        this.authService.checkUsername(username).pipe(
          map(available => available ? 'available' : 'taken'),
          startWith('validating'),
          catchError(() => of('available'))
        )
      ),
      startWith('idle')
    ),
    { initialValue: 'idle' }
  );

  constructor() {
    this.submit$.pipe(
      exhaustMap(() => {
        if (this.form.invalid) return EMPTY;
        this.loading$.next(true);
        return this.authService.register(this.form.value).pipe(
          finalize(() => this.loading$.next(false)),
          catchError(err => { this.handleError(err); return EMPTY; })
        );
      }),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(result => this.router.navigate(['/welcome']));
  }
}
```

---

**Key insight**: Reactive form validation with RxJS separates three concerns — the validation pipeline (what to check), the timing pipeline (when to check), and the state pipeline (how to present the result). `debounceTime` + `distinctUntilChanged` + `switchMap` handles timing; `shareReplay(1)` shares the result; `exhaustMap` prevents double-submission.
