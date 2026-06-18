# RxJS + Reactive Forms: Advanced Patterns

Beyond basic `valueChanges` — async validators, cross-field dependencies, form arrays with dynamic streams, optimistic validation, and form-level state machines.

---

## The Reactive Forms RxJS Surface

```typescript
import { FormControl, FormGroup, FormArray, AbstractControl } from '@angular/forms';

// Every AbstractControl exposes:
control.valueChanges   // Observable<T> — emits on every value change
control.statusChanges  // Observable<'VALID'|'INVALID'|'PENDING'|'DISABLED'>
control.events         // Angular 18+: Observable<ControlEvent> — unified event stream

// Key behavioral notes:
// valueChanges emits BEFORE validators run
// statusChanges emits AFTER all validators (sync and async) complete
// Both are hot, multicast Observables — no need for share()
```

---

## Pattern 1: Cross-Field Validation with `combineLatest`

```typescript
import { Component, inject } from '@angular/core';
import { NonNullableFormBuilder, Validators } from '@angular/forms';
import { combineLatest, map } from 'rxjs';
import { startWith, distinctUntilChanged } from 'rxjs/operators';

@Component({ standalone: true })
export class PasswordFormComponent {
  private readonly fb = inject(NonNullableFormBuilder);

  readonly form = this.fb.group({
    password:        ['', [Validators.required, Validators.minLength(8)]],
    confirmPassword: ['', Validators.required]
  });

  // Cross-field match validation driven by RxJS:
  readonly passwordMismatch$ = combineLatest([
    this.form.get('password')!.valueChanges.pipe(startWith('')),
    this.form.get('confirmPassword')!.valueChanges.pipe(startWith(''))
  ]).pipe(
    map(([pwd, confirm]) => pwd !== confirm && confirm.length > 0),
    distinctUntilChanged()
  );

  // Reactive form with group-level cross-field validator:
  readonly passwordStrength$ = this.form.get('password')!.valueChanges.pipe(
    startWith(''),
    map(pwd => ({
      hasUppercase: /[A-Z]/.test(pwd),
      hasNumber:    /\d/.test(pwd),
      hasSymbol:    /[^a-zA-Z0-9]/.test(pwd),
      longEnough:   pwd.length >= 8
    })),
    map(checks => Object.values(checks).filter(Boolean).length)
  );
}
```

---

## Pattern 2: Async Validators with Debounce

```typescript
import { AbstractControl, AsyncValidatorFn, ValidationErrors } from '@angular/forms';
import { Observable, of, timer } from 'rxjs';
import { switchMap, map, catchError, first } from 'rxjs/operators';

// Async validator factory with debounce:
function usernameAvailableValidator(authService: AuthService): AsyncValidatorFn {
  return (control: AbstractControl): Observable<ValidationErrors | null> => {
    const username = control.value as string;

    if (!username || username.length < 3) {
      return of(null); // no error for too-short usernames — sync validators handle that
    }

    return timer(400).pipe(     // debounce — wait for typing to stop
      switchMap(() => authService.checkUsername$(username)),
      map(available => available ? null : { usernameTaken: true }),
      catchError(() => of(null)), // network error = pass (don't block form)
      first()                     // complete the Observable — required for async validators
    );
  };
}

// Apply to control:
const usernameControl = new FormControl('', {
  validators:      [Validators.required, Validators.minLength(3)],
  asyncValidators: [usernameAvailableValidator(inject(AuthService))],
  updateOn:        'blur'   // only validate on blur, not every keystroke
});

// Show validation state:
readonly usernameStatus$ = usernameControl.statusChanges.pipe(
  startWith(usernameControl.status),
  map(status => ({
    pending: status === 'PENDING',
    valid:   status === 'VALID',
    invalid: status === 'INVALID',
    error:   usernameControl.errors?.['usernameTaken']
  }))
);
```

---

## Pattern 3: Dynamic `FormArray` with Observable Items

```typescript
import { FormArray, FormControl } from '@angular/forms';
import { combineLatest, startWith, switchMap, map } from 'rxjs';

@Component({ standalone: true })
class DynamicTagFormComponent {
  readonly tagsArray = new FormArray<FormControl<string>>([]);

  // Aggregate validity across all dynamic controls:
  readonly allTagsValid$ = this.tagsArray.statusChanges.pipe(
    startWith(this.tagsArray.status),
    map(() => this.tagsArray.controls.every(c => c.valid))
  );

  // Reactive sum of all numeric controls in a FormArray:
  readonly totalCost$ = this.itemsArray.valueChanges.pipe(
    startWith(this.itemsArray.value),
    map(items => (items as number[]).reduce((a, b) => a + (b || 0), 0))
  );

  // Combine array changes with external data:
  readonly selectedItemDetails$ = this.selectedIds.valueChanges.pipe(
    startWith(this.selectedIds.value as string[]),
    switchMap(ids =>
      ids.length === 0
        ? of([])
        : combineLatest(ids.map(id => this.itemService.getItem$(id)))
    )
  );

  addTag(tag = '') {
    this.tagsArray.push(new FormControl(tag, { nonNullable: true }));
  }

  removeTag(index: number) {
    this.tagsArray.removeAt(index);
  }
}
```

---

## Pattern 4: Form-Level State Machine

Model complex form flows (multi-step wizards, draft/submit/confirmed states) as explicit state machines:

```typescript
import { BehaviorSubject, combineLatest } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

type FormState = 'draft' | 'validating' | 'submitting' | 'submitted' | 'error';

@Component({ standalone: true })
class CheckoutFormComponent {
  private readonly formState$ = new BehaviorSubject<FormState>('draft');

  readonly form = inject(NonNullableFormBuilder).group({
    address: ['', Validators.required],
    payment: ['', Validators.required]
  });

  // Derived state signals for template:
  readonly isSubmitting$ = this.formState$.pipe(map(s => s === 'submitting'));
  readonly hasError$     = this.formState$.pipe(map(s => s === 'error'));
  readonly isSubmitted$  = this.formState$.pipe(map(s => s === 'submitted'));

  // Submit button disabled state — combine form validity with form state:
  readonly canSubmit$ = combineLatest([
    this.form.statusChanges.pipe(startWith(this.form.status)),
    this.formState$
  ]).pipe(
    map(([status, state]) => status === 'VALID' && state === 'draft'),
    distinctUntilChanged()
  );

  submit() {
    if (!this.form.valid) return;

    this.formState$.next('submitting');

    this.orderService.place$(this.form.getRawValue()).pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe({
      next:  () => this.formState$.next('submitted'),
      error: () => this.formState$.next('error')
    });
  }

  reset() {
    this.form.reset();
    this.formState$.next('draft');
  }
}
```

---

## Pattern 5: Optimistic Form Submission

Submit immediately, revert on error, and show recovery UI:

```typescript
import { catchError, switchMap, tap } from 'rxjs/operators';

interface FormSnapshot<T> { value: T; status: string }

@Component({ standalone: true })
class ProfileEditComponent {
  private readonly form = inject(NonNullableFormBuilder).group({
    displayName: ['', Validators.required],
    bio:         ['']
  });

  private snapshot: FormSnapshot<typeof this.form.value> | null = null;

  saveChanges() {
    // Snapshot before submit — for rollback:
    this.snapshot = {
      value:  this.form.getRawValue(),
      status: this.form.status
    };

    const update = this.form.getRawValue();
    this.form.disable();  // prevent edits during submit

    this.userService.updateProfile$(update).pipe(
      tap(() => {
        this.form.enable();
        this.snapshot = null;
        this.toast.success('Profile saved');
      }),
      catchError(err => {
        // Optimistic rollback:
        this.form.enable();
        if (this.snapshot) {
          this.form.setValue(this.snapshot.value as typeof this.form.value);
        }
        this.toast.error(`Save failed: ${err.message}. Changes restored.`);
        return EMPTY;
      }),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe();
  }
}
```

---

## Pattern 6: Dependent Field Cascades

When one field's valid value determines another field's options:

```typescript
import { switchMap, startWith, tap } from 'rxjs/operators';

@Component({ standalone: true })
class LocationFormComponent {
  readonly form = inject(NonNullableFormBuilder).group({
    country: [''],
    state:   [''],
    city:    ['']
  });

  // When country changes, reload states and reset downstream:
  readonly states$ = this.form.get('country')!.valueChanges.pipe(
    startWith(this.form.get('country')!.value),
    tap(() => {
      this.form.get('state')!.reset('');  // clear dependent fields
      this.form.get('city')!.reset('');
    }),
    switchMap(country =>
      country ? this.geoService.getStates$(country) : of([])
    )
  );

  // When state changes, reload cities:
  readonly cities$ = this.form.get('state')!.valueChanges.pipe(
    startWith(this.form.get('state')!.value),
    tap(() => this.form.get('city')!.reset('')),
    switchMap(state =>
      state ? this.geoService.getCities$(state) : of([])
    )
  );
}
```

---

## Common Pitfalls

### Subscribing to `valueChanges` Without `startWith`

```typescript
// ❌ combineLatest won't emit until BOTH controls emit at least once:
combineLatest([
  this.form.get('username')!.valueChanges,
  this.form.get('email')!.valueChanges
]).subscribe(([u, e]) => validate(u, e));
// Never emits until both fields are typed in

// ✅ startWith seeds initial value so combineLatest fires immediately:
combineLatest([
  this.form.get('username')!.valueChanges.pipe(startWith('')),
  this.form.get('email')!.valueChanges.pipe(startWith(''))
]).subscribe(([u, e]) => validate(u, e));
```

### Async Validator Observable Must Complete

```typescript
// ❌ Async validator Observable never completes — form stays PENDING forever:
function badValidator(control: AbstractControl): Observable<ValidationErrors | null> {
  return this.api.check$(control.value);
  // If api.check$ is a Subject or infinite stream, form never validates
}

// ✅ Use first() or take(1) to guarantee completion:
function goodValidator(control: AbstractControl): Observable<ValidationErrors | null> {
  return this.api.check$(control.value).pipe(
    first(),  // take first emission and complete
    map(valid => valid ? null : { invalid: true })
  );
}
```

---

**Key insight**: The `valueChanges` / `statusChanges` streams are the bridge between Angular Reactive Forms and RxJS. The three patterns every Angular developer should know: (1) `startWith(control.value)` before `combineLatest` to seed initial state; (2) `first()` in async validators to guarantee completion; (3) `switchMap` in dependent cascades to cancel stale requests when the parent field changes. Form state machines (`BehaviorSubject<FormState>`) replace the ad-hoc `isSubmitting`, `hasError`, `isSuccess` boolean flags that otherwise clutter component classes.
