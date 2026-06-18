# exhaustAll (exhaustMap)

## Identity

- **Name**: `exhaustAll` / `exhaustMap`
- **Category**: Higher-Order Operators / Transformation Operators
- **Type**: Ignore-while-busy flattening — ignores new inner Observables while one is already active
- **Import**:
  ```typescript
  import { exhaustAll } from 'rxjs/operators';
  import { exhaustMap } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function exhaustAll<T>(): OperatorFunction<ObservableInput<T>, T>

  function exhaustMap<T, R>(
    project: (value: T, index: number) => ObservableInput<R>
  ): OperatorFunction<T, R>
  ```

## Functional Specification

**Concept**: The fourth flattening strategy — unlike `mergeAll` (concurrent), `concatAll` (queue), and `switchAll` (cancel previous), `exhaustAll` **ignores** new inner Observables that arrive while an inner Observable is already active.

**Flattening strategy comparison**:

| Operator | New inner arrives while busy | Inner queue |
|----------|------------------------------|-------------|
| `mergeAll` | Subscribed concurrently | No queue — all run |
| `concatAll` | Queued for later | Yes — FIFO |
| `switchAll` | Replaces current (cancel prev) | No queue — latest only |
| `exhaustAll` | **Dropped / ignored** | No queue — current protected |

**`exhaustMap(project)`** = `map(project) + exhaustAll()` — the inline projection form.

**Use case**: Prevents duplicate executions when the user triggers an action faster than it completes. Classic example: a submit button that should not re-submit while a request is in flight.

## Marble Diagram

```
Outer:  --A---------B---|
         |           |
         A: ---1--2--|
         B: ---3--4--|

exhaustAll():  ---1--2--|    (B arrives while A is active → B ignored)

Outer:  --A--B---------C---|
         |   |          |
         A: -----1--|
         B: -----2--|  (B arrives while A active → B DROPPED)
         C: -----3--|  (C arrives after A completes → subscribed)

exhaustAll():  -----1---------3--|   (2 is lost entirely)

exhaustMap(n => timer(n * 100)):
Source: --1-----2--|
        timer(100ms) starts
                (2 arrives while timer(100) active → ignored)
Result: ----0--|   (only timer(1) fires; timer(2) never started)
```

## Type System Integration

```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface FormData { name: string; email: string }

// exhaustMap — type goes from FormData to AjaxResponse
fromEvent(submitButton, 'click').pipe(
  exhaustMap(() =>
    ajax.post<User>('/api/users', formData)
  )
).subscribe(res => showSuccess(res.response));
// While the POST is in flight, subsequent clicks are silently dropped
```

## Examples

### Basic Usage — Button Click Protection
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const submitBtn = document.querySelector('#submit')!;

fromEvent(submitBtn, 'click').pipe(
  exhaustMap(() => ajax.post('/api/submit', getFormData()))
).subscribe({
  next:  res => showSuccess(),
  error: err => showError(err)
});
// User can click as many times as they want —
// only the first click per request fires a POST
```

### Common Pattern — Login / Auth Form
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap, map, catchError } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';
import { of } from 'rxjs';

const loginForm$ = fromEvent(loginForm, 'submit').pipe(
  map(e => { e.preventDefault(); return getCredentials(); })
);

loginForm$.pipe(
  exhaustMap(credentials =>
    ajax.post<AuthResponse>('/api/login', credentials).pipe(
      catchError(err => of({ error: err.message }))
    )
  )
).subscribe(result => handleLoginResult(result));
// Double-submit / fast re-submit ignored while login request is in flight
```

### Common Pattern — Polling With No Overlap
```typescript
import { interval } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Poll every 5 seconds — skip poll if previous request still in flight
interval(5000).pipe(
  exhaustMap(() => ajax.getJSON<Status>('/api/status'))
).subscribe(updateStatusDisplay);
// If a request takes > 5s, the next interval tick is dropped
// Prevents overlapping requests from creating race conditions
```

## Common Pitfalls

### Anti-pattern: Using `exhaustMap` When `switchMap` Is Needed
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap, switchMap, debounceTime } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ WRONG — exhaustMap for search: user must WAIT for first search to finish
// before typing a new query is accepted. Feels broken.
fromEvent(searchInput, 'input').pipe(
  debounceTime(300),
  exhaustMap(e => ajax.getJSON(`/api/search?q=${e.target.value}`))
).subscribe(renderResults);
// If user types 'rx' and search takes 2s, typing 'rxjs' during that time is ignored

// ✅ CORRECT — switchMap for search: cancel previous, use latest query
fromEvent(searchInput, 'input').pipe(
  debounceTime(300),
  switchMap(e => ajax.getJSON(`/api/search?q=${e.target.value}`))
).subscribe(renderResults);

// WHY: exhaustMap protects the current operation — good for submit buttons
// (no re-submit). switchMap always uses the latest — good for search
// (always shows results for what's currently typed). Know which behavior
// you need:
//   exhaustMap → "don't interrupt what's in progress" (forms, auth)
//   switchMap  → "always use the latest" (search, navigation)
```

### Anti-pattern: Expecting Dropped Emissions to Be Queued
```typescript
import { Subject } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';

const trigger$ = new Subject<void>();

trigger$.pipe(
  exhaustMap(() => longOperation$) // takes 3 seconds
).subscribe(console.log);

trigger$.next(); // starts longOperation$
trigger$.next(); // ❌ DROPPED — longOperation still running
trigger$.next(); // ❌ DROPPED — longOperation still running
// After 3s: longOperation completes; next trigger$.next() will be processed

// ✅ CORRECT — use concatMap if dropped triggers should be queued
trigger$.pipe(
  concatMap(() => longOperation$)
).subscribe(console.log);
// All three triggers are queued and executed in order

// WHY: exhaustAll/exhaustMap DISCARDS emissions that arrive while busy.
// There is no queue — dropped values are gone forever. If you need to
// preserve all triggers, concatMap is the right choice.
```

## Related Operators

- **`switchMap`**: Cancels previous inner — use for "latest wins" (search, route navigation)
- **`concatMap`**: Queues all inner Observables — use for ordered sequential execution
- **`mergeMap`**: Runs all concurrently — use for independent parallel operations
- **`exhaustAll`**: `exhaustMap(x => x)` on an Observable-of-Observables
- **`throttleTime`**: Rate-limits by time rather than by in-flight Observable duration

## References
- **RxJS exhaustMap**: [https://rxjs.dev/api/operators/exhaustMap](https://rxjs.dev/api/operators/exhaustMap)
- **RxJS exhaustAll**: [https://rxjs.dev/api/operators/exhaustAll](https://rxjs.dev/api/operators/exhaustAll)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching point**: `exhaustMap` = "ignore while busy." Dropped emissions are **gone** — not queued. Use for submit buttons and polling-without-overlap. Use `switchMap` for search, `concatMap` for ordered queues.
