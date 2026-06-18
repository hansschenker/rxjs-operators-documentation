# debounceTime — Advanced Patterns

For `debounceTime` fundamentals see the core [debounceTime](./debounceTime) doc. This page covers form validation, search, save-on-change, and the comparison with `throttleTime`, `auditTime`, and `distinctUntilChanged`.

---

## The Standard Search Pipeline

The canonical form — debounce, deduplicate, cancel stale requests:

```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, catchError, of } from 'rxjs/operators';

const searchInput = document.getElementById('search') as HTMLInputElement;

fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),           // wait 300ms after last keystroke
  distinctUntilChanged(),      // skip if value hasn't changed
  filter(q => q.length >= 2),  // skip very short queries
  switchMap(q =>               // cancel in-flight if new query arrives
    this.api.search(q).pipe(catchError(() => of([])))
  )
).subscribe(results => renderResults(results));
```

---

## Pattern 1: Per-Field Validation with Different Delays

Different fields have different validation timing requirements:

```typescript
import { FormControl } from '@angular/forms';
import { debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';

// Username availability: debounce longer (network call)
this.usernameControl.valueChanges.pipe(
  debounceTime(600),
  distinctUntilChanged(),
  filter(v => v && v.length >= 3),
  switchMap(username =>
    this.auth.checkAvailability(username).pipe(
      map(available => available ? null : { taken: true }),
      catchError(() => of(null)) // server error = don't show error
    )
  )
).subscribe(error => {
  this.usernameControl.setErrors(error);
});

// Email format: debounce shorter (local validation only)
this.emailControl.valueChanges.pipe(
  debounceTime(200),
  distinctUntilChanged()
).subscribe(email => {
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  this.emailControl.setErrors(valid ? null : { invalidEmail: true });
});
```

---

## Pattern 2: Auto-Save

Save draft after the user stops typing:

```typescript
import { debounceTime, distinctUntilChanged, switchMap, tap } from 'rxjs/operators';

@Component({ ... })
export class EditorComponent implements OnInit {
  readonly form = this.fb.group({ title: '', body: '' });
  saving$ = new BehaviorSubject(false);

  ngOnInit() {
    this.form.valueChanges.pipe(
      debounceTime(1000),                 // wait 1s after last change
      distinctUntilChanged((a, b) =>      // deep comparison — skip unchanged
        JSON.stringify(a) === JSON.stringify(b)
      ),
      tap(() => this.saving$.next(true)),
      switchMap(value =>
        this.api.saveDraft(value).pipe(
          catchError(() => of(null))
        )
      ),
      tap(() => this.saving$.next(false)),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe();
  }
}
```

---

## Pattern 3: Debounce with Immediate First Emission

Emit immediately on first event, then debounce subsequent ones:

```typescript
import { merge, debounceTime, throttleTime } from 'rxjs/operators';

// Emit first keystroke immediately, then debounce subsequent ones:
function debounceWithLeadingEmit<T>(dueTime: number): MonoTypeOperatorFunction<T> {
  return (source$) =>
    merge(
      source$.pipe(throttleTime(dueTime)),     // leading edge: immediate
      source$.pipe(debounceTime(dueTime))      // trailing edge: debounced
    ).pipe(
      distinctUntilChanged()                   // deduplicate overlapping emits
    );
}

searchInput$.pipe(
  debounceWithLeadingEmit(300)
).subscribe(query => performSearch(query));
// First keystroke triggers immediately; subsequent keystrokes debounced 300ms
```

---

## Pattern 4: Debounce + Loading State

Show a loading indicator while the debounce timer is pending:

```typescript
import { Subject, merge } from 'rxjs';
import { debounceTime, switchMap, map, tap } from 'rxjs/operators';

const query$ = new Subject<string>();

// Track pending (debouncing) vs fetching state:
const loading$ = merge(
  query$.pipe(map(() => true)),               // any input → loading starts
  query$.pipe(
    debounceTime(300),
    switchMap(q => this.api.search(q)),
    map(() => false)                           // response received → loading ends
  )
);

const results$ = query$.pipe(
  debounceTime(300),
  switchMap(q => this.api.search(q))
);
```

---

## Pattern 5: Cross-Field Validation

Validate a field based on the value of another:

```typescript
import { combineLatest, debounceTime } from 'rxjs';

// Password confirmation validation:
combineLatest({
  password: this.passwordControl.valueChanges,
  confirm:  this.confirmControl.valueChanges
}).pipe(
  debounceTime(300),   // debounce the combined stream
  distinctUntilChanged((a, b) =>
    a.password === b.password && a.confirm === b.confirm
  )
).subscribe(({ password, confirm }) => {
  const error = password !== confirm ? { mismatch: true } : null;
  this.confirmControl.setErrors(error);
});
```

---

## `debounceTime` vs `throttleTime` vs `auditTime` — Decision Table

| Operator | Emits | Good for |
|---|---|---|
| `debounceTime(ms)` | Last value after quiet period | Search, auto-save, form validation |
| `throttleTime(ms)` | First value, then ignores for ms | Button clicks, resize events, rapid UI |
| `auditTime(ms)` | Last value at fixed interval | Scroll position, window resize sampling |
| `debounce(fn)` | Last value, dynamic delay | Adaptive debounce, scheduler-aware |

```typescript
// debounceTime: user stops typing for 300ms → send the final value
searchInput$.pipe(debounceTime(300))

// throttleTime: first scroll event, then ignore for 100ms → smoother scroll handler
scroll$.pipe(throttleTime(100))

// auditTime: whatever value existed at the 100ms mark → smooth chart updates
sensorData$.pipe(auditTime(100))
```

---

## Common Pitfalls

### Missing `distinctUntilChanged` After `debounceTime`

```typescript
// ❌ Sends request even if user types "abc" then immediately deletes back to "ab"
//    debounceTime fires with "ab" — same as the previous debounced value
searchInput$.pipe(
  debounceTime(300),
  switchMap(q => this.api.search(q)) // duplicate request for "ab"!
)

// ✅ Filter duplicate values:
searchInput$.pipe(
  debounceTime(300),
  distinctUntilChanged(), // "ab" after "ab" is skipped
  switchMap(q => this.api.search(q))
)
// WHY: debounceTime prevents rapid-fire requests but doesn't prevent
// duplicate values. distinctUntilChanged handles that case.
```

### `debounceTime` Resets on Every Emission

```typescript
// ❌ If copy-paste replaces text character-by-character, debounce never fires
// (each character event resets the 300ms timer)

// ✅ Use a minimum delay per unique value, not per keystroke:
// Ensure your input handler uses 'input' event (fires once per change) not
// 'keydown' (fires per key, including modifier keys that don't change value)
fromEvent(input, 'input').pipe(    // ✓ fires once per value change
  // NOT fromEvent(input, 'keydown') — fires for Shift, Ctrl, ArrowLeft, etc.
  debounceTime(300)
)
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Standard recipe**: `debounceTime(300)` + `distinctUntilChanged()` + `switchMap()` covers 90% of search and form validation use cases. Add `filter(q => q.length >= 2)` and `catchError(() => of([]))` for production robustness.
