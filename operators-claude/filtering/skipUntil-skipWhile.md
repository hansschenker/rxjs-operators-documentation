# skipUntil / skipWhile

## Identity

| | `skipUntil` | `skipWhile` |
|---|---|---|
| **Import** | `import { skipUntil } from 'rxjs/operators'` | `import { skipWhile } from 'rxjs/operators'` |
| **Signature** | `skipUntil<T>(notifier: Observable<any>): MonoTypeOperatorFunction<T>` | `skipWhile<T>(predicate: (value: T, index: number) => boolean): MonoTypeOperatorFunction<T>` |
| **Category** | Filtering Operators | Filtering Operators |
| **Skips** | All values until a notifier Observable emits | All values while a predicate returns `true` |

```typescript
function skipUntil<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>

function skipWhile<T>(
  predicate: (value: T, index: number) => boolean
): MonoTypeOperatorFunction<T>
```

## Functional Specification

**`skipUntil(notifier$)`**: Discards all source emissions until `notifier$` emits. Once the notifier fires, ALL subsequent source values pass through. The notifier is then unsubscribed from.

**`skipWhile(predicate)`**: Discards source values as long as `predicate` returns `true`. The moment the predicate returns `false` for the first time, the skip phase ends — ALL subsequent values pass through unconditionally, even if they would have matched the predicate again.

**Critical asymmetry with `take*` operators**:

| Operator | Behavior |
|----------|----------|
| `takeUntil(n$)` | Passes values, then STOPS when notifier fires |
| `skipUntil(n$)` | Skips values, then PASSES ALL when notifier fires |
| `takeWhile(p)` | Passes while `p` true, then STOPS |
| `skipWhile(p)` | Skips while `p` true, then PASSES ALL |

**`skipWhile` one-way latch**: Once the predicate returns `false`, it is never called again — it latches open. This is distinct from `filter`, which evaluates the predicate on every value.

## Marble Diagrams

```
Source:   --1--2--3--4--5--|
Notifier: -----------n--|

skipUntil(notifier$):
Result:   --------4--5--|    (1,2,3 skipped; notifier fires before 4; 4,5 pass)

skipWhile(n => n < 3):
Source:   --1--2--3--4--2--|
Result:   --------3--4--2--|  (1,2 skipped; 3 fails predicate → latch opens)
                          ↑ 2 passes! predicate never re-checked after latch opens

filter(n => n >= 3):
Source:   --1--2--3--4--2--|
Result:   --------3--4-----|  (2 at the end is filtered — predicate re-checked each time)
```

## Type System Integration

```typescript
import { interval, timer, of } from 'rxjs';
import { skipUntil, skipWhile } from 'rxjs/operators';

// skipUntil — type preserved
interval(100).pipe(
  skipUntil(timer(500))
).subscribe((v: number) => console.log(v)); // 4, 5, 6, ...

// skipWhile — type preserved
of(1, 2, 3, 4, 2, 1).pipe(
  skipWhile(n => n < 3)
).subscribe((v: number) => console.log(v)); // 3, 4, 2, 1 (latch opened at 3)
```

## Examples

### Basic Usage
```typescript
import { interval, timer, of } from 'rxjs';
import { skipUntil, skipWhile, take } from 'rxjs/operators';

// skipUntil — delay acceptance by notifier
interval(100).pipe(
  skipUntil(timer(500)),
  take(3)
).subscribe(console.log); // 4, 5, 6  (first 5 values skipped)

// skipWhile — skip warmup period
of(0, 0, 0, 1, 2, 3, 0).pipe(
  skipWhile(n => n === 0)  // skip leading zeros
).subscribe(console.log);  // 1, 2, 3, 0  (trailing 0 passes — latch is open)
```

### Common Pattern — Skip Until User Interaction
```typescript
import { fromEvent, interval } from 'rxjs';
import { skipUntil } from 'rxjs/operators';

// Start processing data only after the user clicks "start"
const startButton = document.getElementById('start')!;
const start$ = fromEvent(startButton, 'click');

interval(1000).pipe(
  skipUntil(start$)   // tick silently until user clicks; then stream all ticks
).subscribe(tick => updateDisplay(tick));
```

### Common Pattern — Skip Initialization / Warmup Values
```typescript
import { BehaviorSubject } from 'rxjs';
import { skip, skipWhile } from 'rxjs/operators';

// BehaviorSubject emits its initial value immediately — sometimes you want to skip it
const theme$ = new BehaviorSubject<'light' | 'dark'>('light');

// Option 1: skip(1) — skip exactly the initial emission
theme$.pipe(skip(1)).subscribe(theme => applyTheme(theme)); // skips 'light'

// Option 2: skipWhile — skip until value changes from default
theme$.pipe(
  skipWhile(t => t === 'light') // skips initial 'light'; passes when user sets dark
).subscribe(theme => console.log('User changed theme to:', theme));

// Option 3: skipUntil — skip until some other event fires
const userInteracted$ = fromEvent(document, 'click');
theme$.pipe(
  skipUntil(userInteracted$)  // ignore automatic theme until after first interaction
).subscribe(applyTheme);
```

### Common Pattern — `skipUntil` for Observable Race Start
```typescript
import { merge, fromEvent, interval } from 'rxjs';
import { skipUntil, takeUntil } from 'rxjs/operators';

// Two competing streams — only accept values after the "gate" opens
const gate$ = someCondition$;

const stream1$ = hotStream1$.pipe(skipUntil(gate$));
const stream2$ = hotStream2$.pipe(skipUntil(gate$));

// Both streams silently discard values until gate$ fires, then both open
merge(stream1$, stream2$).subscribe(processValue);
```

## Common Pitfalls

### Anti-pattern: Mistaking `skipWhile` for `filter`
```typescript
import { of } from 'rxjs';
import { skipWhile, filter } from 'rxjs/operators';

// ❌ WRONG TOOL — using skipWhile expecting it to filter all matching values
of(2, 4, 6, 1, 8, 10).pipe(
  skipWhile(n => n % 2 === 0)  // intent: remove even numbers
).subscribe(console.log);
// Output: 1, 8, 10  — NOT what was intended!
// 1 fails the predicate → latch opens → 8 and 10 pass even though they're even!

// ✅ CORRECT — filter re-evaluates the predicate for every value
of(2, 4, 6, 1, 8, 10).pipe(
  filter(n => n % 2 !== 0)     // keep only odd numbers
).subscribe(console.log);
// Output: 1

// WHY: skipWhile is a ONE-WAY LATCH. Once the predicate returns false for the
// first time, it never runs again — ALL subsequent values pass through.
// Use skipWhile only for "skip a leading prefix, then pass everything."
// Use filter when you need to test every value independently.
```

### Anti-pattern: `skipUntil` With a Never-Emitting Notifier
```typescript
import { interval, NEVER } from 'rxjs';
import { skipUntil } from 'rxjs/operators';

// ❌ SILENT STREAM — notifier never fires, source values are silently discarded
interval(100).pipe(
  skipUntil(NEVER) // nothing ever opens the gate
).subscribe(console.log); // no output, ever — but subscription is alive

// ❌ SIMILAR ISSUE — notifier Subject never gets next() called
const gate$ = new Subject<void>();
interval(100).pipe(skipUntil(gate$)).subscribe(console.log);
// Silent forever unless gate$.next() is called elsewhere

// ✅ CORRECT — ensure the notifier actually fires
const start = Date.now();
interval(100).pipe(
  skipUntil(timer(500))  // will definitely fire after 500ms
).subscribe(console.log);

// WHY: skipUntil passes nothing until the notifier emits. If you're seeing
// a silent stream, check that the notifier Observable actually emits.
// Common causes: Subject.next() never called, NEVER used accidentally,
// or an Observable that only completes (completion doesn't trigger skipUntil).
```

## Related Operators

- **`takeUntil(n$)`**: Symmetric counterpart — passes values UNTIL notifier fires, then stops
- **`takeWhile(p)`**: Symmetric counterpart — passes while predicate true, then stops
- **`filter(p)`**: Re-evaluates predicate on EVERY value — use instead of skipWhile for non-prefix filtering
- **`skip(n)`**: Skip exactly the first N values (simpler than skipWhile for count-based skipping)
- **`debounceTime`**: Skip rapid intermediary values (time-based, not predicate-based)

## References
- **RxJS skipUntil**: [https://rxjs.dev/api/operators/skipUntil](https://rxjs.dev/api/operators/skipUntil)
- **RxJS skipWhile**: [https://rxjs.dev/api/operators/skipWhile](https://rxjs.dev/api/operators/skipWhile)

---

**`skipUntil`** — Cognitive Load: 2/5 | Usage: 3/5 | Key gotcha: completion of notifier does NOT open the gate — only `next()` does.
**`skipWhile`** — Cognitive Load: 2/5 | Usage: 3/5 | Key gotcha: one-way latch — once open, never re-checked. Use `filter` for per-value predicate testing.
**Teaching sequence**: Immediately after `takeUntil` and `takeWhile` — they are the symmetric inverse operations.
