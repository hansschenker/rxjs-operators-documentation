# takeWhile

## Identity

- **Name**: takeWhile
- **Category**: Filtering Operators
- **Type**: Predicate-driven completion — emits values while a condition holds, then completes
- **Import**:
  ```typescript
  import { takeWhile } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function takeWhile<T>(
    predicate: (value: T, index: number) => boolean,
    inclusive?: boolean
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: A stream of `T` values plus a predicate function.

**Output**: Forwards values as long as `predicate(value, index)` returns `true`. The moment the predicate returns `false`:
- Default (`inclusive: false`): completes immediately, discarding the failing value
- `inclusive: true`: emits the failing value, then completes

**Invariants**:
- Predicate is called in order for each value; index starts at 0
- Completes synchronously when the predicate first fails
- If the source completes before the predicate ever fails, output completes normally
- Does NOT error; always terminates via completion

## Marble Diagram

```
Source:    --1--2--3--4--5--|

takeWhile(n => n < 4):
Result:    --1--2--3--|          (4 fails predicate → complete, 4 discarded)

takeWhile(n => n < 4, true):
Result:    --1--2--3--4|         (inclusive: 4 is emitted, then complete)

Source:    --a--b--c--d--|
takeWhile(v => v !== 'c'):
Result:    --a--b--|             ('c' fails → complete before 'd' is ever seen)

takeWhile(v => v !== 'c', true):
Result:    --a--b--c|            ('c' emitted, then complete)
```

## Behavioral Characteristics

**Subscription**: Subscribes to source immediately; unsubscribes from source as soon as predicate fails (triggers upstream teardown).

**Completion**: Always completes (never errors). The completion is the signal that the condition was violated.

**Error handling**: Source errors pass through unchanged.

**Hot/Cold**: Works with both. On hot sources, unsubscription prevents further upstream processing.

**`inclusive` flag**: Critical for range/boundary conditions. When you need "emit up to and including the value that caused the stop," use `inclusive: true`.

## Type System Integration

```typescript
import { of } from 'rxjs';
import { takeWhile } from 'rxjs/operators';

// Type is preserved — no narrowing from predicate
of(1, 2, 3, 4).pipe(
  takeWhile(n => n < 3)
).subscribe((v: number) => console.log(v));

// Type narrowing with type predicate (TypeScript ≥ 4.9)
of(1, null, 2, null, 3).pipe(
  takeWhile((v): v is number => v !== null)
).subscribe((v: number) => console.log(v)); // v is narrowed to number
// Output: 1  (stops at first null)

// With inclusive: true — type unchanged, but includes the boundary value
of(1, 2, 3, 4, 5).pipe(
  takeWhile(n => n < 4, true)
).subscribe((v: number) => console.log(v)); // 1, 2, 3, 4
```

## Examples

### Basic Usage
```typescript
import { of, interval } from 'rxjs';
import { takeWhile } from 'rxjs/operators';

// Simple boundary
of(2, 4, 6, 7, 8, 10).pipe(
  takeWhile(n => n % 2 === 0)
).subscribe(console.log);
// Output: 2, 4, 6  (stops at 7, the first odd — 7 is discarded)

// inclusive: true — emit the failing value before stopping
of(2, 4, 6, 7, 8, 10).pipe(
  takeWhile(n => n % 2 === 0, true)
).subscribe(console.log);
// Output: 2, 4, 6, 7  (7 emitted, then complete)
```

### Common Pattern — Stop an Interval by Condition
```typescript
import { interval } from 'rxjs';
import { takeWhile, map } from 'rxjs/operators';

// Count up until we hit 5, then stop automatically
interval(100).pipe(
  map(n => n + 1),
  takeWhile(n => n <= 5)
).subscribe({
  next: n => console.log(n),       // 1, 2, 3, 4, 5
  complete: () => console.log('done')
});
```

### Common Pattern — Component Lifecycle Without a Subject
```typescript
import { fromEvent } from 'rxjs';
import { takeWhile } from 'rxjs/operators';

@Component({ ... })
class SearchComponent implements OnDestroy {
  private alive = true;

  ngOnInit() {
    // takeWhile(() => this.alive) checks the flag on each emission
    fromEvent(this.inputEl.nativeElement, 'input').pipe(
      takeWhile(() => this.alive)
    ).subscribe(event => this.handleInput(event));
  }

  ngOnDestroy() {
    this.alive = false; // next emission will trigger completion
  }
}

// Note: this requires AT LEAST ONE more emission after alive=false to actually
// unsubscribe. For immediate teardown on destroy, prefer takeUntil(destroy$).
```

### Common Pattern — Stream Values Until Sentinel
```typescript
import { fromEvent } from 'rxjs';
import { takeWhile, map } from 'rxjs/operators';

interface Message { type: string; payload: unknown }

// Consume messages from a WebSocket until we receive 'DONE'
messages$.pipe(
  takeWhile((msg: Message) => msg.type !== 'DONE', true) // inclusive to capture DONE
).subscribe({
  next: msg => processMessage(msg),
  complete: () => console.log('stream closed by DONE sentinel')
});
```

## Common Pitfalls

### Anti-pattern: `takeWhile(() => flag)` for Lifecycle Cleanup
```typescript
import { interval } from 'rxjs';
import { takeWhile } from 'rxjs/operators';

// ❌ SUBTLE BUG — works only if the source keeps emitting after destroy
class Component {
  private alive = true;

  ngOnInit() {
    interval(1000).pipe(
      takeWhile(() => this.alive)
    ).subscribe(tick => this.update(tick));
  }

  ngOnDestroy() {
    this.alive = false;
    // The interval fires every 1000ms. If destroy() is called between ticks,
    // the subscription stays alive until the NEXT tick arrives (up to 1 second later).
    // For event-driven sources that never emit again, cleanup NEVER fires.
  }
}

// ✅ CORRECT — takeUntil with a Subject for immediate teardown
class Component {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(1000).pipe(
      takeUntil(this.destroy$)
    ).subscribe(tick => this.update(tick));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete(); // triggers unsubscription immediately
  }
}

// WHY: takeWhile checks its predicate only when the source emits.
// If the source is slow or event-driven, there can be a delay between
// the flag being set and the subscription being torn down. takeUntil(destroy$)
// unsubscribes immediately when destroy$.next() is called, regardless of
// whether the source emits again.
```

### Anti-pattern: Expecting `takeWhile` to Filter, Not Complete
```typescript
import { of } from 'rxjs';
import { takeWhile, filter } from 'rxjs/operators';

// ❌ WRONG INTENT — wants to skip odd numbers, keep all even numbers
of(1, 2, 3, 4, 5, 6).pipe(
  takeWhile(n => n % 2 === 0) // stops at 1 immediately! emits nothing
).subscribe(console.log); // (no output)

// ✅ CORRECT — use filter to skip, not complete
of(1, 2, 3, 4, 5, 6).pipe(
  filter(n => n % 2 === 0)
).subscribe(console.log); // 2, 4, 6

// WHY: takeWhile COMPLETES on the first false predicate. It does not skip
// non-matching values and continue. For skipping values while keeping
// the stream alive, use filter(). Use takeWhile only when a false predicate
// means "stop the stream entirely."
```

## Related Operators

- **`takeUntil(notifier$)`**: Stops on a signal from another Observable — better for event-driven teardown
- **`take(n)`**: Stops after N values regardless of their content
- **`filter(predicate)`**: Skips non-matching values but keeps the stream alive
- **`skipWhile(predicate)`**: Symmetric — skips values while condition holds, then passes all through
- **`first(predicate)`**: Like `takeWhile(predicate, true).pipe(take(1))` — emits first match and stops

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/takeWhile](https://rxjs.dev/api/operators/takeWhile)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key teaching point**: takeWhile completes on first false — it does not filter. The inclusive flag and the lazy-flag anti-pattern (vs takeUntil) are the two critical distinctions.
