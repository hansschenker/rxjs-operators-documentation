# skipUntil

**Category**: Filtering  
**Import**: `import { skipUntil } from 'rxjs';`

## Description

`skipUntil` ignores all values emitted by the source Observable until a second Observable (the `notifier`) emits its first value. From that point on, the output Observable mirrors the source Observable and passes through all subsequent values.

If the notifier completes without emitting any value, the source is never forwarded — all values remain suppressed. Errors from either Observable are forwarded immediately.

This is the complement of `takeUntil`: `takeUntil` passes values until the notifier fires; `skipUntil` suppresses values until the notifier fires.

## Signature

```typescript
function skipUntil<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| notifier | `ObservableInput<any>` | An Observable (or Promise, array, etc.) whose first emission unlocks the source stream. |

## Return Type

`MonoTypeOperatorFunction<T>` — skips source values until the notifier fires, then passes all subsequent values.

## Marble Diagram

```
Source:   --a--b--c--d--e--|
Notifier: ----------n------|
          skipUntil(notifier)
Output:   -----------d--e--|
```

## Examples

### Example 1: Delay an interval until the user clicks

```typescript
import { interval, fromEvent } from 'rxjs';
import { skipUntil } from 'rxjs';

const tick$ = interval(1000);
const click$ = fromEvent(document, 'click');

tick$.pipe(
  skipUntil(click$)
).subscribe(n => console.log('Tick after click:', n));

// Before click: nothing logged
// After first click: values start coming through
```

### Example 2: Start processing messages after initialization completes

```typescript
import { Subject, timer } from 'rxjs';
import { skipUntil } from 'rxjs';

const messages$ = new Subject<string>();
const ready$ = timer(2000); // simulates async initialization

messages$.pipe(
  skipUntil(ready$)
).subscribe(msg => console.log('Processed:', msg));

messages$.next('too early 1'); // dropped
messages$.next('too early 2'); // dropped

setTimeout(() => {
  messages$.next('just in time');   // may be dropped depending on timing
  messages$.next('after ready');    // processed
}, 2500);
```

### Example 3: Skip until a specific user action

```typescript
import { fromEvent } from 'rxjs';
import { skipUntil, map, filter } from 'rxjs';

const keydown$ = fromEvent<KeyboardEvent>(document, 'keydown');

// Start tracking keys only after the user presses Enter
const enterKey$ = keydown$.pipe(
  filter(ev => ev.key === 'Enter')
);

keydown$.pipe(
  skipUntil(enterKey$),
  map(ev => ev.key)
).subscribe(key => console.log('Key after Enter:', key));
```

## Common Pitfalls

- **Notifier completing without emitting**: If the notifier completes before emitting any value, no source values will ever be forwarded. Ensure the notifier will eventually emit, or handle this edge case explicitly.
- **Values emitted before notifier fires are permanently lost**: There is no buffering. Any source values emitted during the "skip" phase are gone. If you need them, consider `bufferToggle` or `replay` strategies.
- **Race conditions with synchronous sources**: If both the source and notifier are synchronous, the relative order of subscription matters. `skipUntil` subscribes to the notifier first, then the source.

## Related Operators

- `takeUntil` — the complement: passes values until the notifier fires
- `skipWhile` — skips based on a predicate over source values
- `skip` — skips a fixed number of initial values
- `filter` — suppresses values by predicate, without an unlock mechanism
