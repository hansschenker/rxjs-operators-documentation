# AsyncSubject

## Identity

- **Name**: AsyncSubject
- **Category**: Subject / Notification
- **Type**: Completion-gated emitter — emits only the final value, and only when the source completes
- **Import**:
  ```typescript
  import { AsyncSubject } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  class AsyncSubject<T> extends Subject<T> {
    // Inherits: next(), error(), complete(), subscribe(), asObservable()
    // Stores: the last value passed to next() — emitted to all subscribers on complete()
  }
  ```

## Functional Specification

**Concept**: An `AsyncSubject` buffers only the most recent value passed to `next()`. When `complete()` is called, it emits that single buffered value to all current and future subscribers, then completes.

**Behavior rules**:
- Before `complete()`: subscribers receive nothing (no replay of buffered value)
- After `complete()`: every new subscriber immediately receives the single buffered value + completion
- If `next()` is never called before `complete()`: subscribers receive only completion (no value)
- If `error()` is called: all subscribers receive the error; the buffered value is discarded

**Subject Family Comparison**:

| Type | Stored | Emits | When |
|------|--------|-------|------|
| `Subject` | Nothing | All future `next()` values | As they arrive |
| `BehaviorSubject(v)` | Current value | Current value + future | On subscribe + each next() |
| `ReplaySubject(N)` | Last N values | Last N values + future | On subscribe (replay) + each next() |
| `AsyncSubject` | Last value | Last value only | On `complete()` |

## Marble Diagram

```
AsyncSubject:

as.next('a')   as.next('b')   as.next('c')   as.complete()
     |              |              |               |
-----a--------------b--------------c-----------complete

Sub A subscribes at t=0:                          c|
                                     (receives 'c' only, on complete)

Sub B subscribes at t=2 (after complete()):       c|
                                     (immediately receives 'c' + complete)

as.next('x'), as.error(new Error()):
                                     #
                                     (no value emitted, error forwarded)
```

**No `complete()` called**:
```
as.next('a'), as.next('b'), as.next('c')  (no complete)

Sub A at t=0:  (nothing — stream is open, waiting for complete)
               The buffered 'c' will only be emitted when complete() fires.
```

## Type System Integration

```typescript
import { AsyncSubject } from 'rxjs';

const subject = new AsyncSubject<number>();

subject.subscribe(v => console.log('A:', v));

subject.next(1);
subject.next(2);
subject.next(3);

subject.subscribe(v => console.log('B:', v));

subject.complete();
// Output:
// A: 3    ← subscriber A gets 3 on complete()
// B: 3    ← subscriber B gets 3 on complete()

// Late subscriber after complete
subject.subscribe(v => console.log('C:', v));
// Output:
// C: 3    ← immediately receives 3 + completion
```

## Examples

### Basic Usage
```typescript
import { AsyncSubject } from 'rxjs';

const subject = new AsyncSubject<string>();

subject.subscribe({ next: v => console.log('Sub 1:', v), complete: () => console.log('Sub 1 done') });

subject.next('first');
subject.next('second');
subject.next('last');

subject.subscribe({ next: v => console.log('Sub 2:', v), complete: () => console.log('Sub 2 done') });

subject.complete();
// Output:
// Sub 1: last
// Sub 1 done
// Sub 2: last
// Sub 2 done
```

### Common Pattern — Cache a Single Async Result
```typescript
import { AsyncSubject } from 'rxjs';
import { ajax } from 'rxjs/ajax';

class ConfigService {
  private result$ = new AsyncSubject<Config>();
  private loaded = false;

  getConfig(): Observable<Config> {
    if (!this.loaded) {
      this.loaded = true;
      ajax.getJSON<Config>('/api/config').subscribe(this.result$);
    }
    return this.result$.asObservable();
    // Before load: subscribers wait silently
    // After load: subscribers immediately receive the single config value
    // All subsequent subscribers get the cached value instantly (like shareReplay(1))
  }
}

const configService = new ConfigService();
configService.getConfig().subscribe(console.log); // waits
configService.getConfig().subscribe(console.log); // same instance — reuses cached value
```

### Common Pattern — Wrap a Promise With Final-Value Semantics
```typescript
import { AsyncSubject } from 'rxjs';

// AsyncSubject naturally mirrors Promise semantics:
// - Emits exactly once (the final/resolved value)
// - All subscribers get the same value
// - Late subscribers (after completion) get it immediately

function toAsyncSubject<T>(promise: Promise<T>): AsyncSubject<T> {
  const subject = new AsyncSubject<T>();
  promise.then(
    value => { subject.next(value); subject.complete(); },
    error => subject.error(error)
  );
  return subject;
}

const result$ = toAsyncSubject(fetch('/api/data').then(r => r.json()));
result$.subscribe(data => console.log('got data:', data));
```

### Edge Case — `complete()` Without `next()`
```typescript
import { AsyncSubject } from 'rxjs';

const subject = new AsyncSubject<number>();

subject.subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('complete')
});

subject.complete(); // no next() called
// Output:
// complete   ← no value emitted, just completion
```

## Common Pitfalls

### Anti-pattern: Expecting Values Before `complete()`
```typescript
import { AsyncSubject } from 'rxjs';

const subject = new AsyncSubject<string>();

subject.subscribe(v => console.log(v)); // ← subscriber waiting

subject.next('a'); // nothing logged yet
subject.next('b'); // nothing logged yet
subject.next('c'); // nothing logged yet

// ... if complete() is never called, nothing ever logs!
// This is the most common AsyncSubject bug.

// ✅ CORRECT — use BehaviorSubject or ReplaySubject if you need intermediate values
import { BehaviorSubject } from 'rxjs';
const bs = new BehaviorSubject<string>('');
bs.subscribe(v => console.log(v)); // logs immediately: ''
bs.next('a'); // logs: 'a'
bs.next('b'); // logs: 'b'

// WHY: AsyncSubject emits ONLY on completion. If you need live updates as
// values arrive, BehaviorSubject (for current state) or Subject (for events)
// are the correct choice.
```

### Anti-pattern: Using AsyncSubject as a General-Purpose Subject
```typescript
import { AsyncSubject } from 'rxjs';

// ❌ WRONG — using AsyncSubject for event bus (expects multiple live emissions)
const click$ = new AsyncSubject<MouseEvent>();
document.addEventListener('click', e => click$.next(e));
click$.subscribe(e => console.log('clicked:', e));
// Nothing logs until the page is "done" (which never happens)

// ✅ CORRECT — use Subject for event forwarding
import { Subject } from 'rxjs';
const click$ = new Subject<MouseEvent>();
document.addEventListener('click', e => click$.next(e));
click$.subscribe(e => console.log('clicked:', e)); // logs on every click

// WHY: AsyncSubject is specialized for one-shot "give me the final result"
// use cases — caching a completed async operation. For event forwarding or
// live state, use Subject or BehaviorSubject.
```

## Related Types

- **`Subject`**: No buffering — for event forwarding and multicasting without memory
- **`BehaviorSubject(v)`**: Always-current value with initial state — for reactive state
- **`ReplaySubject(N)`**: Last N values replayed to late subscribers — for history
- **`shareReplay(1)`**: Operator equivalent of a single-value cache — prefer over AsyncSubject for pipelines that don't need the Subject interface (next/error/complete control)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/class/AsyncSubject](https://rxjs.dev/api/index/class/AsyncSubject)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 2/5
**Key teaching point**: AsyncSubject emits ONLY on complete() — this is its defining characteristic. It's the reactive analogue of a Promise: one result, available to all subscribers at the moment of resolution.
**Teaching sequence**: Introduce after Subject, BehaviorSubject, and ReplaySubject as "the fourth Subject variant — Promise-style one-shot"
