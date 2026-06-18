# ReplaySubject

## Identity
- **Name**: ReplaySubject
- **Category**: Subject / Notification
- **Type**: Buffered multicast Subject — replays the last `bufferSize` emissions to each new subscriber
- **Import**:
  ```typescript
  import { ReplaySubject } from 'rxjs';
  ```
- **Signature** (class):
  ```typescript
  class ReplaySubject<T> extends Subject<T> {
    constructor(
      bufferSize?: number,    // how many past values to replay (default: Infinity)
      windowTime?: number,    // max age of buffered values in ms (default: Infinity)
      scheduler?: SchedulerLike
    )
    // Inherits: next(), error(), complete(), subscribe(), asObservable()
  }
  ```

## Functional Specification

**Concept**: A `Subject` that maintains an internal circular buffer of the last `bufferSize` emissions. Each new subscriber immediately receives all buffered values (synchronously) before receiving live future emissions.

**How it differs from `Subject` and `BehaviorSubject`**:

| Type | Stored | New subscriber receives |
|------|--------|------------------------|
| `Subject` | Nothing | Only future `next()` calls |
| `BehaviorSubject(v)` | Current value (1) | Current value immediately, then future |
| `ReplaySubject(N)` | Last N values | Last N values immediately, then future |
| `ReplaySubject(1)` | Last value (1) | Last value immediately, then future |

**`ReplaySubject(1)` vs `BehaviorSubject`**:
- `BehaviorSubject(initial)`: requires an initial value; always has a value from the start
- `ReplaySubject(1)`: no initial value; new subscribers receive nothing until first `next()` is called

**Mathematical representation**:
```
ReplaySubject(N) maintains buffer = circular queue of capacity N

On next(v):   buffer.push(v) (evicting oldest if full); emit v to all current subscribers
On subscribe: replay all buffer values synchronously; then add subscriber to live list
On windowTime: values older than windowTime are evicted from buffer on subscribe
```

**Invariants**:
- **Synchronous replay**: All buffered values delivered synchronously on subscription
- **Buffer is shared**: The same buffer is replayed to all new subscribers
- **Oldest evicted first**: When buffer is full, the oldest value is dropped on each `next()`
- **windowTime ages values**: Even if buffer has capacity, values older than `windowTime` are not replayed

## Marble Diagram

```
ReplaySubject(2) — bufferSize = 2:

rs.next('a')   rs.next('b')   rs.next('c')
    |              |              |
----a--------------b--------------c-------

Sub A at t=0:  a--------------b--------------c---
Sub B at t=1 (after a, b): [a][b]------------c---
                             ↑↑ replay a and b synchronously
Sub C at t=2 (after c):    [b][c]---
                             ↑↑ replay b and c (buffer holds last 2)
```

**`ReplaySubject(1)` as a lazy `BehaviorSubject`**:
```
No emissions yet:

Sub A at t=0:  (nothing until next() is called)

BehaviorSubject(0):
Sub A at t=0:  0  (initial value emitted immediately)
```

## Examples

### Basic Usage
```typescript
import { ReplaySubject } from 'rxjs';

const rs$ = new ReplaySubject<number>(3); // buffer last 3

rs$.next(1);
rs$.next(2);
rs$.next(3);
rs$.next(4); // evicts 1 from buffer

// Late subscriber gets buffer: [2, 3, 4]
rs$.subscribe(v => console.log('late:', v));
// Output: late: 2, late: 3, late: 4  (synchronous)

rs$.next(5); // late: 5
```

### Common Pattern — Event Log / History
```typescript
import { ReplaySubject } from 'rxjs';

interface UserAction { type: string; timestamp: number; payload: unknown; }

// Keep last 50 user actions for undo/replay
const actionHistory$ = new ReplaySubject<UserAction>(50);

// New component subscribes and immediately gets last 50 actions to rebuild state
actionHistory$.subscribe(action => applyAction(action));
```

### Common Pattern — `ReplaySubject(1)` as Optional BehaviorSubject
```typescript
import { ReplaySubject } from 'rxjs';

// When there is no sensible initial value, ReplaySubject(1) avoids forcing a default
class ConfigService {
  private config$ = new ReplaySubject<Config>(1);
  readonly config = this.config$.asObservable();

  loadConfig(): void {
    fetch('/api/config')
      .then(r => r.json())
      .then((c: Config) => this.config$.next(c));
  }
}

// Subscribers before loadConfig() completes wait; after, they get the cached config.
// BehaviorSubject would require a fake initial value (null, {}, undefined).
```

### Common Pattern — `windowTime` for Time-Bounded Replay
```typescript
import { ReplaySubject } from 'rxjs';

// Cache sensor readings for the last 5 seconds
const sensor$ = new ReplaySubject<number>(100, 5000); // 100 capacity, 5000ms window

// New subscriber gets all readings from the past 5 seconds
// Readings older than 5s are evicted regardless of buffer capacity
```

## Common Pitfalls

### Anti-pattern: `ReplaySubject(Infinity)` on Unbounded Streams
```typescript
import { ReplaySubject } from 'rxjs';

// ❌ MEMORY LEAK — default bufferSize is Infinity
const events$ = new ReplaySubject<Event>(); // no bufferSize!

// After 1 million events, all are held in memory forever
// New subscribers replay all 1 million events synchronously — UI freeze!

// ✅ CORRECT — always specify a bufferSize
const events$ = new ReplaySubject<Event>(100); // keep last 100
// Or use windowTime:
const events$ = new ReplaySubject<Event>(Infinity, 30_000); // last 30 seconds only
```

### Anti-pattern: Using `ReplaySubject(1)` When `BehaviorSubject` Is More Appropriate
```typescript
import { ReplaySubject, BehaviorSubject } from 'rxjs';

// ❌ WRONG TOOL — using ReplaySubject(1) for state that always has a default
const theme$ = new ReplaySubject<'light' | 'dark'>(1);

// Before first next(): components that subscribe see nothing
// User might see a flash of unstyled content

// ✅ CORRECT — BehaviorSubject when a default value makes sense
const theme$ = new BehaviorSubject<'light' | 'dark'>('light');
// Components always get a theme immediately

// Use ReplaySubject(1) only when "no value yet" is a valid state
// and subscribers should wait for the first real value.
```

## Related Types

- **`Subject`**: No buffer — new subscribers get only future values
- **`BehaviorSubject(v)`**: Buffer of 1, with required initial value
- **`AsyncSubject`**: Emits only the final value on completion
- **`shareReplay(n)`**: Operator equivalent — adds replay to any Observable without exposing a Subject interface

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/class/ReplaySubject](https://rxjs.dev/api/index/class/ReplaySubject)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: Buffered Multicast Observable
**Cognitive Load**: 3/5 — The bufferSize/windowTime parameters and the ReplaySubject(1) vs BehaviorSubject distinction are the key teaching points
**Usage Frequency**: 4/5 — Common for event history, late-subscriber patterns, and "optional initial value" state
**Common with**: `asObservable()`, `scan`, `shareReplay`, `takeUntil`
