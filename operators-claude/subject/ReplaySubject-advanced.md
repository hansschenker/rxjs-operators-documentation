# ReplaySubject — Advanced Patterns

For `ReplaySubject` fundamentals see the core [ReplaySubject](./ReplaySubject) doc. This page covers event sourcing, late subscriber catch-up, windowed buffers, and `ReplaySubject` vs `BehaviorSubject` vs `shareReplay`.

---

## What Makes `ReplaySubject` Unique

`ReplaySubject(N)` remembers the last N values and replays them to every new subscriber:

```typescript
import { ReplaySubject } from 'rxjs';

const replay$ = new ReplaySubject<string>(3); // buffer 3 values

replay$.next('a');
replay$.next('b');
replay$.next('c');
replay$.next('d'); // 'a' evicted, buffer is now ['b','c','d']

// Late subscriber immediately gets buffered history:
replay$.subscribe(v => console.log(v)); // 'b', 'c', 'd' — then live
```

---

## Pattern 1: Event Log / Audit Trail

```typescript
import { ReplaySubject } from 'rxjs';
import { scan, map } from 'rxjs/operators';

interface AuditEvent {
  timestamp: Date;
  userId:    string;
  action:    string;
  resource:  string;
}

@Injectable({ providedIn: 'root' })
export class AuditLogService {
  // Keep last 100 audit events:
  private events$ = new ReplaySubject<AuditEvent>(100);

  readonly log$ = this.events$.asObservable();

  // Late subscribers (e.g., audit panel opened) get full history:
  record(event: Omit<AuditEvent, 'timestamp'>): void {
    this.events$.next({ ...event, timestamp: new Date() });
  }
}

// Component mounts after 10 events already recorded:
auditLogService.log$.subscribe(event => appendToAuditTable(event));
// Gets all 10 historical events, then live updates
```

---

## Pattern 2: Time-Windowed Replay

`ReplaySubject(N, windowTime)` evicts values older than `windowTime` ms:

```typescript
import { ReplaySubject } from 'rxjs';

// Keep last 5 minutes of events, max 1000:
const recentEvents$ = new ReplaySubject<SensorReading>(1000, 5 * 60 * 1000);

// A component that mounts 3 minutes later gets 2 minutes of history:
recentEvents$.subscribe(reading => {
  // Gets all events from the last 5 minutes that haven't expired
});
```

---

## Pattern 3: Event Sourcing — Rebuild State from Events

```typescript
import { ReplaySubject } from 'rxjs';
import { scan, shareReplay } from 'rxjs/operators';

type CartEvent =
  | { type: 'ADD';    item: CartItem }
  | { type: 'REMOVE'; itemId: string }
  | { type: 'CLEAR' };

@Injectable({ providedIn: 'root' })
export class CartEventStore {
  // Unbounded replay — complete event history:
  private events$ = new ReplaySubject<CartEvent>();

  // State is derived by replaying all events through the reducer:
  readonly cart$ = this.events$.pipe(
    scan((cart: CartItem[], event) => {
      switch (event.type) {
        case 'ADD':    return [...cart, event.item];
        case 'REMOVE': return cart.filter(i => i.id !== event.itemId);
        case 'CLEAR':  return [];
      }
    }, []),
    shareReplay(1)
  );

  readonly total$ = this.cart$.pipe(
    map(items => items.reduce((sum, i) => sum + i.price * i.qty, 0))
  );

  dispatch(event: CartEvent): void { this.events$.next(event); }

  // Replay all events to rebuild state (e.g., after page reload from persisted events):
  replayFromHistory(events: CartEvent[]): void {
    events.forEach(e => this.events$.next(e));
  }
}
```

---

## Pattern 4: Catch-Up Subscription (Late Joiners)

```typescript
import { ReplaySubject, Subject } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class CollaborativeSessionService {
  // New participants get last 50 operations to catch up:
  private operations$ = new ReplaySubject<Operation>(50);

  readonly stream$ = this.operations$.asObservable();

  // Called when a collaborator joins mid-session:
  joinSession(participantId: string): void {
    this.operations$.subscribe(op => {
      applyOperation(participantId, op);
    });
    // They immediately receive the 50 most recent operations
    // and then stay subscribed for new ones
  }

  broadcast(op: Operation): void {
    this.operations$.next(op);
  }
}
```

---

## Pattern 5: Multi-Step Wizard State

Replay previous step answers to any step:

```typescript
import { ReplaySubject } from 'rxjs';
import { scan, map } from 'rxjs/operators';

interface WizardStep { step: number; data: unknown; }

@Injectable()
export class WizardService {
  // Replay all step completions — any step can see prior answers:
  private steps$ = new ReplaySubject<WizardStep>();

  readonly completedSteps$ = this.steps$.pipe(
    scan((acc, step) => ({ ...acc, [step.step]: step.data }), {} as Record<number, unknown>),
    shareReplay(1)
  );

  // Step 4 can access step 1 answer:
  getStepData(step: number): Observable<unknown> {
    return this.completedSteps$.pipe(
      map(all => all[step]),
      filter(v => v !== undefined),
      take(1)
    );
  }

  completeStep(step: number, data: unknown): void {
    this.steps$.next({ step, data });
  }
}
```

---

## Pattern 6: Connection Initialization Messages

Send configuration/handshake on subscribe, then stream live data:

```typescript
import { ReplaySubject, merge } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ProtocolService {
  // Replay init messages to late subscribers (e.g., reconnecting clients):
  private initMessages$ = new ReplaySubject<InitMessage>(10);
  private liveMessages$ = new Subject<LiveMessage>();

  readonly messages$ = merge(
    this.initMessages$,   // replayed to every subscriber
    this.liveMessages$    // live only
  );

  addInitMessage(msg: InitMessage): void {
    this.initMessages$.next(msg);
  }

  broadcast(msg: LiveMessage): void {
    this.liveMessages$.next(msg);
  }
}
// Reconnecting client gets all 10 init messages instantly,
// then receives live messages going forward
```

---

## `ReplaySubject` vs `BehaviorSubject` vs `shareReplay(1)`

```typescript
// BehaviorSubject(initial) — exactly 1 buffered value, requires initial:
const bs$ = new BehaviorSubject(0);
// ✓ Always has a value (.getValue())
// ✓ Synchronous initial value
// ✗ Only buffers 1 value
// ✗ Requires initial value at construction

// ReplaySubject(1) — exactly 1 buffered value, no initial required:
const rs1$ = new ReplaySubject<number>(1);
// ✓ No initial value needed — late subscribers wait until first emission
// ✓ Buffers 1 value exactly like BehaviorSubject
// ✗ No .getValue() — async only

// ReplaySubject(N) — buffers N values:
const rsN$ = new ReplaySubject<number>(10);
// ✓ Historical replay of last N values
// ✓ Late subscribers get history
// ✗ No sync read

// shareReplay(N) — multicasts an Observable with N-buffer:
source$.pipe(shareReplay(1))
// ✓ Wraps any Observable (not just Subject)
// ✗ Can't push values imperatively (it's not a Subject)
```

**Decision guide**:
| Need | Use |
|---|---|
| Current value always available synchronously | `BehaviorSubject` |
| State with no obvious initial value | `ReplaySubject(1)` |
| Last N values for late subscribers | `ReplaySubject(N)` |
| Time-bounded recent history | `ReplaySubject(N, windowMs)` |
| Cache an HTTP response / computed stream | `shareReplay(1)` |

---

## Common Pitfalls

### Unbounded Buffer Without `complete()`

```typescript
// ❌ Infinite buffer, never completed — memory leak:
const events$ = new ReplaySubject<Event>(); // no buffer limit!
events$.next(bigEvent); // accumulates forever
// 10,000 events × 1KB each = 10MB held in memory

// ✅ Set a reasonable buffer limit:
const events$ = new ReplaySubject<Event>(100);           // last 100
const events$ = new ReplaySubject<Event>(1000, 60_000);  // last 1000 within 1 minute
```

### Exposing `ReplaySubject` Directly

```typescript
// ❌ Callers can push values into the ReplaySubject:
@Injectable()
class EventStore {
  readonly events$ = new ReplaySubject<Event>(50); // exposed — anyone can .next()
}

// ✅ Expose as Observable:
@Injectable()
class EventStore {
  private _events$ = new ReplaySubject<Event>(50);
  readonly events$ = this._events$.asObservable();
  record(e: Event) { this._events$.next(e); }
}
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `ReplaySubject` is the right tool whenever late subscribers need history — audit logs, event sourcing, collaborative catch-up, wizard steps. Use `ReplaySubject(1)` as a `BehaviorSubject` without an initial value requirement. Always set a buffer limit to prevent memory growth.
