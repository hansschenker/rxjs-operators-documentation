# generate / using — Advanced Patterns

For fundamentals see the core docs for [generate](./generate) and [using](./using). This page covers `generate` for mathematical sequences and state machines, `using` for resource lifecycle management, and comparisons with async generators and `defer`.

---

## `generate` — Synchronous Sequence Factory

```typescript
import { generate } from 'rxjs';

// generate(initialState, condition, iterate, resultSelector?)
// Synchronous equivalent of a for-loop as an Observable

// Basic: equivalent to for(let i = 0; i < 5; i++)
generate({
  initialState: 0,
  condition:    i => i < 5,
  iterate:      i => i + 1
}).subscribe(console.log); // 0, 1, 2, 3, 4

// With result selector — transform state to value:
generate({
  initialState:   1,
  condition:      n => n <= 1000,
  iterate:        n => n * 2,
  resultSelector: n => `2^${Math.log2(n)} = ${n}`
}).subscribe(console.log);
// '2^0 = 1', '2^1 = 2', '2^2 = 4', ... '2^9 = 512'
```

**When `generate` beats alternatives**:
- State carries more than a simple counter (object state with multiple fields)
- The iteration step is complex or conditional
- You want synchronous lazy generation without creating a generator function

---

## Pattern 1: Mathematical Sequences with Object State

```typescript
import { generate } from 'rxjs';
import { takeWhile, map } from 'rxjs/operators';

// Fibonacci with object state (no external variables):
generate({
  initialState:   { a: 0, b: 1, n: 0 },
  condition:      s => s.a < 1_000_000,
  iterate:        s => ({ a: s.b, b: s.a + s.b, n: s.n + 1 }),
  resultSelector: s => ({ value: s.a, index: s.n })
}).subscribe(({ value, index }) => console.log(`F(${index}) = ${value}`));
// F(0) = 0, F(1) = 1, F(2) = 1, F(3) = 2, ..., F(19) = 4181 (< 1M)

// Amortization schedule — loan payment breakdown:
interface AmortizationState {
  month:     number;
  balance:   number;
  payment:   number;
  rate:      number;
}

function amortizationSchedule$(
  principal: number,
  annualRate: number,
  months: number
): Observable<{ month: number; payment: number; principal: number; interest: number; balance: number }> {
  const monthlyRate = annualRate / 12;
  const payment = principal * monthlyRate / (1 - Math.pow(1 + monthlyRate, -months));

  return generate({
    initialState:   { month: 1, balance: principal, payment, rate: monthlyRate },
    condition:      s => s.month <= months && s.balance > 0.01,
    iterate:        s => ({
      ...s,
      month:   s.month + 1,
      balance: Math.max(0, s.balance * (1 + s.rate) - s.payment)
    }),
    resultSelector: s => {
      const interest  = s.balance * s.rate;
      const principal = s.payment - interest;
      return {
        month:     s.month,
        payment:   Math.round(s.payment * 100) / 100,
        principal: Math.round(principal * 100) / 100,
        interest:  Math.round(interest * 100) / 100,
        balance:   Math.round(s.balance * 100) / 100
      };
    }
  });
}

amortizationSchedule$(300_000, 0.05, 360).pipe(
  toArray()
).subscribe(schedule => renderAmortizationTable(schedule));
```

---

## Pattern 2: `generate` for State Machine Transitions

Enumerate all reachable states from an initial state:

```typescript
import { generate } from 'rxjs';

interface TrafficLightState {
  color:    'red' | 'yellow' | 'green';
  duration: number; // ms
  cycle:    number;
}

const trafficLightCycle$ = generate({
  initialState: { color: 'red' as const, duration: 3000, cycle: 0 },
  condition:    s => s.cycle < 12, // 4 full cycles
  iterate:      s => {
    const next = s.color === 'red'    ? 'green'  :
                 s.color === 'green'  ? 'yellow' : 'red';
    const dur  = next === 'red'    ? 3000 :
                 next === 'green'  ? 2500 : 500;
    return { color: next, duration: dur, cycle: s.cycle + 1 };
  }
});

// Emit each state for its duration using concatMap:
trafficLightCycle$.pipe(
  concatMap(state => timer(state.duration).pipe(map(() => state)))
).subscribe(state => updateTrafficLight(state.color));

// Grid/board game position enumeration:
generate({
  initialState:   { row: 0, col: 0 },
  condition:      s => s.row < 8,
  iterate:        s => s.col < 7
    ? { row: s.row, col: s.col + 1 }
    : { row: s.row + 1, col: 0 },
  resultSelector: s => ({ row: s.row, col: s.col, label: `${String.fromCharCode(65 + s.col)}${s.row + 1}` })
}).subscribe(cell => renderChessCell(cell));
// A1, B1, C1, ... H1, A2, B2, ... H8
```

---

## `using` — Resource Lifecycle Management

```typescript
import { using, interval, Observable } from 'rxjs';

// using(resourceFactory, observableFactory)
// Creates a resource, creates an Observable that uses it,
// and GUARANTEES the resource is disposed when the Observable unsubscribes

// Basic pattern:
using(
  () => ({ connection: db.connect(), unsubscribe() { this.connection.close(); } }),
  resource => from(resource.connection.query('SELECT * FROM users'))
).subscribe({
  next:     users => render(users),
  error:    err   => handleError(err),
  complete: ()    => console.log('Done') // connection already closed
});
// connection.close() called whether Observable errors, completes, or is unsubscribed
```

---

## Pattern 3: `using` for WebSocket Lifecycle

```typescript
import { using, Subject, Observable, fromEvent } from 'rxjs';
import { map, filter, takeUntil } from 'rxjs/operators';

interface WebSocketResource {
  socket:      WebSocket;
  unsubscribe: () => void;
}

function managedWebSocket$(url: string): Observable<MessageEvent> {
  return using(
    (): WebSocketResource => {
      const socket = new WebSocket(url);
      return {
        socket,
        unsubscribe: () => socket.close()
      };
    },
    resource => new Observable<MessageEvent>(subscriber => {
      const { socket } = resource;

      const onMessage = (e: MessageEvent) => subscriber.next(e);
      const onError   = (e: Event)        => subscriber.error(e);
      const onClose   = ()                => subscriber.complete();

      socket.addEventListener('message', onMessage);
      socket.addEventListener('error',   onError);
      socket.addEventListener('close',   onClose);

      return () => {
        socket.removeEventListener('message', onMessage);
        socket.removeEventListener('error',   onError);
        socket.removeEventListener('close',   onClose);
      };
    })
  );
}

// WebSocket closes automatically when unsubscribed:
const subscription = managedWebSocket$('wss://api.example.com/stream').pipe(
  map(e => JSON.parse(e.data)),
  filter(msg => msg.type === 'update'),
  takeUntilDestroyed()
).subscribe(handleUpdate);
// On unsubscribe: WebSocket.close() is called — no leaked connections
```

---

## Pattern 4: `using` for File Handles and Locks

```typescript
import { using, from } from 'rxjs';
import { mergeMap, toArray } from 'rxjs/operators';
import * as fs from 'fs';

// File handle lifecycle — file always closed after processing:
function processFile$(filePath: string): Observable<string[]> {
  return using(
    () => {
      const handle = fs.openSync(filePath, 'r');
      return {
        handle,
        unsubscribe: () => fs.closeSync(handle)
      };
    },
    resource => {
      const stream = fs.createReadStream(filePath, {
        fd:       resource.handle,
        encoding: 'utf8'
      });
      return fromNodeReadable$(stream).pipe(
        mergeMap(chunk => from((chunk as string).split('\n'))),
        filter(line => line.trim().length > 0)
      );
    }
  );
}

// Database transaction lifecycle:
function withTransaction$<T>(
  db: Database,
  work: (tx: Transaction) => Observable<T>
): Observable<T> {
  return using(
    () => {
      const tx = db.beginTransaction();
      return {
        tx,
        unsubscribe: () => tx.rollback() // rollback on unsubscribe/error
      };
    },
    resource => work(resource.tx).pipe(
      tap({ complete: () => resource.tx.commit() })
    )
  );
}

withTransaction$(db, tx =>
  from([order1, order2, order3]).pipe(
    concatMap(order => tx.insert('orders', order))
  )
).subscribe({
  complete: () => console.log('All orders committed'),
  error:    () => console.log('Transaction rolled back')
});
```

---

## `generate` vs `from(generator())` vs `range`

```typescript
// range(start, count) — simple integer sequences:
range(1, 10).subscribe(console.log); // 1–10
// Best for: simple integer iteration

// generate — stateful iteration with condition and transform:
generate({ initialState: 1, condition: n => n <= 100, iterate: n => n * 2 })
// Best for: non-linear sequences, object state, conditional termination

// from(generator()) — async or sync generators, maximum flexibility:
function* powers(base: number) { let n = 1; while (n < Infinity) { yield n; n *= base; } }
from(powers(2)).pipe(take(10))
// Best for: infinite sequences with take(), complex logic, reusable generator functions

// Decision:
// Simple integers          → range()
// Stateful, synchronous    → generate()
// Complex/async/reusable   → from(generator()) or from(asyncGenerator())
```

---

## `using` vs `defer` vs `finalize`

```typescript
// finalize — run cleanup when Observable terminates (complete or error or unsubscribe):
source$.pipe(
  finalize(() => cleanup())
)
// Good for: cleanup that doesn't affect what Observable is returned
// Not good for: cleanup of a resource created at subscription time

// defer — create Observable lazily at subscription time:
defer(() => {
  const resource = acquireResource();
  return from(resource.getData()).pipe(
    finalize(() => resource.release())
  );
})
// Good for: most resource lifecycle cases (simpler than using)

// using — create resource AND Observable together, guaranteed cleanup:
using(
  () => acquireResource(),
  resource => from(resource.getData())
)
// resource.unsubscribe() called automatically
// Good for: when the resource and observable factory are cleanly separable
// Equivalent to defer + finalize but with a cleaner API contract
```

---

**Cognitive Load**: 3/5 (generate), 3/5 (using) | **Usage Frequency**: 1/5 each | **Composability**: 3/5 each
**Key insight**: `generate` is a synchronous loop disguised as an Observable — reach for it when you have object state that evolves through discrete steps (amortization, state machines, board positions) and `range` isn't expressive enough. `using` is the resource-lifecycle operator: it guarantees `resource.unsubscribe()` is called when the Observable terminates for any reason. In practice, `defer` + `finalize` covers most `using` use cases with less ceremony, but `using` wins when the resource and Observable factory are cleanly separable.
