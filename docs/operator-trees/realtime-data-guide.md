# Real-Time Data with RxJS

RxJS patterns for WebSockets, Server-Sent Events, polling, and other streaming data sources.

---

## Choosing a Real-Time Strategy

```
Do you need to SEND data to the server?
├─ Yes → WebSocket (bidirectional)
└─ No
   ├─ Does the server push data continuously?
   │   ├─ Yes → Server-Sent Events (SSE)
   │   └─ No → Polling (timer + switchMap)
   └─ Does data change less than once per 5s?
       ├─ Yes → Long polling or polling
       └─ No → WebSocket or SSE
```

---

## WebSocket with `webSocket()`

RxJS ships `webSocket()` in `rxjs/webSocket` — it wraps the browser WebSocket API.

### Basic Setup

```typescript
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { retry, filter, map } from 'rxjs/operators';
import { timer } from 'rxjs';

interface ServerMessage {
  type: string;
  payload: unknown;
}

const ws$ = webSocket<ServerMessage>({
  url:              'wss://api.example.com/ws',
  openObserver:     { next: () => console.log('WS connected') },
  closeObserver:    { next: () => console.log('WS closed') },
});

// Subscribe to messages:
ws$.pipe(
  filter(msg => msg.type === 'update'),
  map(msg  => msg.payload as Update)
).subscribe(handleUpdate);

// Send a message:
ws$.next({ type: 'subscribe', payload: { channel: 'prices' } });
```

### Auto-Reconnect

```typescript
import { webSocket } from 'rxjs/webSocket';
import { retry, share } from 'rxjs/operators';
import { timer } from 'rxjs';

function createReconnectingSocket<T>(url: string) {
  return webSocket<T>(url).pipe(
    retry({
      delay: (_, attempt) =>
        timer(Math.min(1000 * Math.pow(2, attempt), 30_000)),
      // 1s, 2s, 4s, 8s, 16s, 30s, 30s...
    }),
    share() // share the connection across multiple subscribers
  );
}

const prices$ = createReconnectingSocket<PriceUpdate>('wss://prices.example.com/ws');
```

### Multiplexing (One Socket, Multiple Channels)

```typescript
import { webSocket } from 'rxjs/webSocket';

const ws$ = webSocket<{ type: string; channel: string; data: unknown }>('wss://api.example.com');

// multiplex creates a channel that subscribes/unsubscribes via protocol messages:
const btcPrices$ = ws$.multiplex(
  () => ({ type: 'subscribe',   channel: 'BTC-USD' }), // send on subscribe
  () => ({ type: 'unsubscribe', channel: 'BTC-USD' }), // send on unsubscribe
  msg => msg.channel === 'BTC-USD'                     // filter for this channel
);

const ethPrices$ = ws$.multiplex(
  () => ({ type: 'subscribe',   channel: 'ETH-USD' }),
  () => ({ type: 'unsubscribe', channel: 'ETH-USD' }),
  msg => msg.channel === 'ETH-USD'
);

// Each subscription sends the subscribe message; unsubscribe sends the unsub message
btcPrices$.subscribe(update => updateBtcChart(update));
ethPrices$.subscribe(update => updateEthChart(update));
```

---

## Server-Sent Events (SSE)

Use `fromEvent` or a custom Observable wrapper:

```typescript
import { Observable, fromEvent, NEVER } from 'rxjs';
import { map, share, retry, switchMap } from 'rxjs/operators';
import { timer } from 'rxjs';

function fromSSE<T>(url: string): Observable<T> {
  return new Observable<T>(subscriber => {
    const source = new EventSource(url);

    source.onmessage = (event) => {
      try {
        subscriber.next(JSON.parse(event.data) as T);
      } catch {
        subscriber.next(event.data as unknown as T);
      }
    };

    source.onerror = () => {
      subscriber.error(new Error('SSE connection error'));
    };

    return () => source.close(); // cleanup on unsubscribe
  });
}

// Usage with reconnect:
const updates$ = fromSSE<StatusUpdate>('/api/events').pipe(
  retry({ delay: (_, n) => timer(1000 * n) }),
  share()
);

updates$.subscribe(update => renderDashboard(update));
```

### Named SSE Events

```typescript
function fromSSEEvent<T>(url: string, eventName: string): Observable<T> {
  return new Observable<T>(subscriber => {
    const source = new EventSource(url);

    const handler = (event: MessageEvent) => {
      subscriber.next(JSON.parse(event.data) as T);
    };

    source.addEventListener(eventName, handler);
    source.onerror = () => subscriber.error(new Error('SSE error'));

    return () => {
      source.removeEventListener(eventName, handler);
      source.close();
    };
  });
}

// Separate streams for different event types on one SSE connection:
const orders$   = fromSSEEvent<Order>('/api/events', 'order-created');
const payments$ = fromSSEEvent<Payment>('/api/events', 'payment-confirmed');
```

---

## Polling Patterns

### Fixed-Interval Polling

```typescript
import { timer, switchMap, share } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

// Poll every 10 seconds, starting immediately:
const status$ = timer(0, 10_000).pipe(
  switchMap(() => this.api.getStatus()),
  distinctUntilChanged((a, b) => a.version === b.version), // only emit on change
  share()
);
```

### Adaptive Polling (Back Off When Quiet)

```typescript
import { BehaviorSubject, switchMap, exhaustMap } from 'rxjs';
import { scan, distinctUntilChanged } from 'rxjs/operators';

// Poll faster when there's activity, slower when quiet:
let lastChangeVersion = 0;
let interval = 5000;

timer(0, interval).pipe(
  exhaustMap(() => this.api.getStatus()),
  scan((prev, curr) => {
    if (curr.version !== prev?.version) {
      interval = 2000; // activity — poll faster
    } else {
      interval = Math.min(interval * 1.5, 30_000); // quiet — back off
    }
    return curr;
  }, null as Status | null),
  distinctUntilChanged((a, b) => a?.version === b?.version)
).subscribe(renderStatus);
```

### Long Polling

```typescript
import { defer, EMPTY } from 'rxjs';
import { expand, tap, delay } from 'rxjs/operators';

// Long poll: each request waits for a response, then immediately polls again:
function longPoll<T>(url: string): Observable<T> {
  return defer(() => this.http.get<T>(url)).pipe(
    expand(() => this.http.get<T>(url).pipe(delay(100))),
    // delay(100) prevents hammering if server responds immediately
  );
}

longPoll<Notification[]>('/api/notifications/poll')
  .pipe(takeUntilDestroyed(this.destroyRef))
  .subscribe(notifications => this.inbox$.next(notifications));
```

---

## Pattern: Live Dashboard

Combine WebSocket real-time data with initial HTTP fetch:

```typescript
import { merge, of } from 'rxjs';
import { switchMap, startWith, scan, share } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private ws$ = webSocket<DashboardEvent>('wss://api.example.com/dashboard').pipe(
    retry({ delay: (_, n) => timer(Math.min(1000 * 2 ** n, 30_000)) }),
    share()
  );

  // Load current state, then apply incremental updates:
  readonly state$: Observable<DashboardState> = this.http.get<DashboardState>('/api/dashboard').pipe(
    switchMap(initial =>
      this.ws$.pipe(
        scan((state, event) => applyEvent(state, event), initial),
        startWith(initial)
      )
    ),
    share()
  );
}
```

---

## Pattern: Presence / Online Status

Track which users are online using a WebSocket heartbeat:

```typescript
interface PresenceMessage {
  type: 'join' | 'leave' | 'ping';
  userId: string;
}

const presence$ = ws$.pipe(
  filter((msg): msg is PresenceMessage => ['join','leave','ping'].includes(msg.type)),
  scan((online: Set<string>, msg) => {
    const next = new Set(online);
    if (msg.type === 'join' || msg.type === 'ping') next.add(msg.userId);
    if (msg.type === 'leave') next.delete(msg.userId);
    return next;
  }, new Set<string>()),
  distinctUntilChanged((a, b) => a.size === b.size && [...a].every(id => b.has(id)))
);
```

---

## Pattern: Optimistic Updates + Server Confirmation

```typescript
import { Subject, merge } from 'rxjs';
import { scan, mergeMap, map } from 'rxjs/operators';

const optimisticUpdates$ = new Subject<Item>();
const confirmedUpdates$  = ws$.pipe(filter(m => m.type === 'item-updated'));

// Apply optimistic immediately; confirmed updates overwrite on arrival:
const items$ = merge(
  optimisticUpdates$.pipe(map(item => ({ ...item, pending: true }))),
  confirmedUpdates$.pipe( map(item => ({ ...item, pending: false })))
).pipe(
  scan((map, item) => new Map([...map, [item.id, item]]), new Map<string, Item>()),
  map(m => [...m.values()])
);

// When user performs action:
function updateItem(item: Item) {
  optimisticUpdates$.next(item); // show change immediately
  ws$.next({ type: 'update-item', payload: item }); // send to server
}
```

---

## Common Pitfalls

### `webSocket()` Subject vs Observable

```typescript
// ❌ Calling complete() on the WebSocketSubject closes the connection FOR ALL SUBSCRIBERS
const ws$ = webSocket('wss://...');
ws$.subscribe(subscriber1);
ws$.subscribe(subscriber2);
ws$.complete(); // closes the socket — both subscribers stop receiving!

// ✅ Use share() + individual unsubscribe for independent consumers:
const shared$ = webSocket('wss://...').pipe(share());
const sub1 = shared$.subscribe(subscriber1);
const sub2 = shared$.subscribe(subscriber2);
sub1.unsubscribe(); // only subscriber1 unsubscribes, socket stays open
```

### Missing `share()` Creates Multiple Connections

```typescript
// ❌ Two socket connections created — each subscriber gets its own!
const ws$ = webSocket<Event>('wss://api.example.com');
ws$.subscribe(renderChart);
ws$.subscribe(updateTable);

// ✅ share() ensures one connection, multiple consumers:
const ws$ = webSocket<Event>('wss://api.example.com').pipe(share());
ws$.subscribe(renderChart); // same connection
ws$.subscribe(updateTable); // same connection
```

### Polling Without `exhaustMap` or `switchMap`

```typescript
// ❌ OVERLAP — if the request takes > interval, multiple in-flight requests pile up
timer(0, 5000).pipe(
  mergeMap(() => this.api.getStatus()) // concurrent requests!
).subscribe(render);

// ✅ exhaustMap ignores ticks while a request is in flight:
timer(0, 5000).pipe(
  exhaustMap(() => this.api.getStatus())
).subscribe(render);

// ✅ switchMap cancels in-flight request on next tick:
timer(0, 5000).pipe(
  switchMap(() => this.api.getStatus())
).subscribe(render);
```
