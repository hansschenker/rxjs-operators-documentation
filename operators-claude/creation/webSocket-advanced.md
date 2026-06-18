# webSocket — Advanced Patterns

For `webSocket` fundamentals see the core [webSocket](./webSocket) doc. This page covers reconnection with exponential backoff, multiplexing, heartbeat/ping, and backpressure.

---

## The `WebSocketSubject` API

RxJS's `webSocket()` returns a `WebSocketSubject<T>` — it's both an Observable (incoming messages) and an Observer (send messages via `.next()`):

```typescript
import { webSocket } from 'rxjs/webSocket';

const ws$ = webSocket<ServerMessage>('wss://api.example.com/ws');

// Send:
ws$.next({ type: 'subscribe', channel: 'prices' });

// Receive:
ws$.subscribe(msg => handleMessage(msg));

// Close:
ws$.complete();
```

---

## Pattern 1: Auto-Reconnect with Exponential Backoff

```typescript
import { webSocket } from 'rxjs/webSocket';
import { Subject, timer, throwError } from 'rxjs';
import { retryWhen, delayWhen, tap, scan } from 'rxjs/operators';

function createReconnectingWS<T>(
  url: string,
  maxRetries = 10
): Observable<T> {
  return webSocket<T>(url).pipe(
    retryWhen(errors =>
      errors.pipe(
        scan((attempt, err) => {
          if (attempt >= maxRetries) throw err; // give up after maxRetries
          return attempt + 1;
        }, 0),
        delayWhen(attempt => {
          const delay = Math.min(1000 * Math.pow(2, attempt), 30_000); // cap at 30s
          const jitter = Math.random() * 1000;
          console.log(`WS reconnect attempt ${attempt}, waiting ${delay + jitter}ms`);
          return timer(delay + jitter);
        })
      )
    )
  );
}

// Usage:
const stream$ = createReconnectingWS<MarketData>('wss://market.example.com/feed');
stream$.subscribe(renderTicker);
```

---

## Pattern 2: Reconnect with State Notification

Expose connection state to the UI:

```typescript
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { BehaviorSubject, Subject, timer } from 'rxjs';
import { tap, catchError, switchMap, retryWhen, delayWhen, scan, share } from 'rxjs/operators';

type ConnectionState = 'connecting' | 'connected' | 'disconnected' | 'reconnecting';

@Injectable({ providedIn: 'root' })
export class ReconnectingWebSocketService<T> {
  private ws$: WebSocketSubject<T> | null = null;
  readonly connectionState$ = new BehaviorSubject<ConnectionState>('disconnected');

  connect(url: string): Observable<T> {
    this.ws$ = webSocket<T>({
      url,
      openObserver:  { next: () => this.connectionState$.next('connected') },
      closeObserver: { next: () => this.connectionState$.next('disconnected') }
    });

    this.connectionState$.next('connecting');

    return this.ws$.pipe(
      retryWhen(errors =>
        errors.pipe(
          tap(() => this.connectionState$.next('reconnecting')),
          scan((n, _) => n + 1, 0),
          delayWhen(n => timer(Math.min(1000 * 2 ** n, 30_000)))
        )
      ),
      share()
    );
  }

  send(msg: T): void { this.ws$?.next(msg); }
  disconnect(): void { this.ws$?.complete(); }
}
```

---

## Pattern 3: Channel Multiplexing with `multiplex`

The `multiplex` method subscribes to a specific channel and automatically unsubscribes:

```typescript
import { webSocket } from 'rxjs/webSocket';

const ws$ = webSocket('wss://api.example.com/ws');

// Subscribe to a specific channel:
function subscribeToChannel<T>(channel: string): Observable<T> {
  return ws$.multiplex(
    () => ({ type: 'subscribe',   channel }),    // sent on subscribe
    () => ({ type: 'unsubscribe', channel }),    // sent on unsubscribe
    msg => (msg as any).channel === channel      // filter messages for this channel
  ) as Observable<T>;
}

// Three independent channel subscriptions — one WebSocket connection:
const prices$  = subscribeToChannel<PriceUpdate>('prices');
const news$    = subscribeToChannel<NewsItem>('news');
const alerts$  = subscribeToChannel<Alert>('alerts');

prices$.subscribe(renderPrices);
news$.subscribe(appendNews);
alerts$.subscribe(showAlert);
// On last unsubscribe from a channel, sends { type: 'unsubscribe', channel }
```

---

## Pattern 4: Request-Response Over WebSocket

Match responses to their requests by correlation ID:

```typescript
import { Subject, filter, take, timeout } from 'rxjs';
import { map } from 'rxjs/operators';

interface WsRequest  { id: string; type: string; payload: unknown; }
interface WsResponse { id: string; result?: unknown; error?: string; }

@Injectable({ providedIn: 'root' })
export class WsRpcService {
  private ws$ = webSocket<WsRequest | WsResponse>('wss://api.example.com/rpc');

  call<T>(type: string, payload: unknown): Observable<T> {
    const id = crypto.randomUUID();

    return new Observable<T>(observer => {
      // Subscribe to matching response:
      const sub = this.ws$.pipe(
        filter((msg): msg is WsResponse => 'result' in msg && msg.id === id),
        take(1),
        timeout(10_000),
        map(msg => {
          if (msg.error) throw new Error(msg.error);
          return msg.result as T;
        })
      ).subscribe(observer);

      // Send request:
      this.ws$.next({ id, type, payload });

      return () => sub.unsubscribe();
    });
  }
}

// Usage:
this.wsRpc.call<User[]>('users.list', { active: true }).subscribe(render);
```

---

## Pattern 5: Heartbeat / Ping-Pong

Keep connection alive and detect silent disconnects:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { timer, merge, EMPTY } from 'rxjs';
import { switchMap, tap, timeout, catchError } from 'rxjs/operators';

function withHeartbeat<T>(
  ws$: Observable<T>,
  send: (msg: unknown) => void,
  intervalMs = 30_000,
  timeoutMs  = 10_000
): Observable<T> {
  // Send ping every intervalMs, expect pong within timeoutMs:
  const heartbeat$ = timer(intervalMs, intervalMs).pipe(
    tap(() => send({ type: 'ping' })),
    switchMap(() =>
      ws$.pipe(
        filter((msg: any) => msg.type === 'pong'),
        take(1),
        timeout(timeoutMs),
        catchError(() => {
          throw new Error('Heartbeat timeout — connection dead');
        })
      )
    )
  );

  return merge(
    ws$.pipe(filter((msg: any) => msg.type !== 'pong')), // pass non-pong messages
    heartbeat$.pipe(switchMap(() => EMPTY))               // side-effect only
  );
}
```

---

## Pattern 6: Backpressure — Buffer Outgoing Messages

Buffer messages when the socket isn't ready:

```typescript
import { Subject, BehaviorSubject } from 'rxjs';
import { filter, concatMap, withLatestFrom } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class BufferedWebSocketService<T> {
  private ws$      = webSocket<T>('wss://api.example.com/ws');
  private outbox$  = new Subject<T>();
  private connected$ = new BehaviorSubject(false);

  constructor() {
    // Track connection:
    this.ws$.subscribe({
      error:    () => this.connected$.next(false),
      complete: () => this.connected$.next(false)
    });
    this.connected$.next(true);

    // Flush outbox when connected:
    this.outbox$.pipe(
      concatMap(msg =>
        this.connected$.pipe(
          filter(Boolean),       // wait until connected
          take(1),
          tap(() => this.ws$.next(msg))
        )
      )
    ).subscribe();
  }

  send(msg: T): void { this.outbox$.next(msg); }
  messages$   = this.ws$.asObservable();
}
```

---

## Pattern 7: Typed Message Routing

Route different message types to separate streams:

```typescript
import { Subject } from 'rxjs';
import { filter, map, share } from 'rxjs/operators';

type ServerEvent =
  | { type: 'PRICE_UPDATE'; symbol: string; price: number }
  | { type: 'ORDER_FILLED'; orderId: string; price: number }
  | { type: 'ERROR'; code: number; message: string };

@Injectable({ providedIn: 'root' })
export class TypedWebSocketService {
  private ws$ = webSocket<ServerEvent>('wss://trading.example.com/ws').pipe(
    share()
  );

  readonly prices$ = this.ws$.pipe(
    filter((e): e is Extract<ServerEvent, { type: 'PRICE_UPDATE' }> =>
      e.type === 'PRICE_UPDATE'
    )
  );

  readonly orders$ = this.ws$.pipe(
    filter((e): e is Extract<ServerEvent, { type: 'ORDER_FILLED' }> =>
      e.type === 'ORDER_FILLED'
    )
  );

  readonly errors$ = this.ws$.pipe(
    filter((e): e is Extract<ServerEvent, { type: 'ERROR' }> =>
      e.type === 'ERROR'
    )
  );
}
```

---

## Common Pitfalls

### Subscribing Multiple Times Creates Multiple Connections

```typescript
// ❌ Each subscribe opens a new WebSocket connection:
const ws$ = webSocket('wss://api.example.com/ws');
ws$.subscribe(handlePrices);   // connection 1
ws$.subscribe(handleNews);     // connection 2!

// ✅ Share one connection:
const ws$ = webSocket('wss://api.example.com/ws').pipe(share());
ws$.subscribe(handlePrices);   // connection 1
ws$.subscribe(handleNews);     // joins connection 1
```

### Not Handling Reconnect After `complete()`

```typescript
// ❌ calling .complete() closes permanently — cannot reuse:
ws$.complete(); // closed
ws$.next({ type: 'ping' }); // no-op — already completed

// ✅ Create a fresh subject to reconnect:
let ws$ = webSocket(url);
function reconnect() { ws$ = webSocket(url); }
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Production checklist**: Always add `retryWhen`/`retry` with exponential backoff. Use `share()` so multiple subscribers don't open multiple connections. Use `multiplex()` for channel-based subscriptions. Add a heartbeat for long-idle connections. Expose `connectionState$` so the UI can show "Reconnecting…".
