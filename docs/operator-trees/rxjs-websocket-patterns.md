# WebSocket Patterns with RxJS

Production-grade WebSocket management — typed message protocols, reconnection with exponential backoff, multiplexing, presence tracking, and binary message handling.

---

## Foundation: `webSocket()` from RxJS

```typescript
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';

const ws$ = webSocket<ServerMessage>('wss://api.example.com/ws');

// ws$ is both Observable (incoming) and Observer (outgoing):
ws$.subscribe(msg => console.log('received:', msg));
ws$.next({ type: 'ping' }); // send message
ws$.complete();             // close connection
```

`WebSocketSubject` is a `Subject` — subscribe to receive messages, call `next()` to send.

---

## Pattern 1: Typed Message Protocol

Define a discriminated union for all message types:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { filter, map, share } from 'rxjs/operators';

// All messages the server can send:
type ServerMessage =
  | { type: 'chat';      roomId: string; userId: string; text: string; ts: number }
  | { type: 'presence';  userId: string; status: 'online' | 'offline' }
  | { type: 'error';     code: number;   message: string }
  | { type: 'pong' }
  | { type: 'ack';       messageId: string };

// All messages we can send:
type ClientMessage =
  | { type: 'join';    roomId: string }
  | { type: 'leave';   roomId: string }
  | { type: 'message'; roomId: string; text: string; id: string }
  | { type: 'ping' };

class ChatSocket {
  private ws$ = webSocket<ServerMessage | ClientMessage>('wss://chat.example.com/ws');

  // Typed selector helpers:
  chat$     = (this.ws$ as Observable<ServerMessage>).pipe(
    filter((m): m is Extract<ServerMessage, { type: 'chat' }> => m.type === 'chat'),
    share()
  );

  presence$ = (this.ws$ as Observable<ServerMessage>).pipe(
    filter((m): m is Extract<ServerMessage, { type: 'presence' }> => m.type === 'presence'),
    share()
  );

  errors$   = (this.ws$ as Observable<ServerMessage>).pipe(
    filter((m): m is Extract<ServerMessage, { type: 'error' }> => m.type === 'error'),
    share()
  );

  send(msg: ClientMessage): void {
    this.ws$.next(msg);
  }

  joinRoom(roomId: string): void {
    this.send({ type: 'join', roomId });
  }
}
```

---

## Pattern 2: Auto-Reconnect with Exponential Backoff

`webSocket()` closes its internal Subject on disconnect — use `defer` + `retry` to reconnect:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { defer, timer, Subject } from 'rxjs';
import { retry, share, tap } from 'rxjs/operators';

function persistentWebSocket<T>(url: string): Observable<T> {
  return defer(() => {
    const ws$ = webSocket<T>({
      url,
      openObserver: {
        next: () => console.log('WebSocket connected')
      },
      closeObserver: {
        next: event => console.log('WebSocket closed', event.code)
      }
    });
    return ws$;
  }).pipe(
    retry({
      count: Infinity, // retry forever
      delay: (error, retryCount) => {
        const backoff = Math.min(30_000, 1000 * Math.pow(2, retryCount - 1));
        console.log(`Reconnecting in ${backoff}ms (attempt ${retryCount})...`);
        return timer(backoff);
      },
      resetOnSuccess: true // reset retry count on successful connection
    }),
    share() // single shared connection for multiple subscribers
  );
}

// Usage:
const messages$ = persistentWebSocket<ServerMessage>('wss://api.example.com/ws');

// Connection status indicator:
const connectionState$ = new BehaviorSubject<'connecting' | 'connected' | 'disconnected'>('connecting');

defer(() => {
  connectionState$.next('connecting');
  return webSocket({ url: 'wss://api.example.com/ws',
    openObserver:  { next: () => connectionState$.next('connected') },
    closeObserver: { next: () => connectionState$.next('disconnected') }
  });
}).pipe(
  retry({ delay: (_, n) => timer(Math.min(30_000, 1000 * 2 ** n)) }),
  share()
).subscribe();
```

---

## Pattern 3: Multiplexed Topics (Channels)

Subscribe to specific channels without creating multiple connections:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { filter, map, finalize } from 'rxjs/operators';

interface ChannelEnvelope {
  channel: string;
  payload: unknown;
}

class MultiplexedSocket {
  private ws$ = webSocket<ChannelEnvelope>({
    url: 'wss://pubsub.example.com/ws',
    serializer:   msg => JSON.stringify(msg),
    deserializer: event => JSON.parse(event.data)
  });

  channel$<T>(channelId: string): Observable<T> {
    return this.ws$.multiplex(
      // Subscribe message sent when first subscriber arrives:
      () => ({ action: 'subscribe', channel: channelId }),
      // Unsubscribe message sent when last subscriber leaves:
      () => ({ action: 'unsubscribe', channel: channelId }),
      // Filter messages for this channel:
      msg => msg.channel === channelId
    ).pipe(
      map(msg => msg.payload as T)
    );
  }
}

// Multiple consumers, one WebSocket connection:
const socket = new MultiplexedSocket();

socket.channel$<PriceUpdate>('prices:AAPL').pipe(
  takeUntilDestroyed()
).subscribe(price => updateChart(price));

socket.channel$<NewsItem>('news:tech').pipe(
  takeUntilDestroyed()
).subscribe(news => renderNewsFeed(news));
// Both use the same underlying WebSocket
```

---

## Pattern 4: Request-Response Over WebSocket

Simulate HTTP request-response semantics over a persistent WebSocket:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { filter, map, take, timeout } from 'rxjs/operators';

interface RpcRequest  { id: string; method: string; params: unknown; }
interface RpcResponse { id: string; result?: unknown; error?: { code: number; message: string }; }

class WebSocketRPC {
  private ws$ = webSocket<RpcRequest | RpcResponse>('wss://rpc.example.com/ws');
  private responses$ = (this.ws$ as Observable<RpcResponse>).pipe(
    filter(msg => 'result' in msg || 'error' in msg),
    share()
  );

  call<T>(method: string, params: unknown, timeoutMs = 10_000): Observable<T> {
    const id = crypto.randomUUID();

    return new Observable<T>(subscriber => {
      // Listen for this request's response:
      const sub = this.responses$.pipe(
        filter(msg => msg.id === id),
        take(1),
        timeout({ each: timeoutMs, with: () => throwError(() => new Error(`RPC timeout: ${method}`)) }),
        map(msg => {
          if (msg.error) throw new Error(`RPC error ${msg.error.code}: ${msg.error.message}`);
          return msg.result as T;
        })
      ).subscribe(subscriber);

      // Send the request:
      (this.ws$ as WebSocketSubject<RpcRequest>).next({ id, method, params });

      return () => sub.unsubscribe();
    });
  }
}

const rpc = new WebSocketRPC();

// Type-safe RPC calls:
rpc.call<User[]>('users.list', { page: 1 }).subscribe(users => renderUsers(users));
rpc.call<void>('room.join', { roomId: '42' }).subscribe();
```

---

## Pattern 5: Presence System

Track which users are online using a WebSocket heartbeat + timeout:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { timer, merge, Subject } from 'rxjs';
import { switchMap, map, scan, distinctUntilChanged } from 'rxjs/operators';

interface PresenceEvent {
  userId:  string;
  status:  'online' | 'offline';
  lastSeen: number;
}

interface PresenceState { [userId: string]: { status: 'online' | 'offline'; lastSeen: number } }

function createPresenceTracker(ws$: Observable<PresenceEvent>): Observable<PresenceState> {
  // Every 30s, emit a sweep to mark timed-out users offline:
  const sweep$ = timer(0, 30_000).pipe(
    map(() => ({ type: 'sweep', now: Date.now() }))
  );

  return merge(ws$, sweep$).pipe(
    scan((state: PresenceState, event) => {
      if ('type' in event) {
        // Sweep: mark users who haven't been seen in 60s as offline:
        const updated: PresenceState = {};
        for (const [userId, info] of Object.entries(state)) {
          updated[userId] = event.now - info.lastSeen > 60_000
            ? { ...info, status: 'offline' }
            : info;
        }
        return updated;
      }

      return {
        ...state,
        [event.userId]: { status: event.status, lastSeen: event.lastSeen }
      };
    }, {} as PresenceState),
    distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b))
  );
}

const ws$ = persistentWebSocket<PresenceEvent>('wss://presence.example.com/ws');
const presence$ = createPresenceTracker(ws$);

presence$.pipe(
  map(state => Object.values(state).filter(u => u.status === 'online').length),
  distinctUntilChanged()
).subscribe(count => updateOnlineCount(count));
```

---

## Pattern 6: Binary Message Handling

Send and receive binary data (ArrayBuffer, Blob) over WebSocket:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { filter, map } from 'rxjs/operators';

// Custom serializer/deserializer for binary protocol:
const binaryWs$ = webSocket<ArrayBuffer>({
  url:          'wss://binary.example.com/ws',
  binaryType:   'arraybuffer',
  serializer:   (msg: ArrayBuffer) => msg,
  deserializer: (event: MessageEvent) => event.data as ArrayBuffer
});

// Protocol: first byte = message type, rest = payload
const TYPES = { SENSOR_DATA: 0x01, COMMAND: 0x02, ACK: 0x03 } as const;

function parseMessage(buffer: ArrayBuffer): { type: number; payload: DataView } {
  const view = new DataView(buffer);
  return { type: view.getUint8(0), payload: new DataView(buffer, 1) };
}

function buildCommand(commandId: number, params: number[]): ArrayBuffer {
  const buffer = new ArrayBuffer(2 + params.length);
  const view   = new DataView(buffer);
  view.setUint8(0, TYPES.COMMAND);
  view.setUint8(1, commandId);
  params.forEach((p, i) => view.setUint8(2 + i, p));
  return buffer;
}

// Receive sensor readings:
binaryWs$.pipe(
  map(parseMessage),
  filter(msg => msg.type === TYPES.SENSOR_DATA),
  map(msg => ({
    temperature: msg.payload.getFloat32(0),
    humidity:    msg.payload.getFloat32(4),
    timestamp:   msg.payload.getUint32(8)
  })),
  takeUntilDestroyed()
).subscribe(reading => updateSensorDashboard(reading));

// Send command:
binaryWs$.next(buildCommand(0x10, [255, 0, 0])); // set LED to red
```

---

## Pattern 7: Connection Quality Monitoring

Track round-trip latency and connection health with ping/pong:

```typescript
import { interval, timer } from 'rxjs';
import { switchMap, timeout, catchError, map, scan } from 'rxjs/operators';

interface PingResult { latencyMs: number; success: boolean; }

function monitorConnection$(
  ws$:            WebSocketSubject<unknown>,
  pingInterval =  10_000,
  pingTimeout  =   5_000
): Observable<PingResult> {
  return interval(pingInterval).pipe(
    switchMap(() => {
      const sentAt = Date.now();
      ws$.next({ type: 'ping' });

      return ws$.pipe(
        filter((msg: any) => msg.type === 'pong'),
        take(1),
        map(() => ({ latencyMs: Date.now() - sentAt, success: true })),
        timeout({
          each: pingTimeout,
          with: () => of({ latencyMs: pingTimeout, success: false })
        })
      );
    })
  );
}

// Rolling average latency + disconnect detection:
monitorConnection$(ws$).pipe(
  scan((acc, result) => {
    const history = [...acc.history.slice(-9), result]; // keep last 10
    const avgLatency = history.reduce((s, r) => s + r.latencyMs, 0) / history.length;
    const failureRate = history.filter(r => !r.success).length / history.length;
    return { history, avgLatency, failureRate };
  }, { history: [] as PingResult[], avgLatency: 0, failureRate: 0 }),
  takeUntilDestroyed()
).subscribe(stats => {
  latencyDisplay.textContent = `${Math.round(stats.avgLatency)}ms`;
  if (stats.failureRate > 0.5) showConnectionWarning();
});
```

---

## Common Pitfalls

### Creating a New `webSocket()` on Each Subscription

```typescript
// ❌ New WebSocket connection per subscriber — 3 subscribers = 3 connections:
get messages$(): Observable<ServerMessage> {
  return webSocket<ServerMessage>('wss://api.example.com/ws'); // created each time!
}

// ✅ Create once, share:
private ws$ = webSocket<ServerMessage>('wss://api.example.com/ws').pipe(share());
// Or use a class property — one Subject, many observers
```

### Not Handling the Close Subject

```typescript
// ❌ webSocket Subject completes when connection closes — no more messages:
ws$.subscribe({
  next: msg => process(msg),
  complete: () => console.log('done') // fires on every disconnect!
});
// After disconnect, new subscribe() won't reconnect automatically

// ✅ Use defer() + retry() for auto-reconnect (Pattern 2 above):
defer(() => webSocket(url)).pipe(
  retry({ delay: (_, n) => timer(1000 * 2 ** n) })
).subscribe(process);
```

### Sending Before Connection Opens

```typescript
// ❌ next() before WebSocket is OPEN drops the message silently:
const ws$ = webSocket('wss://api.example.com/ws');
ws$.next({ type: 'auth', token }); // may be lost if not yet connected

// ✅ Use openObserver to wait for open, or buffer with ReplaySubject:
const pending$ = new ReplaySubject<ClientMessage>();

defer(() => {
  const ws = webSocket<ServerMessage>({ url, openObserver: { next: () => {
    pending$.subscribe(msg => ws.next(msg)); // drain buffer on open
  }}});
  return ws;
}).pipe(retry({ delay: (_, n) => timer(1000 * 2 ** n) })).subscribe();
```
