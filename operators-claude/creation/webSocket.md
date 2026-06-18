# webSocket

## Identity

- **Name**: webSocket
- **Category**: Creation Operators (Interop)
- **Type**: WebSocket adapter — creates a bidirectional Observable/Subject over a WebSocket connection
- **Import**:
  ```typescript
  import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
  ```
- **Signature**:
  ```typescript
  function webSocket<T>(
    urlConfigOrSource: string | WebSocketSubjectConfig<T>
  ): WebSocketSubject<T>

  interface WebSocketSubjectConfig<T> {
    url: string;
    protocol?: string | string[];
    deserializer?: (e: MessageEvent) => T;
    serializer?: (value: T) => WebSocketMessage;
    openObserver?: NextObserver<Event>;
    closeObserver?: NextObserver<CloseEvent>;
    closingObserver?: NextObserver<void>;
    binaryType?: 'blob' | 'arraybuffer';
  }
  ```

## Functional Specification

**Concept**: `webSocket()` returns a `WebSocketSubject<T>` — an object that is simultaneously an Observable (for receiving messages) and an Observer (for sending messages via `.next()`). The WebSocket connection is opened lazily on first subscription and closed when all subscribers unsubscribe.

**`WebSocketSubject<T>` dual role**:
- **As Observable**: Subscribe to receive incoming messages (deserialized as `T`)
- **As Observer**: Call `.next(value)` to send a message to the server
- **As Subject**: Multicast — multiple subscribers share one WebSocket connection

**`multiplex(subMsg, unsubMsg, filter)`**: Creates a virtual sub-channel over the same WebSocket. Sends `subMsg` on subscribe, `unsubMsg` on unsubscribe, and filters incoming messages with `filter`. Enables multiple logical channels over a single connection.

**Connection lifecycle**:
- Opens on first `subscribe()`
- Stays open while at least one subscriber is active
- Closes when all subscribers unsubscribe (sends close frame)
- On error: WebSocket closes; subscribers receive error notification

## Marble Diagram

```
webSocket('wss://api.example.com'):

subscribe() → WebSocket opens
server sends: --msgA--msgB--msgC--...
Result:        --msgA--msgB--msgC--...  (continuous stream)

ws$.next(payload) → sends payload to server (does not appear in stream)

unsubscribe() → WebSocket closes (if last subscriber)

Error / server closes:
Result:  --msgA--msgB--#  (error notification; can retry with retry())
```

## Type System Integration

```typescript
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';

interface IncomingMessage { type: string; payload: unknown }
interface OutgoingMessage { action: string; data: unknown }

// WebSocketSubject<T> — T is the incoming message type
const ws$: WebSocketSubject<IncomingMessage> = webSocket<IncomingMessage>(
  'wss://api.example.com/stream'
);

// Subscribe to receive
ws$.subscribe((msg: IncomingMessage) => console.log(msg.type));

// Send messages
ws$.next({ action: 'subscribe', data: { channel: 'prices' } } as any);
```

## Examples

### Basic Usage
```typescript
import { webSocket } from 'rxjs/webSocket';

interface Quote { symbol: string; price: number }

const quotes$ = webSocket<Quote>('wss://stream.example.com/quotes');

quotes$.subscribe({
  next:     q => console.log(`${q.symbol}: ${q.price}`),
  error:    e => console.error('WebSocket error:', e),
  complete: () => console.log('WebSocket closed')
});

// Send a message to the server
quotes$.next({ symbol: 'AAPL', price: 0 }); // sends JSON to server
```

### Common Pattern — Auto-Reconnect With `retry`
```typescript
import { webSocket } from 'rxjs/webSocket';
import { retry, share } from 'rxjs/operators';
import { timer } from 'rxjs';

interface Message { type: string; data: unknown }

// Reconnect on disconnect with exponential backoff
const ws$ = webSocket<Message>('wss://api.example.com/stream').pipe(
  retry({
    delay: (_, retryCount) => timer(Math.min(retryCount * 1000, 30_000))
  }),
  share() // one connection shared across multiple subscribers
);

ws$.subscribe(handleMessage);
```

### Common Pattern — `multiplex` for Logical Channels
```typescript
import { webSocket } from 'rxjs/webSocket';

interface WsMessage { channel: string; data: unknown }

const ws$ = webSocket<WsMessage>('wss://api.example.com/ws');

// Subscribe to 'prices' channel — multiplexed over one WebSocket
const prices$ = ws$.multiplex(
  () => ({ action: 'subscribe',   channel: 'prices' }),  // sent on subscribe
  () => ({ action: 'unsubscribe', channel: 'prices' }),  // sent on unsubscribe
  msg => msg.channel === 'prices'                         // filter incoming
);

// Subscribe to 'news' channel — same WebSocket connection
const news$ = ws$.multiplex(
  () => ({ action: 'subscribe',   channel: 'news' }),
  () => ({ action: 'unsubscribe', channel: 'news' }),
  msg => msg.channel === 'news'
);

prices$.subscribe(updatePriceDisplay);
news$.subscribe(updateNewsFeed);
// One WebSocket connection; two independent logical streams
```

### Common Pattern — Custom Serializer/Deserializer
```typescript
import { webSocket } from 'rxjs/webSocket';

// Binary WebSocket with custom deserializer
const binaryWs$ = webSocket<ArrayBuffer>({
  url: 'wss://api.example.com/binary',
  binaryType: 'arraybuffer',
  deserializer: (e: MessageEvent<ArrayBuffer>) => e.data,
  serializer: (value: ArrayBuffer) => value
});

// Custom JSON with envelope
interface Envelope<T> { id: string; type: string; payload: T }
const rpc$ = webSocket<Envelope<unknown>>({
  url: 'wss://api.example.com/rpc',
  deserializer: e => JSON.parse(e.data) as Envelope<unknown>,
  serializer: v => JSON.stringify(v)
});
```

## Common Pitfalls

### Anti-pattern: Re-creating `WebSocketSubject` on Each Subscribe
```typescript
import { webSocket } from 'rxjs/webSocket';

// ❌ NEW CONNECTION per call — each component creates its own WebSocket
function getMarketData(): Observable<Quote> {
  return webSocket<Quote>('wss://stream.example.com/quotes'); // new WS each time!
}
componentA.subscribe(getMarketData()); // WS connection 1
componentB.subscribe(getMarketData()); // WS connection 2

// ✅ CORRECT — create once, share across subscribers
const marketData$ = webSocket<Quote>('wss://stream.example.com/quotes').pipe(
  share()
);
componentA.subscribe(marketData$); // one connection
componentB.subscribe(marketData$); // same connection

// WHY: webSocket() creates a new WebSocketSubject each call. Without sharing,
// each subscriber (or component) opens its own WebSocket connection.
// Create the subject once at module/service level and use share() or
// shareReplay() to distribute to multiple consumers.
```

### Anti-pattern: No Error Handling on WebSocket Disconnect
```typescript
import { webSocket } from 'rxjs/webSocket';

// ❌ STREAM DIES on any disconnect — error propagates and subscription ends
webSocket('wss://api.example.com/stream').subscribe({
  next: handleMessage,
  error: e => console.error('disconnected:', e)
  // After error: stream is dead; no automatic reconnect
});

// ✅ CORRECT — retry for automatic reconnect
import { retry } from 'rxjs/operators';
webSocket('wss://api.example.com/stream').pipe(
  retry({ delay: 3000 }) // reconnect after 3s
).subscribe({
  next:  handleMessage,
  error: e => console.error('permanently failed:', e)
});

// WHY: Network interruptions are normal for WebSockets. Without retry(),
// any disconnect permanently kills the subscription. Always add retry()
// with a delay for production WebSocket streams.
```

## Related Operators

- **`ajax`**: HTTP (XHR) equivalent — one-shot request/response
- **`fromEvent`**: DOM/Node event listener — unidirectional (receive only)
- **`share`**: Required companion when multiple components subscribe to the same WebSocket
- **`retry`**: Essential for auto-reconnect on disconnect
- **`filter`**: Route messages by type (alternative to `multiplex` for simple cases)

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/webSocket/webSocket](https://rxjs.dev/api/webSocket/webSocket)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key teaching points**:
1. `WebSocketSubject` is both Observable (receive) and Observer (send via `.next()`)
2. Lazy connection — opens on first subscribe, closes when all unsubscribe
3. Always `share()` at service level; always `retry()` for auto-reconnect
4. `multiplex()` for multiple logical channels over one physical connection
