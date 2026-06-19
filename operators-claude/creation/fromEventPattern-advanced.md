# fromEventPattern — Advanced Patterns

> **Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 5/5
> **Teaching Sequence**: After `fromEvent` — bridges arbitrary subscription APIs that don't follow the DOM/Node.js event interface

---

## Advanced Behavioral Model

`fromEventPattern(addHandler, removeHandler?, resultSelector?)` is the universal event bridge. It handles the full lifecycle:

```
subscribe   → addHandler(handler)        → returns signal (stored internally)
emission    → handler(...args) called    → Observable emits
unsubscribe → removeHandler(handler, signal) → cleanup
```

The `signal` return value from `addHandler` is the critical feature: it lets you pass a subscription token (handle, ID, reference) to `removeHandler` — solving the problem of APIs that don't accept the listener function for removal.

```
addHandler: (handler) => {
  const token = api.subscribe(handler);
  return token;          ← stored as signal
}
removeHandler: (handler, signal) => {
  api.unsubscribe(signal); ← use token, not handler
}
```

---

## Type System Integration

```typescript
import { fromEventPattern } from 'rxjs';

// Without resultSelector: handler args determine type
// Single-arg handler → T
const click$: Observable<MouseEvent> = fromEventPattern<MouseEvent>(
  handler => document.addEventListener('click', handler),
  handler => document.removeEventListener('click', handler),
);

// Multi-arg handler without resultSelector → any[]
const multiArg$ = fromEventPattern(
  handler => emitter.on('data', handler),
  handler => emitter.off('data', handler),
); // Observable<any[]>

// Multi-arg handler with resultSelector → typed
interface DataEvent { id: string; payload: unknown }

const typed$: Observable<DataEvent> = fromEventPattern<DataEvent>(
  handler => emitter.on('data', handler),
  handler => emitter.off('data', handler),
  (id: string, payload: unknown): DataEvent => ({ id, payload }),
);

// Signal typing: addHandler return type → signal parameter type
const withSignal$ = fromEventPattern<string>(
  (handler): SubscriptionToken => api.subscribe('channel', handler),
  (handler, signal: SubscriptionToken) => api.unsubscribe(signal),
);
```

---

## Advanced Patterns

### 1. Firebase / Firestore Real-Time Listener

Firestore's `onSnapshot` returns an unsubscribe function, not a listener reference — the classic signal pattern.

```typescript
import { fromEventPattern } from 'rxjs';
import { map, shareReplay } from 'rxjs/operators';
import { collection, onSnapshot, QuerySnapshot } from 'firebase/firestore';

function collectionStream<T>(
  db: Firestore,
  path: string,
): Observable<T[]> {
  return fromEventPattern<QuerySnapshot>(
    handler => onSnapshot(collection(db, path), handler),
    (_, unsubscribe: () => void) => unsubscribe(), // signal = unsubscribe fn
  ).pipe(
    map(snapshot => snapshot.docs.map(d => ({ id: d.id, ...d.data() } as T))),
    shareReplay(1),
  );
}

const users$ = collectionStream<User>(db, 'users');
users$.subscribe(users => renderUserList(users));
// Unsubscribing calls Firestore's cleanup automatically
```

### 2. WebRTC DataChannel as Observable

WebRTC uses `addEventListener`/`removeEventListener` but the channel object requires specific teardown — wrap with `fromEventPattern` for full lifecycle control.

```typescript
import { fromEventPattern, merge } from 'rxjs';
import { map, takeUntil, filter } from 'rxjs/operators';

function dataChannelMessages$(channel: RTCDataChannel): Observable<string> {
  const message$ = fromEventPattern<MessageEvent>(
    handler => channel.addEventListener('message', handler),
    handler => channel.removeEventListener('message', handler),
    (event: MessageEvent) => event.data as string,
  );

  const close$ = fromEventPattern(
    handler => channel.addEventListener('close', handler),
    handler => channel.removeEventListener('close', handler),
  );

  return message$.pipe(
    takeUntil(close$),  // auto-complete when channel closes
  );
}

const peer = new RTCPeerConnection();
const channel = peer.createDataChannel('chat');

dataChannelMessages$(channel).subscribe(msg => displayMessage(msg));
```

### 3. ResizeObserver Bridge

`ResizeObserver` has a non-standard API: single callback for multiple entries, and `observe`/`unobserve`/`disconnect` for lifecycle.

```typescript
import { fromEventPattern, Observable } from 'rxjs';
import { map, share, filter } from 'rxjs/operators';

function elementResize$(element: Element): Observable<ResizeObserverEntry> {
  let observer: ResizeObserver;

  return fromEventPattern<ResizeObserverEntry[]>(
    handler => {
      observer = new ResizeObserver(handler);
      observer.observe(element);
      return observer; // signal = observer instance
    },
    (_, obs: ResizeObserver) => obs.disconnect(),
  ).pipe(
    map(entries => entries[0]), // ResizeObserver batches; take first
    filter((entry): entry is ResizeObserverEntry => entry !== undefined),
    share(),
  );
}

elementResize$(document.querySelector('#panel')!).pipe(
  map(entry => entry.contentRect.width),
  distinctUntilChanged(),
).subscribe(width => adjustLayout(width));
```

### 4. gRPC Streaming Calls

gRPC streaming returns a `ClientReadableStream` with `.on`/`.removeListener` and needs explicit `.cancel()` for cleanup.

```typescript
import { fromEventPattern, Observable } from 'rxjs';
import { takeUntil, Subject } from 'rxjs';

function grpcStream$<T>(
  createStream: () => ClientReadableStream<T>,
): Observable<T> {
  let stream: ClientReadableStream<T>;

  return fromEventPattern<T>(
    handler => {
      stream = createStream();
      stream.on('data', handler);
      return stream; // signal = stream instance
    },
    (handler, s: ClientReadableStream<T>) => {
      s.removeListener('data', handler);
      s.cancel(); // gRPC-specific cleanup
    },
  );
}

const stockUpdates$ = grpcStream$(() =>
  grpcClient.watchStocks({ symbols: ['AAPL', 'GOOG'] })
);

stockUpdates$.pipe(
  takeUntil(destroy$),
).subscribe(update => updateTicker(update));
// On unsubscribe: removeListener + cancel both called
```

### 5. Custom Pub/Sub System with Token-Based Unsubscription

Many custom event buses return subscription IDs rather than accepting the handler for removal.

```typescript
import { fromEventPattern, Observable } from 'rxjs';

interface EventBus {
  subscribe(event: string, handler: (data: unknown) => void): string; // returns ID
  unsubscribe(id: string): void;
}

function busEvent$<T>(bus: EventBus, eventName: string): Observable<T> {
  return fromEventPattern<T>(
    handler => bus.subscribe(eventName, handler as (data: unknown) => void),
    (_, id: string) => bus.unsubscribe(id), // signal = subscription ID
  );
}

const orderUpdates$ = busEvent$<OrderUpdate>(eventBus, 'order:updated');
orderUpdates$.subscribe(order => refreshOrderView(order));
```

### 6. Angular EventEmitter Bridge

Angular's `EventEmitter` extends `Subject` but older patterns or library emitters may use subscribe/unsubscribe differently.

```typescript
import { fromEventPattern } from 'rxjs';
import { EventEmitter } from '@angular/core';

function fromAngularEmitter<T>(emitter: EventEmitter<T>): Observable<T> {
  return fromEventPattern<T>(
    handler => {
      const sub = emitter.subscribe(handler);
      return sub; // signal = Subscription
    },
    (_, sub: Subscription) => sub.unsubscribe(),
  );
}

// Use when you need pipe() operators on an Angular EventEmitter
const throttledOutput$ = fromAngularEmitter(this.outputEmitter).pipe(
  throttleTime(100),
  distinctUntilChanged(),
);
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — not providing removeHandler (memory leak)
const leak$ = fromEventPattern<MouseEvent>(
  handler => document.addEventListener('mousemove', handler),
  // no removeHandler — listener never removed on unsubscribe
);

// ✅ CORRECT — always provide removeHandler
const safe$ = fromEventPattern<MouseEvent>(
  handler => document.addEventListener('mousemove', handler),
  handler => document.removeEventListener('mousemove', handler),
);
// WHY: without removeHandler, the DOM listener accumulates on each
// subscribe and is never cleaned up — a classic memory leak.


// ❌ INCORRECT — using handler directly when API needs signal
fromEventPattern(
  handler => {
    const token = customApi.on('event', handler);
    // token not returned → signal is undefined
  },
  (handler, signal) => {
    customApi.off(signal); // signal is undefined — fails silently
  },
);

// ✅ CORRECT — return the token from addHandler
fromEventPattern(
  handler => customApi.on('event', handler), // return value captured as signal
  (handler, signal) => customApi.off(signal),
);
// WHY: the return value of addHandler IS the signal passed to removeHandler.
// Forgetting to return it breaks cleanup for token-based APIs.


// ❌ INCORRECT — using fromEventPattern when fromEvent works
fromEventPattern(
  handler => button.addEventListener('click', handler),
  handler => button.removeEventListener('click', handler),
);

// ✅ CORRECT — use fromEvent for standard DOM/Node.js APIs
fromEvent(button, 'click');
// WHY: fromEventPattern is for non-standard APIs. fromEvent handles
// addEventListener/removeEventListener and EventEmitter automatically.
```

---

## fromEvent vs fromEventPattern Decision Guide

```
Standard DOM EventTarget (addEventListener/removeEventListener)?  → fromEvent
Node.js EventEmitter (on/off/addListener/removeListener)?        → fromEvent
Returns a token/handle for removal?                              → fromEventPattern
Single callback-based subscription?                              → fromEventPattern
Need to pass token to unsubscribe?                               → fromEventPattern
Library with non-standard event API?                             → fromEventPattern
```

---

## Related Operators

- **`fromEvent`** — simplified version for standard DOM/Node.js event APIs
- **`webSocket`** — dedicated WebSocket creation operator
- **`from`** — wraps Promises and iterables without handler lifecycle
- **`defer`** — create a new Observable per subscriber; alternative for imperative subscriptions
- **`Subject`** — push values imperatively when `fromEventPattern` is overly complex
