# fromEventPattern

## Identity

- **Name**: fromEventPattern
- **Category**: Creation Operators
- **Type**: Custom event bridge — wraps any add/remove listener API as a cold, cancellable Observable
- **Import**:
  ```typescript
  import { fromEventPattern } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function fromEventPattern<T>(
    addHandler:    (handler: NodeEventHandler) => any,
    removeHandler?: (handler: NodeEventHandler, signal?: any) => void,
    resultSelector?: (...args: any[]) => T
  ): Observable<T | T[]>
  ```

## Functional Specification

`fromEventPattern` is the generalized version of `fromEvent`. While `fromEvent` works with DOM `EventTarget` and Node.js `EventEmitter` interfaces, `fromEventPattern` works with **any** add/remove listener API — including custom event systems, third-party libraries, and subscription-based APIs that don't follow the standard interface.

**When to use `fromEventPattern` over `fromEvent`**:
- The event API doesn't use `addEventListener`/`removeEventListener` naming
- You need to pass a token or handle from `addHandler` to `removeHandler`
- The subscription registration returns a handle you must use to unsubscribe
- You're wrapping a WebSocket, gRPC stream, or custom pub/sub system

**Lifecycle**:
1. On subscription: `addHandler(handler)` is called; its return value is stored as `signal`
2. Emissions: every time `handler(value)` is called, the Observable emits
3. On unsubscription: `removeHandler(handler, signal)` is called for cleanup

## Marble Diagram

```
Subscribe   → addHandler(handler) called    → listener registered
handler(a)  → Observable emits a
handler(b)  → Observable emits b
Unsubscribe → removeHandler(handler, signal) → listener removed

Observable: --a--b--c-...  (continues until unsubscribed)
```

## Examples

### Basic Usage — Custom Event Emitter
```typescript
import { fromEventPattern } from 'rxjs';

// Wrap a library that uses .on()/.off() instead of addEventListener
declare const emitter: { on(event: string, fn: Function): void; off(event: string, fn: Function): void };

const data$ = fromEventPattern<DataEvent>(
  handler => emitter.on('data', handler),
  handler => emitter.off('data', handler)
);

data$.subscribe(event => process(event));
// emitter.on() called on subscribe; emitter.off() called on unsubscribe
```

### Common Pattern — API That Returns a Subscription Token
```typescript
import { fromEventPattern } from 'rxjs';

// Many pub/sub libraries return a subscription ID needed for unsubscribe
declare const pubsub: {
  subscribe(channel: string, fn: Function): string;  // returns token
  unsubscribe(token: string): void;
};

const messages$ = fromEventPattern<Message>(
  handler => pubsub.subscribe('my-channel', handler), // returns token
  (handler, token) => pubsub.unsubscribe(token)       // token passed as signal
);

messages$.subscribe(msg => handleMessage(msg));
// pubsub.unsubscribe(token) called automatically on unsubscription
```

### Common Pattern — Geolocation API
```typescript
import { fromEventPattern } from 'rxjs';

// Geolocation uses watchPosition/clearWatch with a numeric ID
const position$ = fromEventPattern<GeolocationPosition>(
  handler => navigator.geolocation.watchPosition(handler),  // returns watchId
  (handler, watchId) => navigator.geolocation.clearWatch(watchId)
);

position$.subscribe({
  next:  pos => updateMap(pos.coords),
  error: err => showLocationError(err)
});
// clearWatch(watchId) called when subscriber unsubscribes
```

### Common Pattern — Wrapping Callbacks as Streams
```typescript
import { fromEventPattern } from 'rxjs';

// gRPC streaming call
declare const grpcClient: {
  watch(path: string): { on(event: string, fn: Function): void; cancel(): void }
};

const stream$ = fromEventPattern<FileChange>(
  handler => {
    const call = grpcClient.watch('/files');
    call.on('data', handler);
    return call; // return the call object as signal
  },
  (handler, call) => call.cancel() // use signal to cancel
);

stream$.subscribe(change => handleFileChange(change));
```

### Using `resultSelector` for Multi-Argument Events
```typescript
import { fromEventPattern } from 'rxjs';

// Some emitters call the handler with multiple arguments
declare const emitter: { on(event: string, fn: (...args: any[]) => void): void; off(event: string, fn: Function): void };

const combined$ = fromEventPattern<{ code: number; message: string }>(
  handler => emitter.on('status', handler),
  handler => emitter.off('status', handler),
  (code, message) => ({ code, message }) // resultSelector maps multi-arg to one value
);

combined$.subscribe(({ code, message }) => console.log(code, message));
```

## Common Pitfalls

### Anti-pattern: Forgetting `removeHandler` (Memory Leak)
```typescript
import { fromEventPattern } from 'rxjs';

// ❌ MEMORY LEAK — no removeHandler; listener never cleaned up
const data$ = fromEventPattern<number>(
  handler => someEmitter.on('data', handler)
  // missing removeHandler!
);

const sub = data$.subscribe(console.log);
sub.unsubscribe(); // unsubscription does NOT remove the listener
// handler still registered → memory leak

// ✅ CORRECT — always provide removeHandler
const data$ = fromEventPattern<number>(
  handler => someEmitter.on('data', handler),
  handler => someEmitter.off('data', handler) // ← cleanup
);

// WHY: Without removeHandler, unsubscription only stops the Observable
// from emitting — the underlying listener keeps firing indefinitely.
```

## Related Operators

- **`fromEvent`**: Simpler API for DOM `EventTarget` and Node.js `EventEmitter` — use this first
- **`defer`**: Lazy Observable creation from a factory — for non-event async patterns
- **`using`**: Resource management with teardown — when creation produces a disposable resource

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/fromEventPattern](https://rxjs.dev/api/index/function/fromEventPattern)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching point**: The return value of `addHandler` is passed as `signal` to `removeHandler` — use this to pass subscription tokens, watch IDs, or call objects needed for cleanup. Always provide `removeHandler` or you'll leak listeners.
