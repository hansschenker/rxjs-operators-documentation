# using

## Identity

- **Name**: using
- **Category**: Creation Operators
- **Type**: Resource factory — creates a disposable resource tied to an Observable's lifetime
- **Import**:
  ```typescript
  import { using } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function using<T>(
    resourceFactory:  () => Unsubscribable | void,
    observableFactory: (resource: Unsubscribable | void) => ObservableInput<T> | void
  ): Observable<T>

  interface Unsubscribable {
    unsubscribe(): void
  }
  ```

## Functional Specification

`using` ties a **resource's lifetime** to an **Observable's subscription lifetime**. When the Observable is subscribed to:
1. `resourceFactory()` is called to create the resource
2. `observableFactory(resource)` is called to create the Observable (the resource is passed in)
3. The Observable runs normally

When the Observable is unsubscribed from (or completes/errors):
4. `resource.unsubscribe()` is called automatically for cleanup

**Mental model**: Like a `try/finally` block for Observables — the resource is guaranteed to be cleaned up when the stream ends, for any reason.

**`using` vs `defer` + `finalize`**:

| | `using` | `defer` + `finalize` |
|---|---|---|
| Resource scope | Created and destroyed per subscription | Can reference outer scope |
| Resource passed to Observable | Yes — via `observableFactory` param | No — captured in closure |
| Cleanup guarantee | `resource.unsubscribe()` | `finalize` callback |
| Clarity | Resource + Observable tightly coupled | Explicit teardown logic |

## Marble Diagram

```
subscribe:
  1. resourceFactory() → creates resource
  2. observableFactory(resource) → creates Observable
  3. Observable runs normally

unsubscribe / complete / error:
  4. resource.unsubscribe() → cleanup

Timeline: [create]---a---b---c---[cleanup]
```

## Examples

### Basic Usage — Database Connection Per Subscription
```typescript
import { using, interval } from 'rxjs';
import { take, map } from 'rxjs/operators';

function openDbConnection(): { query: (sql: string) => any; unsubscribe: () => void } {
  const conn = db.connect();
  console.log('connection opened');
  return {
    query: (sql) => conn.query(sql),
    unsubscribe: () => { conn.close(); console.log('connection closed'); }
  };
}

const stream$ = using(
  () => openDbConnection(),
  (conn) => interval(1000).pipe(
    take(5),
    map(() => conn.query('SELECT * FROM events LIMIT 1'))
  )
);

stream$.subscribe(result => console.log(result));
// connection opened
// (5 query results)
// connection closed  ← guaranteed on unsubscription or completion
```

### Common Pattern — File Handle Lifecycle
```typescript
import { using, from } from 'rxjs';

const fileStream$ = using(
  () => ({
    handle: openFile('/data/events.log'),
    unsubscribe() { this.handle.close(); }
  }),
  (resource) => from(resource.handle.readLines())
);

fileStream$.subscribe({
  next:     line     => processLine(line),
  complete: ()       => console.log('done'),
  error:    err      => console.error(err)
});
// File handle closed in all three cases: complete, error, and unsubscription
```

### Common Pattern — WebWorker Scoped to Subscription
```typescript
import { using, fromEvent } from 'rxjs';
import { map, takeUntil } from 'rxjs/operators';

const computation$ = using(
  () => {
    const worker = new Worker('/workers/compute.js');
    return {
      worker,
      unsubscribe() { worker.terminate(); }
    };
  },
  (resource) => {
    resource.worker.postMessage({ start: true });
    return fromEvent<MessageEvent>(resource.worker, 'message').pipe(
      map(e => e.data)
    );
  }
);

// Worker is terminated when subscriber unsubscribes
const sub = computation$.subscribe(result => render(result));
setTimeout(() => sub.unsubscribe(), 5000); // terminates worker after 5s
```

## Common Pitfalls

### Anti-pattern: Resource Not Returned from `resourceFactory`
```typescript
import { using, of } from 'rxjs';

// ❌ RESOURCE NEVER CLEANED UP — resourceFactory doesn't return anything
const stream$ = using(
  () => {
    const conn = openConnection();
    // forgot to return conn
  },
  (_resource) => of(1, 2, 3) // resource is undefined
);

stream$.subscribe(console.log);
// conn.unsubscribe() is never called — connection leaks

// ✅ CORRECT — always return the resource
const stream$ = using(
  () => ({
    conn: openConnection(),
    unsubscribe() { this.conn.close(); }
  }),
  (resource) => queryStream(resource.conn)
);

// WHY: `using` calls resource.unsubscribe() only if resourceFactory
// returns an object with an unsubscribe method. If undefined is returned,
// no cleanup happens — the resource leaks silently.
```

## Related Operators

- **`defer`**: Lazy Observable creation per subscription — without a resource to clean up
- **`finalize`**: Add teardown logic to an existing Observable — simpler when resource is in scope
- **`fromEventPattern`**: Clean add/remove handler bridge — uses a similar lifecycle pattern

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/using](https://rxjs.dev/api/index/function/using)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 1/5 | **Composability**: 3/5
**Key teaching point**: `resourceFactory` must return an object with `unsubscribe()` — if it returns `void`, no cleanup occurs. For most teardown needs, `finalize()` is simpler; reach for `using` when the resource object itself must be passed into the Observable factory.
