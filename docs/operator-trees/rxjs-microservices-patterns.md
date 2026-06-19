# RxJS in Microservices — Event-Driven Patterns

Using RxJS in Node.js microservice architectures: event buses, message brokers, service-to-service communication, and stream processing.

---

## Why RxJS in Microservices

Node.js microservices are naturally event-driven. RxJS provides:

- **Composable pipelines** over message streams (Kafka, RabbitMQ, Redis Pub/Sub)
- **Backpressure handling** via buffer/throttle operators
- **Error resilience** via retry, catchError, timeout
- **Stream transformation** via map, filter, scan, groupBy
- **Service orchestration** via forkJoin, combineLatest, merge

---

## Pattern 1: Message Broker as Observable

Wrap a message broker subscription (Kafka, RabbitMQ, Redis) as a typed Observable.

### Kafka with kafkajs

```typescript
import { Observable, Subject, fromEventPattern } from 'rxjs';
import { share, filter, map, takeUntil } from 'rxjs/operators';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

function kafkaConsumer$<T>(
  kafka: Kafka,
  topic: string,
  groupId: string,
): Observable<T> {
  return new Observable<T>(observer => {
    const consumer: Consumer = kafka.consumer({ groupId });

    const setup = async () => {
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          try {
            const value = JSON.parse(message.value!.toString()) as T;
            observer.next(value);
          } catch (err) {
            observer.error(err);
          }
        },
      });
    };

    setup().catch(err => observer.error(err));

    return async () => {
      await consumer.disconnect();
    };
  }).pipe(share()); // share among multiple downstream operators
}

// Usage
interface OrderEvent {
  type: 'created' | 'updated' | 'cancelled';
  orderId: string;
  payload: Order;
}

const kafka = new Kafka({ brokers: ['kafka:9092'] });
const orderEvents$ = kafkaConsumer$<OrderEvent>(kafka, 'order-events', 'order-service');

// Route events to handlers
orderEvents$.pipe(
  filter(e => e.type === 'created'),
).subscribe(e => processNewOrder(e.payload));

orderEvents$.pipe(
  filter(e => e.type === 'cancelled'),
).subscribe(e => handleCancellation(e.payload));
```

### Redis Pub/Sub

```typescript
import { fromEventPattern } from 'rxjs';
import { map, filter, share } from 'rxjs/operators';
import { createClient } from 'redis';

function redisChannel$<T>(
  channelName: string,
  redisUrl: string,
): Observable<T> {
  return new Observable<T>(observer => {
    const client = createClient({ url: redisUrl });

    client.connect().then(() => {
      client.subscribe(channelName, (message) => {
        try {
          observer.next(JSON.parse(message) as T);
        } catch (err) {
          observer.error(err);
        }
      });
    }).catch(err => observer.error(err));

    return () => {
      client.unsubscribe(channelName);
      client.quit();
    };
  }).pipe(share());
}
```

---

## Pattern 2: Service-to-Service HTTP Orchestration

Coordinate calls across multiple services with retry, timeout, and circuit breaker.

```typescript
import { forkJoin, throwError, timer, of } from 'rxjs';
import { switchMap, retry, timeout, catchError, map, retryWhen, delayWhen, scan } from 'rxjs/operators';

// Circuit breaker state
class CircuitBreaker {
  private failureCount = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';
  private lastFailureTime = 0;

  constructor(
    private readonly threshold = 5,
    private readonly resetTimeMs = 30_000,
  ) {}

  wrap<T>(source$: Observable<T>): Observable<T> {
    if (this.state === 'open') {
      const elapsed = Date.now() - this.lastFailureTime;
      if (elapsed < this.resetTimeMs) {
        return throwError(() => new Error('Circuit open — service unavailable'));
      }
      this.state = 'half-open';
    }

    return source$.pipe(
      catchError(err => {
        this.failureCount++;
        this.lastFailureTime = Date.now();
        if (this.failureCount >= this.threshold) {
          this.state = 'open';
          console.error(`Circuit opened after ${this.failureCount} failures`);
        }
        return throwError(() => err);
      }),
      tap(() => {
        this.failureCount = 0;
        this.state = 'closed';
      }),
    );
  }
}

// Orchestrate: call user-service, inventory-service, pricing-service in parallel
function buildProductPage(productId: string, userId: string): Observable<ProductPage> {
  const circuitBreaker = new CircuitBreaker();

  const user$ = circuitBreaker.wrap(
    userService.getUser(userId).pipe(timeout(2000))
  );

  const product$ = circuitBreaker.wrap(
    inventoryService.getProduct(productId).pipe(
      timeout(2000),
      retry(2),
    )
  );

  const pricing$ = circuitBreaker.wrap(
    pricingService.getPrice(productId, userId).pipe(timeout(1500))
  ).pipe(
    catchError(() => of({ price: null, currency: 'USD' })) // price is optional
  );

  return forkJoin({ user, product, pricing: pricing$ }).pipe(
    map(({ user, product, pricing }) => buildPage(product, user, pricing)),
  );
}
```

---

## Pattern 3: Event Stream Processing with groupBy + scan

Process high-volume event streams by entity — aggregate per-user or per-resource metrics.

```typescript
import { groupBy, mergeMap, scan, debounceTime, map } from 'rxjs/operators';
import { Observable } from 'rxjs';

interface ClickEvent {
  userId: string;
  elementId: string;
  timestamp: number;
}

interface UserActivity {
  userId: string;
  clickCount: number;
  lastSeen: number;
  topElements: Record<string, number>;
}

function aggregateUserActivity(
  events$: Observable<ClickEvent>,
): Observable<UserActivity> {
  return events$.pipe(
    groupBy(e => e.userId),
    mergeMap(userGroup$ =>
      userGroup$.pipe(
        scan((activity, event): UserActivity => ({
          userId: event.userId,
          clickCount: activity.clickCount + 1,
          lastSeen: event.timestamp,
          topElements: {
            ...activity.topElements,
            [event.elementId]: (activity.topElements[event.elementId] ?? 0) + 1,
          },
        }), {
          userId: userGroup$.key,
          clickCount: 0,
          lastSeen: 0,
          topElements: {},
        }),
        debounceTime(5000), // emit aggregated state after 5s of inactivity
      )
    ),
  );
}

// Usage
const clickStream$ = kafkaConsumer$<ClickEvent>(kafka, 'clicks', 'analytics');
aggregateUserActivity(clickStream$).subscribe(activity => {
  analyticsDb.upsert(activity);
});
```

---

## Pattern 4: Request-Reply over Message Bus

Implement synchronous request-reply semantics over an async message bus.

```typescript
import { Subject, race, timer, throwError } from 'rxjs';
import { filter, take, map, switchMap } from 'rxjs/operators';
import { v4 as uuidv4 } from 'uuid';

class MessageBusClient {
  private replies$ = new Subject<{ correlationId: string; payload: unknown }>();

  constructor(private publisher: RedisPublisher, private subscriber: RedisSubscriber) {
    // Listen for all reply messages
    redisChannel$<{ correlationId: string; payload: unknown }>(
      'service.replies',
      process.env.REDIS_URL!,
    ).subscribe(reply => this.replies$.next(reply));
  }

  request<TRequest, TResponse>(
    targetService: string,
    payload: TRequest,
    timeoutMs = 5000,
  ): Observable<TResponse> {
    const correlationId = uuidv4();

    // Publish request
    const publish$ = new Observable<never>(observer => {
      this.publisher.publish(
        `${targetService}.requests`,
        JSON.stringify({ correlationId, payload }),
      ).then(() => observer.complete());
    });

    // Wait for matching reply
    const reply$ = this.replies$.pipe(
      filter(r => r.correlationId === correlationId),
      take(1),
      map(r => r.payload as TResponse),
    );

    return publish$.pipe(
      switchMap(() =>
        race(
          reply$,
          timer(timeoutMs).pipe(
            switchMap(() => throwError(() => new Error(`Request timeout: ${targetService}`)))
          ),
        )
      ),
    );
  }
}

// Usage
const bus = new MessageBusClient(publisher, subscriber);

bus.request<{ userId: string }, User>('user-service', { userId: '123' }).pipe(
  switchMap(user => bus.request('order-service', { userId: user.id })),
).subscribe(orders => console.log(orders));
```

---

## Pattern 5: Backpressure — Handling High-Volume Streams

```typescript
import { bufferTime, filter, concatMap, from, mergeMap, throttleTime } from 'rxjs/operators';

// Batch high-frequency events into DB writes
function batchWriter<T>(
  events$: Observable<T>,
  writeToDb: (batch: T[]) => Promise<void>,
  options = { batchMs: 500, maxBatchSize: 100 },
): Observable<void> {
  return events$.pipe(
    bufferTime(options.batchMs, null, options.maxBatchSize),
    filter(batch => batch.length > 0),
    concatMap(batch => from(writeToDb(batch))), // sequential writes, no overlap
  );
}

// Usage
const highVolumeEvents$ = kafkaConsumer$<AnalyticsEvent>(kafka, 'events', 'writer');

batchWriter(highVolumeEvents$, async batch => {
  await db.bulkInsert('analytics_events', batch);
  console.log(`Wrote ${batch.length} events`);
}).subscribe({
  error: err => console.error('Write error:', err),
});
```

---

## Pattern 6: Saga-Style Distributed Transaction

Coordinate a multi-step distributed transaction with compensating actions on failure.

```typescript
import { switchMap, catchError, concatMap, from } from 'rxjs/operators';

interface OrderSagaState {
  orderId: string;
  paymentId?: string;
  inventoryReserved?: boolean;
}

function orderSaga(order: OrderRequest): Observable<SagaResult> {
  let state: OrderSagaState = { orderId: order.id };

  return from(paymentService.reserve(order.total)).pipe(
    switchMap(paymentId => {
      state.paymentId = paymentId;
      return from(inventoryService.reserve(order.items));
    }),
    switchMap(() => {
      state.inventoryReserved = true;
      return from(orderService.confirm(state.orderId));
    }),
    map(() => ({ success: true, orderId: state.orderId })),

    // Compensating transactions on failure
    catchError(err => {
      const compensations: Array<() => Promise<void>> = [];

      if (state.paymentId) {
        compensations.push(() => paymentService.release(state.paymentId!));
      }
      if (state.inventoryReserved) {
        compensations.push(() => inventoryService.release(order.items));
      }

      // Run all compensations, even if some fail
      return from(compensations).pipe(
        concatMap(compensate =>
          from(compensate()).pipe(
            catchError(e => { console.error('Compensation failed:', e); return of(null); })
          )
        ),
        toArray(),
        switchMap(() => throwError(() => err)),
      );
    }),
  );
}
```

---

## Pattern 7: Health Check Stream

Expose service health as a reactive stream — aggregate dependency health for a `/health` endpoint.

```typescript
import { combineLatest, interval, of } from 'rxjs';
import { switchMap, catchError, map, startWith, shareReplay } from 'rxjs/operators';

type HealthStatus = 'healthy' | 'degraded' | 'unhealthy';

interface DependencyHealth {
  name: string;
  status: HealthStatus;
  latencyMs: number;
}

function checkDependency(name: string, check: () => Promise<void>): Observable<DependencyHealth> {
  return interval(10_000).pipe(
    startWith(0),
    switchMap(async () => {
      const start = Date.now();
      try {
        await check();
        return { name, status: 'healthy' as const, latencyMs: Date.now() - start };
      } catch {
        return { name, status: 'unhealthy' as const, latencyMs: Date.now() - start };
      }
    }),
    shareReplay(1),
  );
}

const dbHealth$      = checkDependency('postgres', () => db.query('SELECT 1'));
const cacheHealth$   = checkDependency('redis',    () => redisClient.ping());
const brokerHealth$  = checkDependency('kafka',    () => kafka.admin().connect());

const serviceHealth$ = combineLatest([dbHealth$, cacheHealth$, brokerHealth$]).pipe(
  map(deps => ({
    status: deps.every(d => d.status === 'healthy') ? 'healthy'
           : deps.some(d => d.status === 'unhealthy') ? 'unhealthy'
           : 'degraded',
    dependencies: deps,
    timestamp: new Date().toISOString(),
  })),
  shareReplay(1),
);

// Express health endpoint
app.get('/health', async (req, res) => {
  const health = await firstValueFrom(serviceHealth$);
  res.status(health.status === 'healthy' ? 200 : 503).json(health);
});
```

---

## Operator Selection for Microservices

| Scenario | Operator |
|---|---|
| Fan-out to multiple services in parallel | `forkJoin` |
| Sequential service calls | `concatMap` / `switchMap` chain |
| Route events by type | `groupBy` + `mergeMap` |
| Aggregate per entity | `groupBy` + `scan` |
| Batch writes to DB | `bufferTime` + `concatMap` |
| Circuit breaker | Custom operator with `catchError` + `scan` |
| Request-reply timeout | `race(reply$, timer(ms))` |
| Long polling | `interval` + `switchMap` + `distinctUntilChanged` |

---

## Related Guides

- **[WebSocket Patterns](./rxjs-websocket-patterns.md)** — real-time connections
- **[Error Resilience Patterns](./rxjs-error-resilience-patterns.md)** — retry and circuit breaker
- **[Polling Patterns](./rxjs-polling-patterns.md)** — periodic service checks
- **[Concurrency Patterns](./concurrency-guide.md)** — managing parallel operations
- **[Node.js Patterns](./nodejs-rxjs-patterns.md)** — Node.js-specific RxJS usage
