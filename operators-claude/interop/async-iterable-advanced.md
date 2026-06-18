# Async Iterable & ReadableStream — Advanced Patterns

For fundamentals see the core [Async Iterable / ReadableStream](./async-iterable) doc. This page covers streaming fetch responses, `for await` integration, backpressure via ReadableStream, Node.js stream bridging, and async generator composition.

---

## Mental Model

```typescript
import { from } from 'rxjs';

// Any AsyncIterable → Observable via from():
async function* count() { yield 1; yield 2; yield 3; }
from(count()).subscribe(console.log); // 1, 2, 3

// ReadableStream → Observable (browser Fetch API):
const response = await fetch('/api/data');
from(response.body!).subscribe(chunk => process(chunk));
// response.body is a ReadableStream<Uint8Array> — implements AsyncIterable

// Observable → AsyncIterable (RxJS 7):
for await (const value of interval(100).pipe(take(5))) {
  console.log(value); // 0, 1, 2, 3, 4
}
```

**Key distinction**: AsyncIterables are pull-based (consumer asks for next item); Observables are push-based (source pushes values). `from()` bridges the gap with a buffer; `for await` on an Observable also buffers — neither side can naturally apply backpressure to the other.

---

## Pattern 1: Streaming Fetch with `ReadableStream`

Process a large response body as it arrives, without buffering the whole response:

```typescript
import { from, Observable } from 'rxjs';
import { scan, map, filter } from 'rxjs/operators';

interface StreamChunk { type: 'data' | 'done'; payload?: unknown; }

function streamJson<T>(url: string): Observable<T> {
  return new Observable<T>(subscriber => {
    const controller = new AbortController();

    fetch(url, { signal: controller.signal })
      .then(async response => {
        if (!response.ok) {
          subscriber.error(new Error(`HTTP ${response.status}`));
          return;
        }

        const decoder = new TextDecoder();
        let buffer = '';

        // response.body is ReadableStream<Uint8Array>:
        for await (const chunk of response.body!) {
          if (subscriber.closed) break;

          buffer += decoder.decode(chunk, { stream: true });

          // Parse complete newline-delimited JSON objects:
          const lines = buffer.split('\n');
          buffer = lines.pop()!; // keep incomplete last line

          for (const line of lines) {
            if (line.trim()) {
              try {
                subscriber.next(JSON.parse(line) as T);
              } catch {
                // skip malformed lines
              }
            }
          }
        }

        // Flush remaining buffer:
        if (buffer.trim()) {
          try { subscriber.next(JSON.parse(buffer) as T); } catch { /* skip */ }
        }

        subscriber.complete();
      })
      .catch(err => {
        if (err.name !== 'AbortError') subscriber.error(err);
      });

    return () => controller.abort();
  });
}

// Stream 10,000 user records without loading all into memory:
streamJson<User>('/api/users/export').pipe(
  filter(user => user.active),
  scan((acc, user) => [...acc, user], [] as User[]),
  takeUntilDestroyed()
).subscribe(users => renderUserList(users));
```

---

## Pattern 2: Server-Sent Events (SSE) as Observable

SSE is a `ReadableStream` of UTF-8 text — parse the event-stream protocol:

```typescript
import { Observable } from 'rxjs';
import { share, retry, filter } from 'rxjs/operators';

interface SSEEvent { id?: string; event?: string; data: string; }

function sseStream$(url: string, options?: RequestInit): Observable<SSEEvent> {
  return new Observable<SSEEvent>(subscriber => {
    const controller = new AbortController();

    fetch(url, { ...options, signal: controller.signal, headers: {
      ...options?.headers,
      Accept: 'text/event-stream'
    }})
      .then(async response => {
        const decoder = new TextDecoder();
        let buffer = '';
        let currentEvent: Partial<SSEEvent> = {};

        for await (const chunk of response.body!) {
          if (subscriber.closed) break;

          buffer += decoder.decode(chunk, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop()!;

          for (const line of lines) {
            if (line === '') {
              // Empty line = dispatch event:
              if (currentEvent.data !== undefined) {
                subscriber.next(currentEvent as SSEEvent);
              }
              currentEvent = {};
            } else if (line.startsWith('data:')) {
              currentEvent.data = line.slice(5).trim();
            } else if (line.startsWith('event:')) {
              currentEvent.event = line.slice(6).trim();
            } else if (line.startsWith('id:')) {
              currentEvent.id = line.slice(3).trim();
            }
          }
        }
        subscriber.complete();
      })
      .catch(err => {
        if (err.name !== 'AbortError') subscriber.error(err);
      });

    return () => controller.abort();
  });
}

// Live price feed with auto-reconnect:
sseStream$('/api/prices/stream').pipe(
  filter(e => e.event === 'price'),
  map(e => JSON.parse(e.data) as PriceUpdate),
  retry({ delay: err => timer(2000) }),
  share()
).subscribe(price => updatePriceDisplay(price));
```

---

## Pattern 3: Async Generator Composition

Async generators are AsyncIterables — compose them with `from()`:

```typescript
import { from, merge } from 'rxjs';
import { mergeMap, take } from 'rxjs/operators';

// Async generator that pages through an API:
async function* paginate<T>(
  fetchPage: (cursor: string | null) => Promise<{ items: T[]; nextCursor: string | null }>
): AsyncGenerator<T> {
  let cursor: string | null = null;

  while (true) {
    const page = await fetchPage(cursor);
    yield* page.items;

    if (!page.nextCursor) break;
    cursor = page.nextCursor;
  }
}

// Convert paginator to Observable:
from(paginate(cursor => api.getUsers({ cursor, limit: 100 }))).pipe(
  filter(user => user.active),
  take(1000), // safety cap
  toArray(),
  takeUntilDestroyed()
).subscribe(activeUsers => renderTable(activeUsers));

// Multiple paginators in parallel:
const sources = [
  paginate(cursor => api.getProducts({ cursor, category: 'electronics' })),
  paginate(cursor => api.getProducts({ cursor, category: 'clothing' })),
];

merge(...sources.map(s => from(s))).pipe(
  distinct(p => p.id),
  toArray()
).subscribe(allProducts => console.log(allProducts.length));
```

---

## Pattern 4: Observable → AsyncIterable (for RxJS interop with async/await code)

Use `Symbol.asyncIterator` support (RxJS 7.2+) to consume Observables in async contexts:

```typescript
import { interval, take } from 'rxjs';

// Consume an Observable with for-await-of:
async function processStream(): Promise<void> {
  const source$ = interval(100).pipe(take(5));

  for await (const value of source$) {
    await doAsyncWork(value); // sequential processing
    console.log('processed:', value);
  }

  console.log('stream complete');
}

// Interop with libraries expecting AsyncIterable (e.g., OpenAI streaming):
async function* toAsyncIterable<T>(obs$: Observable<T>): AsyncGenerator<T> {
  for await (const value of obs$) {
    yield value;
  }
}

// Feed RxJS Observable into any AsyncIterable consumer:
const stream$ = webSocket$<ChatChunk>('/api/chat').pipe(
  map(msg => msg.delta)
);

for await (const delta of toAsyncIterable(stream$)) {
  appendToUI(delta);
}
```

---

## Pattern 5: Node.js Readable Stream → Observable

Bridge Node.js streams (file reads, HTTP, `child_process` stdout) to RxJS:

```typescript
import { Readable } from 'stream';
import { Observable } from 'rxjs';

function fromNodeStream<T = Buffer>(readable: Readable): Observable<T> {
  return new Observable<T>(subscriber => {
    const onData     = (chunk: T)   => subscriber.next(chunk);
    const onError    = (err: Error) => subscriber.error(err);
    const onEnd      = ()           => subscriber.complete();

    readable.on('data',  onData);
    readable.on('error', onError);
    readable.on('end',   onEnd);

    return () => {
      readable.off('data',  onData);
      readable.off('error', onError);
      readable.off('end',   onEnd);
      if (!readable.destroyed) readable.destroy();
    };
  });
}

// Read large file line by line:
import { createReadStream } from 'fs';
import { createInterface } from 'readline';

function readLines$(filePath: string): Observable<string> {
  return new Observable<string>(subscriber => {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });

    rl.on('line',  line  => subscriber.next(line));
    rl.on('close', ()    => subscriber.complete());
    rl.on('error', err   => subscriber.error(err));

    return () => rl.close();
  });
}

// Process a CSV file:
readLines$('/data/users.csv').pipe(
  skip(1), // header row
  map(line => line.split(',')),
  map(([id, name, email]) => ({ id, name, email })),
  filter(u => u.email.includes('@company.com')),
  toArray()
).subscribe(internalUsers => writeResults(internalUsers));
```

---

## Pattern 6: `ReadableStream` Backpressure with `pipeTo`

For true backpressure (slow consumer slows producer), use the WHATWG Streams API directly:

```typescript
// ReadableStream with backpressure — consumer controls the pull rate:
function observableToReadableStream<T>(obs$: Observable<T>): ReadableStream<T> {
  let subscription: Subscription;

  return new ReadableStream<T>({
    start(controller) {
      subscription = obs$.subscribe({
        next:     v   => controller.enqueue(v),
        error:    err => controller.error(err),
        complete: ()  => controller.close()
      });
    },
    cancel() {
      subscription?.unsubscribe();
    }
  });
}

// With a CountQueuingStrategy — apply backpressure when queue > N:
function observableToBackpressuredStream<T>(
  obs$: Observable<T>,
  highWaterMark = 16
): ReadableStream<T> {
  let subscription: Subscription;
  let paused = false;

  const stream = new ReadableStream<T>(
    {
      start(controller) {
        subscription = obs$.subscribe({
          next: v => {
            controller.enqueue(v);
            // Pause source if queue is full:
            if (controller.desiredSize !== null && controller.desiredSize <= 0 && !paused) {
              paused = true;
              (obs$ as any).pause?.(); // Node.js Readable pause
            }
          },
          error:    err => controller.error(err),
          complete: ()  => controller.close()
        });
      },
      pull() {
        // Consumer pulled — resume source:
        if (paused) {
          paused = false;
          (obs$ as any).resume?.();
        }
      },
      cancel() {
        subscription?.unsubscribe();
      }
    },
    new CountQueuingStrategy({ highWaterMark })
  );

  return stream;
}
```

---

## Pattern 7: LLM Streaming Responses

Convert OpenAI / Anthropic streaming completions to Observable:

```typescript
import { Observable } from 'rxjs';
import { scan, map } from 'rxjs/operators';

function streamCompletion$(prompt: string): Observable<string> {
  return new Observable<string>(subscriber => {
    let cancelled = false;

    (async () => {
      try {
        const stream = await openai.chat.completions.create({
          model:    'gpt-4o',
          messages: [{ role: 'user', content: prompt }],
          stream:   true
        });

        for await (const chunk of stream) {
          if (cancelled) break;
          const delta = chunk.choices[0]?.delta?.content ?? '';
          if (delta) subscriber.next(delta);
        }

        if (!cancelled) subscriber.complete();
      } catch (err) {
        if (!cancelled) subscriber.error(err);
      }
    })();

    return () => { cancelled = true; };
  });
}

// Accumulate streaming text for display:
streamCompletion$('Explain RxJS backpressure').pipe(
  scan((acc, delta) => acc + delta, ''),
  takeUntilDestroyed()
).subscribe(text => updateTextArea(text));

// Parse structured JSON as it streams:
streamCompletion$('List 5 RxJS operators as JSON array').pipe(
  scan((acc, delta) => acc + delta, ''),
  filter(text => {
    try { JSON.parse(text); return true; }
    catch { return false; }
  }),
  first(),
  map(text => JSON.parse(text) as string[])
).subscribe(operators => renderList(operators));
```

---

## Common Pitfalls

### Unhandled Backpressure in `from(asyncIterable$)`

```typescript
// ❌ from() buffers eagerly — fast producer overwhelms slow consumer:
from(fastAsyncGenerator()).pipe(
  mergeMap(item => slowProcessing(item))
  // Generator produces 1000 items/sec; processing takes 100ms each
  // Buffer grows unboundedly
)

// ✅ Use concatMap to process one at a time (implicit backpressure):
from(fastAsyncGenerator()).pipe(
  concatMap(item => slowProcessing(item))
  // Pulls next item only after current processing completes
)

// ✅ Or limit concurrency with mergeMap:
from(fastAsyncGenerator()).pipe(
  mergeMap(item => slowProcessing(item), 5) // max 5 concurrent
)
```

### Forgetting to Abort Fetch on Unsubscribe

```typescript
// ❌ No cleanup — fetch continues after unsubscribe:
function fetchStream$(url: string): Observable<Uint8Array> {
  return new Observable(subscriber => {
    fetch(url).then(async response => {
      for await (const chunk of response.body!) {
        subscriber.next(chunk);
      }
    });
    // No return value — no cleanup!
  });
}

// ✅ Use AbortController and return cleanup:
function fetchStream$(url: string): Observable<Uint8Array> {
  return new Observable(subscriber => {
    const controller = new AbortController();
    fetch(url, { signal: controller.signal }).then(async response => {
      for await (const chunk of response.body!) {
        if (subscriber.closed) break;
        subscriber.next(chunk);
      }
      subscriber.complete();
    }).catch(err => {
      if (err.name !== 'AbortError') subscriber.error(err);
    });
    return () => controller.abort(); // ✅ cleanup
  });
}
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `from()` is the universal bridge from AsyncIterable to Observable — use it for generators, ReadableStreams, and Node.js streams. For true backpressure, step outside RxJS and use the WHATWG Streams `pipeTo`/`pipeThrough` API. The most common real-world use is streaming HTTP responses (Fetch body, SSE, LLM completions) where `AbortController` cleanup on unsubscribe is non-negotiable.
