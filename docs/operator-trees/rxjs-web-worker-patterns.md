# Web Worker Patterns with RxJS

Offloading CPU work to Web Workers, bidirectional communication channels, worker pools, and Comlink integration — all wrapped in Observables.

---

## The Core Pattern: `fromEvent` on a Worker

A Web Worker communicates via `postMessage` / `onmessage`. Wrapping this in Observables gives you composition, cancellation, and error propagation for free.

```typescript
import { fromEvent, Subject } from 'rxjs';
import { map, filter, take } from 'rxjs/operators';

const worker = new Worker(new URL('./heavy.worker.ts', import.meta.url));

// Incoming messages as an Observable:
const workerMessages$ = fromEvent<MessageEvent>(worker, 'message').pipe(
  map(e => e.data)
);

// Send work and receive one result:
function runInWorker<T>(payload: unknown): Observable<T> {
  return new Observable<T>(subscriber => {
    worker.postMessage(payload);
    const sub = workerMessages$.pipe(take(1)).subscribe({
      next:     result => subscriber.next(result as T),
      error:    err    => subscriber.error(err),
      complete: ()     => subscriber.complete()
    });
    return () => sub.unsubscribe();
  });
}
```

---

## Pattern 1: Request-Response Worker Channel

Correlate requests to responses by ID — supports concurrent calls:

```typescript
import { fromEvent, Subject } from 'rxjs';
import { filter, map, take } from 'rxjs/operators';

interface WorkerRequest  { id: string; type: string; payload: unknown; }
interface WorkerResponse { id: string; result?: unknown; error?: string; }

class WorkerChannel {
  private worker   = new Worker(new URL('./worker.ts', import.meta.url));
  private messages$ = fromEvent<MessageEvent<WorkerResponse>>(this.worker, 'message').pipe(
    map(e => e.data)
  );
  private errors$ = fromEvent<ErrorEvent>(this.worker, 'error');

  call<T>(type: string, payload: unknown): Observable<T> {
    const id = crypto.randomUUID();

    return new Observable<T>(subscriber => {
      // Send request:
      this.worker.postMessage({ id, type, payload } satisfies WorkerRequest);

      // Wait for matching response:
      const sub = this.messages$.pipe(
        filter(msg => msg.id === id),
        take(1),
        map(msg => {
          if (msg.error) throw new Error(msg.error);
          return msg.result as T;
        })
      ).subscribe(subscriber);

      return () => sub.unsubscribe();
    });
  }

  terminate(): void { this.worker.terminate(); }
}

// Usage:
const channel = new WorkerChannel();

channel.call<number[]>('SORT', largeArray).pipe(
  takeUntilDestroyed()
).subscribe(sorted => renderSortedList(sorted));

channel.call<ImageData>('PROCESS_IMAGE', rawImageData).subscribe(
  processed => drawToCanvas(processed)
);
```

---

## Pattern 2: Streaming Worker Output

Workers can stream progress back — useful for long computations:

```typescript
// worker.ts:
self.onmessage = (e: MessageEvent) => {
  const { id, data } = e.data;
  const total = data.length;

  for (let i = 0; i < total; i++) {
    processItem(data[i]);
    if (i % 100 === 0) {
      self.postMessage({ id, type: 'progress', percent: (i / total) * 100 });
    }
  }
  self.postMessage({ id, type: 'complete', result: finalResult });
};

// main thread:
interface WorkerEvent {
  id:       string;
  type:     'progress' | 'complete' | 'error';
  percent?: number;
  result?:  unknown;
  error?:   string;
}

class StreamingWorkerChannel {
  private worker    = new Worker(new URL('./worker.ts', import.meta.url));
  private messages$ = fromEvent<MessageEvent<WorkerEvent>>(this.worker, 'message').pipe(
    map(e => e.data),
    share()
  );

  stream<T>(payload: unknown): Observable<{ progress: number; result?: T; done: boolean }> {
    const id = crypto.randomUUID();

    return new Observable(subscriber => {
      this.worker.postMessage({ id, data: payload });

      return this.messages$.pipe(
        filter(msg => msg.id === id),
        takeWhile(msg => msg.type !== 'complete' && msg.type !== 'error', true),
        map(msg => {
          if (msg.type === 'error') throw new Error(msg.error);
          return {
            progress: msg.percent ?? 100,
            result:   msg.result as T | undefined,
            done:     msg.type === 'complete'
          };
        })
      ).subscribe(subscriber);
    });
  }
}

// Usage:
const worker = new StreamingWorkerChannel();
worker.stream<ProcessedData>(hugeDataset).subscribe(({ progress, result, done }) => {
  updateProgressBar(progress);
  if (done && result) renderFinalResult(result);
});
```

---

## Pattern 3: Worker Pool

Distribute work across multiple workers, return results as they complete:

```typescript
import { Subject, from, merge } from 'rxjs';
import { mergeMap, map, take } from 'rxjs/operators';

class WorkerPool {
  private workers:   WorkerChannel[];
  private available$ = new Subject<WorkerChannel>();

  constructor(size: number, workerUrl: string) {
    this.workers = Array.from({ length: size }, () =>
      new WorkerChannel(new Worker(workerUrl))
    );
    // Initially all workers available:
    this.workers.forEach(w => this.available$.next(w));
  }

  // Execute task on next available worker:
  execute<T>(type: string, payload: unknown): Observable<T> {
    return this.available$.pipe(
      take(1),         // grab first available worker
      mergeMap(worker =>
        worker.call<T>(type, payload).pipe(
          finalize(() => this.available$.next(worker)) // return to pool
        )
      )
    );
  }

  // Fan out tasks across pool concurrently:
  executeAll<T, I>(type: string, items: I[]): Observable<T> {
    return from(items).pipe(
      mergeMap(item => this.execute<T>(type, item), this.workers.length)
    );
  }
}

// Usage — image processing pool with 4 workers:
const imagePool = new WorkerPool(4, new URL('./image.worker.ts', import.meta.url).href);

imagePool.executeAll<ProcessedImage, RawImage>('PROCESS', images).pipe(
  toArray(), // collect all when done
  takeUntilDestroyed()
).subscribe(processedImages => renderGallery(processedImages));
```

---

## Pattern 4: Shared Worker (Cross-Tab Communication)

`SharedWorker` is accessible from multiple tabs simultaneously:

```typescript
import { fromEvent } from 'rxjs';
import { map, filter, share } from 'rxjs/operators';

class SharedWorkerChannel {
  private worker: SharedWorker;
  private port:   MessagePort;
  private messages$: Observable<unknown>;

  constructor(url: string) {
    this.worker   = new SharedWorker(url);
    this.port     = this.worker.port;
    this.messages$ = fromEvent<MessageEvent>(this.port, 'message').pipe(
      map(e => e.data),
      share()
    );
    this.port.start(); // required for SharedWorker
  }

  listen<T>(type: string): Observable<T> {
    return this.messages$.pipe(
      filter((msg: any) => msg.type === type),
      map((msg: any) => msg.payload as T)
    );
  }

  send(type: string, payload: unknown): void {
    this.port.postMessage({ type, payload });
  }
}

// Usage — all tabs share one worker, worker broadcasts to all connected tabs:
const shared = new SharedWorkerChannel(new URL('./shared.worker.ts', import.meta.url).href);

shared.listen<AppState>('STATE_UPDATE').pipe(
  takeUntilDestroyed()
).subscribe(state => applyStateUpdate(state));

// One tab computes, all tabs receive:
computeExpensiveResult$.subscribe(result => {
  shared.send('COMPUTATION_DONE', result);
});
```

---

## Pattern 5: Comlink Integration

[Comlink](https://github.com/GoogleChromeLabs/comlink) wraps worker methods as async functions. Wrap Comlink in Observables for full RxJS integration:

```typescript
import * as Comlink from 'comlink';

// worker.ts:
const api = {
  async processLargeArray(data: number[]): Promise<number[]> {
    return data.sort((a, b) => a - b);
  },
  async generateReport(params: ReportParams): Promise<ReportData> {
    return buildReport(params);
  }
};
Comlink.expose(api);

// main thread:
type WorkerApi = typeof api;

const worker    = new Worker(new URL('./worker.ts', import.meta.url));
const workerApi = Comlink.wrap<WorkerApi>(worker);

// Wrap Comlink promise in Observable for cancellation + RxJS composition:
function fromWorker<T>(task: () => Promise<T>): Observable<T> {
  return defer(() => task()); // defer ensures cold, lazy execution
}

// Usage:
fromWorker(() => workerApi.processLargeArray(data)).pipe(
  switchMap(sorted => renderObservable$(sorted)),
  takeUntilDestroyed()
).subscribe();

// Multiple parallel Comlink calls:
forkJoin({
  sorted: fromWorker(() => workerApi.processLargeArray(data)),
  report: fromWorker(() => workerApi.generateReport(params))
}).subscribe(({ sorted, report }) => renderDashboard(sorted, report));
```

---

## Common Pitfalls

### Transferable Objects Not Transferred

```typescript
// ❌ Copying large ArrayBuffer instead of transferring:
worker.postMessage({ data: largeBuffer });
// largeBuffer is serialised (copied) — expensive for large data

// ✅ Transfer ownership — zero-copy, O(1) regardless of size:
worker.postMessage({ data: largeBuffer }, [largeBuffer]);
// largeBuffer is now unusable in main thread — worker owns it
// For ImageData, AudioBuffer, etc.
```

### Worker Error Not Caught

```typescript
// ❌ Worker errors (uncaught exceptions) are silent:
const worker = new Worker(url);
worker.onmessage = e => handleResult(e.data); // no error handler

// ✅ Always handle both message and error events:
const worker$ = fromEvent<MessageEvent>(worker, 'message');
const error$  = fromEvent<ErrorEvent>(worker, 'error').pipe(
  map(e => { throw new Error(e.message); })
);

merge(worker$, error$).subscribe(handleResult);
```

### Not Terminating Workers on Component Destroy

```typescript
// ❌ Worker keeps running after Angular component is destroyed:
ngOnInit() {
  this.workerResult$ = this.channel.call('PROCESS', data);
}

// ✅ Terminate worker on destroy:
ngOnDestroy() {
  this.channel.terminate(); // calls worker.terminate()
}
// Or: use takeUntilDestroyed() + finalize(() => worker.terminate())
```
