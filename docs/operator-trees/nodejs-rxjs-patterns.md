# Node.js + RxJS Patterns

RxJS in a Node.js environment — file I/O, EventEmitter interop, HTTP servers, worker threads, and stream processing.

---

## 1. EventEmitter → Observable

```typescript
import { fromEvent, fromEventPattern } from 'rxjs';
import { EventEmitter } from 'events';

const emitter = new EventEmitter();

// Simple case — fromEvent works with EventEmitter:
const data$ = fromEvent<Buffer>(emitter, 'data');
const error$ = fromEvent<Error>(emitter, 'error');

// For custom add/remove methods (e.g., once, prependListener):
const once$ = fromEventPattern<string>(
  handler => emitter.once('close', handler),
  handler => emitter.off('close', handler)
);
```

---

## 2. Node.js ReadableStream → Observable

```typescript
import { fromEvent, race, throwError } from 'rxjs';
import { takeUntil, mergeMap, map } from 'rxjs/operators';
import * as fs from 'fs';

function readableToObservable(stream: NodeJS.ReadableStream): Observable<Buffer> {
  return new Observable<Buffer>(subscriber => {
    const onData  = (chunk: Buffer) => subscriber.next(chunk);
    const onEnd   = ()              => subscriber.complete();
    const onError = (err: Error)    => subscriber.error(err);

    stream.on('data',  onData);
    stream.on('end',   onEnd);
    stream.on('error', onError);

    return () => {
      stream.off('data',  onData);
      stream.off('end',   onEnd);
      stream.off('error', onError);
      if (!stream.destroyed) stream.destroy();
    };
  });
}

// Usage:
readableToObservable(fs.createReadStream('./large-file.csv')).pipe(
  map(chunk => chunk.toString('utf8')),
  scan((acc, chunk) => acc + chunk, ''),
  last()
).subscribe(content => console.log('File loaded:', content.length, 'chars'));
```

---

## 3. File Processing Pipeline

```typescript
import * as fs from 'fs';
import * as readline from 'readline';
import { from, Observable } from 'rxjs';
import { mergeMap, filter, map, bufferCount } from 'rxjs/operators';

// Process a large CSV line by line without loading it all into memory:
function readLines(filePath: string): Observable<string> {
  return new Observable<string>(subscriber => {
    const rl = readline.createInterface({
      input:     fs.createReadStream(filePath),
      crlfDelay: Infinity
    });
    rl.on('line',  line => subscriber.next(line));
    rl.on('close', ()   => subscriber.complete());
    rl.on('error', err  => subscriber.error(err));
    return () => rl.close();
  });
}

// Process CSV: skip header, parse, filter, batch-insert
readLines('./users.csv').pipe(
  skip(1),                              // skip header row
  map(line => line.split(',') as [string, string, string]),
  map(([id, name, email]) => ({ id, name, email })),
  filter(user => user.email.includes('@')),
  bufferCount(100),                     // batch 100 rows
  mergeMap(batch => db.insertMany(batch), 4) // 4 concurrent inserts
).subscribe({
  next:     batch => console.log('Inserted', batch.length),
  error:    err   => console.error('Failed:', err),
  complete: ()    => console.log('CSV import complete')
});
```

---

## 4. HTTP Server Request Handler

```typescript
import * as http from 'http';
import { Subject, fromEvent, Observable } from 'rxjs';
import { mergeMap, map, takeUntil, filter } from 'rxjs/operators';

// Wrap Node HTTP server as Observable stream of requests:
function createServer(port: number): Observable<[http.IncomingMessage, http.ServerResponse]> {
  return new Observable(subscriber => {
    const server = http.createServer((req, res) => {
      subscriber.next([req, res]);
    });
    server.on('error', err => subscriber.error(err));
    server.listen(port, () => console.log(`Listening on ${port}`));
    return () => server.close();
  });
}

// Route and handle requests:
createServer(3000).pipe(
  filter(([req]) => req.method === 'GET'),
  mergeMap(([req, res]) =>
    handleRequest(req).pipe(
      map(data => ({ res, data })),
      catchError(err => {
        res.writeHead(500);
        res.end(JSON.stringify({ error: err.message }));
        return EMPTY;
      })
    )
  )
).subscribe(({ res, data }) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
});
```

---

## 5. Worker Threads with RxJS

```typescript
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { Subject, Observable } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Wrap a Worker as an Observable — send tasks, receive results:
function runInWorker<TInput, TResult>(
  workerFile: string,
  input: TInput
): Observable<TResult> {
  return new Observable<TResult>(subscriber => {
    const worker = new Worker(workerFile, { workerData: input });
    worker.on('message', result => { subscriber.next(result); subscriber.complete(); });
    worker.on('error',   err    => subscriber.error(err));
    return () => worker.terminate();
  });
}

// Parallelise CPU-intensive tasks across worker pool:
from(heavyDatasets).pipe(
  mergeMap(
    dataset => runInWorker<Dataset, ProcessedData>('./worker.js', dataset),
    4 // max 4 workers concurrently
  )
).subscribe(result => saveResult(result));
```

---

## 6. Child Process Output

```typescript
import { spawn } from 'child_process';
import { merge } from 'rxjs';
import { map } from 'rxjs/operators';

function spawnProcess(cmd: string, args: string[]): Observable<string> {
  return new Observable<string>(subscriber => {
    const proc = spawn(cmd, args);

    const stdout$ = readableToObservable(proc.stdout!).pipe(
      map(chunk => chunk.toString())
    );
    const stderr$ = readableToObservable(proc.stderr!).pipe(
      map(chunk => `[stderr] ${chunk.toString()}`)
    );

    const sub = merge(stdout$, stderr$).subscribe(subscriber);

    return () => { sub.unsubscribe(); proc.kill(); };
  });
}

// Stream output of a long-running process:
spawnProcess('npm', ['run', 'build']).pipe(
  filter(line => !line.startsWith('[stderr]')),
  takeUntil(buildComplete$)
).subscribe(line => buildLog.push(line));
```

---

## 7. Database Streaming (Cursor-Based)

```typescript
import { Observable } from 'rxjs';
import { bufferCount, mergeMap } from 'rxjs/operators';

// Stream rows from a database cursor without loading all into memory:
function streamQuery<T>(query: string, params: unknown[]): Observable<T> {
  return new Observable<T>(subscriber => {
    const cursor = db.query(query, params).cursor();
    cursor.on('data',  (row: T) => subscriber.next(row));
    cursor.on('end',   ()       => subscriber.complete());
    cursor.on('error', err      => subscriber.error(err));
    return () => cursor.close();
  });
}

// ETL: read → transform → batch write:
streamQuery<UserRow>('SELECT * FROM users WHERE migrated = false', []).pipe(
  map(row => transformUser(row)),
  bufferCount(500),
  mergeMap(batch => writeToNewDb(batch), 2) // 2 concurrent writes
).subscribe({
  complete: () => console.log('Migration complete')
});
```

---

## 8. File Watcher

```typescript
import * as fs from 'fs';
import { fromEvent } from 'rxjs';
import { debounceTime, filter, mergeMap } from 'rxjs/operators';

function watchFiles(pattern: string): Observable<string> {
  const watcher = fs.watch(pattern, { recursive: true });
  return fromEvent<[string, string]>(watcher, 'change').pipe(
    map(([, filename]) => filename),
    filter(Boolean),
    finalize(() => watcher.close())
  );
}

watchFiles('./src').pipe(
  filter(file => file.endsWith('.ts')),
  debounceTime(200),             // coalesce rapid saves
  mergeMap(file => runTests(file))
).subscribe(result => reportResult(result));
```

---

## 9. Rate-Limited External API Client

```typescript
import { concatMap, delay, from } from 'rxjs';

// Respect API rate limit: max 100 req/s → 10ms between requests
function rateLimitedFetch<T>(urls: string[]): Observable<T> {
  return from(urls).pipe(
    concatMap(url =>
      fetch(url).then(r => r.json() as Promise<T>)
    ),
    // No explicit delay needed if API latency > rate-limit interval.
    // For strict rate limiting:
  );
}

// Strict 10ms minimum gap:
from(apiUrls).pipe(
  mergeMap(
    url => from(fetch(url).then(r => r.json())),
    10  // max 10 concurrent requests
  ),
  concatMap(result => of(result).pipe(delay(10))) // throttle output
).subscribe(processResult);
```

---

## Common Node.js Pitfalls

### Not Cleaning Up Stream Listeners

```typescript
// ❌ LISTENER LEAK — no cleanup on unsubscribe
function fromEmitter<T>(ee: EventEmitter, event: string): Observable<T> {
  return new Observable<T>(sub => {
    ee.on(event, (v: T) => sub.next(v));
    // Missing: return teardown function!
  });
}

// ✅ Always return teardown:
function fromEmitter<T>(ee: EventEmitter, event: string): Observable<T> {
  return new Observable<T>(sub => {
    const handler = (v: T) => sub.next(v);
    ee.on(event, handler);
    return () => ee.off(event, handler);
    // WHY: Without teardown, the listener stays active after unsubscribe,
    // causing memory leaks and potential MaxListenersExceededWarning.
  });
}
```

### Backpressure — Producing Faster Than Consuming

```typescript
// ❌ Unbounded mergeMap on fast stream can exhaust memory
fastStream$.pipe(
  mergeMap(item => slowDbWrite(item)) // unlimited concurrency
).subscribe();

// ✅ Bound concurrency to create natural backpressure
fastStream$.pipe(
  mergeMap(item => slowDbWrite(item), 10) // max 10 concurrent writes
).subscribe();
// WHY: Without a concurrency limit, all items are processed simultaneously.
// With 10 concurrent writes, the upstream slows to match downstream capacity.
```
