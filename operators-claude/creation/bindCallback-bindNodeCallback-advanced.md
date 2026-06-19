# bindCallback / bindNodeCallback — Advanced Patterns

> **Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
> **Teaching Sequence**: After `fromEventPattern` — bridges the oldest JavaScript async pattern (callbacks) into the reactive world

---

## Advanced Behavioral Model

`bindCallback` and `bindNodeCallback` both return **Observable factories**, not Observables. The wrapped function is called at subscription time, making each subscription a fresh invocation.

```
const readFile$ = bindNodeCallback(fs.readFile);

// readFile$ is a factory — no I/O happens yet
const file1$ = readFile$('/etc/hosts', 'utf8');  // still no I/O
const file2$ = readFile$('/etc/passwd', 'utf8'); // still no I/O

file1$.subscribe(data => use(data)); // ← I/O starts here
file2$.subscribe(data => use(data)); // ← separate I/O call
```

**Key behavioral invariants:**

| | `bindCallback` | `bindNodeCallback` |
|---|---|---|
| Callback convention | `cb(...results)` | `cb(err, ...results)` |
| Error handling | Not built-in — errors via callback args | `err` → Observable error channel |
| Single result | Emits the value | Emits the value |
| Multiple results | Emits as array | Emits as array |
| Completes after | First callback invocation | First callback invocation |

**Multi-result behavior:**
```typescript
// If callback receives multiple args:
const fn = (cb: (a: string, b: number) => void) => cb('x', 42);
const fn$ = bindCallback(fn);
fn$().subscribe(result => console.log(result));
// Output: ['x', 42]  ← wrapped in array

// Single arg — emitted directly (not wrapped):
const fn2 = (cb: (a: string) => void) => cb('x');
const fn2$ = bindCallback(fn2);
fn2$().subscribe(result => console.log(result));
// Output: 'x'
```

---

## Type System Integration

```typescript
import { bindCallback, bindNodeCallback } from 'rxjs';

// bindCallback: requires explicit type parameter for safety
type GetCurrentPosition = (
  cb: (position: GeolocationPosition) => void
) => void;

const getCurrentPosition$ = bindCallback(
  navigator.geolocation.getCurrentPosition.bind(navigator.geolocation) as GetCurrentPosition
);
// Returns: () => Observable<GeolocationPosition>

// bindNodeCallback: err is automatically extracted
import { readFile } from 'fs';

const readFile$ = bindNodeCallback(readFile);
// Returns: (path: PathLike, options: ...) => Observable<Buffer | string>

// Typed with overloads:
const readFileUtf8$ = bindNodeCallback(
  (path: string, encoding: BufferEncoding, cb: (err: NodeJS.ErrnoException | null, data: string) => void) =>
    readFile(path, encoding, cb)
);
// Returns: (path: string, encoding: BufferEncoding) => Observable<string>

// With scheduler for deterministic testing
import { asyncScheduler } from 'rxjs';
const scheduledRead$ = bindNodeCallback(readFile, asyncScheduler);
```

---

## Advanced Patterns

### 1. Migrating a Callback API Layer to Observables

When wrapping an entire module of callback-based functions, create a mapped façade.

```typescript
import { bindNodeCallback } from 'rxjs';
import * as fs from 'fs';

// Reactive filesystem façade
const rxFs = {
  readFile: bindNodeCallback(fs.readFile),
  writeFile: bindNodeCallback(fs.writeFile),
  readdir:   bindNodeCallback(fs.readdir),
  stat:      bindNodeCallback(fs.stat),
  unlink:    bindNodeCallback(fs.unlink),
};

// Usage: chainable, composable, cancellation-aware
rxFs.readFile('/config.json', 'utf8').pipe(
  map(content => JSON.parse(content as string)),
  switchMap(config => rxFs.writeFile('/config.backup.json', JSON.stringify(config))),
).subscribe({
  complete: () => console.log('backup written'),
  error: err => console.error('failed:', err),
});
```

### 2. Parallel File Processing with mergeMap

Transform sequential callback-based file reads into concurrent reactive pipelines.

```typescript
import { bindNodeCallback, from } from 'rxjs';
import { mergeMap, map, toArray, filter } from 'rxjs/operators';
import * as fs from 'fs';

const readFile$ = bindNodeCallback(fs.readFile);
const stat$     = bindNodeCallback(fs.stat);

function processDirectory(dir: string, pattern: RegExp): Observable<ProcessedFile> {
  const readdir$ = bindNodeCallback(fs.readdir);

  return readdir$(dir).pipe(
    map(files => (files as string[]).filter(f => pattern.test(f))),
    mergeMap(files => from(files)),
    mergeMap(filename =>
      readFile$(`${dir}/${filename}`, 'utf8').pipe(
        map(content => ({
          filename,
          content: content as string,
          lineCount: (content as string).split('\n').length,
        }))
      ),
      4 // concurrency: process 4 files at a time
    ),
    filter(file => file.lineCount > 0),
    toArray(),
  );
}

processDirectory('./src', /\.ts$/).subscribe(files => {
  console.log(`Processed ${files.length} TypeScript files`);
});
```

### 3. Geolocation with Timeout and Fallback

`bindCallback` wraps browser APIs that use plain callbacks — add reactive operators for timeout and fallback.

```typescript
import { bindCallback, of } from 'rxjs';
import { timeout, catchError, map } from 'rxjs/operators';

interface Coordinates { lat: number; lng: number }

type GeoSuccess = (pos: GeolocationPosition) => void;
type GeoError = (err: GeolocationPositionError) => void;

// Note: getCurrentPosition takes success + optional error callbacks
// bindCallback wraps the success path; errors need special handling
const getCurrentPosition$ = bindCallback(
  (cb: GeoSuccess) =>
    navigator.geolocation.getCurrentPosition(cb, () => cb(null as any))
);

function getUserLocation(defaultLocation: Coordinates): Observable<Coordinates> {
  return getCurrentPosition$().pipe(
    map(pos => {
      if (!pos) throw new Error('Geolocation denied');
      return { lat: pos.coords.latitude, lng: pos.coords.longitude };
    }),
    timeout(5000),
    catchError(() => of(defaultLocation)),
  );
}

getUserLocation({ lat: 51.5, lng: -0.1 })
  .subscribe(coords => showMap(coords));
```

### 4. Node.js crypto with bindNodeCallback

Wrap async crypto operations for composable, non-blocking cryptography.

```typescript
import { bindNodeCallback, forkJoin } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';
import { randomBytes, pbkdf2, scrypt } from 'crypto';

const randomBytes$ = bindNodeCallback(randomBytes);
const pbkdf2$ = bindNodeCallback(pbkdf2);

function hashPassword(password: string): Observable<{ hash: string; salt: string }> {
  return randomBytes$(32).pipe(
    switchMap(salt =>
      pbkdf2$(password, salt, 100_000, 64, 'sha512').pipe(
        map(derivedKey => ({
          hash: (derivedKey as Buffer).toString('hex'),
          salt: (salt as Buffer).toString('hex'),
        }))
      )
    ),
  );
}

function verifyPassword(
  password: string,
  storedHash: string,
  storedSalt: string,
): Observable<boolean> {
  const saltBuffer = Buffer.from(storedSalt, 'hex');

  return pbkdf2$(password, saltBuffer, 100_000, 64, 'sha512').pipe(
    map(derivedKey => (derivedKey as Buffer).toString('hex') === storedHash),
  );
}

// Usage
hashPassword('mySecret').pipe(
  switchMap(({ hash, salt }) => verifyPassword('mySecret', hash, salt)),
).subscribe(valid => console.log('valid:', valid));
// Output: valid: true
```

### 5. Scheduler Injection for Testable Callback Wrappers

Pass a scheduler to make callback-based code deterministic in tests.

```typescript
import { bindNodeCallback, TestScheduler } from 'rxjs';
import { delay } from 'rxjs/operators';

// Production: uses default async behavior
const prodReadFile$ = bindNodeCallback(fs.readFile);

// Test: inject TestScheduler for virtual time control
describe('file processing pipeline', () => {
  it('processes file content', () => {
    const testScheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });

    // Fake callback function for testing
    const fakeReadFile = (
      path: string,
      encoding: string,
      cb: (err: null, data: string) => void
    ) => setTimeout(() => cb(null, 'file content'), 100);

    const testReadFile$ = bindNodeCallback(fakeReadFile, testScheduler);

    testScheduler.run(({ expectObservable }) => {
      const result$ = testReadFile$('/test.txt', 'utf8');
      expectObservable(result$).toBe('100ms (x|)', { x: 'file content' });
    });
  });
});
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — using bindCallback for Node.js error-first callbacks
const readFile$ = bindCallback(fs.readFile);
readFile$('/nonexistent.txt', 'utf8').subscribe({
  next: result => console.log(result),
  // If file doesn't exist, result is [ErrnoException, undefined]
  // Error never reaches the error channel!
});

// ✅ CORRECT — use bindNodeCallback for (err, result) callbacks
const readFile$ = bindNodeCallback(fs.readFile);
readFile$('/nonexistent.txt', 'utf8').subscribe({
  next: data => console.log(data),
  error: err => console.error('File error:', err), // ErrnoException here
});
// WHY: bindNodeCallback understands the (err, result) convention and
// routes err to the Observable's error channel automatically.


// ❌ INCORRECT — calling the factory and treating it as an Observable
const factory = bindNodeCallback(fs.readFile);
factory.subscribe(); // TypeError: factory.subscribe is not a function

// ✅ CORRECT — call the factory with arguments to get an Observable
const factory = bindNodeCallback(fs.readFile);
factory('/etc/hosts', 'utf8').subscribe(data => use(data));
// WHY: bindNodeCallback returns a FUNCTION (factory), not an Observable.
// You must call it with arguments to produce the Observable.


// ❌ INCORRECT — forgetting .bind() when wrapping object methods
const readFile$ = bindNodeCallback(fs.readFile);
// fs.readFile loses its `this` context — may work for fs but fails
// for APIs that rely on `this` internally.

// ✅ CORRECT — bind the method or use an arrow wrapper
const readFile$ = bindNodeCallback(fs.readFile.bind(fs));
// OR
const readFile$ = bindNodeCallback(
  (path: string, enc: BufferEncoding, cb: Parameters<typeof fs.readFile>[2]) =>
    fs.readFile(path, enc, cb)
);
// WHY: bindCallback/bindNodeCallback calls the function — it doesn't
// know about method context. Bind or arrow-wrap to preserve `this`.
```

---

## Migration Path: Callbacks → Promises → Observables

```typescript
// Callback (legacy)
fs.readFile('/data.json', 'utf8', (err, data) => {
  if (err) return handleError(err);
  process(data);
});

// Promise (intermediate)
fs.promises.readFile('/data.json', 'utf8')
  .then(process)
  .catch(handleError);

// Observable via bindNodeCallback (reactive, composable)
const readFile$ = bindNodeCallback(fs.readFile);
readFile$('/data.json', 'utf8').pipe(
  map(data => JSON.parse(data as string)),
  retry(3),
  timeout(5000),
).subscribe({ next: process, error: handleError });

// Observable via from + Promise (simplest when Promise API exists)
from(fs.promises.readFile('/data.json', 'utf8')).pipe(
  // same operators
).subscribe({ next: process, error: handleError });
```

**Prefer `from(promise)` when a Promise API already exists. Use `bindNodeCallback`/`bindCallback` only when wrapping callback-only APIs.**

---

## Related Operators

- **`fromEventPattern`** — for add/remove listener APIs (event-based, not single-shot)
- **`from`** — wraps Promises directly; simpler when a Promise API is available
- **`defer`** — lazy Observable creation without callback wrapping
- **`scheduled`** — inject schedulers into creation; composable with bind* operators
