# from — Advanced Patterns

For fundamentals see the core [from](./from) doc. This page covers `from()` with every iterable type, async generator composition, lazy conversion strategies, and the subtle differences between `from`, `of`, and `defer`.

---

## Mental Model: What `from()` Accepts

```typescript
import { from } from 'rxjs';

// from() converts any "iterable-like" into an Observable:

// 1. Array (or any Iterable):
from([1, 2, 3]).subscribe(console.log); // 1, 2, 3 synchronously

// 2. Promise:
from(fetch('/api/data').then(r => r.json())).subscribe(console.log);
// Emits once when promise resolves, then completes

// 3. AsyncIterable (includes ReadableStream, async generators):
async function* gen() { yield 1; yield 2; }
from(gen()).subscribe(console.log); // 1, 2 asynchronously

// 4. Observable (passthrough — for interop):
from(someObservable$).subscribe(); // same as someObservable$.subscribe()

// 5. String (iterable of characters):
from('RxJS').subscribe(console.log); // 'R', 'x', 'J', 'S'
```

**Key characteristic**: `from()` is synchronous for synchronous iterables (arrays, Sets, Maps, strings) and asynchronous for Promises and AsyncIterables. This matters for scheduler composition.

---

## Pattern 1: `from` with Generators (Lazy Sequences)

Generators produce values on demand — `from()` pulls them lazily:

```typescript
import { from, take } from 'rxjs';

// Infinite generator — safe with take():
function* naturals(start = 0): Generator<number> {
  let n = start;
  while (true) yield n++;
}

from(naturals()).pipe(
  take(1000), // stops pulling after 1000 values
  filter(n => n % 2 === 0),
  toArray()
).subscribe(evens => console.log(`First 500 evens: ${evens.length}`));

// Fibonacci sequence generator:
function* fibonacci(): Generator<number> {
  let [a, b] = [0, 1];
  while (true) {
    yield a;
    [a, b] = [b, a + b];
  }
}

from(fibonacci()).pipe(
  takeWhile(n => n < 1_000_000),
  toArray()
).subscribe(fibs => console.log(fibs));

// Recursive tree traversal as a generator:
function* depthFirst(node: TreeNode): Generator<TreeNode> {
  yield node;
  for (const child of node.children) {
    yield* depthFirst(child);
  }
}

from(depthFirst(rootNode)).pipe(
  filter(n => n.type === 'leaf'),
  map(n => n.value),
  toArray()
).subscribe(leaves => processLeaves(leaves));
```

---

## Pattern 2: `from` with Async Generators — Bridging Async Iteration

```typescript
import { from } from 'rxjs';
import { mergeMap, map } from 'rxjs/operators';

// Async generator that pages through an API:
async function* paginate<T>(
  url: string,
  pageSize = 100
): AsyncGenerator<T[]> {
  let cursor: string | null = null;

  while (true) {
    const params = new URLSearchParams({ limit: String(pageSize) });
    if (cursor) params.set('cursor', cursor);

    const response = await fetch(`${url}?${params}`);
    const page: { items: T[]; nextCursor: string | null } = await response.json();

    yield page.items;

    if (!page.nextCursor) break;
    cursor = page.nextCursor;
  }
}

// Stream all users page by page, flattening pages into individual items:
from(paginate<User>('/api/users')).pipe(
  mergeMap(page => from(page)), // flatten page arrays into individual User emissions
  filter(user => user.active),
  take(500),
  toArray()
).subscribe(activeUsers => renderUserTable(activeUsers));

// Process CSV file line by line using async generator:
async function* readLines(filePath: string): AsyncGenerator<string> {
  const file   = await fs.promises.open(filePath, 'r');
  const stream = file.createReadStream({ encoding: 'utf8' });
  let buffer = '';

  for await (const chunk of stream) {
    buffer += chunk;
    const lines = buffer.split('\n');
    buffer = lines.pop()!;
    yield* lines;
  }
  if (buffer) yield buffer;
  await file.close();
}

from(readLines('/data/large-import.csv')).pipe(
  skip(1), // skip header
  map(line => line.split(',')),
  filter(cols => cols.length === 5),
  map(([id, name, email, role, dept]) => ({ id, name, email, role, dept })),
  bufferCount(100),
  concatMap(batch => db.insertMany(batch))
).subscribe({
  complete: () => console.log('Import complete'),
  error:    err => console.error('Import failed:', err)
});
```

---

## Pattern 3: `from(Promise)` vs `defer(() => from(Promise))`

The most important `from` subtlety: Promises execute eagerly, `defer` makes them lazy:

```typescript
import { from, defer } from 'rxjs';

// ❌ Eager Promise — starts executing immediately on creation:
const eager$ = from(fetch('/api/data')); // fetch starts NOW
// Even if nobody subscribes yet, the HTTP request is already in flight

// ✅ Lazy Promise via defer — only executes when subscribed:
const lazy$ = defer(() => from(fetch('/api/data'))); // fetch starts on subscribe

// The difference matters for:
// 1. Multiple subscribers — eager$ shares one fetch; lazy$ creates a new fetch per subscriber
// 2. Conditional execution — with eager$, side effects already happened before you check conditions
// 3. Retry — retry() re-subscribes; with eager$ the same resolved promise is reused; with defer each retry re-creates the fetch

// Practical retry example:
const withRetry$ = defer(() => from(
  fetch('/api/unstable-endpoint').then(r => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  })
)).pipe(
  retry({ count: 3, delay: (_, n) => timer(1000 * n) })
);
// Each retry attempt creates a new fetch() call — correct behavior
// Without defer, retry would just re-emit the same rejected promise value

// Factory pattern — one function, lazy by design:
function getUser$(id: string): Observable<User> {
  return defer(() => from(
    fetch(`/api/users/${id}`).then(r => r.json() as Promise<User>)
  ));
}
// Safe to call getUser$() without subscribing — nothing happens until subscribe()
```

---

## Pattern 4: `from` with Sets and Maps

`from()` works with any ES6 iterable — including `Set` and `Map`:

```typescript
import { from } from 'rxjs';
import { map, distinct } from 'rxjs/operators';

// Set → Observable of unique values:
const uniqueIds = new Set(['a', 'b', 'c', 'a', 'b']);
from(uniqueIds).subscribe(console.log); // 'a', 'b', 'c' (Set already deduped)

// Map → Observable of [key, value] pairs:
const userMap = new Map([
  ['user1', { name: 'Alice', role: 'admin' }],
  ['user2', { name: 'Bob',   role: 'user'  }],
]);

from(userMap).pipe(
  map(([id, user]) => ({ id, ...user }))
).subscribe(user => console.log(user));
// { id: 'user1', name: 'Alice', role: 'admin' }
// { id: 'user2', name: 'Bob',   role: 'user'  }

// Map.keys(), Map.values(), Map.entries() are all iterable:
from(userMap.values()).pipe(
  filter(user => user.role === 'admin')
).subscribe(admin => sendAdminWelcome(admin));

// Real-world: process deduplicated IDs from a live Set:
const processedIds = new Set<string>();

incomingEvents$.pipe(
  filter(event => !processedIds.has(event.id)),
  tap(event => processedIds.add(event.id)),
  mergeMap(event => processEvent(event))
).subscribe(result => handleResult(result));
```

---

## Pattern 5: Converting Callback-Based APIs with `from`

```typescript
import { from, Observable } from 'rxjs';

// IndexedDB request → Observable via Promise wrapper:
function idbGet<T>(store: IDBObjectStore, key: IDBValidKey): Observable<T> {
  return from(new Promise<T>((resolve, reject) => {
    const request = store.get(key);
    request.onsuccess = () => resolve(request.result);
    request.onerror   = () => reject(request.error);
  }));
}

// FileReader → Observable via Promise:
function readFileAsText(file: File): Observable<string> {
  return from(new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload  = () => resolve(reader.result as string);
    reader.onerror = () => reject(reader.error);
    reader.readAsText(file);
  }));
}

// Geolocation → Observable via Promise:
function getCurrentPosition$(): Observable<GeolocationPosition> {
  return defer(() => from(
    new Promise<GeolocationPosition>((resolve, reject) =>
      navigator.geolocation.getCurrentPosition(resolve, reject)
    )
  ));
}

// Image load → Observable:
function loadImage$(src: string): Observable<HTMLImageElement> {
  return defer(() => from(
    new Promise<HTMLImageElement>((resolve, reject) => {
      const img = new Image();
      img.onload  = () => resolve(img);
      img.onerror = () => reject(new Error(`Failed to load: ${src}`));
      img.src = src;
    })
  ));
}

// Process multiple files with concurrency control:
fileList$.pipe(
  mergeMap(file => readFileAsText(file), 3), // max 3 concurrent reads
  map(text => parseCSV(text)),
  mergeAll(), // flatten parsed rows
  bufferCount(500),
  concatMap(batch => db.insertBatch(batch))
).subscribe();
```

---

## Pattern 6: `from` vs `of` — When to Use Each

```typescript
// from([1, 2, 3]) — iterates the array, emits each element separately:
from([1, 2, 3]).pipe(toArray()).subscribe(v => console.log(v)); // [1, 2, 3]
// Emits: 1, then 2, then 3

// of([1, 2, 3]) — emits the array AS A SINGLE VALUE:
of([1, 2, 3]).pipe(toArray()).subscribe(v => console.log(v)); // [[1, 2, 3]]
// Emits: [1, 2, 3] as one item

// of(1, 2, 3) — variadic, emits each argument:
of(1, 2, 3).pipe(toArray()).subscribe(v => console.log(v)); // [1, 2, 3]
// Emits: 1, then 2, then 3 (same as from([1, 2, 3]))

// Decision rule:
// Use from() when you have one collection to iterate
// Use of() when you have individual values (or want to emit one array as-is)

// Practical: converting a Map of grouped items to per-item stream:
const grouped = new Map([['fruits', ['apple', 'banana']], ['vegs', ['carrot']]]);

from(grouped.values()).pipe(
  mergeMap(items => from(items)), // flatten: Map values → arrays → individual items
).subscribe(item => console.log(item)); // 'apple', 'banana', 'carrot'

// vs of() which keeps structure:
of(...grouped.values()).subscribe(group => processGroup(group)); // ['apple', 'banana'], ['carrot']
```

---

## Common Pitfalls

### `from(Promise)` Does Not Retry on Re-Subscribe

```typescript
// ❌ Retry re-subscribes to the Observable, but the Promise is already resolved:
const alreadyFetched$ = from(fetch('/api/data')); // Promise created NOW

alreadyFetched$.pipe(
  retry(3) // re-subscribes 3 times, but same resolved/rejected promise — no new fetches
).subscribe();

// ✅ Wrap in defer to create a new Promise on each subscribe:
defer(() => from(fetch('/api/data'))).pipe(
  retry(3) // each retry creates a brand new fetch()
).subscribe();
```

### `from(string)` Iterates Characters, Not the Whole String

```typescript
// ❌ Expecting from() to emit the whole string:
from('hello').subscribe(console.log);
// Emits: 'h', 'e', 'l', 'l', 'o'  ← not what you wanted

// ✅ Wrap string in an array if you want a single emission:
from(['hello']).subscribe(console.log); // 'hello'
of('hello').subscribe(console.log);     // 'hello'
```

### Synchronous `from([])` vs Asynchronous `from(asyncGen())`

```typescript
// from(array) is synchronous — subscriber.next() called before subscribe() returns:
let emitted = false;
from([1, 2, 3]).subscribe(() => { emitted = true; });
console.log(emitted); // true — already ran synchronously

// from(asyncGen()) is asynchronous — values arrive in future microtasks:
emitted = false;
from(asyncGenerator()).subscribe(() => { emitted = true; });
console.log(emitted); // false — not yet run
// Implication: don't assume synchronous execution with from(asyncIterable$)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**Key insight**: `from()` is the universal adapter — arrays, Promises, generators, AsyncIterables, ReadableStreams, Sets, Maps all become Observables with one call. The single most important pattern to internalize: always wrap `from(promise)` in `defer()` when the Observable needs to be retried, shared lazily, or created but not immediately subscribed. Without `defer`, the Promise executes eagerly at creation time, not subscription time.
