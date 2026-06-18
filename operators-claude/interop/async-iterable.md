# RxJS ↔ Async Iterable Interop

## Identity

- **Category**: Interop
- **Operators**: `from(asyncIterable)`, `lastValueFrom`, `firstValueFrom`, async generator bridging
- **Added**: Full async iterable support in RxJS 7.0

## Overview

RxJS 7 supports `AsyncIterable` natively as an input to `from()`. This bridges the gap between the two major async abstractions in modern JavaScript:

| | Observable | AsyncIterable |
|---|---|---|
| Push vs Pull | Push (producer drives) | Pull (consumer drives) |
| Cancellation | Unsubscribe | `return()` on iterator |
| Operators | Full RxJS pipeline | `for await...of`, generator helpers |
| Multicasting | share/shareReplay | Each iterator is independent |
| Error handling | `catchError`, `retry` | try/catch in `for await` |

---

## `from(asyncIterable)` — Async Iterable → Observable

```typescript
import { from } from 'rxjs';

// Async generator → Observable:
async function* generatePages(url: string) {
  let cursor: string | null = null;
  do {
    const response = await fetch(`${url}?cursor=${cursor ?? ''}`);
    const data = await response.json();
    yield data.items;
    cursor = data.nextCursor;
  } while (cursor);
}

from(generatePages('/api/items')).pipe(
  mergeAll(),      // flatten page arrays into individual items
  filter(isActive),
  take(100)
).subscribe(render);
```

### `ReadableStream` → Observable

```typescript
import { from } from 'rxjs';

// Fetch streaming response:
const response = await fetch('/api/stream');
const stream   = response.body!.pipeThrough(new TextDecoderStream());

// ReadableStream is AsyncIterable in modern browsers:
from(stream).pipe(
  scan((acc, chunk) => acc + chunk, ''),
  debounceTime(0)
).subscribe(partialText => renderProgressively(partialText));
```

---

## Observable → Async Iterable (`for await...of`)

An Observable can be consumed with `for await...of` — the Observable must complete for the loop to finish.

```typescript
import { interval, take } from 'rxjs';

// Consume an Observable in async/await style:
async function processAll() {
  for await (const value of interval(100).pipe(take(5))) {
    console.log(value); // 0, 1, 2, 3, 4
    await doSomethingAsync(value);
  }
  console.log('done');
}
```

**Important**: `for await...of` on an Observable that never completes will loop forever. Always use `take`, `takeUntil`, or `takeWhile` to bound it.

---

## Bridging with `lastValueFrom` / `firstValueFrom`

```typescript
import { firstValueFrom, lastValueFrom } from 'rxjs';

// Use RxJS pipeline then get result as Promise (for async/await):
async function getTopUsers(limit: number): Promise<User[]> {
  return lastValueFrom(
    userStream$.pipe(
      filter(u => u.score > 100),
      take(limit),
      toArray()
    )
  );
}

// With a timeout:
async function fetchWithTimeout<T>(source$: Observable<T>, ms: number): Promise<T> {
  return firstValueFrom(
    source$.pipe(timeout(ms))
  );
}
```

---

## Async Generator as Observable Source

```typescript
import { from } from 'rxjs';

// Server-Sent Events via async generator:
async function* sseStream(url: string): AsyncGenerator<string> {
  const response = await fetch(url, { headers: { Accept: 'text/event-stream' } });
  const reader   = response.body!.getReader();
  const decoder  = new TextDecoder();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      yield decoder.decode(value);
    }
  } finally {
    reader.cancel(); // cleanup when Observable unsubscribes
  }
}

from(sseStream('/api/events')).pipe(
  map(parseSSELine),
  filter(Boolean),
  takeUntil(destroy$)
).subscribe(handleEvent);
```

---

## Converting Observable to AsyncIterable

```typescript
// Observables are AsyncIterable — use directly in for await loops:
const source$ = interval(500).pipe(take(10));

for await (const v of source$) {
  await processAsync(v);
}

// Or consume with Symbol.asyncIterator explicitly:
const iterator = source$[Symbol.asyncIterator]();
const { value } = await iterator.next(); // first value
await iterator.return?.(); // cancel subscription early
```

---

## Parallel Async Iterables with RxJS

```typescript
import { from, merge } from 'rxjs';

// Run multiple async generators in parallel:
async function* streamA() { /* ... */ }
async function* streamB() { /* ... */ }

merge(
  from(streamA()),
  from(streamB())
).pipe(
  mergeMap(item => processItem(item))
).subscribe(handleResult);
// Both generators run concurrently; results interleaved in arrival order
```

---

## Common Pitfalls

### Consuming an Infinite Observable with `for await`

```typescript
// ❌ INFINITE LOOP — interval never completes
for await (const v of interval(100)) {
  console.log(v); // runs forever
}

// ✅ Always bound with take/takeUntil:
for await (const v of interval(100).pipe(take(5))) {
  console.log(v); // 0, 1, 2, 3, 4 — then exits
}
// WHY: for await...of exits when the iterator is done (Observable completes).
// Infinite Observables never complete, so the loop never exits.
```

### `from(asyncIterable)` Is Cold — Each Subscribe Restarts the Generator

```typescript
async function* counter() {
  let i = 0;
  while (true) yield i++;
}

const counter$ = from(counter()).pipe(take(3));

counter$.subscribe(v => console.log('A:', v)); // 0, 1, 2
counter$.subscribe(v => console.log('B:', v)); // 0, 1, 2 — fresh generator!

// WHY: from() creates a cold Observable. Each subscription calls the
// async generator factory independently. If you need sharing, use share()
// or shareReplay() — but note that sharing an async generator between
// subscribers means only one consumer pulls values.
```

---

## Related

- **`from`**: Accepts AsyncIterable in addition to arrays, Promises, and Observables
- **`firstValueFrom`** / **`lastValueFrom`**: Observable → Promise bridge
- **`scheduled`**: Emit async iterable values with a specific scheduler
- **`webSocket`**: Push-based alternative to SSE for browser real-time streaming

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Teaching note**: Teach after `from()` basics. The key mental model: `from(asyncIterable)` = pull-to-push adapter (RxJS drives the pull via subscription).
