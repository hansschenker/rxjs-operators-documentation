# RxJS in Svelte

Patterns for using RxJS in Svelte 5 and Svelte 4 applications: store integration, reactive declarations, lifecycle cleanup, and composable patterns.

---

## Why RxJS in Svelte?

Svelte has excellent built-in reactivity. RxJS adds value for:

- **Complex async pipelines** — debounce + switchMap + retry in one chain
- **Shared streams across components** — `shareReplay` for one HTTP request many consumers
- **WebSocket / SSE** — real-time data with auto-reconnect
- **Cross-framework code sharing** — RxJS services usable in Angular, React, Vue, and Svelte

---

## Svelte 5: RxJS with `$state` and Runes

Svelte 5's runes make RxJS integration straightforward:

```svelte
<!-- UserProfile.svelte (Svelte 5) -->
<script>
  import { onDestroy } from 'svelte';
  import { userService } from '$lib/services/userService';

  let user = $state(null);
  let loading = $state(true);

  const sub = userService.currentUser$.subscribe(u => {
    user = u;
    loading = false;
  });

  onDestroy(() => sub.unsubscribe());
</script>

{#if loading}
  <Spinner />
{:else if user}
  <h1>{user.name}</h1>
{/if}
```

---

## Utility: `fromObservable` Rune Helper

```typescript
// lib/rxjs.svelte.ts
import { onDestroy } from 'svelte';
import type { Observable } from 'rxjs';

export function fromObservable<T>(obs$: Observable<T>, initial: T) {
  let value = $state(initial);

  const sub = obs$.subscribe(v => { value = v; });
  onDestroy(() => sub.unsubscribe());

  return {
    get current() { return value; }
  };
}
```

```svelte
<!-- Usage: -->
<script>
  import { fromObservable } from '$lib/rxjs.svelte';
  import { priceService } from '$lib/services';

  const price = fromObservable(priceService.btcPrice$, 0);
</script>

<p>BTC: ${price.current.toFixed(2)}</p>
```

---

## Svelte 4: Readable Store from Observable

Svelte 4 stores and RxJS Observables share the same subscription contract (`subscribe(fn)` returning an unsubscribe function). You can use an Observable directly in a template with `$`:

```svelte
<!-- Svelte 4: Observable used like a store -->
<script>
  import { from, interval } from 'rxjs';
  import { map } from 'rxjs/operators';

  // Observable with .subscribe() returning a function — compatible with Svelte stores:
  const clock$ = interval(1000).pipe(
    map(() => new Date().toLocaleTimeString())
  );
  // Use $clock$ in template — Svelte auto-subscribes and unsubscribes
</script>

<p>Time: {$clock$}</p>
```

> **Important**: Svelte 4's `$store` syntax works with any object that has a `subscribe(fn)` method returning an unsubscribe function — which RxJS Observables satisfy. No adapter needed.

---

## Bridging: RxJS Observable → Svelte Readable

For explicit store creation with initial value:

```typescript
// lib/fromObservable.ts
import { readable, type Readable } from 'svelte/store';
import type { Observable } from 'rxjs';

export function fromObservable<T>(obs$: Observable<T>, initialValue: T): Readable<T> {
  return readable(initialValue, set => {
    const sub = obs$.subscribe(set);
    return () => sub.unsubscribe(); // Svelte calls this on last unsubscribe
  });
}
```

```svelte
<script>
  import { fromObservable } from '$lib/fromObservable';
  import { userService } from '$lib/services';

  const user = fromObservable(userService.currentUser$, null);
</script>

{#if $user}
  <p>Hello, {$user.name}</p>
{/if}
```

---

## Bridging: Svelte Writable → RxJS Observable

```typescript
// lib/fromWritable.ts
import { writable, type Writable } from 'svelte/store';
import { Observable } from 'rxjs';

export function fromWritable<T>(store: Writable<T>): Observable<T> {
  return new Observable<T>(subscriber => {
    const unsubscribe = store.subscribe(value => subscriber.next(value));
    return unsubscribe;
  });
}
```

---

## Pattern: Search with RxJS Pipeline

```svelte
<script lang="ts">
  import { onDestroy } from 'svelte';
  import { Subject } from 'rxjs';
  import { debounceTime, distinctUntilChanged, filter, switchMap, catchError } from 'rxjs/operators';
  import { of } from 'rxjs';

  let query = '';
  let results: Product[] = [];
  let searching = false;

  const query$ = new Subject<string>();

  const sub = query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    filter(q => q.length >= 2 || q.length === 0),
    switchMap(q => {
      if (!q) return of([] as Product[]);
      searching = true;
      return productApi.search(q).pipe(
        catchError(() => of([] as Product[]))
      );
    })
  ).subscribe(r => {
    results = r;
    searching = false;
  });

  onDestroy(() => sub.unsubscribe());

  $: query$.next(query); // react to query changes
</script>

<input bind:value={query} placeholder="Search products..." />
{#if searching}<Spinner />{/if}
<ProductList {results} />
```

---

## Pattern: Shared Service (Singleton Observable)

```typescript
// lib/services/priceService.ts
import { timer } from 'rxjs';
import { switchMap, shareReplay, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

class PriceService {
  // One polling stream, shared across all components:
  readonly prices$ = timer(0, 5000).pipe(
    switchMap(() => fetch('/api/prices').then(r => r.json())),
    catchError(() => of(null)),
    shareReplay(1) // all components share one fetch
  );
}

export const priceService = new PriceService();

// In any Svelte component:
// const prices = fromObservable(priceService.prices$, null);
// When last component unmounts and re-mounts: gets cached last price immediately
```

---

## Pattern: WebSocket in Svelte

```svelte
<script lang="ts">
  import { onDestroy } from 'svelte';
  import { webSocket } from 'rxjs/webSocket';
  import { retry, share } from 'rxjs/operators';
  import { timer } from 'rxjs';

  let messages: Message[] = [];
  let connected = false;

  const ws$ = webSocket<Message>({
    url: 'wss://api.example.com/ws',
    openObserver:  { next: () => { connected = true; } },
    closeObserver: { next: () => { connected = false; } }
  }).pipe(
    retry({ delay: (_, n) => timer(Math.min(1000 * 2 ** n, 30_000)) }),
    share()
  );

  const sub = ws$.subscribe(msg => {
    messages = [...messages, msg].slice(-100); // keep last 100
  });

  onDestroy(() => sub.unsubscribe());

  function send(text: string) {
    (ws$ as any).next({ type: 'message', text });
  }
</script>

<div class:connected>
  {#each messages as msg}<MessageBubble {msg} />{/each}
</div>
```

---

## Pattern: Polling with Pause/Resume

```svelte
<script lang="ts">
  import { onDestroy } from 'svelte';
  import { timer, BehaviorSubject } from 'rxjs';
  import { switchMap, filter, catchError } from 'rxjs/operators';
  import { of } from 'rxjs';

  let data: Status | null = null;
  let paused = false;

  const pause$ = new BehaviorSubject(false);

  const sub = pause$.pipe(
    switchMap(isPaused =>
      isPaused
        ? of(null) // emit null when paused (no polling)
        : timer(0, 5000).pipe(
            switchMap(() => statusApi.get().pipe(catchError(() => of(null))))
          )
    ),
    filter(v => v !== null)
  ).subscribe(result => { data = result; });

  onDestroy(() => sub.unsubscribe());

  function togglePause() {
    paused = !paused;
    pause$.next(paused);
  }
</script>

<button on:click={togglePause}>{paused ? 'Resume' : 'Pause'}</button>
<StatusDisplay {data} />
```

---

## Svelte Store vs RxJS — When to Use Each

| | Svelte Store | RxJS Observable |
|---|---|---|
| Simple reactive state | ✓ Perfect | Overkill |
| Derived values | `derived()` | `map()` / `combineLatest` |
| Async transformation chains | Verbose | ✓ Natural |
| Debounce / throttle | Manual | ✓ Built-in |
| WebSocket / SSE | Manual | ✓ Built-in |
| Error retry logic | Manual | ✓ Built-in |
| Shared across many components | `writable` + exports | `shareReplay(1)` |
| Framework-agnostic code | — | ✓ Portable |

---

## Common Pitfalls

### Missing `onDestroy` Cleanup

```svelte
<!-- ❌ Memory leak — subscription outlives component: -->
<script>
  import { interval } from 'rxjs';
  interval(1000).subscribe(updateClock); // never unsubscribed!
</script>

<!-- ✅ Always clean up: -->
<script>
  import { interval } from 'rxjs';
  import { onDestroy } from 'svelte';

  const sub = interval(1000).subscribe(updateClock);
  onDestroy(() => sub.unsubscribe());
</script>
```

### Svelte 4 `$obs$` Syntax With Subjects That Never Complete

```svelte
<!-- ✅ Works correctly — Svelte 4 manages subscribe/unsubscribe via store protocol: -->
<script>
  const count$ = new BehaviorSubject(0);
</script>
<p>{$count$}</p>
<!-- Svelte calls count$.subscribe() on mount, stores return value as unsubscribe fn -->
<!-- On component destroy, Svelte calls that unsubscribe fn automatically -->
```
