# RxJS in Vue

Patterns for using RxJS in Vue 3 applications: composables, reactive integration, Pinia store patterns, and cleanup.

---

## Why RxJS in Vue?

Vue's `ref`/`computed`/`watch` handle most reactivity needs. RxJS adds value for:

- **Complex async coordination** — debounce + switchMap + retry in one pipeline
- **Cross-component streams** — shared Observables with `shareReplay`
- **Real-time data** — WebSocket with auto-reconnect
- **Operator pipelines** — combining streams that Vue's reactivity can't express naturally

---

## Core Pattern: `useObservable` Composable

```typescript
// composables/useObservable.ts
import { ref, onUnmounted, type Ref } from 'vue';
import type { Observable } from 'rxjs';

export function useObservable<T>(
  observable$: Observable<T>,
  initialValue: T
): Ref<T> {
  const value = ref<T>(initialValue) as Ref<T>;

  const subscription = observable$.subscribe(v => {
    value.value = v;
  });

  onUnmounted(() => subscription.unsubscribe());

  return value;
}

// Usage:
// <script setup>
import { useObservable } from '@/composables/useObservable';
import { userService } from '@/services/userService';

const user = useObservable(userService.currentUser$, null);
// </script>
// <template>{{ user?.name }}</template>
```

---

## Composable: `useObservableState` (loading / error)

```typescript
// composables/useObservableState.ts
import { ref, onUnmounted, type Ref } from 'vue';
import type { Observable } from 'rxjs';

interface AsyncState<T> {
  data:    T | null;
  loading: boolean;
  error:   string | null;
}

export function useObservableState<T>(
  factory: () => Observable<T>
): { data: Ref<T | null>; loading: Ref<boolean>; error: Ref<string | null> } {
  const data    = ref<T | null>(null) as Ref<T | null>;
  const loading = ref(true);
  const error   = ref<string | null>(null);

  const sub = factory().subscribe({
    next:     v   => { data.value = v; loading.value = false; },
    error:    err => { error.value = err.message; loading.value = false; },
    complete: ()  => { loading.value = false; }
  });

  onUnmounted(() => sub.unsubscribe());

  return { data, loading, error };
}

// Usage:
// <script setup>
const { data: products, loading, error } = useObservableState(
  () => productService.getProducts()
);
// </script>
```

---

## Composable: `useSearchObservable`

```typescript
// composables/useSearch.ts
import { ref, watch, onUnmounted } from 'vue';
import { Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

export function useSearch<T>(searchFn: (q: string) => Observable<T[]>) {
  const query   = ref('');
  const results = ref<T[]>([]);
  const loading = ref(false);

  const query$ = new Subject<string>();

  const sub = query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    filter(q => q.length >= 2 || q.length === 0),
    switchMap(q => {
      if (!q) return of([] as T[]);
      loading.value = true;
      return searchFn(q).pipe(catchError(() => of([] as T[])));
    })
  ).subscribe(r => {
    results.value = r;
    loading.value = false;
  });

  // Bridge Vue ref to RxJS Subject:
  watch(query, q => query$.next(q));

  onUnmounted(() => sub.unsubscribe());

  return { query, results, loading };
}

// Usage in component:
// const { query, results, loading } = useSearch(q => productApi.search(q));
// <input v-model="query" />
// <product-list :items="results" />
```

---

## Pattern: Shared Service (singleton Observable)

```typescript
// services/userService.ts
import { BehaviorSubject, Observable } from 'rxjs';
import { switchMap, shareReplay } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';
import { of } from 'rxjs';

class UserService {
  private userId$ = new BehaviorSubject<string | null>(null);

  readonly currentUser$: Observable<User | null> = this.userId$.pipe(
    switchMap(id =>
      id ? ajax.getJSON<User>(`/api/users/${id}`) : of(null)
    ),
    shareReplay(1) // shared across all components — one HTTP request
  );

  setUser(id: string) { this.userId$.next(id); }
  logout()            { this.userId$.next(null); }
}

export const userService = new UserService();

// In any component:
// const user = useObservable(userService.currentUser$, null);
// Works across Header, Profile, Settings — all share one subscription
```

---

## Pattern: Pinia + RxJS

Combine Pinia stores with RxJS for complex async logic:

```typescript
// stores/products.ts
import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import { Subject } from 'rxjs';
import { switchMap, catchError, tap } from 'rxjs/operators';
import { of } from 'rxjs';

export const useProductStore = defineStore('products', () => {
  // State
  const items   = ref<Product[]>([]);
  const loading = ref(false);
  const error   = ref<string | null>(null);

  // Internal RxJS stream for search:
  const search$ = new Subject<string>();

  const searchSub = search$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    tap(() => { loading.value = true; error.value = null; }),
    switchMap(q =>
      productApi.search(q).pipe(
        catchError(err => {
          error.value = err.message;
          return of([] as Product[]);
        })
      )
    ),
    tap(() => { loading.value = false; })
  ).subscribe(results => { items.value = results; });

  // Actions
  function search(query: string) { search$.next(query); }
  function cleanup() { searchSub.unsubscribe(); }

  // Computed
  const activeItems = computed(() => items.value.filter(p => p.active));

  return { items, loading, error, activeItems, search, cleanup };
});
```

---

## Pattern: Reactive Route Params

React to Vue Router param changes:

```typescript
// composables/useRouteParam.ts
import { watch, ref, onUnmounted } from 'vue';
import { useRoute }   from 'vue-router';
import { Subject, switchMap } from 'rxjs';

export function useRouteParamData<T>(
  paramName: string,
  fetchFn: (id: string) => Observable<T>
) {
  const route = useRoute();
  const data  = ref<T | null>(null);
  const param$ = new Subject<string>();

  const sub = param$.pipe(
    distinctUntilChanged(),
    switchMap(id => fetchFn(id).pipe(catchError(() => of(null))))
  ).subscribe(result => { data.value = result; });

  watch(
    () => route.params[paramName] as string,
    id => { if (id) param$.next(id); },
    { immediate: true }
  );

  onUnmounted(() => sub.unsubscribe());
  return data;
}

// Usage:
// const product = useRouteParamData('id', id => productApi.get(id));
```

---

## Pattern: WebSocket in Vue

```typescript
// composables/useWebSocket.ts
import { ref, onUnmounted } from 'vue';
import { webSocket } from 'rxjs/webSocket';
import { retry, share } from 'rxjs/operators';
import { timer } from 'rxjs';

export function useWebSocket<T>(url: string) {
  const lastMessage = ref<T | null>(null);
  const connected   = ref(false);

  const ws$ = webSocket<T>({
    url,
    openObserver:  { next: () => { connected.value = true; } },
    closeObserver: { next: () => { connected.value = false; } }
  }).pipe(
    retry({ delay: (_, n) => timer(Math.min(1000 * 2 ** n, 30_000)) }),
    share()
  );

  const sub = ws$.subscribe(msg => { lastMessage.value = msg; });

  function send(msg: T) { ws$.subscribe().closed || (ws$ as any).next(msg); }

  onUnmounted(() => sub.unsubscribe());

  return { lastMessage, connected, send };
}
```

---

## Bridging Vue's `watch` ↔ RxJS

```typescript
import { watch, ref } from 'vue';
import { Subject, Observable } from 'rxjs';

// Vue ref → RxJS Observable:
function refToObservable<T>(source: () => T): Observable<T> {
  return new Observable<T>(subscriber => {
    const stop = watch(source, val => subscriber.next(val), { immediate: true });
    return () => stop();
  });
}

// RxJS Observable → Vue ref:
function observableToRef<T>(obs$: Observable<T>, initial: T): Ref<T> {
  const r = ref(initial) as Ref<T>;
  const sub = obs$.subscribe(v => r.value = v);
  onUnmounted(() => sub.unsubscribe());
  return r;
}

// Usage:
const searchQuery = ref('');
const results = observableToRef(
  refToObservable(() => searchQuery.value).pipe(
    debounceTime(300),
    switchMap(q => search(q))
  ),
  []
);
```

---

## Common Pitfalls

### Forgetting `onUnmounted` Cleanup

```typescript
// ❌ Memory leak — subscription outlives the component:
const sub = interval(1000).subscribe(updateClock);
// sub.unsubscribe() never called!

// ✅ Always clean up in onUnmounted:
const sub = interval(1000).subscribe(updateClock);
onUnmounted(() => sub.unsubscribe());
```

### Creating Subject Outside Composable (Shared Between Instances)

```typescript
// ❌ Subject created at module level — shared across ALL component instances:
const clicks$ = new Subject<void>(); // shared!

export function useClickCounter() {
  const count = ref(0);
  clicks$.subscribe(() => count.value++);
  // All instances share the SAME Subject — clicking in component A affects component B
}

// ✅ Create Subject inside the composable — one per instance:
export function useClickCounter() {
  const clicks$ = new Subject<void>(); // one per composable call
  const count   = ref(0);
  const sub     = clicks$.subscribe(() => count.value++);
  onUnmounted(() => sub.unsubscribe());
  return { count, click: () => clicks$.next() };
}
```
