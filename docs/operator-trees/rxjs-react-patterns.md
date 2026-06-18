# RxJS in React

Patterns for using RxJS inside React applications: custom hooks, component integration, state management, and cleanup.

---

## Why RxJS in React?

React's built-in primitives (`useState`, `useEffect`) handle simple async well. RxJS adds value for:

- **Complex async coordination** — combining multiple streams, debounce + cancel + retry together
- **Event buses** — cross-component communication without prop drilling or Context overhead
- **Real-time data** — WebSocket / SSE streams with auto-reconnect
- **Shared expensive subscriptions** — `shareReplay` across components
- **Operator pipelines** — debounce, throttle, distinctUntilChanged on user events

---

## The Core Hook: `useObservable`

```typescript
import { useEffect, useState } from 'react';
import { Observable } from 'rxjs';

function useObservable<T>(observable$: Observable<T>, initialValue: T): T {
  const [value, setValue] = useState<T>(initialValue);

  useEffect(() => {
    const subscription = observable$.subscribe(setValue);
    return () => subscription.unsubscribe(); // cleanup on unmount
  }, [observable$]); // re-subscribe if observable reference changes

  return value;
}

// Usage:
function UserProfile({ userId }: { userId: string }) {
  const user = useObservable(
    userService.getUser(userId),
    null
  );

  if (!user) return <Spinner />;
  return <div>{user.name}</div>;
}
```

---

## Hook: `useObservableState` (with loading/error)

```typescript
import { useEffect, useState } from 'react';
import { Observable } from 'rxjs';

interface AsyncState<T> {
  data:    T | null;
  loading: boolean;
  error:   string | null;
}

function useObservableState<T>(
  factory: () => Observable<T>,
  deps: React.DependencyList = []
): AsyncState<T> {
  const [state, setState] = useState<AsyncState<T>>({
    data: null, loading: true, error: null
  });

  useEffect(() => {
    setState({ data: null, loading: true, error: null });

    const sub = factory().subscribe({
      next:     data  => setState({ data, loading: false, error: null }),
      error:    err   => setState({ data: null, loading: false, error: err.message }),
      complete: ()    => setState(s => ({ ...s, loading: false }))
    });

    return () => sub.unsubscribe();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return state;
}

// Usage:
function OrderHistory({ userId }: { userId: string }) {
  const { data: orders, loading, error } = useObservableState(
    () => orderService.getOrders(userId),
    [userId] // re-fetch when userId changes
  );

  if (loading) return <Spinner />;
  if (error)   return <ErrorMessage message={error} />;
  return <OrderList orders={orders!} />;
}
```

---

## Hook: `useObservableCallback` (Event → Stream)

Turn a React event handler into an Observable pipeline:

```typescript
import { useCallback, useEffect, useRef } from 'react';
import { Subject, Observable } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

function useSearch(searchFn: (q: string) => Observable<SearchResult[]>) {
  const subject$ = useRef(new Subject<string>());
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const sub = subject$.current.pipe(
      debounceTime(300),
      distinctUntilChanged(),
      filter(q => q.length >= 2),
      tap(() => setLoading(true)),
      switchMap(q =>
        searchFn(q).pipe(
          catchError(() => of([]))
        )
      ),
      tap(() => setLoading(false))
    ).subscribe(setResults);

    return () => sub.unsubscribe();
  }, [searchFn]);

  const search = useCallback((q: string) => {
    subject$.current.next(q);
  }, []);

  return { search, results, loading };
}

// Usage:
function SearchBar() {
  const { search, results, loading } = useSearch(
    q => productService.search(q)
  );

  return (
    <>
      <input onChange={e => search(e.target.value)} placeholder="Search..." />
      {loading && <Spinner />}
      <ResultList items={results} />
    </>
  );
}
```

---

## Pattern: Shared Service with `shareReplay`

Multiple components subscribe to the same Observable without duplicate requests:

```typescript
// userService.ts — singleton service pattern:
import { shareReplay, switchMap } from 'rxjs/operators';
import { BehaviorSubject, Observable } from 'rxjs';
import { ajax } from 'rxjs/ajax';

class UserService {
  private userId$ = new BehaviorSubject<string | null>(null);

  // shareReplay(1) — one HTTP request shared across all components
  readonly currentUser$: Observable<User | null> = this.userId$.pipe(
    switchMap(id =>
      id
        ? ajax.getJSON<User>(`/api/users/${id}`)
        : of(null)
    ),
    shareReplay(1)
  );

  setUser(id: string) { this.userId$.next(id); }
  clearUser()         { this.userId$.next(null); }
}

export const userService = new UserService();

// Header.tsx — subscribes to shared stream:
function Header() {
  const user = useObservable(userService.currentUser$, null);
  return <nav>{user ? <UserMenu user={user} /> : <LoginButton />}</nav>;
}

// ProfilePage.tsx — same stream, no second request:
function ProfilePage() {
  const user = useObservable(userService.currentUser$, null);
  return user ? <ProfileForm user={user} /> : <Redirect to="/login" />;
}
```

---

## Pattern: Event Bus (Cross-Component Communication)

```typescript
// eventBus.ts:
import { Subject, filter } from 'rxjs';

interface AppEvent {
  type: string;
  payload?: unknown;
}

const bus$ = new Subject<AppEvent>();

export const eventBus = {
  emit: (event: AppEvent) => bus$.next(event),
  on:   <T>(type: string) =>
    bus$.pipe(
      filter(e => e.type === type),
      map(e => e.payload as T)
    )
};

// ProductCard.tsx — emits event:
function ProductCard({ product }: { product: Product }) {
  return (
    <button onClick={() => eventBus.emit({ type: 'ADD_TO_CART', payload: product })}>
      Add to Cart
    </button>
  );
}

// CartIcon.tsx — listens to event:
function CartIcon() {
  const [count, setCount] = useState(0);

  useEffect(() => {
    const sub = eventBus.on<Product>('ADD_TO_CART').subscribe(() => {
      setCount(c => c + 1);
    });
    return () => sub.unsubscribe();
  }, []);

  return <span>Cart ({count})</span>;
}
```

---

## Pattern: Polling with Pause/Resume

```typescript
import { timer, Subject, merge } from 'rxjs';
import { switchMap, takeUntil, filter, exhaustMap } from 'rxjs/operators';

function usePolling<T>(
  fetch: () => Observable<T>,
  intervalMs: number,
  enabled: boolean
) {
  const [data, setData] = useState<T | null>(null);
  const enabledRef = useRef(enabled);
  enabledRef.current = enabled;

  useEffect(() => {
    if (!enabled) return;

    const sub = timer(0, intervalMs).pipe(
      exhaustMap(() => fetch().pipe(catchError(() => of(null))))
    ).subscribe(result => {
      if (result !== null) setData(result);
    });

    return () => sub.unsubscribe();
  }, [enabled, intervalMs]); // re-subscribe when enabled/interval changes

  return data;
}

// Usage:
function LivePriceDisplay({ symbol }: { symbol: string }) {
  const [visible, setVisible] = useState(true);
  const price = usePolling(
    () => priceService.getPrice(symbol),
    5000,
    visible // only poll when component is visible
  );

  return (
    <div onMouseLeave={() => setVisible(false)} onMouseEnter={() => setVisible(true)}>
      {price ? `$${price.toFixed(2)}` : 'Loading…'}
    </div>
  );
}
```

---

## Pattern: `fromEvent` for DOM Events

```typescript
import { fromEvent, merge } from 'rxjs';
import { map, debounceTime, distinctUntilChanged } from 'rxjs/operators';
import { useEffect, useRef, useState } from 'react';

function useWindowSize() {
  const [size, setSize] = useState({ width: window.innerWidth, height: window.innerHeight });

  useEffect(() => {
    const sub = fromEvent(window, 'resize').pipe(
      debounceTime(100),
      map(() => ({ width: window.innerWidth, height: window.innerHeight })),
      distinctUntilChanged((a, b) => a.width === b.width && a.height === b.height)
    ).subscribe(setSize);

    return () => sub.unsubscribe();
  }, []);

  return size;
}
```

---

## Common Pitfalls

### Creating Observable Inside Component Without `useRef`

```typescript
// ❌ New Subject on every render — loses queued values
function Bad() {
  const subject$ = new Subject<string>(); // recreated on every render!

  useEffect(() => {
    subject$.pipe(debounceTime(300)).subscribe(search);
  }, []);

  return <input onChange={e => subject$.next(e.target.value)} />;
}

// ✅ useRef keeps the same Subject across renders
function Good() {
  const subject$ = useRef(new Subject<string>());

  useEffect(() => {
    const sub = subject$.current.pipe(debounceTime(300)).subscribe(search);
    return () => sub.unsubscribe();
  }, []);

  return <input onChange={e => subject$.current.next(e.target.value)} />;
}
```

### Not Cleaning Up Subscriptions

```typescript
// ❌ Memory leak — subscription outlives the component
useEffect(() => {
  observable$.subscribe(setData); // no cleanup!
}, []);

// ✅ Always return cleanup function from useEffect
useEffect(() => {
  const sub = observable$.subscribe(setData);
  return () => sub.unsubscribe(); // runs on unmount
}, []);
```

### Observable in Deps Array Causing Infinite Loop

```typescript
// ❌ New Observable reference on every render → infinite re-subscription
useEffect(() => {
  const sub = from(fetch('/api/data')).subscribe(setData);
  return () => sub.unsubscribe();
}, [from(fetch('/api/data'))]); // new reference every render!

// ✅ Create the Observable outside the component or use useRef:
const data$ = useMemo(() => from(fetch('/api/data')), []); // stable reference

useEffect(() => {
  const sub = data$.subscribe(setData);
  return () => sub.unsubscribe();
}, [data$]);
```
