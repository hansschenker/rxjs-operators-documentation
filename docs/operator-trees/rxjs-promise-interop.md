# Promise ↔ Observable Interop

Converting between Promises and Observables, async/await with RxJS, and bridging legacy code.

---

## Converting Promise → Observable

```typescript
import { from, defer } from 'rxjs';

// from() — wraps an already-running Promise:
const obs$ = from(fetch('/api/data').then(r => r.json()));
// ⚠️ fetch() already started — from() just wraps the result

// defer() — lazy: creates a NEW Promise on each subscribe:
const obs$ = defer(() => fetch('/api/data').then(r => r.json()));
// ✓ Each subscriber triggers a fresh fetch — cold Observable semantics
// ✓ Composable with retry(), catchError(), etc.
```

**Rule**: Almost always use `defer()` rather than `from()` for async factory functions. `from(promise)` subscribes to a single, already-running promise — retry will re-subscribe but the promise has already resolved.

---

## Converting Observable → Promise

```typescript
import { firstValueFrom, lastValueFrom } from 'rxjs';

// firstValueFrom — resolves with first emission, rejects on error:
const user = await firstValueFrom(this.api.getUser(id));

// lastValueFrom — resolves with last emission before complete:
const allItems = await lastValueFrom(this.api.getItems().pipe(toArray()));

// With default value if stream completes empty:
const user = await firstValueFrom(
  this.api.getUser(id),
  { defaultValue: null }
);
```

---

## Pattern 1: Async/Await Style with RxJS Pipelines

```typescript
// Combine async/await at the call site with Observable pipelines internally:
async function loadDashboard(): Promise<DashboardData> {
  const [users, products, orders] = await Promise.all([
    firstValueFrom(userService.users$.pipe(take(1))),
    firstValueFrom(productService.topProducts$),
    firstValueFrom(orderService.recentOrders$)
  ]);
  return { users, products, orders };
}

// Better: keep everything as Observable for composability:
const dashboard$ = combineLatest({
  users:    userService.users$,
  products: productService.topProducts$,
  orders:   orderService.recentOrders$
});
// Only convert to Promise at the outermost boundary if absolutely needed
```

---

## Pattern 2: Wrapping Callback APIs

```typescript
import { Observable, bindCallback, bindNodeCallback } from 'rxjs';

// bindCallback — for (value) => void callbacks:
const readFileObs = bindCallback(fs.readFile);
readFileObs('/path/to/file', 'utf8').subscribe(content => console.log(content));

// bindNodeCallback — for Node.js (err, value) => void callbacks:
const readFileNode = bindNodeCallback(fs.readFile);
readFileNode('/path/to/file', 'utf8').subscribe({
  next:  content => console.log(content),
  error: err     => console.error(err)
});

// Manual wrapping for one-off cases:
function readFileObs(path: string): Observable<string> {
  return new Observable(observer => {
    fs.readFile(path, 'utf8', (err, data) => {
      if (err) { observer.error(err); return; }
      observer.next(data);
      observer.complete();
    });
  });
}
```

---

## Pattern 3: Mixing async/await and Operators

```typescript
import { switchMap, from } from 'rxjs/operators';

// switchMap accepts a function returning Observable OR Promise:
userId$.pipe(
  switchMap(id => this.userRepository.findById(id)) // returns Promise — works!
)

// from() not needed — switchMap wraps automatically:
userId$.pipe(
  switchMap(async id => {
    const user = await this.db.users.findById(id);
    return { ...user, displayName: `${user.first} ${user.last}` };
  })
)
```

---

## Pattern 4: Converting Event-Based APIs

```typescript
import { Observable } from 'rxjs';

// IndexedDB (callback-based):
function openDatabase(name: string, version: number): Observable<IDBDatabase> {
  return new Observable(observer => {
    const request = indexedDB.open(name, version);
    request.onsuccess = () => { observer.next(request.result); observer.complete(); };
    request.onerror   = () => observer.error(request.error);
    request.onupgradeneeded = e => setupSchema(e);
    // No cleanup needed — one-shot
  });
}

// Geolocation (continuous):
function watchPosition(): Observable<GeolocationPosition> {
  return new Observable(observer => {
    const id = navigator.geolocation.watchPosition(
      pos => observer.next(pos),
      err => observer.error(err),
      { enableHighAccuracy: true }
    );
    return () => navigator.geolocation.clearWatch(id); // cleanup on unsubscribe
  });
}
```

---

## Pattern 5: `toPromise()` Migration (Deprecated)

`toPromise()` is deprecated in RxJS 7 — migrate to `firstValueFrom`/`lastValueFrom`:

```typescript
// ❌ Deprecated:
const result = await source$.toPromise();

// ✅ Migrate based on semantics:
// If you want the FIRST value:
const result = await firstValueFrom(source$);

// If you want the LAST value (stream must complete):
const result = await lastValueFrom(source$);

// If stream might be empty:
const result = await firstValueFrom(source$, { defaultValue: null });
```

---

## Pattern 6: Zone-Aware Promise → Observable (Angular)

Angular's zone patching works on native Promises. When converting inside Angular:

```typescript
import { NgZone } from '@angular/core';

@Injectable()
export class ZoneAwareService {
  constructor(private zone: NgZone) {}

  // Wrap third-party library that returns unzoned Promises:
  fromThirdParty<T>(promise: Promise<T>): Observable<T> {
    return from(promise).pipe(
      observeOn(new ZoneScheduler(this.zone))  // bring back into Angular zone
    );
  }
}
```

---

## `firstValueFrom` vs `lastValueFrom` vs `toArray` + `lastValueFrom`

```typescript
// firstValueFrom — first emission, then unsubscribes:
await firstValueFrom(click$)          // resolves on first click
await firstValueFrom(http.get('/x'))  // resolves when response arrives

// lastValueFrom — waits for complete, resolves with last value:
await lastValueFrom(timer(1000))          // resolves after 1s (timer emits 0, completes)
await lastValueFrom(source$.pipe(take(5))) // resolves with 5th value

// Collect ALL values into array:
const all = await lastValueFrom(
  source$.pipe(toArray())
);
// same as: const all = await firstValueFrom(source$.pipe(toArray()))
```

---

## Common Pitfalls

### `from(asyncFunction())` Starts Immediately

```typescript
// ❌ Promise already running before anyone subscribes:
const obs$ = from(expensiveApiCall()); // API called NOW
obs$.subscribe(result => ...);         // just wraps an already-running promise

// ✅ Use defer for lazy evaluation:
const obs$ = defer(() => expensiveApiCall()); // API NOT called yet
obs$.subscribe(result => ...);               // API called NOW, on subscribe
obs$.subscribe(result => ...);               // API called AGAIN — fresh call
```

### `firstValueFrom` on Infinite Stream Without Take

```typescript
// ❌ Never resolves — infinite stream never completes:
await firstValueFrom(interval(1000)); // resolves with 0 immediately ✓ (actually ok)

// ⚠️ lastValueFrom on infinite stream — hangs forever:
await lastValueFrom(interval(1000)); // never resolves!

// ✅ Bound with take:
await lastValueFrom(interval(1000).pipe(take(5))); // resolves with 4 after 5s
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**The single most important rule**: Use `defer(() => promiseFactory())` not `from(promiseFactory())` when converting async functions to Observables. `defer` preserves cold Observable semantics — each subscriber gets a fresh execution. `from(alreadyRunningPromise)` just wraps a Promise that's already in flight.
