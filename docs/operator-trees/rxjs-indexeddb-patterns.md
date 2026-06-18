# IndexedDB Patterns with RxJS

Wrapping IndexedDB's callback API in Observables, offline-first reads/writes, reactive queries, and sync-on-reconnect patterns.

---

## Wrapping IDB Operations

IndexedDB is entirely callback/event-based. The core pattern: wrap each operation in `new Observable` and call `complete()` in `onsuccess`:

```typescript
import { Observable } from 'rxjs';

function idbRequest<T>(createRequest: (db: IDBDatabase) => IDBRequest<T>): (db: IDBDatabase) => Observable<T> {
  return (db: IDBDatabase) =>
    new Observable<T>(subscriber => {
      const request = createRequest(db);
      request.onsuccess = () => {
        subscriber.next(request.result);
        subscriber.complete();
      };
      request.onerror = () => subscriber.error(request.error);
    });
}

// Open a database — emit db on success, error on failure:
function openDb(name: string, version: number, upgrade: (db: IDBDatabase) => void): Observable<IDBDatabase> {
  return new Observable<IDBDatabase>(subscriber => {
    const request = indexedDB.open(name, version);
    request.onupgradeneeded = e => upgrade((e.target as IDBOpenDBRequest).result);
    request.onsuccess = () => {
      subscriber.next(request.result);
      subscriber.complete();
    };
    request.onerror   = () => subscriber.error(request.error);
    request.onblocked = () => subscriber.error(new Error('IDB blocked by another tab'));
  });
}
```

---

## Pattern 1: Typed IDB Store Wrapper

```typescript
import { Observable, from } from 'rxjs';
import { switchMap, shareReplay } from 'rxjs/operators';

class IDBStore<T extends { id: string }> {
  private db$: Observable<IDBDatabase>;

  constructor(
    private dbName:    string,
    private storeName: string,
    private version:   number
  ) {
    this.db$ = openDb(dbName, version, db => {
      if (!db.objectStoreNames.contains(storeName)) {
        db.createObjectStore(storeName, { keyPath: 'id' });
      }
    }).pipe(shareReplay(1));
  }

  get(id: string): Observable<T | undefined> {
    return this.db$.pipe(
      switchMap(db =>
        idbRequest<T>(db => {
          const tx = db.transaction(this.storeName, 'readonly');
          return tx.objectStore(this.storeName).get(id);
        })(db)
      )
    );
  }

  getAll(): Observable<T[]> {
    return this.db$.pipe(
      switchMap(db =>
        idbRequest<T[]>(db => {
          const tx = db.transaction(this.storeName, 'readonly');
          return tx.objectStore(this.storeName).getAll();
        })(db)
      )
    );
  }

  put(item: T): Observable<IDBValidKey> {
    return this.db$.pipe(
      switchMap(db =>
        idbRequest<IDBValidKey>(db => {
          const tx = db.transaction(this.storeName, 'readwrite');
          return tx.objectStore(this.storeName).put(item);
        })(db)
      )
    );
  }

  delete(id: string): Observable<void> {
    return this.db$.pipe(
      switchMap(db =>
        new Observable<void>(subscriber => {
          const tx      = db.transaction(this.storeName, 'readwrite');
          const request = tx.objectStore(this.storeName).delete(id);
          request.onsuccess = () => { subscriber.next(); subscriber.complete(); };
          request.onerror   = () => subscriber.error(request.error);
        })
      )
    );
  }
}

// Usage:
interface Todo { id: string; text: string; done: boolean; }
const todos = new IDBStore<Todo>('my-app', 'todos', 1);

todos.getAll().subscribe(items => renderTodos(items));
todos.put({ id: '1', text: 'Learn RxJS', done: false }).subscribe();
```

---

## Pattern 2: Reactive Query (Live Store)

Re-query the store whenever a write occurs:

```typescript
import { Subject, merge, of } from 'rxjs';
import { switchMap, debounceTime, startWith } from 'rxjs/operators';

class ReactiveIDBStore<T extends { id: string }> extends IDBStore<T> {
  private writes$ = new Subject<void>();

  // Every put/delete notifies the write stream:
  put(item: T): Observable<IDBValidKey> {
    return super.put(item).pipe(tap(() => this.writes$.next()));
  }

  delete(id: string): Observable<void> {
    return super.delete(id).pipe(tap(() => this.writes$.next()));
  }

  // Live query: re-runs on every write
  query(
    filter: (item: T) => boolean = () => true
  ): Observable<T[]> {
    return this.writes$.pipe(
      debounceTime(50),     // batch rapid writes
      startWith(null),      // emit immediately on subscribe
      switchMap(() => this.getAll().pipe(
        map(items => items.filter(filter))
      ))
    );
  }
}

// Usage — live todo list that updates on writes:
const todoStore = new ReactiveIDBStore<Todo>('my-app', 'todos', 1);

todoStore.query(t => !t.done).pipe(
  takeUntilDestroyed()
).subscribe(activeTodos => renderTodoList(activeTodos));

// Adding a todo auto-updates the list:
addTodo$.pipe(
  switchMap(text => todoStore.put({ id: crypto.randomUUID(), text, done: false }))
).subscribe();
```

---

## Pattern 3: Offline Write Queue with IndexedDB Persistence

Persist the sync queue in IndexedDB so it survives page reloads:

```typescript
interface QueuedWrite<T> {
  id:        string;
  operation: 'create' | 'update' | 'delete';
  entity:    string;
  payload:   T;
  ts:        number;
  retries:   number;
}

class PersistentWriteQueue<T> {
  private queue = new IDBStore<QueuedWrite<T>>('sync-queue', 'writes', 1);
  private flush$ = new Subject<void>();

  // Add to persistent queue:
  enqueue(op: Omit<QueuedWrite<T>, 'id' | 'ts' | 'retries'>): Observable<void> {
    const item: QueuedWrite<T> = {
      ...op,
      id:      crypto.randomUUID(),
      ts:      Date.now(),
      retries: 0
    };
    return this.queue.put(item).pipe(
      tap(() => this.flush$.next()),
      map(() => void 0)
    );
  }

  // Process queue when online:
  startProcessing(api: { sync: (op: QueuedWrite<T>) => Observable<void> }): Subscription {
    const online$ = merge(
      fromEvent(window, 'online').pipe(map(() => true)),
      fromEvent(window, 'offline').pipe(map(() => false))
    ).pipe(startWith(navigator.onLine));

    return online$.pipe(
      switchMap(online => online ? this.processQueue$(api) : EMPTY)
    ).subscribe();
  }

  private processQueue$(api: { sync: (op: QueuedWrite<T>) => Observable<void> }) {
    return this.queue.getAll().pipe(
      switchMap(items =>
        from(items.sort((a, b) => a.ts - b.ts)).pipe(
          concatMap(item =>
            api.sync(item).pipe(
              switchMap(() => this.queue.delete(item.id)),
              catchError(() => {
                if (item.retries >= 3) {
                  return this.queue.delete(item.id); // give up after 3 retries
                }
                return this.queue.put({ ...item, retries: item.retries + 1 });
              })
            )
          )
        )
      )
    );
  }
}
```

---

## Pattern 4: IDB Cursor for Paginated Reads

Read large stores page-by-page using a cursor:

```typescript
function idbCursorPage<T>(
  db:        IDBDatabase,
  storeName: string,
  page:      number,
  pageSize:  number
): Observable<T[]> {
  return new Observable<T[]>(subscriber => {
    const tx      = db.transaction(storeName, 'readonly');
    const store   = tx.objectStore(storeName);
    const results: T[] = [];
    let   skip    = page * pageSize;

    const request = store.openCursor();

    request.onsuccess = e => {
      const cursor = (e.target as IDBRequest<IDBCursorWithValue>).result;
      if (!cursor) {
        subscriber.next(results);
        subscriber.complete();
        return;
      }
      if (skip > 0) {
        skip--;
        cursor.advance(skip + 1); // skip to page start
        return;
      }
      if (results.length < pageSize) {
        results.push(cursor.value as T);
        cursor.continue();
      } else {
        subscriber.next(results);
        subscriber.complete();
      }
    };

    request.onerror = () => subscriber.error(request.error);
  });
}
```

---

## Pattern 5: IDB as Reactive Cache (Cache-Then-Network)

Show cached data immediately, update with fresh network data:

```typescript
import { merge, of } from 'rxjs';
import { map } from 'rxjs/operators';

function cacheFirst<T extends { id: string }>(
  key:      string,
  store:    IDBStore<{ id: string; data: T; cachedAt: number }>,
  network$: Observable<T>
): Observable<{ data: T; fromCache: boolean }> {
  const cached$ = store.get(key).pipe(
    filter(entry => entry !== undefined),
    map(entry => ({ data: entry!.data, fromCache: true }))
  );

  const fresh$ = network$.pipe(
    tap(data => store.put({ id: key, data, cachedAt: Date.now() }).subscribe()),
    map(data => ({ data, fromCache: false }))
  );

  return merge(
    cached$,   // emit cached immediately (may be undefined — filtered above)
    fresh$     // then emit fresh data when it arrives
  ).pipe(
    take(2),   // at most: 1 cached + 1 fresh
    distinctUntilChanged((a, b) => JSON.stringify(a.data) === JSON.stringify(b.data))
  );
}

// Usage:
cacheFirst(
  `user-${userId}`,
  userCache,
  this.api.getUser(userId)
).pipe(
  takeUntilDestroyed()
).subscribe(({ data, fromCache }) => {
  renderUser(data);
  if (fromCache) showStaleIndicator();
  else           hideStaleIndicator();
});
```

---

## Common Pitfalls

### Transaction Closes Before Observable Subscribes

```typescript
// ❌ Creating transaction outside Observable — IDB transactions auto-close:
const tx      = db.transaction('todos', 'readonly');
const request = tx.objectStore('todos').getAll();
return new Observable(subscriber => {
  // By the time subscriber callback runs, transaction may already be closed!
  request.onsuccess = () => subscriber.next(request.result);
});

// ✅ Always open transaction INSIDE the Observable callback:
return new Observable(subscriber => {
  const tx      = db.transaction('todos', 'readonly'); // opened synchronously here
  const request = tx.objectStore('todos').getAll();
  request.onsuccess = () => { subscriber.next(request.result); subscriber.complete(); };
  request.onerror   = () => subscriber.error(request.error);
});
```

### Not Handling `onblocked`

```typescript
// ❌ Ignoring blocked event during version upgrade:
const request = indexedDB.open('app', 2);
request.onupgradeneeded = e => migrateSchema(e);
request.onsuccess = () => resolve(request.result);
// If another tab has the DB open at version 1, this hangs silently

// ✅ Handle blocked — prompt user to close other tabs:
request.onblocked = () => {
  showMessage('Please close other tabs to allow the database upgrade.');
  subscriber.error(new Error('IDB upgrade blocked'));
};
```
