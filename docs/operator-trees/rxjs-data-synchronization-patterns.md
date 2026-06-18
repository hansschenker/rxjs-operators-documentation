# Data Synchronization Patterns with RxJS

Offline-first, conflict resolution, sync queues, and keeping local state in sync with a remote source.

---

## Core Synchronization Primitives

| Primitive | Role | RxJS tool |
|---|---|---|
| **Sync queue** | Buffer writes made offline | `Subject` + `BehaviorSubject` |
| **Remote state stream** | Push latest server state to UI | `WebSocketSubject` / polling |
| **Optimistic update** | Apply change locally before server confirms | `scan` + rollback on error |
| **Conflict resolver** | Merge diverged state | Pure function applied in `map` |
| **Change tracking** | Detect local vs remote drift | `distinctUntilChanged` + `combineLatest` |

---

## Pattern 1: Offline Sync Queue

Buffer mutations made while offline; flush when connectivity returns:

```typescript
import { BehaviorSubject, fromEvent, merge, EMPTY } from 'rxjs';
import { filter, switchMap, concatMap, retryWhen, scan, tap, catchError } from 'rxjs/operators';
import { timer } from 'rxjs';

interface SyncOperation {
  id:      string;
  type:    'create' | 'update' | 'delete';
  entity:  string;
  payload: unknown;
  ts:      number;
}

class OfflineSyncQueue {
  private queue$   = new BehaviorSubject<SyncOperation[]>([]);
  private online$  = merge(
    fromEvent(window, 'online').pipe(map(() => true)),
    fromEvent(window, 'offline').pipe(map(() => false))
  ).pipe(startWith(navigator.onLine), shareReplay(1));

  readonly pendingCount$ = this.queue$.pipe(map(q => q.length));

  enqueue(op: Omit<SyncOperation, 'id' | 'ts'>): void {
    const full: SyncOperation = {
      ...op,
      id: crypto.randomUUID(),
      ts: Date.now()
    };
    const queue = this.queue$.getValue();
    this.queue$.next([...queue, full]);
  }

  startFlushing(api: DataApi): Subscription {
    return this.online$.pipe(
      switchMap(online => online ? this.flush$(api) : EMPTY)
    ).subscribe();
  }

  private flush$(api: DataApi) {
    return this.queue$.pipe(
      filter(q => q.length > 0),
      concatMap(queue => {
        const [head, ...rest] = queue;
        return api.sync(head).pipe(
          tap(() => this.queue$.next(rest)), // dequeue on success
          retryWhen(errors =>
            errors.pipe(
              scan((n, err) => {
                if (err.status === 409) throw err; // conflict — don't retry
                if (n >= 3) throw new Error('Max retries exceeded');
                return n + 1;
              }, 0),
              delayWhen(n => timer(1000 * Math.pow(2, n)))
            )
          ),
          catchError(err => {
            if (err.status === 409) {
              this.handleConflict(head, err.serverState);
              this.queue$.next(rest); // dequeue conflicted op
            }
            return EMPTY;
          })
        );
      })
    );
  }

  private handleConflict(op: SyncOperation, serverState: unknown): void {
    // Emit to conflict resolution UI or apply merge strategy
    console.warn('Conflict:', op.entity, op.id, serverState);
  }
}
```

---

## Pattern 2: Optimistic Local State with Server Reconciliation

Apply changes immediately; reconcile when server responds:

```typescript
import { BehaviorSubject, Subject } from 'rxjs';
import { scan, switchMap, startWith, catchError } from 'rxjs/operators';

type Action =
  | { type: 'APPLY_OPTIMISTIC'; tempId: string; item: TodoItem }
  | { type: 'CONFIRM';          tempId: string; serverItem: TodoItem }
  | { type: 'ROLLBACK';         tempId: string }
  | { type: 'SET_SERVER_STATE'; items: TodoItem[] };

interface TodoState {
  items:    (TodoItem & { pending?: boolean })[];
  tempIds:  Map<string, string>; // tempId → serverId
}

const actions$ = new Subject<Action>();

const state$ = actions$.pipe(
  scan((state, action): TodoState => {
    switch (action.type) {
      case 'APPLY_OPTIMISTIC':
        return {
          ...state,
          items: [...state.items, { ...action.item, pending: true }]
        };

      case 'CONFIRM':
        return {
          ...state,
          items: state.items.map(item =>
            item.id === action.tempId
              ? { ...action.serverItem, pending: false }
              : item
          ),
          tempIds: new Map([...state.tempIds, [action.tempId, action.serverItem.id]])
        };

      case 'ROLLBACK':
        return {
          ...state,
          items: state.items.filter(item => item.id !== action.tempId)
        };

      case 'SET_SERVER_STATE':
        // Merge: keep pending items not yet confirmed
        const pending = state.items.filter(i => i.pending);
        return { ...state, items: [...action.items, ...pending] };
    }
  }, { items: [], tempIds: new Map() }),
  shareReplay(1)
);

// Usage:
function addTodo(text: string) {
  const tempId = `temp_${Date.now()}`;
  const tempItem: TodoItem = { id: tempId, text, done: false };

  actions$.next({ type: 'APPLY_OPTIMISTIC', tempId, item: tempItem });

  api.createTodo(text).subscribe({
    next:  serverItem => actions$.next({ type: 'CONFIRM', tempId, serverItem }),
    error: ()         => actions$.next({ type: 'ROLLBACK', tempId })
  });
}
```

---

## Pattern 3: Last-Write-Wins Conflict Resolution

For non-collaborative data where the most recent write always wins:

```typescript
import { combineLatest } from 'rxjs';
import { map, distinctUntilChanged } from 'rxjs/operators';

interface VersionedState<T> {
  data:      T;
  version:   number;
  updatedAt: number;
  source:    'local' | 'remote';
}

function lastWriteWins<T>(
  local$:  Observable<VersionedState<T>>,
  remote$: Observable<VersionedState<T>>
): Observable<T> {
  return combineLatest([local$, remote$]).pipe(
    map(([local, remote]) =>
      remote.updatedAt >= local.updatedAt ? remote.data : local.data
    ),
    distinctUntilChanged()
  );
}

// With vector clocks for distributed systems:
interface VectorClocked<T> {
  data:   T;
  clock:  Record<string, number>; // nodeId → counter
}

function dominates(a: Record<string, number>, b: Record<string, number>): boolean {
  const allKeys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...allKeys].every(k => (a[k] ?? 0) >= (b[k] ?? 0))
      && [...allKeys].some(k  => (a[k] ?? 0) >  (b[k] ?? 0));
}

function mergeVectorClocked<T>(
  a: VectorClocked<T>,
  b: VectorClocked<T>,
  merge: (x: T, y: T) => T
): VectorClocked<T> {
  if (dominates(a.clock, b.clock)) return a;
  if (dominates(b.clock, a.clock)) return b;
  // Concurrent: merge data
  const clock = Object.fromEntries(
    [...new Set([...Object.keys(a.clock), ...Object.keys(b.clock)])].map(
      k => [k, Math.max(a.clock[k] ?? 0, b.clock[k] ?? 0)]
    )
  );
  return { data: merge(a.data, b.data), clock };
}
```

---

## Pattern 4: Real-Time Sync with Polling Fallback

WebSocket with automatic polling fallback when socket unavailable:

```typescript
import { webSocket } from 'rxjs/webSocket';
import { interval, of } from 'rxjs';
import { switchMap, catchError, shareReplay, retryWhen, scan } from 'rxjs/operators';

function createRealtimeSync<T>(wsUrl: string, pollUrl: string, pollInterval = 5000) {
  const ws$ = webSocket<T>(wsUrl).pipe(
    retryWhen(errors =>
      errors.pipe(
        scan((n, err) => { if (n >= 5) throw err; return n + 1; }, 0),
        delayWhen(n => timer(1000 * Math.pow(2, n)))
      )
    )
  );

  const poll$ = interval(pollInterval).pipe(
    switchMap(() => fromFetch(pollUrl).pipe(
      switchMap(r => r.json() as Promise<T>),
      catchError(() => EMPTY)
    ))
  );

  // Try WebSocket, fall back to polling after 10 seconds:
  return race([
    ws$,
    timer(10_000).pipe(switchMap(() => poll$))
  ]).pipe(
    shareReplay(1)
  );
}

// Usage:
const entities$ = createRealtimeSync<EntityUpdate>(
  'wss://api.example.com/sync',
  '/api/entities/latest'
);

entities$.pipe(
  scan((state, update) => applyUpdate(state, update), initialState),
  takeUntilDestroyed()
).subscribe(renderState);
```

---

## Pattern 5: Delta Sync (Sync Only Changed Records)

Track version/cursor and fetch only what changed:

```typescript
import { BehaviorSubject, timer } from 'rxjs';
import { switchMap, scan, tap, shareReplay } from 'rxjs/operators';

interface SyncCursor {
  lastSyncedAt: number;
  version:      string;
}

class DeltaSyncManager<T extends { id: string; updatedAt: number }> {
  private cursor$ = new BehaviorSubject<SyncCursor>(
    this.loadCursor() ?? { lastSyncedAt: 0, version: '0' }
  );

  private entities$ = new BehaviorSubject<Map<string, T>>(new Map());

  readonly state$ = this.entities$.pipe(
    map(m => [...m.values()]),
    shareReplay(1)
  );

  startSync(api: { fetchDelta: (cursor: SyncCursor) => Observable<{ items: T[]; cursor: SyncCursor }> }): Subscription {
    return timer(0, 10_000).pipe( // initial sync + poll every 10s
      switchMap(() =>
        api.fetchDelta(this.cursor$.getValue()).pipe(
          tap(({ items, cursor }) => {
            // Apply delta to local map:
            const map = new Map(this.entities$.getValue());
            for (const item of items) {
              if ((item as any).deleted) map.delete(item.id);
              else                        map.set(item.id, item);
            }
            this.entities$.next(map);
            this.cursor$.next(cursor);
            this.saveCursor(cursor);
          }),
          catchError(err => {
            console.error('Delta sync failed:', err);
            return EMPTY;
          })
        )
      )
    ).subscribe();
  }

  private loadCursor(): SyncCursor | null {
    const stored = localStorage.getItem('sync-cursor');
    return stored ? JSON.parse(stored) : null;
  }

  private saveCursor(cursor: SyncCursor): void {
    localStorage.setItem('sync-cursor', JSON.stringify(cursor));
  }
}
```

---

## Pattern 6: Bi-Directional Sync State Machine

Full state machine for tracking sync status:

```typescript
import { BehaviorSubject } from 'rxjs';

type SyncStatus = 'idle' | 'syncing' | 'error' | 'offline' | 'up-to-date';

interface SyncState {
  status:     SyncStatus;
  lastSynced: number | null;
  error:      Error | null;
  pending:    number;
}

class SyncStateMachine {
  private state$ = new BehaviorSubject<SyncState>({
    status: 'idle', lastSynced: null, error: null, pending: 0
  });

  readonly status$ = this.state$.pipe(
    map(s => s.status),
    distinctUntilChanged()
  );

  transition(event: 'START' | 'SUCCESS' | 'ERROR' | 'OFFLINE' | 'ONLINE', payload?: unknown): void {
    const current = this.state$.getValue();
    const next    = this.reduce(current, event, payload);
    if (next !== current) this.state$.next(next);
  }

  private reduce(s: SyncState, event: string, payload?: unknown): SyncState {
    switch (`${s.status}:${event}`) {
      case 'idle:START':
      case 'up-to-date:START':
      case 'error:START':
        return { ...s, status: 'syncing', error: null };
      case 'syncing:SUCCESS':
        return { ...s, status: 'up-to-date', lastSynced: Date.now(), error: null };
      case 'syncing:ERROR':
        return { ...s, status: 'error', error: payload as Error };
      case 'idle:OFFLINE':
      case 'syncing:OFFLINE':
      case 'error:OFFLINE':
        return { ...s, status: 'offline' };
      case 'offline:ONLINE':
        return { ...s, status: 'idle' };
      default:
        return s;
    }
  }
}
```

---

## Common Pitfalls

### Not Deduplicating Sync Operations

```typescript
// ❌ Enqueue every change event — floods server with redundant ops:
formControl.valueChanges.pipe(
  switchMap(value => api.save(value)) // fires on every keystroke
)

// ✅ Debounce + deduplicate by ID before queueing:
formControl.valueChanges.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(value => api.save(value))
)
```

### Race Condition on Reconnect

```typescript
// ❌ Applying queued writes AND server state in undefined order:
online$.pipe(
  switchMap(() => merge(
    queue.flush$(api),       // writes local changes
    api.fetchLatest()        // overwrites with server state
  ))
)
// If fetchLatest() arrives before flush completes → lost writes

// ✅ Flush queue first, then fetch server state:
online$.pipe(
  switchMap(() =>
    queue.flush$(api).pipe(
      last(null, null),       // wait for queue to drain
      switchMap(() => api.fetchLatest())
    )
  )
)
```
