# Optimistic UI Patterns with RxJS

Update the UI before the server confirms, roll back on failure — making apps feel instant.

---

## What Is Optimistic UI?

**Pessimistic (normal)**: Click button → wait for server → update UI.
**Optimistic**: Click button → update UI immediately → server confirms → done (or roll back on error).

Optimistic UI eliminates perceived latency for common write operations. RxJS makes it tractable because rollback is just emitting the previous state.

---

## Pattern 1: Toggle with Rollback

The simplest optimistic pattern — a like/follow button:

```typescript
import { Subject, BehaviorSubject, EMPTY } from 'rxjs';
import { switchMap, catchError, tap } from 'rxjs/operators';

@Injectable()
export class LikeService {
  private liked$  = new BehaviorSubject(false);
  private toggle$ = new Subject<void>();

  readonly isLiked$ = this.liked$.asObservable();

  constructor(private api: ApiService) {
    this.toggle$.pipe(
      switchMap(() => {
        const previous = this.liked$.getValue();
        const next     = !previous;

        // Optimistic update:
        this.liked$.next(next);

        // Confirm with server — roll back on failure:
        return this.api.setLike(postId, next).pipe(
          catchError(() => {
            this.liked$.next(previous); // roll back
            showToast('Failed to update — please try again');
            return EMPTY;
          })
        );
      })
    ).subscribe();
  }

  toggle(): void { this.toggle$.next(); }
}
```

---

## Pattern 2: Optimistic List Item Add

```typescript
import { BehaviorSubject, Subject } from 'rxjs';
import { exhaustMap, tap, catchError } from 'rxjs/operators';

interface Todo { id: string; text: string; done: boolean; _optimistic?: boolean; }

@Injectable()
export class OptimisticTodoService {
  private todos$ = new BehaviorSubject<Todo[]>([]);
  readonly list$ = this.todos$.asObservable();

  add(text: string): void {
    const tempId = `temp-${Date.now()}`;
    const optimistic: Todo = { id: tempId, text, done: false, _optimistic: true };

    // Optimistic add:
    this.todos$.next([...this.todos$.getValue(), optimistic]);

    this.api.createTodo(text).pipe(
      tap(saved => {
        // Replace temp with real:
        this.todos$.next(
          this.todos$.getValue().map(t => t.id === tempId ? saved : t)
        );
      }),
      catchError(err => {
        // Roll back:
        this.todos$.next(
          this.todos$.getValue().filter(t => t.id !== tempId)
        );
        showError('Failed to add todo');
        return EMPTY;
      })
    ).subscribe();
  }
}
```

---

## Pattern 3: Optimistic Delete

```typescript
@Injectable()
export class OptimisticDeleteService {
  private items$ = new BehaviorSubject<Item[]>([]);
  readonly list$ = this.items$.asObservable();

  delete(id: string): void {
    const snapshot = this.items$.getValue();
    const item     = snapshot.find(i => i.id === id);
    if (!item) return;

    // Optimistic remove:
    this.items$.next(snapshot.filter(i => i.id !== id));

    this.api.deleteItem(id).pipe(
      catchError(() => {
        // Roll back — restore the item at its original position:
        const current = this.items$.getValue();
        const index   = snapshot.findIndex(i => i.id === id);
        const restored = [...current.slice(0, index), item, ...current.slice(index)];
        this.items$.next(restored);
        showError('Delete failed — item restored');
        return EMPTY;
      })
    ).subscribe();
  }
}
```

---

## Pattern 4: Optimistic Reorder (Drag and Drop)

```typescript
@Injectable()
export class OptimisticReorderService {
  private items$ = new BehaviorSubject<Item[]>([]);
  readonly list$ = this.items$.asObservable();

  reorder(fromIndex: number, toIndex: number): void {
    const snapshot = [...this.items$.getValue()];
    const [moved]  = snapshot.splice(fromIndex, 1);
    snapshot.splice(toIndex, 0, moved);

    const previous = this.items$.getValue();

    // Optimistic reorder:
    this.items$.next(snapshot);

    this.api.saveOrder(snapshot.map(i => i.id)).pipe(
      catchError(() => {
        this.items$.next(previous); // roll back
        showError('Failed to save order');
        return EMPTY;
      })
    ).subscribe();
  }
}
```

---

## Pattern 5: Optimistic Edit with Pending Indicator

Show a visual pending state while saving, revert if it fails:

```typescript
interface OptimisticItem<T> {
  data:     T;
  pending:  boolean;
  error:    string | null;
}

@Injectable()
export class OptimisticEditService {
  private items$ = new BehaviorSubject<Map<string, OptimisticItem<Item>>>(new Map());
  readonly items$ = this.items$.asObservable();

  edit(id: string, changes: Partial<Item>): void {
    const map      = new Map(this.items$.getValue());
    const existing = map.get(id);
    if (!existing) return;

    const original = existing.data;

    // Optimistic: apply changes, mark pending:
    map.set(id, { data: { ...original, ...changes }, pending: true, error: null });
    this.items$.next(map);

    this.api.updateItem(id, changes).pipe(
      tap(saved => {
        const m = new Map(this.items$.getValue());
        m.set(id, { data: saved, pending: false, error: null });
        this.items$.next(m);
      }),
      catchError(err => {
        // Roll back + show error on the item:
        const m = new Map(this.items$.getValue());
        m.set(id, { data: original, pending: false, error: err.message });
        this.items$.next(m);
        return EMPTY;
      })
    ).subscribe();
  }
}
```

---

## Pattern 6: CRUD Service with Unified Optimistic Updates

```typescript
type CrudAction<T> =
  | { type: 'ADD';    item: T }
  | { type: 'UPDATE'; id: string; changes: Partial<T> }
  | { type: 'DELETE'; id: string }
  | { type: 'REVERT'; snapshot: T[] };

@Injectable()
export class CrudStore<T extends { id: string }> {
  private items$ = new BehaviorSubject<T[]>([]);
  readonly list$ = this.items$.asObservable();

  private applyAction(action: CrudAction<T>): void {
    const current = this.items$.getValue();
    switch (action.type) {
      case 'ADD':
        this.items$.next([...current, action.item]);
        break;
      case 'UPDATE':
        this.items$.next(current.map(i =>
          i.id === action.id ? { ...i, ...action.changes } : i
        ));
        break;
      case 'DELETE':
        this.items$.next(current.filter(i => i.id !== action.id));
        break;
      case 'REVERT':
        this.items$.next(action.snapshot);
        break;
    }
  }

  private optimistic(
    action: CrudAction<T>,
    api$: Observable<unknown>
  ): void {
    const snapshot = this.items$.getValue();
    this.applyAction(action);

    api$.pipe(
      catchError(err => {
        this.applyAction({ type: 'REVERT', snapshot });
        showError(`Operation failed: ${err.message}`);
        return EMPTY;
      })
    ).subscribe();
  }

  add(item: Omit<T, 'id'>): void {
    const tempItem = { ...item, id: `temp-${Date.now()}` } as T;
    this.optimistic(
      { type: 'ADD', item: tempItem },
      this.api.create(item).pipe(
        tap(saved => this.applyAction({ type: 'UPDATE', id: tempItem.id, changes: saved as Partial<T> }))
      )
    );
  }

  update(id: string, changes: Partial<T>): void {
    this.optimistic({ type: 'UPDATE', id, changes }, this.api.update(id, changes));
  }

  delete(id: string): void {
    this.optimistic({ type: 'DELETE', id }, this.api.delete(id));
  }
}
```

---

## Pattern 7: Conflict Resolution

Handle server returning a different value than the optimistic guess:

```typescript
@Injectable()
export class ConflictAwareService {
  private items$ = new BehaviorSubject<Item[]>([]);

  update(id: string, changes: Partial<Item>): void {
    const snapshot = this.items$.getValue();
    const original = snapshot.find(i => i.id === id)!;

    // Optimistic update:
    this.items$.next(
      snapshot.map(i => i.id === id ? { ...i, ...changes } : i)
    );

    this.api.update(id, changes).pipe(
      tap(serverVersion => {
        // Server may return computed fields different from our guess:
        this.items$.next(
          this.items$.getValue().map(i =>
            i.id === id ? serverVersion : i  // use server truth
          )
        );
      }),
      catchError(() => {
        this.items$.next(snapshot); // full roll back
        return EMPTY;
      })
    ).subscribe();
  }
}
```

---

## Decision: When to Use Optimistic UI

| Scenario | Use Optimistic? | Why |
|---|---|---|
| Toggle like/follow | Yes | Instant feel, easy rollback |
| Add list item | Yes | Reduces perceived latency |
| Delete item | Yes with undo | Easy to restore |
| Edit text field | Yes with pending indicator | User expects instant |
| Financial transaction | No | Accuracy critical, don't guess |
| Permission/role change | No | Security-sensitive |
| Multi-step wizard | No | Too complex to roll back |
| File upload | No | Can't fake progress |

---

## Common Pitfalls

### Using `switchMap` for Optimistic Operations

```typescript
// ❌ switchMap cancels previous — if user edits fast, intermediate saves cancelled:
editEvents$.pipe(
  switchMap(changes => this.api.save(changes).pipe(/* rollback on catchError */))
)
// Third edit cancels second save — server never gets second edit

// ✅ concatMap queues edits; exhaustMap deduplicates rapid clicks:
editEvents$.pipe(
  exhaustMap(changes => this.api.save(changes).pipe(/* rollback */))
)
// Or for fast typists where only final value matters, debounce first:
editEvents$.pipe(
  debounceTime(500),
  switchMap(changes => this.api.save(changes).pipe(/* rollback */))
)
```

### Forgetting to Roll Back All Derived State

```typescript
// ❌ Rolling back items but not derived counts:
this.items$.next(snapshot);         // rolled back
// this.totalCount$ is derived via combineLatest — auto-updates ✓
// this.selectedIds$ is a separate BehaviorSubject — NOT auto-updated ✗

// ✅ Keep all state in one object:
this.state$.next({ items: snapshot, selectedIds: snapshotSelectedIds });
```
