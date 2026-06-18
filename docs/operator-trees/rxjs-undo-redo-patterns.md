# Undo/Redo Patterns with RxJS

From simple snapshot-based undo to event sourcing with full temporal navigation.

---

## Pattern 1: Snapshot Stack (Simplest)

```typescript
import { BehaviorSubject } from 'rxjs';
import { map } from 'rxjs/operators';

class UndoableState<T> {
  private past:    T[] = [];
  private future:  T[] = [];
  private current$ = new BehaviorSubject<T>(this.initial);

  readonly state$   = this.current$.asObservable();
  readonly canUndo$ = this.current$.pipe(map(() => this.past.length > 0));
  readonly canRedo$ = this.current$.pipe(map(() => this.future.length > 0));

  constructor(private initial: T) {}

  set(newState: T): void {
    this.past.push(this.current$.getValue()); // save current
    this.future = [];                         // clear redo stack
    this.current$.next(newState);
  }

  undo(): void {
    const prev = this.past.pop();
    if (prev === undefined) return;
    this.future.push(this.current$.getValue());
    this.current$.next(prev);
  }

  redo(): void {
    const next = this.future.pop();
    if (next === undefined) return;
    this.past.push(this.current$.getValue());
    this.current$.next(next);
  }
}

// Usage:
const doc = new UndoableState({ text: '' });
doc.set({ text: 'Hello' });
doc.set({ text: 'Hello World' });
doc.undo(); // → { text: 'Hello' }
doc.redo(); // → { text: 'Hello World' }
```

---

## Pattern 2: Event-Sourced Undo (Replay from Events)

Store events, not snapshots. Undo by replaying N-1 events:

```typescript
import { BehaviorSubject, Subject } from 'rxjs';
import { scan, map, shareReplay } from 'rxjs/operators';

type EditorEvent =
  | { type: 'INSERT'; pos: number; text: string }
  | { type: 'DELETE'; pos: number; length: number }
  | { type: 'FORMAT'; pos: number; length: number; bold: boolean };

function applyEvent(state: EditorState, event: EditorEvent): EditorState {
  switch (event.type) {
    case 'INSERT': return insertText(state, event.pos, event.text);
    case 'DELETE': return deleteText(state, event.pos, event.length);
    case 'FORMAT': return formatText(state, event.pos, event.length, event.bold);
  }
}

class EventSourcedEditor {
  private events$ = new BehaviorSubject<EditorEvent[]>([]);
  private pointer$ = new BehaviorSubject<number>(0); // index into events

  readonly state$ = this.pointer$.pipe(
    map(ptr => {
      const evts = this.events$.getValue().slice(0, ptr);
      return evts.reduce(applyEvent, initialEditorState);
    }),
    shareReplay(1)
  );

  readonly canUndo$ = this.pointer$.pipe(map(ptr => ptr > 0));
  readonly canRedo$ = this.pointer$.pipe(
    map(ptr => ptr < this.events$.getValue().length)
  );

  dispatch(event: EditorEvent): void {
    const ptr    = this.pointer$.getValue();
    const events = this.events$.getValue().slice(0, ptr); // discard future
    this.events$.next([...events, event]);
    this.pointer$.next(ptr + 1);
  }

  undo(): void {
    const ptr = this.pointer$.getValue();
    if (ptr > 0) this.pointer$.next(ptr - 1);
  }

  redo(): void {
    const ptr = this.pointer$.getValue();
    if (ptr < this.events$.getValue().length) this.pointer$.next(ptr + 1);
  }

  // Time-travel: jump to any point in history:
  jumpTo(index: number): void {
    const max = this.events$.getValue().length;
    this.pointer$.next(Math.max(0, Math.min(max, index)));
  }
}
```

---

## Pattern 3: Batched Undo (Group Rapid Changes)

Group changes within a time window into a single undo step:

```typescript
import { Subject } from 'rxjs';
import { bufferTime, filter } from 'rxjs/operators';

class BatchedUndoStore<T> {
  private changes$ = new Subject<T>();
  private history:  T[][] = [];
  private state$   = new BehaviorSubject<T>(this.initial);

  constructor(
    private initial: T,
    batchWindowMs = 500
  ) {
    this.changes$.pipe(
      bufferTime(batchWindowMs),
      filter(batch => batch.length > 0)
    ).subscribe(batch => {
      const before = this.state$.getValue();
      this.history.push([before]); // snapshot before batch
      this.state$.next(batch[batch.length - 1]); // apply final state
    });
  }

  change(newState: T): void { this.changes$.next(newState); }

  undo(): void {
    const snapshot = this.history.pop();
    if (snapshot) this.state$.next(snapshot[0]);
  }
}
```

---

## Pattern 4: Keyboard-Driven Undo/Redo

```typescript
import { fromEvent, merge } from 'rxjs';
import { filter, map } from 'rxjs/operators';

const keydown$ = fromEvent<KeyboardEvent>(document, 'keydown');

const undo$ = keydown$.pipe(
  filter(e => (e.ctrlKey || e.metaKey) && e.key === 'z' && !e.shiftKey),
  map(() => 'undo' as const)
);

const redo$ = keydown$.pipe(
  filter(e => (e.ctrlKey || e.metaKey) && (e.key === 'y' || (e.key === 'z' && e.shiftKey))),
  map(() => 'redo' as const)
);

merge(undo$, redo$).pipe(
  takeUntilDestroyed()
).subscribe(action => {
  if (action === 'undo') store.undo();
  else                   store.redo();
});
```

---

## Pattern 5: Selective Undo (Undo Specific Changes)

```typescript
import { BehaviorSubject } from 'rxjs';

interface VersionedChange<T> {
  id:        string;
  timestamp: number;
  before:    T;
  after:     T;
  label:     string;
}

class SelectiveUndoStore<T> {
  private changelog: VersionedChange<T>[] = [];
  private state$ = new BehaviorSubject<T>(this.initial);

  constructor(private initial: T) {}

  readonly history$ = this.state$.pipe(map(() => [...this.changelog]));

  apply(change: Omit<VersionedChange<T>, 'id' | 'timestamp'>): void {
    this.changelog.push({
      ...change,
      id:        crypto.randomUUID(),
      timestamp: Date.now()
    });
    this.state$.next(change.after);
  }

  // Undo a specific change (not necessarily the latest):
  revert(id: string): void {
    const idx = this.changelog.findLastIndex(c => c.id === id);
    if (idx === -1) return;

    // Re-apply all changes except the reverted one:
    const remaining = this.changelog.filter((_, i) => i !== idx);
    const newState  = remaining.reduce((s, c) => c.after, this.initial);
    this.changelog.splice(idx, 1);
    this.state$.next(newState);
  }
}
```

---

## Pattern 6: Undo with Persistence (LocalStorage)

```typescript
import { BehaviorSubject } from 'rxjs';
import { tap, debounceTime } from 'rxjs/operators';

class PersistentUndoStore<T> extends UndoableState<T> {
  private readonly storageKey: string;

  constructor(key: string, initial: T) {
    super(initial);
    this.storageKey = `undo-store:${key}`;
    this.rehydrate();

    // Persist on every state change (debounced):
    this.state$.pipe(
      debounceTime(500),
      tap(state => localStorage.setItem(this.storageKey, JSON.stringify({
        state,
        past:   this['past'],
        future: this['future']
      })))
    ).subscribe();
  }

  private rehydrate(): void {
    const stored = localStorage.getItem(this.storageKey);
    if (!stored) return;
    try {
      const { state, past, future } = JSON.parse(stored);
      this['past']    = past;
      this['future']  = future;
      this['current$'].next(state);
    } catch { /* ignore corrupt storage */ }
  }
}
```

---

## Choosing an Undo Strategy

| Strategy | Memory | Complexity | Best for |
|---|---|---|---|
| Snapshot stack | O(N × state size) | Low | Small state, infrequent changes |
| Event sourcing | O(N × event size) | Medium | Large state, events are small |
| Batched undo | O(N × state size) | Low | Text editors, form fields |
| Selective undo | O(N × state size) | High | Non-linear workflows |
| Persistent | O(N) + localStorage | Medium | Survives page reload |

---

## Common Pitfalls

### Storing References Instead of Copies

```typescript
// ❌ past array holds same object references — mutations affect history:
this.past.push(this.current$.getValue()); // shallow reference!
this.current$.next({ ...currentState, items: [...currentState.items, newItem] });
// currentState.items was mutated — past state is now corrupted

// ✅ Deep clone before pushing to history:
this.past.push(JSON.parse(JSON.stringify(this.current$.getValue())));
// Or use structuredClone() (modern browsers):
this.past.push(structuredClone(this.current$.getValue()));
```

### Unbounded History

```typescript
// ❌ History grows forever — memory leak for long sessions:
this.past.push(snapshot); // no limit!

// ✅ Cap history at a reasonable size:
this.past.push(snapshot);
if (this.past.length > 100) this.past.shift(); // remove oldest
```
