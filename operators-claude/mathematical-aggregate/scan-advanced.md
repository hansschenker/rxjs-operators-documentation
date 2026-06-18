# scan — Advanced Patterns

For `scan` fundamentals, see the core [scan](./scan) doc. This page covers finite state machines, ring buffers, undo/redo, and complex accumulation.

---

## Pattern 1: Finite State Machine

`scan` is ideal for implementing FSMs — each action transitions state.

```typescript
import { scan, map } from 'rxjs/operators';
import { merge, Subject } from 'rxjs';

type State = 'idle' | 'loading' | 'success' | 'error';
type Action =
  | { type: 'FETCH' }
  | { type: 'SUCCESS'; data: unknown }
  | { type: 'FAILURE'; error: string }
  | { type: 'RESET' };

const transitions: Record<State, Partial<Record<Action['type'], State>>> = {
  idle:    { FETCH: 'loading' },
  loading: { SUCCESS: 'success', FAILURE: 'error' },
  success: { FETCH: 'loading', RESET: 'idle' },
  error:   { FETCH: 'loading', RESET: 'idle' }
};

const actions$ = new Subject<Action>();

const state$ = actions$.pipe(
  scan((state: State, action) => {
    const next = transitions[state][action.type];
    return next ?? state; // ignore invalid transitions
  }, 'idle' as State)
);

state$.subscribe(state => console.log('State:', state));
actions$.next({ type: 'FETCH' });     // → loading
actions$.next({ type: 'SUCCESS', data: {} }); // → success
actions$.next({ type: 'RESET' });     // → idle
actions$.next({ type: 'SUCCESS', data: {} }); // ignored (not valid from idle)
```

---

## Pattern 2: Ring Buffer (Fixed-Size Window)

```typescript
import { scan } from 'rxjs/operators';

function ringBuffer<T>(size: number) {
  return scan<T, T[]>((buffer, value) => {
    const next = [...buffer, value];
    return next.length > size ? next.slice(next.length - size) : next;
  }, []);
}

// Keep last 5 mouse positions:
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  map(e => ({ x: e.clientX, y: e.clientY })),
  ringBuffer(5)
).subscribe(trail => drawTrail(trail));
// Always emits an array of the last 5 positions
```

---

## Pattern 3: Undo/Redo History

```typescript
import { Subject, merge } from 'rxjs';
import { scan, map, filter } from 'rxjs/operators';

interface HistoryState<T> {
  past:    T[];
  present: T;
  future:  T[];
  canUndo: boolean;
  canRedo: boolean;
}

const edit$  = new Subject<string>();
const undo$  = new Subject<void>();
const redo$  = new Subject<void>();

type HistoryAction =
  | { type: 'EDIT';  value: string }
  | { type: 'UNDO' }
  | { type: 'REDO' };

const history$ = merge(
  edit$.pipe(map(value => ({ type: 'EDIT', value } as HistoryAction))),
  undo$.pipe(map(() => ({ type: 'UNDO' } as HistoryAction))),
  redo$.pipe(map(() => ({ type: 'REDO' } as HistoryAction)))
).pipe(
  scan((h: HistoryState<string>, action): HistoryState<string> => {
    switch (action.type) {
      case 'EDIT':
        return {
          past:    [...h.past, h.present],
          present: action.value,
          future:  [],
          canUndo: true,
          canRedo: false
        };
      case 'UNDO':
        if (!h.canUndo) return h;
        return {
          past:    h.past.slice(0, -1),
          present: h.past[h.past.length - 1],
          future:  [h.present, ...h.future],
          canUndo: h.past.length > 1,
          canRedo: true
        };
      case 'REDO':
        if (!h.canRedo) return h;
        return {
          past:    [...h.past, h.present],
          present: h.future[0],
          future:  h.future.slice(1),
          canUndo: true,
          canRedo: h.future.length > 1
        };
    }
  }, { past: [], present: '', future: [], canUndo: false, canRedo: false })
);

history$.subscribe(({ present, canUndo, canRedo }) => {
  editorEl.value = present;
  undoBtn.disabled = !canUndo;
  redoBtn.disabled = !canRedo;
});
```

---

## Pattern 4: Event Counting and Rate Tracking

```typescript
import { scan, map, distinctUntilChanged } from 'rxjs/operators';
import { interval, combineLatest } from 'rxjs';

// Count events with rolling statistics:
const stats$ = events$.pipe(
  scan((stats, event) => ({
    total:      stats.total + 1,
    byType:     {
      ...stats.byType,
      [event.type]: (stats.byType[event.type] ?? 0) + 1
    },
    lastSeen:   event.timestamp,
    perMinute:  stats.total / ((event.timestamp - stats.startTime) / 60_000)
  }), {
    total: 0,
    byType: {} as Record<string, number>,
    lastSeen: 0,
    perMinute: 0,
    startTime: Date.now()
  })
);
```

---

## Pattern 5: Accumulate Until Condition, Then Reset

```typescript
import { scan, filter, map } from 'rxjs/operators';

interface Batch<T> { items: T[]; done: boolean; }

// Accumulate items until a sentinel value, emit the batch, reset:
dataStream$.pipe(
  scan<Item | null, Batch<Item>>((batch, item) => {
    if (item === null) {
      return { items: batch.items, done: true }; // sentinel: emit and reset
    }
    return { items: [...batch.items, item], done: false };
  }, { items: [], done: false }),
  filter(batch => batch.done),
  map(batch => batch.items)
  // next emission resets because scan accumulator starts fresh from the last state
).subscribe(completeBatch => processBatch(completeBatch));
```

---

## Pattern 6: Moving Average

```typescript
import { scan, map } from 'rxjs/operators';

function movingAverage(windowSize: number) {
  return (source$: Observable<number>) => source$.pipe(
    scan<number, number[]>((window, value) => {
      const next = [...window, value];
      return next.length > windowSize ? next.slice(1) : next;
    }, []),
    filter(window => window.length === windowSize), // only emit full windows
    map(window => window.reduce((a, b) => a + b, 0) / windowSize)
  );
}

sensorData$.pipe(
  movingAverage(10) // 10-sample moving average
).subscribe(avg => updateChart(avg));
```

---

## Pattern 7: Deduplicated Event Log

```typescript
import { scan, distinctUntilChanged, map } from 'rxjs/operators';

// Build a deduplicated event log — no consecutive duplicates:
const log$ = events$.pipe(
  scan<AppEvent, AppEvent[]>((log, event) => {
    const last = log[log.length - 1];
    if (last?.id === event.id) return log; // skip duplicate
    return [...log.slice(-99), event];     // keep last 100 entries
  }, [])
);
```

---

## `scan` vs `reduce` — When to Use Each

| | `scan` | `reduce` |
|---|---|---|
| Emits | After every value | Only on completion |
| Source must complete | No | Yes |
| Live/streaming data | ✅ | ❌ |
| One final aggregate | ❌ (use reduce) | ✅ |
| State machines | ✅ | ❌ |
| Running totals | ✅ | ❌ |

---

## Common Pitfalls

### Mutating the Accumulator

```typescript
// ❌ MUTATION — modifies the accumulator array in place
events$.pipe(
  scan((log, event) => {
    log.push(event); // mutates the existing array!
    return log;
  }, [] as AppEvent[])
).subscribe(log => {
  // Angular/React may not detect the change (same reference)
  // Previous states in undo history are corrupted
});

// ✅ Immutable update — always return a new reference
events$.pipe(
  scan((log, event) => [...log, event], [] as AppEvent[])
).subscribe(render);
// WHY: scan passes the same accumulator reference back into the next
// iteration. Mutating it corrupts previous emissions and breaks
// change detection in frameworks that use reference equality.
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**Key insight**: `scan` is the RxJS reduce that keeps going. Any time you need "state that evolves over time" — counters, buffers, FSMs, history — `scan` is the operator. The accumulator is your state; each emission is an action.
