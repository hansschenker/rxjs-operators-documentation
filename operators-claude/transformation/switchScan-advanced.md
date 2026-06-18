# switchScan — Advanced Patterns

For `switchScan` fundamentals see the core [switchScan](./switchScan) doc. This page covers progressive search, cancellable accumulation, state machines with cancellation, and the key differences from `scan` and `switchMap`.

---

## What `switchScan` Does

`switchScan` is `scan` + `switchMap` in one operator. For each source emission it calls a project function with `(accumulator, value)` that returns an Observable. When a new emission arrives, the previous inner Observable is cancelled (like `switchMap`), and the new one starts with the *accumulated* state.

```typescript
import { switchScan } from 'rxjs/operators';

source$.pipe(
  switchScan(
    (acc, value) => innerObservable$(acc, value),
    initialAccumulator
  )
)
```

This is the operator for: "I need state that accumulates across emissions, but each new emission should cancel the previous async operation."

---

## Pattern 1: Progressive Search with Accumulated Results

Load results incrementally — each new query cancels the previous load and resets accumulation:

```typescript
import { switchScan } from 'rxjs/operators';
import { of } from 'rxjs';

interface SearchState {
  query:   string;
  results: SearchResult[];
  page:    number;
  done:    boolean;
}

const initialState: SearchState = { query: '', results: [], page: 0, done: false };

// loadMore$ emits when user scrolls to bottom:
const loadMore$ = new Subject<void>();

query$.pipe(
  switchScan(
    (acc, query) => {
      // New query → reset accumulator, cancel previous page loads:
      if (query !== acc.query) {
        return this.api.search(query, 1).pipe(
          map(results => ({ query, results, page: 1, done: results.length === 0 }))
        );
      }
      // Same query, load next page:
      if (acc.done) return of(acc); // nothing more to load
      return this.api.search(acc.query, acc.page + 1).pipe(
        map(newResults => ({
          ...acc,
          results: [...acc.results, ...newResults],
          page:    acc.page + 1,
          done:    newResults.length === 0
        }))
      );
    },
    initialState
  )
).subscribe(renderResults);

// Trigger page loads:
loadMore$.subscribe(() => query$.next(query$.getValue()));
```

---

## Pattern 2: Auto-Save with Accumulated Draft

Accumulate edits and save the latest — cancel in-flight saves when new edits arrive:

```typescript
import { switchScan, debounceTime } from 'rxjs/operators';

interface SaveState {
  draft:      DocumentContent;
  savedAt:    Date | null;
  saving:     boolean;
  dirtyCount: number;
}

edits$.pipe(
  switchScan(
    (state, edit) => {
      const draft = applyEdit(state.draft, edit);
      return this.api.saveDraft(draft).pipe(
        map(saved => ({
          draft,
          savedAt:    new Date(),
          saving:     false,
          dirtyCount: 0
        })),
        startWith({
          draft,
          savedAt:    state.savedAt,
          saving:     true,
          dirtyCount: state.dirtyCount + 1
        })
      );
    },
    { draft: initialContent, savedAt: null, saving: false, dirtyCount: 0 }
  )
).subscribe(updateSaveIndicator);
// New edit arrives mid-save → previous save cancelled, latest draft saves instead
```

---

## Pattern 3: Retry with Accumulated Context

Carry context across retry attempts:

```typescript
import { switchScan, retryWhen, delay, scan } from 'rxjs/operators';
import { timer } from 'rxjs';

interface RetryState {
  attempts: number;
  lastError: Error | null;
  result:    unknown | null;
}

operations$.pipe(
  switchScan(
    (state, operation) =>
      this.api.execute(operation).pipe(
        map(result => ({ attempts: state.attempts + 1, lastError: null, result })),
        retryWhen(errors =>
          errors.pipe(
            scan((attempt, err) => ({ attempt: attempt.attempt + 1, err }), { attempt: 0, err: null }),
            tap(({ attempt, err }) => {
              updateRetryUI(attempt, err); // show retry state using accumulated info
            }),
            delayWhen(({ attempt }) => timer(1000 * Math.pow(2, attempt))),
            take(3)
          )
        ),
        catchError(err => of({ attempts: state.attempts + 1, lastError: err, result: null }))
      ),
    { attempts: 0, lastError: null, result: null }
  )
).subscribe(handleResult);
```

---

## Pattern 4: Streaming Aggregation (Running Totals Per Category)

```typescript
import { switchScan } from 'rxjs/operators';

interface CategoryTotals { [category: string]: number; }

// Each event updates running totals; expensive re-aggregate only when category changes:
events$.pipe(
  switchScan(
    (totals, event) =>
      this.stats.getUpdatedTotals(totals, event).pipe(
        // Returns updated totals; if event is for a new category,
        // cancels and re-runs the full aggregate
        startWith(totals) // show existing totals while calculating
      ),
    {} as CategoryTotals
  )
).subscribe(renderTotals);
```

---

## `switchScan` vs `scan` vs `switchMap` + `scan`

```typescript
// scan — synchronous accumulation, no async, no cancellation:
source$.pipe(scan((acc, val) => newAcc, initial))
// ✓ Simple, synchronous
// ✗ Cannot handle async operations

// switchMap then scan — async but no accumulated state in project:
source$.pipe(
  switchMap(val => asyncOp$(val)),
  scan((acc, result) => [...acc, result], [])
)
// ✓ Async operations
// ✓ Accumulation
// ✗ Accumulator not available when starting new async op

// switchScan — async WITH accumulated state available in project:
source$.pipe(
  switchScan(
    (acc, val) => asyncOp$(acc, val), // acc available!
    initial
  )
)
// ✓ Async operations
// ✓ Accumulation in project function
// ✓ Previous inner Observable cancelled on new emission
```

---

## Common Pitfalls

### Accumulator Mutation

```typescript
// ❌ Mutating the accumulator — causes reference issues:
source$.pipe(
  switchScan((acc, val) => {
    acc.items.push(val); // mutation! acc is the same object
    return of(acc);
  }, { items: [] })
)

// ✅ Always return new objects:
source$.pipe(
  switchScan((acc, val) => of({ ...acc, items: [...acc.items, val] }), { items: [] })
)
```

### Forgetting That `switchScan` Emits Accumulator, Not Inner Values

```typescript
// ❌ Expecting inner Observable values directly:
source$.pipe(
  switchScan((acc, val) => this.api.fetch(val).pipe(
    map(result => [...acc, result])
  ), [])
)
// ✓ This is CORRECT — the map produces the new accumulator
// The outer stream emits the accumulator (array), not individual results
```

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `switchScan` fills a specific niche — when you need `scan`'s running state AND `switchMap`'s cancellation semantics in the same operation. The most common real-world use is progressive loading where each new query should cancel in-flight page loads but accumulate results for the same query.
