# mergeMap

## Identity
- **Name**: mergeMap (alias: flatMap)
- **Category**: Transformation Operators
- **Type**: Higher-order transformation operator (flattening strategy)
- **Import**: 
  ```typescript
  import { mergeMap } from 'rxjs/operators';
  
  // Alias (identical functionality)
  import { flatMap } from 'rxjs/operators';
  ```
- **Signature**: 
  ```typescript
  function mergeMap<T, R, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O,
    resultSelector?: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R,
    concurrent?: number
  ): OperatorFunction<T, ObservedValueOf<O> | R>;
  
  // Most common form (without deprecated resultSelector)
  function mergeMap<T, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O,
    concurrent?: number
  ): OperatorFunction<T, ObservedValueOf<O>>;
  ```

## Functional Specification

**Input**: Observable<T> emitting values v₁, v₂, v₃, ...

**Output**: Observable<R> emitting flattened values from all inner Observables

**Transformation**: For each source emission, `mergeMap` applies the projection function to create an inner Observable, subscribes to it immediately, and merges all emissions from all active inner Observables into the output stream. Multiple inner Observables can emit concurrently.

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let project: T → Observable<R> be the projection function
Let O_i = project(v_i) be the inner Observable for each v_i

Output = merge(O₁, O₂, O₃, ...)

Where merge subscribes to all O_i concurrently and forwards all emissions
to the output stream in the order they arrive.
```

**Invariants**:
- **Concurrent execution**: All inner Observables run simultaneously (default behavior)
- **Order non-deterministic**: Output order depends on inner Observable timing, not source order
- **No cancellation**: Starting a new inner Observable does NOT cancel previous ones
- **Memory growth**: Active inner subscriptions accumulate (bounded only by source emissions)
- **Immediate subscription**: Subscribes to each inner Observable as soon as source emits
- **Eager evaluation**: Does not wait for previous inner Observables to complete

## Marble Diagram

```
Source:    --1-----2-----3-----|
              |     |     |
              v     v     v
          project(x) = Observable that emits x*10 after x*100ms
              |     |     |
Inner 1:      -----10|
Inner 2:            -----20|
Inner 3:                  -----30|
              mergeMap(project)
Result:    -------10--20--30----|

Legend:
  - : time unit (10ms)
  1,2,3 : source values
  10,20,30 : inner Observable emissions
  | : completion
  Inner observables run concurrently, merge their outputs
```

**More complex example with overlapping emissions:**
```
Source:    --a-----b--c------|
              |     |  |
              v     v  v
          project(x) = interval that emits x1, x2, x3
              |     |  |
Inner a:      --a1--a2--a3|
Inner b:            --b1--b2--b3|
Inner c:               --c1--c2--c3|
              mergeMap(project)
Result:    ----a1--a2b1a3c1b2--c2b3--c3|

Key observation: Emissions interleave based on timing, not source order
```

## Behavioral Characteristics

**Subscription**: 
- Subscribes to source Observable immediately upon subscription
- Creates a new inner subscription for EACH source emission
- All inner Observables run concurrently (no queuing by default)
- Does NOT cancel previous inner subscriptions when new ones start
- Concurrent limit can be specified (default: Infinity)

**Completion semantics**:
- Waits for BOTH source AND all inner Observables to complete
- Source completion does not cancel active inner Observables
- Result completes only when:
  1. Source has completed AND
  2. All inner Observables created from source emissions have completed
- If source completes but inner Observables never complete, result never completes
- Empty source (completes without emitting) causes immediate completion

**Error handling**:
- Any error from source propagates immediately to output
- Any error from ANY inner Observable propagates immediately to output
- First error wins (unsubscribes from source and all inner Observables)
- No built-in error recovery mechanism
- Errors cancel all pending inner subscriptions

**Backpressure**:
- No built-in backpressure handling
- Fast source + slow inner Observables = memory accumulation
- Number of concurrent inner subscriptions limited by `concurrent` parameter
- When concurrent limit reached, new inner Observables are queued
- Default concurrent = Infinity (unlimited parallelism)

**Concurrency control**:
```typescript
// Unlimited concurrency (default)
source$.pipe(mergeMap(project))

// Limit to 3 concurrent inner Observables
source$.pipe(mergeMap(project, 3))

// Sequential processing (equivalent to concatMap)
source$.pipe(mergeMap(project, 1))
```

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source Observable emission type
 *   O extends ObservableInput<any> - Inner Observable type (can be Observable, Promise, Array, etc.)
 *   R - Result type (inferred from inner Observable emissions)
 * 
 * Input Type: Observable<T>
 * 
 * Output Type: Observable<ObservedValueOf<O>>
 *   - Extracts the emission type from the inner ObservableInput
 *   - Flattens nested Observable structure
 * 
 * Type Narrowing:
 *   - Infers R from project function return type
 *   - Supports Promises, Arrays, Iterables (automatically converted to Observables)
 *   - Preserves union types through flattening
 *   - Index parameter type-safe (number)
 * 
 * Type Safety:
 *   - Compile-time verification of project function signature
 *   - Type-safe flattening (no Observable<Observable<T>> in output)
 *   - Enforces ObservableInput constraint on project return type
 *   - Type inference for nested asynchronous operations
 */

// Example: Type preservation with Promises
import { of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Source: Observable<number>
const numbers$ = of(1, 2, 3);

// Project returns Promise<string>
// Result type: Observable<string> (not Observable<Promise<string>>)
const result$ = numbers$.pipe(
  mergeMap(async (n) => {
    const response = await fetch(`/api/data/${n}`);
    return response.text(); // Returns Promise<string>
  })
);

result$.subscribe((text: string) => {
  console.log(text); // Type-safe: text is string, not Promise<string>
});

// Example: Union type handling
interface Success { type: 'success'; data: string; }
interface Error { type: 'error'; message: string; }

const requests$ = of(1, 2, 3);

// Project returns Observable<Success | Error>
// Result type: Observable<Success | Error>
const responses$ = requests$.pipe(
  mergeMap(id => 
    ajax.getJSON<string>(`/api/${id}`).pipe(
      map((data): Success => ({ type: 'success', data })),
      catchError((err): Observable<Error> => 
        of({ type: 'error', message: err.message })
      )
    )
  )
);

responses$.subscribe(response => {
  // TypeScript knows response is Success | Error
  if (response.type === 'success') {
    console.log(response.data); // Type narrowed to Success
  } else {
    console.log(response.message); // Type narrowed to Error
  }
});

// Example: Array flattening
const arrays$ = of([1, 2], [3, 4], [5, 6]);

// mergeMap automatically converts arrays to observables
const flattened$ = arrays$.pipe(
  mergeMap(arr => arr) // arr is number[], returns Observable<number>
);
// Result type: Observable<number>

flattened$.subscribe(n => console.log(n)); // 1, 2, 3, 4, 5, 6
```

## Examples

### Basic Usage - HTTP Request per Item
```typescript
import { of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Fetch user details for each ID
const userIds$ = of(1, 2, 3);

const users$ = userIds$.pipe(
  mergeMap(id => 
    ajax.getJSON(`https://api.example.com/users/${id}`)
  )
);

users$.subscribe(user => console.log('User:', user));

// Output (order may vary based on response timing):
// User: { id: 1, name: 'Alice' }
// User: { id: 2, name: 'Bob' }
// User: { id: 3, name: 'Charlie' }

// Note: All 3 requests happen concurrently!
```

### Common Pattern - Search with Concurrent Requests
```typescript
import { fromEvent } from 'rxjs';
import { debounceTime, distinctUntilChanged, mergeMap, map } from 'rxjs/operators';

interface SearchResult {
  query: string;
  results: string[];
}

// Search input element
const searchInput = document.getElementById('search') as HTMLInputElement;
const search$ = fromEvent(searchInput, 'input');

const searchResults$ = search$.pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300),
  distinctUntilChanged(),
  mergeMap(query => 
    ajax.getJSON<string[]>(`/api/search?q=${query}`).pipe(
      map((results): SearchResult => ({ query, results }))
    )
  )
);

searchResults$.subscribe(({ query, results }) => {
  console.log(`Results for "${query}":`, results);
  updateSearchUI(results);
});

// Behavior:
// User types: "rx"
// Wait 300ms...
// Request for "rx" starts
// User types: "rxjs" (within 300ms)
// Wait 300ms...
// Request for "rxjs" starts (previous "rx" request NOT cancelled)
// Both requests may complete, order depends on server response time
```

### Edge Cases - Error Handling and Concurrency Control
```typescript
import { of, throwError, interval } from 'rxjs';
import { mergeMap, take, catchError } from 'rxjs/operators';

// Edge case 1: Error from inner Observable
const withError$ = of(1, 2, 3).pipe(
  mergeMap(n => 
    n === 2 
      ? throwError(() => new Error(`Error processing ${n}`))
      : of(n * 10)
  )
);

withError$.subscribe({
  next: v => console.log('Value:', v),
  error: err => console.log('Error caught:', err.message)
});
// Output:
// Value: 10  (from n=1)
// Error caught: Error processing 2
// (n=3 never processes due to error)

// Edge case 2: Concurrency limit
const limited$ = of(1, 2, 3, 4, 5).pipe(
  mergeMap(
    n => interval(1000).pipe(
      take(3),
      map(i => `Item ${n}-${i}`)
    ),
    2  // Maximum 2 concurrent inner Observables
  )
);

limited$.subscribe(v => console.log(v));
// Output (over time):
// t=1s: Item 1-0, Item 2-0  (first 2 start)
// t=2s: Item 1-1, Item 2-1
// t=3s: Item 1-2, Item 2-2
// t=3s: Item 3-0, Item 4-0  (next 2 start after first 2 complete)
// t=4s: Item 3-1, Item 4-1
// ...

// Edge case 3: Inner Observable never completes
const neverCompletes$ = of(1, 2).pipe(
  mergeMap(n => interval(1000)) // Creates infinite inner Observables
);

const subscription = neverCompletes$.subscribe(v => console.log(v));

// Cleanup after 5 seconds
setTimeout(() => {
  subscription.unsubscribe();
  console.log('Unsubscribed from never-completing stream');
}, 5000);

// Without manual unsubscribe, memory leak occurs!

// Edge case 4: Empty source
const empty$ = of().pipe(
  mergeMap(n => of(n * 10))
);

empty$.subscribe({
  next: v => console.log('Value:', v),
  complete: () => console.log('Completed')
});
// Output: Completed (immediately, no values emitted)
```

### Advanced Pattern - Parallel Data Processing with Error Recovery
```typescript
import { from } from 'rxjs';
import { mergeMap, retry, catchError, map } from 'rxjs/operators';

interface DataItem {
  id: number;
  data: string;
}

interface ProcessResult {
  id: number;
  status: 'success' | 'error';
  result?: any;
  error?: string;
}

const dataItems: DataItem[] = [
  { id: 1, data: 'item1' },
  { id: 2, data: 'item2' },
  { id: 3, data: 'item3' }
];

// Process items in parallel with retry and error handling
const processed$ = from(dataItems).pipe(
  mergeMap(
    item => processItem(item).pipe(
      retry(2), // Retry failed items twice
      map((result): ProcessResult => ({
        id: item.id,
        status: 'success',
        result
      })),
      catchError((error): Observable<ProcessResult> => 
        of({
          id: item.id,
          status: 'error',
          error: error.message
        })
      )
    ),
    3 // Process max 3 items concurrently
  )
);

processed$.subscribe(result => {
  if (result.status === 'success') {
    console.log(`✓ Item ${result.id} processed:`, result.result);
  } else {
    console.log(`✗ Item ${result.id} failed:`, result.error);
  }
});

function processItem(item: DataItem): Observable<any> {
  return ajax.post('/api/process', item).pipe(
    map(response => response.response)
  );
}

// Benefits:
// - Parallel processing (up to 3 concurrent)
// - Individual error handling (one failure doesn't stop others)
// - Automatic retries for transient failures
// - Type-safe result handling
```

### Advanced Pattern - Race Condition Demonstration
```typescript
import { of } from 'rxjs';
import { mergeMap, delay } from 'rxjs/operators';

// Demonstrates non-deterministic ordering
const source$ = of('first', 'second', 'third');

const raceCondition$ = source$.pipe(
  mergeMap(value => {
    // Simulate varying response times
    const delayTime = value === 'first' ? 300 : 
                      value === 'second' ? 100 : 
                      200;
    
    return of(`${value} processed`).pipe(delay(delayTime));
  })
);

raceCondition$.subscribe(result => console.log(result));

// Output (NOT in source order):
// "second processed"  (100ms)
// "third processed"   (200ms)
// "first processed"   (300ms)

// This demonstrates why mergeMap is NOT suitable when order matters!
// Use concatMap for ordered processing or switchMap for latest-only.
```

## Common Pitfalls

### Anti-pattern 1: Using mergeMap When Order Matters
```typescript
// ❌ INCORRECT: Assuming output order matches source order
import { of } from 'rxjs';
import { mergeMap, delay } from 'rxjs/operators';

const sequence$ = of(1, 2, 3);

const incorrectOrder$ = sequence$.pipe(
  mergeMap(n => 
    of(n * 10).pipe(delay(Math.random() * 1000))
  )
);

incorrectOrder$.subscribe(v => console.log(v));
// Output: 30, 10, 20 (or any random order!)
// User expects: 10, 20, 30

// ✅ CORRECT: Use concatMap for ordered processing
import { concatMap } from 'rxjs/operators';

const correctOrder$ = sequence$.pipe(
  concatMap(n => 
    of(n * 10).pipe(delay(Math.random() * 1000))
  )
);

correctOrder$.subscribe(v => console.log(v));
// Output: 10, 20, 30 (always in source order)

// WHY: mergeMap subscribes to all inner Observables concurrently
// WHEN TO USE mergeMap: When order doesn't matter and parallelism is desired
// WHEN TO USE concatMap: When sequential processing and order preservation are required
```

### Anti-pattern 2: Memory Leaks from Unmanaged Subscriptions
```typescript
// ❌ INCORRECT: Creating infinite inner Observables without cleanup
import { interval } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

const clicks$ = fromEvent(document, 'click');

const leak$ = clicks$.pipe(
  mergeMap(() => interval(1000)) // Each click creates a new interval!
);

leak$.subscribe(n => console.log('Tick:', n));

// Problem: Each click creates a new interval that never completes
// After 100 clicks: 100 active intervals running forever
// Memory usage grows unbounded!

// ✅ CORRECT: Use switchMap to cancel previous inner Observable
import { switchMap } from 'rxjs/operators';

const noLeak$ = clicks$.pipe(
  switchMap(() => interval(1000)) // Cancels previous interval on new click
);

noLeak$.subscribe(n => console.log('Tick:', n));

// Alternative: Use take/takeUntil to limit inner Observable lifetime
const limited$ = clicks$.pipe(
  mergeMap(() => 
    interval(1000).pipe(
      take(10) // Each interval stops after 10 emissions
    )
  )
);

// WHY: mergeMap never cancels inner Observables automatically
// SOLUTION: Use switchMap for latest-only, or limit inner Observable duration
// MEMORY: Always ensure inner Observables complete or use takeUntil
```

### Anti-pattern 3: Not Handling Errors from Inner Observables
```typescript
// ❌ INCORRECT: One error kills the entire stream
import { of, throwError } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

const tasks$ = of(1, 2, 3, 4, 5);

const fragile$ = tasks$.pipe(
  mergeMap(n => 
    n === 3 
      ? throwError(() => new Error('Task 3 failed'))
      : of(`Task ${n} completed`)
  )
);

fragile$.subscribe({
  next: v => console.log(v),
  error: err => console.log('Stream died:', err.message)
});
// Output:
// Task 1 completed
// Task 2 completed
// Stream died: Task 3 failed
// Tasks 4 and 5 never run!

// ✅ CORRECT: Handle errors within inner Observables
import { catchError } from 'rxjs/operators';

const resilient$ = tasks$.pipe(
  mergeMap(n => 
    (n === 3 
      ? throwError(() => new Error('Task 3 failed'))
      : of(`Task ${n} completed`)
    ).pipe(
      catchError(err => of(`Task ${n} error: ${err.message}`))
    )
  )
);

resilient$.subscribe(v => console.log(v));
// Output:
// Task 1 completed
// Task 2 completed
// Task 3 error: Task 3 failed
// Task 4 completed
// Task 5 completed

// WHY: Errors in inner Observables propagate to outer stream
// SOLUTION: Use catchError within the inner Observable pipeline
// PRINCIPLE: Handle errors at the appropriate level (inner vs outer)
```

### Anti-pattern 4: Overloading with Too Many Concurrent Operations
```typescript
// ❌ INCORRECT: Unlimited concurrency overwhelming resources
import { range } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Process 10,000 items with unlimited concurrency
const overload$ = range(1, 10000).pipe(
  mergeMap(n => 
    ajax.post('/api/process', { id: n })
  )
);

overload$.subscribe();
// Problem: 
// - Opens 10,000 simultaneous HTTP connections
// - Overwhelms browser connection pool
// - May overwhelm server
// - High memory usage

// ✅ CORRECT: Limit concurrency to reasonable number
const controlled$ = range(1, 10000).pipe(
  mergeMap(
    n => ajax.post('/api/process', { id: n }),
    6 // Maximum 6 concurrent requests (browser limit is ~6-8)
  )
);

controlled$.subscribe();
// Benefits:
// - Respects browser connection limits
// - Server load remains manageable
// - Predictable memory usage
// - Still benefits from parallelism

// Rule of thumb for concurrency limits:
// - HTTP requests: 4-6 (browser connection pool limit)
// - CPU-intensive tasks: navigator.hardwareConcurrency
// - I/O operations: 10-20 depending on resource constraints
// - Database queries: Based on connection pool size

// WHY: Unlimited concurrency can exhaust system resources
// PERFORMANCE: Measure and tune concurrency for your specific use case
// DEFAULT: mergeMap uses Infinity - always consider adding a limit for production
```

### Performance: High-Frequency Sources
**When this matters**: 
- Source emits very frequently (>100 Hz)
- Inner Observables are long-lived
- Memory constraints exist

**What to do**:
```typescript
// Use debounce/throttle to reduce inner Observable creation
source$.pipe(
  debounceTime(100),  // Wait for 100ms of silence
  mergeMap(createInnerObservable)
);

// Or audit to sample at regular intervals
source$.pipe(
  auditTime(100),  // At most one emission per 100ms
  mergeMap(createInnerObservable)
);

// Limit concurrency to prevent memory explosion
source$.pipe(
  mergeMap(createInnerObservable, 10)  // Max 10 concurrent
);

// Consider switchMap if only latest matters
source$.pipe(
  switchMap(createInnerObservable)  // Cancels previous
);
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`concatMap`**: Sequential version of mergeMap - waits for each inner Observable to complete before starting the next. Use when order matters or you need sequential processing.
- **`switchMap`**: Cancels previous inner Observable when source emits. Use when only the latest result matters (like search, navigation).
- **`exhaustMap`**: Ignores new source emissions while an inner Observable is active. Use for preventing duplicate operations (like preventing double-clicks).
- **`mergeAll`**: Flattens Observable<Observable<T>> to Observable<T> without transformation. Use when you already have nested Observables.

**Complementary Operators**:
- **`catchError`**: Handle errors from inner Observables → `mergeMap(x => doWork(x).pipe(catchError(...)))`
- **`retry`**: Retry failed inner Observables → `mergeMap(x => doWork(x).pipe(retry(3)))`
- **`take` / `takeUntil`**: Limit inner Observable duration → `mergeMap(x => interval(100).pipe(take(10)))`
- **`debounceTime`**: Rate-limit source before mergeMap → `source$.pipe(debounceTime(300), mergeMap(...))`
- **`shareReplay`**: Share inner Observable results → `mergeMap(x => expensive(x).pipe(shareReplay(1)))`

**Alternatives by Use Case**:

| Use Case | Instead of mergeMap | Use This | Why |
|----------|-------------------|----------|-----|
| Ordered processing | `mergeMap(x => process(x))` | `concatMap(x => process(x))` | Preserves source order |
| Latest result only | `mergeMap(x => search(x))` | `switchMap(x => search(x))` | Cancels outdated searches |
| Prevent duplicates | `mergeMap(x => save(x))` | `exhaustMap(x => save(x))` | Ignores clicks during save |
| Simple flattening | `mergeMap(x => x)` | `mergeAll()` | More explicit intent |
| Promise conversion | `mergeMap(x => Promise)` | `from(Promise)` or keep mergeMap | Both work, mergeMap for chaining |
| Parallel HTTP (no transform) | `mergeMap(x => http.get(x))` | Use mergeMap | ✓ Correct choice |

**Comparison Table**:

| Operator | Concurrency | Cancellation | Order Preserved | Use When |
|----------|-------------|--------------|-----------------|----------|
| `mergeMap` | Unlimited (configurable) | No | No | Parallel ops, order doesn't matter |
| `concatMap` | 1 (sequential) | No | Yes | Order matters, sequential processing |
| `switchMap` | 1 (latest only) | Yes (previous) | No | Only latest matters (search, nav) |
| `exhaustMap` | 1 (ignore new) | No | N/A | Prevent duplicates (save, submit) |

**Migration from Deprecated API**:
```typescript
// RxJS 5 (deprecated resultSelector)
source$.pipe(
  mergeMap(
    outer => inner$,
    (outer, inner) => ({ outer, inner })
  )
)

// RxJS 6+ (current)
source$.pipe(
  mergeMap(outer => 
    inner$.pipe(
      map(inner => ({ outer, inner }))
    )
  )
)
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/mergeMap](https://rxjs.dev/api/operators/mergeMap)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/mergeMap.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/mergeMap.ts)
- **RxJS Team Guide**: "Choosing the right flattening operator" - [RxJS Blog](https://blog.rxjs.dev)
- **Academic Foundation**: Functional Reactive Programming - Higher-order observable composition

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: 
- **Pattern**: Concurrent Composition Strategy (Parallel Flattening)
- **Cognitive Load**: High (4/5) - Requires understanding of higher-order Observables, concurrency, and flattening
- **Usage Frequency**: Very High (5/5) - One of the most commonly used operators for async operations
- **Composability**: High (5/5) - Fundamental building block for complex async flows

**Problem Domain**: 
Transforming each source emission into an asynchronous operation (HTTP request, database query, file I/O) and merging all results as they complete, maximizing throughput through parallelism.

**When to Teach**: 
After students understand basic operators (map, filter) and have grasped the concept of Observables. Before introducing concatMap/switchMap/exhaustMap, as mergeMap is the foundation for understanding flattening strategies. Essential prerequisite for async programming patterns.

**Common Misconceptions**:
1. **"mergeMap preserves order"** - No, use concatMap for order preservation
2. **"mergeMap is always the best choice for HTTP"** - No, switchMap is often better for user-driven requests
3. **"Unlimited concurrency is fine"** - No, always consider adding a concurrency limit in production
4. **"mergeMap cancels previous subscriptions"** - No, that's switchMap

**Teaching Progression**:
1. Start with `map` (synchronous transformation)
2. Introduce the problem: "What if the transformation returns an Observable?"
3. Show Observable<Observable<T>> nesting problem
4. Introduce `mergeMap` as the solution (flattening)
4. Demonstrate concurrency with marble diagrams
5. Compare with `concatMap` (sequential) and `switchMap` (cancelling)
6. Teach error handling and concurrency control
7. Practice with real HTTP request scenarios
