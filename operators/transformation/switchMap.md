# switchMap

## Identity
- **Name**: switchMap
- **Category**: Transformation Operators
- **Type**: Higher-order transformation operator (flattening strategy - cancelling)
- **Import**: 
  ```typescript
  import { switchMap } from 'rxjs/operators';
  ```
- **Signature**: 
  ```typescript
  function switchMap<T, R, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O,
    resultSelector?: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R
  ): OperatorFunction<T, ObservedValueOf<O> | R>;
  
  // Most common form (without deprecated resultSelector)
  function switchMap<T, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O
  ): OperatorFunction<T, ObservedValueOf<O>>;
  ```

## Functional Specification

**Input**: Observable<T> emitting values v₁, v₂, v₃, ...

**Output**: Observable<R> emitting values from the most recent inner Observable only

**Transformation**: For each source emission, `switchMap` applies the projection function to create an inner Observable, **cancels any previous inner Observable**, subscribes to the new one, and emits its values. Only the most recent inner Observable is active at any time.

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let project: T → Observable<R> be the projection function
Let O_i = project(v_i) be the inner Observable for each v_i

At time t:
  Output = emissions from O_j where j = max{i : v_i emitted before t}

When v_k emits:
  1. Unsubscribe from O_(k-1) (cancel previous inner Observable)
  2. Subscribe to O_k (new inner Observable)
  3. Forward all emissions from O_k until v_(k+1) emits
```

**Invariants**:
- **Single active subscription**: Only one inner Observable is subscribed at any time
- **Automatic cancellation**: Previous inner Observable is unsubscribed when new source value arrives
- **Latest-wins semantics**: Only the most recent inner Observable produces output
- **Memory bounded**: At most one inner subscription active (O(1) memory)
- **Immediate cancellation**: Previous inner unsubscribed immediately, synchronously
- **No queuing**: Source emissions are processed immediately, not buffered

## Marble Diagram

```
Source:    --a-----b--c------|
              |     |  |
              v     v  v
          project(x) = Observable that emits x1, x2, x3 over time
              |     |  |
Inner a:      --a1--a2--a3...  (cancelled when b arrives)
                    ✂️
Inner b:            --b1--b2... (cancelled when c arrives)
                       ✂️
Inner c:               --c1--c2--c3|
              switchMap(project)
Result:    ----a1--a2--b1--c1--c2--c3|

Legend:
  - : time unit (10ms)
  a,b,c : source values
  a1,a2,a3 : emissions from inner Observable for 'a'
  ✂️ : cancellation point
  | : completion
  
Key observation: Only emissions from the active inner Observable reach output
```

**More detailed example showing cancellation**:
```
Source:    --1--------2------|
              |        |
              v        v
          project(n) = HTTP request taking 500ms
              |        |
Inner 1:      ----response1  (cancelled before completing!)
                   ✂️
Inner 2:               ----response2|
              switchMap(project)
Result:    ---------------response2|

Cancelled: response1 never appears in output
Active: Only response2 completes and emits
```

## Behavioral Characteristics

**Subscription**: 
- Subscribes to source Observable immediately upon subscription
- Creates a new inner subscription for EACH source emission
- **Cancels (unsubscribes from) previous inner Observable immediately**
- Only one inner Observable is active at any time
- Cancellation is synchronous and immediate

**Completion semantics**:
- Waits for BOTH source AND current inner Observable to complete
- Source completion does NOT cancel the active inner Observable
- Result completes when:
  1. Source has completed AND
  2. The final inner Observable has completed
- If source completes while an inner Observable is active, waits for it
- Empty source (completes without emitting) causes immediate completion

**Error handling**:
- Any error from source propagates immediately to output
- Any error from the current inner Observable propagates immediately
- Error cancels any active inner subscription
- Previous inner Observables that were cancelled cannot emit errors
- No built-in error recovery mechanism

**Backpressure**:
- Naturally handles fast sources through cancellation
- No buffering or memory accumulation
- Previous incomplete operations are cancelled automatically
- Ideal for user-driven events (search, navigation, form input)
- Memory usage: O(1) - only current inner Observable maintained

**Cancellation behavior**:
```typescript
// Demonstrates automatic cancellation
const source$ = interval(100).pipe(take(5)); // Emits 0,1,2,3,4

const result$ = source$.pipe(
  switchMap(n => {
    console.log(`Starting inner ${n}`);
    return interval(200).pipe(
      take(3),
      map(i => `${n}-${i}`),
      finalize(() => console.log(`Inner ${n} finalized`))
    );
  })
);

// Output shows cancellations:
// Starting inner 0
// Inner 0 finalized (cancelled by inner 1)
// Starting inner 1
// Inner 1 finalized (cancelled by inner 2)
// ...
// Starting inner 4
// Inner 4 completes naturally
```

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source Observable emission type
 *   O extends ObservableInput<any> - Inner Observable type (Observable, Promise, Array, etc.)
 *   R - Result type (inferred from inner Observable emissions)
 * 
 * Input Type: Observable<T>
 * 
 * Output Type: Observable<ObservedValueOf<O>>
 *   - Extracts emission type from inner ObservableInput
 *   - Flattens nested Observable structure
 * 
 * Type Narrowing:
 *   - Infers R from project function return type
 *   - Supports Promises, Arrays, Iterables (auto-converted to Observables)
 *   - Preserves union types through flattening
 *   - Index parameter type-safe (number)
 * 
 * Type Safety:
 *   - Compile-time verification of project function signature
 *   - Type-safe flattening (no Observable<Observable<T>> in output)
 *   - Enforces ObservableInput constraint on project return type
 *   - Cancellation is type-transparent (doesn't affect types)
 */

// Example: Type-safe search with cancellation
import { fromEvent } from 'rxjs';
import { switchMap, debounceTime, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface SearchResult {
  id: number;
  title: string;
  description: string;
}

const searchInput = document.getElementById('search') as HTMLInputElement;
const search$ = fromEvent(searchInput, 'input');

// Type: Observable<SearchResult[]>
const results$ = search$.pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300),
  switchMap(query => 
    ajax.getJSON<SearchResult[]>(`/api/search?q=${query}`)
  )
);

results$.subscribe(results => {
  // TypeScript knows: results is SearchResult[]
  results.forEach(r => {
    console.log(r.title); // Type-safe access
  });
});

// Example: Promise handling with type inference
async function fetchUserData(id: number): Promise<{ name: string; email: string }> {
  const response = await fetch(`/api/users/${id}`);
  return response.json();
}

const userId$ = of(1, 2, 3);

// Type: Observable<{ name: string; email: string }>
const userData$ = userId$.pipe(
  switchMap(id => fetchUserData(id)) // Promise automatically converted
);

userData$.subscribe(user => {
  console.log(user.name); // Type-safe: user has name property
  console.log(user.email); // Type-safe: user has email property
});

// Example: Conditional type handling
const trigger$ = fromEvent(button, 'click');

// Type: Observable<string | number>
const mixed$ = trigger$.pipe(
  switchMap(() => 
    Math.random() > 0.5 
      ? of('string result')
      : of(42)
  )
);

mixed$.subscribe(value => {
  // TypeScript knows: value is string | number
  if (typeof value === 'string') {
    console.log(value.toUpperCase()); // Narrowed to string
  } else {
    console.log(value.toFixed(2)); // Narrowed to number
  }
});
```

## Examples

### Basic Usage - Search with Automatic Cancellation
```typescript
import { fromEvent } from 'rxjs';
import { map, debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const searchBox = document.getElementById('search') as HTMLInputElement;
const search$ = fromEvent(searchBox, 'input');

const searchResults$ = search$.pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300), // Wait for user to stop typing
  distinctUntilChanged(), // Only if query changed
  switchMap(query => 
    ajax.getJSON(`/api/search?q=${query}`)
  )
);

searchResults$.subscribe(results => {
  displayResults(results);
});

// Behavior:
// User types: "r" → Request 1 starts
// User types: "rx" → Request 1 CANCELLED, Request 2 starts
// User types: "rxjs" → Request 2 CANCELLED, Request 3 starts
// Wait 300ms... → Request 3 completes
// Only the latest search result is displayed!

function displayResults(results: any[]) {
  console.log('Search results:', results);
}
```

### Common Pattern - Route Navigation with Cancellation
```typescript
import { Subject } from 'rxjs';
import { switchMap } from 'rxjs/operators';

interface Route {
  path: string;
  params: Record<string, string>;
}

interface PageData {
  title: string;
  content: string;
  metadata: any;
}

// Router emits route changes
const router$ = new Subject<Route>();

// Load page data, cancelling previous load if route changes
const pageData$ = router$.pipe(
  switchMap(route => loadPageData(route))
);

pageData$.subscribe(data => {
  renderPage(data);
});

function loadPageData(route: Route): Observable<PageData> {
  console.log(`Loading page: ${route.path}`);
  
  return ajax.getJSON<PageData>(`/api/pages${route.path}`).pipe(
    finalize(() => console.log(`Load complete/cancelled: ${route.path}`))
  );
}

// User navigation:
router$.next({ path: '/home', params: {} });
// Loading page: /home

setTimeout(() => {
  router$.next({ path: '/about', params: {} });
  // Load complete/cancelled: /home (cancelled!)
  // Loading page: /about
}, 100);

// Only /about page data is rendered, /home was cancelled

function renderPage(data: PageData) {
  console.log('Rendering:', data.title);
}
```

### Edge Cases - Completion and Error Handling
```typescript
import { of, throwError, interval, EMPTY } from 'rxjs';
import { switchMap, take, delay } from 'rxjs/operators';

// Edge case 1: Source completes before inner Observable
const quickSource$ = of(1);
const slowInner$ = quickSource$.pipe(
  switchMap(n => interval(1000).pipe(take(3)))
);

slowInner$.subscribe({
  next: v => console.log('Value:', v),
  complete: () => console.log('Complete')
});
// Output:
// Value: 0 (at 1s)
// Value: 1 (at 2s)
// Value: 2 (at 3s)
// Complete
// Source completed but inner Observable continued

// Edge case 2: Error in inner Observable
const withError$ = of(1, 2, 3).pipe(
  switchMap(n => 
    n === 2
      ? throwError(() => new Error('Error in inner'))
      : of(n * 10).pipe(delay(100))
  )
);

withError$.subscribe({
  next: v => console.log('Value:', v),
  error: err => console.log('Error:', err.message)
});
// Output:
// Value: 10 (from n=1)
// Error: Error in inner
// (n=3 never processes)

// Edge case 3: Empty inner Observable
const empty$ = of(1, 2, 3).pipe(
  switchMap(n => 
    n === 2 ? EMPTY : of(n * 10)
  )
);

empty$.subscribe({
  next: v => console.log('Value:', v),
  complete: () => console.log('Complete')
});
// Output:
// Value: 10 (from n=1)
// (n=2 produces no output - EMPTY)
// Value: 30 (from n=3)
// Complete

// Edge case 4: Synchronous inner Observables
const sync$ = of('a', 'b', 'c').pipe(
  switchMap(letter => of(`${letter}1`, `${letter}2`, `${letter}3`))
);

sync$.subscribe(v => console.log(v));
// Output:
// a1, a2, a3 (cancelled immediately)
// b1, b2, b3 (cancelled immediately)
// c1, c2, c3 (completes)
// Only final inner Observable's values appear!
```

### Advanced Pattern - Cancellable HTTP Requests with Retry
```typescript
import { fromEvent, timer } from 'rxjs';
import { switchMap, retry, catchError, map, tap } from 'rxjs/operators';

interface Task {
  id: string;
  status: 'pending' | 'success' | 'error';
  data?: any;
  error?: string;
}

const refreshButton = document.getElementById('refresh') as HTMLButtonElement;
const refresh$ = fromEvent(refreshButton, 'click');

// Fetch data with retry, but cancel if user clicks refresh again
const taskData$ = refresh$.pipe(
  tap(() => console.log('Starting new fetch...')),
  switchMap(() => 
    ajax.getJSON<Task[]>('/api/tasks').pipe(
      retry({
        count: 3,
        delay: 1000 // Retry with 1s delay
      }),
      map((tasks): { status: 'success'; data: Task[] } => ({
        status: 'success',
        data: tasks
      })),
      catchError((error): Observable<{ status: 'error'; error: string }> =>
        of({
          status: 'error',
          error: error.message
        })
      ),
      finalize(() => console.log('Fetch completed or cancelled'))
    )
  )
);

taskData$.subscribe(result => {
  if (result.status === 'success') {
    console.log('Tasks loaded:', result.data);
    updateUI(result.data);
  } else {
    console.log('Error loading tasks:', result.error);
    showError(result.error);
  }
});

// Behavior:
// Click 1: Start fetch, retry on failure up to 3 times
// Click 2 (during fetch): CANCEL previous (including retries), start new fetch
// Only the latest click's result is processed

function updateUI(tasks: Task[]) {
  console.log('Updating UI with', tasks.length, 'tasks');
}

function showError(error: string) {
  console.error('UI Error:', error);
}
```

### Advanced Pattern - Form Auto-save with Cancellation
```typescript
import { fromEvent, merge } from 'rxjs';
import { debounceTime, switchMap, map, distinctUntilChanged, tap } from 'rxjs/operators';

interface FormData {
  title: string;
  content: string;
  lastSaved?: Date;
}

const titleInput = document.getElementById('title') as HTMLInputElement;
const contentInput = document.getElementById('content') as HTMLTextAreaElement;

const titleChange$ = fromEvent(titleInput, 'input').pipe(
  map(() => titleInput.value)
);

const contentChange$ = fromEvent(contentInput, 'input').pipe(
  map(() => contentInput.value)
);

// Combine both inputs
const formChange$ = merge(
  titleChange$.pipe(map(title => ({ field: 'title', value: title }))),
  contentChange$.pipe(map(content => ({ field: 'content', value: content })))
);

const autoSave$ = formChange$.pipe(
  debounceTime(1000), // Wait for 1s of no typing
  map(() => ({
    title: titleInput.value,
    content: contentInput.value
  })),
  distinctUntilChanged((prev, curr) => 
    prev.title === curr.title && prev.content === curr.content
  ),
  tap(() => console.log('Saving...')),
  switchMap(formData => 
    ajax.post('/api/save', formData).pipe(
      map(response => ({
        ...formData,
        lastSaved: new Date()
      })),
      finalize(() => console.log('Save complete/cancelled'))
    )
  )
);

autoSave$.subscribe({
  next: result => {
    console.log('Saved at:', result.lastSaved);
    showSaveIndicator('Saved');
  },
  error: err => {
    console.error('Save failed:', err);
    showSaveIndicator('Error');
  }
});

// Behavior:
// User types → Wait 1s → Start save
// User types again during save → Cancel previous save, wait 1s, start new save
// Prevents multiple concurrent saves, always saves latest content

function showSaveIndicator(status: string) {
  console.log('Status:', status);
}
```

## Common Pitfalls

### Anti-pattern 1: Using switchMap When You Need All Results
```typescript
// ❌ INCORRECT: Processing all items but only getting last result
import { of } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

const items$ = of(1, 2, 3, 4, 5);

const processed$ = items$.pipe(
  switchMap(item => 
    processItem(item).pipe(delay(100))
  )
);

processed$.subscribe(result => {
  console.log('Processed:', result);
});
// Output: Processed: 5
// Items 1-4 were cancelled! Only the last one completes.

function processItem(item: number): Observable<string> {
  console.log('Processing item', item);
  return of(`Result ${item}`);
}

// ✅ CORRECT: Use mergeMap for parallel processing
import { mergeMap } from 'rxjs/operators';

const allProcessed$ = items$.pipe(
  mergeMap(item => 
    processItem(item).pipe(delay(100))
  )
);

allProcessed$.subscribe(result => {
  console.log('Processed:', result);
});
// Output:
// Processed: Result 1
// Processed: Result 2
// Processed: Result 3
// Processed: Result 4
// Processed: Result 5

// WHY: switchMap cancels previous inner Observables
// WHEN TO USE switchMap: User-driven actions where only latest matters (search, navigation)
// WHEN TO USE mergeMap: Processing collections where all results needed
```

### Anti-pattern 2: Not Handling Cancellation Side Effects
```typescript
// ❌ INCORRECT: Cancelled operations leave side effects
import { interval } from 'rxjs';
import { switchMap, take } from 'rxjs/operators';

let activeConnections = 0;

const clicks$ = fromEvent(button, 'click');

const dataStream$ = clicks$.pipe(
  switchMap(() => {
    activeConnections++;
    console.log('Opening connection, active:', activeConnections);
    
    return interval(1000).pipe(
      take(10),
      map(n => `Data ${n}`)
    );
    // Problem: No cleanup when cancelled!
  })
);

dataStream$.subscribe(data => console.log(data));

// After 5 clicks: activeConnections = 5
// But only 1 connection is actually active!
// Resource tracking is incorrect

// ✅ CORRECT: Use finalize for cleanup
import { finalize } from 'rxjs/operators';

const cleanDataStream$ = clicks$.pipe(
  switchMap(() => {
    activeConnections++;
    console.log('Opening connection, active:', activeConnections);
    
    return interval(1000).pipe(
      take(10),
      map(n => `Data ${n}`),
      finalize(() => {
        activeConnections--;
        console.log('Closing connection, active:', activeConnections);
      })
    );
  })
);

cleanDataStream$.subscribe(data => console.log(data));

// Now activeConnections accurately reflects reality
// Cancelled connections properly decrement the counter

// WHY: Cancellation doesn't automatically clean up resources
// SOLUTION: Use finalize() for cleanup logic that must run on cancellation
// PRINCIPLE: Always clean up resources (connections, timers, subscriptions)
```

### Anti-pattern 3: Assuming Synchronous Cancellation Prevents All Emissions
```typescript
// ❌ INCORRECT: Thinking cancellation prevents synchronous emissions
import { of } from 'rxjs';
import { switchMap } from 'rxjs/operators';

const source$ = of(1, 2, 3);

const result$ = source$.pipe(
  switchMap(n => of(n * 10, n * 20, n * 30))
);

result$.subscribe(v => console.log(v));

// What developer expects:
// Just: 30, 60, 90 (only from n=3)

// What actually happens:
// 10, 20, 30 (from n=1, cancelled but synchronous emissions complete)
// 20, 40, 60 (from n=2, cancelled but synchronous emissions complete)
// 30, 60, 90 (from n=3, completes)

// All synchronous emissions complete before cancellation!

// ✅ CORRECT: Understanding synchronous vs asynchronous cancellation
const async$ = of(1, 2, 3).pipe(
  switchMap(n => 
    of(n * 10, n * 20, n * 30).pipe(
      delay(0) // Make it asynchronous
    )
  )
);

async$.subscribe(v => console.log(v));
// Output: 30, 60, 90 (only from n=3)
// Previous inner Observables cancelled before emitting

// WHY: Synchronous Observables complete before cancellation can occur
// UNDERSTANDING: switchMap cancellation is synchronous but happens between ticks
// REALITY: Most real-world Observables (HTTP, timers) are async anyway
```

### Anti-pattern 4: Using switchMap for Sequential Operations
```typescript
// ❌ INCORRECT: Using switchMap when order matters
import { from } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

const tasks = [
  { id: 1, name: 'Initialize' },
  { id: 2, name: 'Process' },
  { id: 3, name: 'Finalize' }
];

const taskExecution$ = from(tasks).pipe(
  switchMap(task => 
    executeTask(task).pipe(delay(100))
  )
);

taskExecution$.subscribe(result => {
  console.log('Completed:', result.name);
});

// Output: Completed: Finalize
// Initialize and Process were cancelled!

function executeTask(task: { id: number; name: string }): Observable<any> {
  console.log('Starting:', task.name);
  return of({ ...task, completed: true });
}

// This breaks sequential workflows!

// ✅ CORRECT: Use concatMap for sequential operations
import { concatMap } from 'rxjs/operators';

const sequential$ = from(tasks).pipe(
  concatMap(task => 
    executeTask(task).pipe(delay(100))
  )
);

sequential$.subscribe(result => {
  console.log('Completed:', result.name);
});

// Output:
// Completed: Initialize
// Completed: Process
// Completed: Finalize
// All tasks complete in order!

// WHY: switchMap cancels previous operations
// WHEN TO USE switchMap: User actions where only latest matters
// WHEN TO USE concatMap: Sequential workflows, ordered processing, chains of dependent operations
```

### Performance: Excessive Cancellations
**When this matters**: 
- Very high-frequency sources (mouse movements, scroll events)
- Expensive setup/teardown in inner Observables
- Resource-intensive operations being repeatedly cancelled

**What to do**:
```typescript
// Reduce cancellation frequency with debounce/throttle
source$.pipe(
  debounceTime(100),  // Wait for quiet period
  switchMap(createExpensiveObservable)
);

// Or use audit to sample less frequently
source$.pipe(
  auditTime(100),  // At most one per 100ms
  switchMap(createExpensiveObservable)
);

// For mouse/scroll events, consider throttle
fromEvent(window, 'scroll').pipe(
  throttleTime(100, asyncScheduler, { leading: true, trailing: true }),
  switchMap(loadVisibleContent)
);
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`mergeMap`**: Concurrent version - does NOT cancel previous inner Observables. Use when you need all results (parallel processing of collections).
- **`concatMap`**: Sequential version - queues source emissions, processes one at a time. Use when order matters (sequential workflows, dependent operations).
- **`exhaustMap`**: Ignoring version - ignores new source emissions while inner Observable is active. Use to prevent duplicate operations (form submissions, save buttons).
- **`switchAll`**: Flattens Observable<Observable<T>> without transformation. Use when you already have nested Observables.

**Complementary Operators**:
- **`debounceTime`**: Delay source emissions → `source$.pipe(debounceTime(300), switchMap(...))` for search
- **`distinctUntilChanged`**: Skip duplicate emissions → prevents unnecessary switchMap calls
- **`catchError`**: Handle inner Observable errors → `switchMap(x => doWork(x).pipe(catchError(...)))`
- **`retry`**: Retry failed inner Observables → `switchMap(x => doWork(x).pipe(retry(3)))`
- **`finalize`**: Cleanup on cancellation → `switchMap(x => work(x).pipe(finalize(cleanup)))`
- **`tap`**: Debug cancellations → `switchMap(x => work(x).pipe(tap(...), finalize(...)))`

**Alternatives by Use Case**:

| Use Case | Instead of switchMap | Use This | Why |
|----------|---------------------|----------|-----|
| Process all items | `from(items).pipe(switchMap(...))` | `mergeMap(...)` | Need all results, not just last |
| Sequential processing | `switchMap(x => process(x))` | `concatMap(x => process(x))` | Order matters, no cancellation |
| Prevent duplicates | `clicks$.pipe(switchMap(save))` | `exhaustMap(save)` | Ignore clicks during save |
| Latest search only | `search$.pipe(switchMap(...))` | Use switchMap ✓ | Correct choice |
| User navigation | `route$.pipe(switchMap(load))` | Use switchMap ✓ | Correct choice |
| Real-time updates | `switchMap(() => interval(...))` | Use switchMap ✓ | Cancel old, use new |

**Comparison Table**:

| Operator | Concurrency | Cancellation | Order Preserved | Best For |
|----------|-------------|--------------|-----------------|----------|
| `switchMap` | 1 (latest only) | Yes (previous) | No | User-driven actions, latest wins |
| `mergeMap` | Unlimited | No | No | Parallel ops, need all results |
| `concatMap` | 1 (sequential) | No | Yes | Sequential workflows, order matters |
| `exhaustMap` | 1 (ignore new) | No | N/A | Prevent duplicates, ignore spam |

**Common Patterns**:

```typescript
// Pattern 1: Search (debounce + switchMap)
searchInput$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query => search(query))
);

// Pattern 2: Navigation (switchMap)
routeChange$.pipe(
  switchMap(route => loadPage(route))
);

// Pattern 3: Real-time data (switchMap)
selectedItem$.pipe(
  switchMap(item => 
    interval(1000).pipe(
      switchMap(() => fetchLatestData(item))
    )
  )
);

// Pattern 4: Cancellable long operations
button$.pipe(
  switchMap(() => 
    longRunningTask().pipe(
      takeUntil(cancelButton$)
    )
  )
);
```

**Migration Notes**:
```typescript
// RxJS 5 (deprecated resultSelector)
source$.pipe(
  switchMap(
    outer => inner$,
    (outer, inner) => ({ outer, inner })
  )
)

// RxJS 6+ (current)
source$.pipe(
  switchMap(outer => 
    inner$.pipe(
      map(inner => ({ outer, inner }))
    )
  )
)
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/switchMap](https://rxjs.dev/api/operators/switchMap)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/switchMap.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/switchMap.ts)
- **RxJS Team Guide**: "Choosing the right flattening operator"
- **Best Practices**: "When to use switchMap vs mergeMap" - RxJS documentation

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: 
- **Pattern**: Cancelling Composition Strategy (Latest-Only Flattening)
- **Cognitive Load**: High (4/5) - Requires understanding cancellation semantics and appropriate use cases
- **Usage Frequency**: Very High (5/5) - Essential for user-driven async operations
- **Composability**: High (5/5) - Works excellently with debounce, distinctUntilChanged, retry

**Problem Domain**: 
User-driven asynchronous operations where only the most recent action's result matters, and previous operations should be cancelled to prevent wasted resources and race conditions. Classic examples: search-as-you-type, route navigation, real-time data updates.

**When to Teach**: 
After students understand mergeMap (concurrent flattening). switchMap is easier to understand when contrasted with mergeMap's "keep all" semantics. Teach alongside debounceTime for the canonical search pattern. Essential before teaching production patterns.

**Common Misconceptions**:
1. **"switchMap is always better than mergeMap"** - No, use mergeMap when you need all results
2. **"Cancellation is wasteful"** - No, it prevents race conditions and resource waste
3. **"switchMap guarantees no race conditions"** - Mostly true, but beware of synchronous inner Observables
4. **"Cancelled operations throw errors"** - No, they silently unsubscribe
5. **"switchMap preserves order"** - No, but typically doesn't matter since only latest completes

**Teaching Progression**:
1. Start with the problem: search that fires multiple HTTP requests
2. Show the race condition: later request might complete before earlier one
3. Introduce switchMap as automatic cancellation solution
4. Demonstrate with marble diagrams showing cancellation
5. Practice with search, navigation, and auto-save patterns
6. Teach finalize() for cleanup on cancellation
7. Compare with mergeMap (all) and concatMap (sequential)
8. Show when NOT to use switchMap (processing collections)

**Canonical Use Cases**:
- **Search-as-you-type**: debounceTime + switchMap
- **Route navigation**: Cancel previous page load on navigation
- **Real-time data**: Cancel old polling, start new
- **Form auto-save**: Cancel old save, use latest form state
- **Typeahead suggestions**: Only show latest results
- **Preview generation**: Cancel old preview, generate new

**Anti-use Cases** (when NOT to use):
- Processing arrays/collections (use mergeMap)
- Sequential workflows (use concatMap)
- Preventing duplicate submissions (use exhaustMap)
- Operations that must complete (use mergeMap or concatMap)
