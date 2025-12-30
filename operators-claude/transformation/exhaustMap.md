# exhaustMap

## Identity
- **Name**: exhaustMap
- **Category**: Transformation Operators
- **Type**: Higher-order transformation operator (flattening strategy - ignoring)
- **Import**: 
  ```typescript
  import { exhaustMap } from 'rxjs/operators';
  ```
- **Signature**: 
  ```typescript
  function exhaustMap<T, R, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O,
    resultSelector?: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R
  ): OperatorFunction<T, ObservedValueOf<O> | R>;
  
  // Most common form (without deprecated resultSelector)
  function exhaustMap<T, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O
  ): OperatorFunction<T, ObservedValueOf<O>>;
  ```

## Functional Specification

**Input**: Observable<T> emitting values v₁, v₂, v₃, ...

**Output**: Observable<R> emitting values from inner Observables, but only when no inner Observable is currently active

**Transformation**: For each source emission, `exhaustMap` checks if an inner Observable is currently active. If **yes**, the source emission is **ignored/dropped**. If **no**, it applies the projection function to create and subscribe to a new inner Observable. Only processes source emissions when idle.

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let project: T → Observable<R> be the projection function
Let O_i = project(v_i) be the inner Observable for each v_i
Let active(t) = true if an inner Observable is running at time t

For each source emission v_i at time t:
  if active(t) = false:
    Subscribe to O_i
    Emit all values from O_i
  else:
    Drop v_i (ignore it completely)

Invariant: At most one inner Observable is active at any time
Strategy: First-wins (opposite of switchMap's latest-wins)
```

**Invariants**:
- **Single active subscription**: Only one inner Observable subscribed at any time
- **Ignoring behavior**: Source emissions are dropped while inner Observable is active
- **First-wins semantics**: First operation completes; subsequent attempts during execution are ignored
- **Memory bounded**: O(1) memory - no queueing, no buffering
- **No cancellation**: Active inner Observable always completes (unless error)
- **No queueing**: Ignored emissions are dropped forever, not queued

## Marble Diagram

```
Source:    --a--b--c--d--e--f------|
              |  🚫 🚫 |  🚫 |
              v        v     v
          project(x) = Observable taking 300ms
              |        |     |
Inner a:      ---a1--a2|
                  (b,c ignored - inner active)
Inner d:                ---d1--d2|
                           (e ignored)
Inner f:                        ---f1--f2|
              exhaustMap(project)
Result:    -----a1--a2---d1--d2---f1--f2|

Legend:
  - : time unit (10ms)
  a,b,c : source values
  🚫 : ignored/dropped emission
  a1,a2 : emissions from inner Observable
  | : completion
  
Key observation: b, c, and e are completely ignored because
inner Observables were active when they emitted
```

**Demonstrating ignoring behavior**:
```
Source:    -1-2-3-4-5-6-7-8-9-10|  (rapid emissions every 100ms)
             | 🚫🚫🚫 | 🚫🚫🚫 |
             v         v         v
          project(n) = HTTP POST taking 500ms
             |         |         |
Inner 1:     -----✓|
                (2,3,4 ignored)
Inner 5:              -----✓|
                         (6,7,8 ignored)
Inner 9:                       -----✓|
              exhaustMap(project)
Result:    -------✓-----✓-----✓|

Only 3 operations executed out of 10 source emissions!
Perfect for preventing duplicate form submissions.
```

## Behavioral Characteristics

**Subscription**: 
- Subscribes to source Observable immediately upon subscription
- Creates inner subscription ONLY when no inner Observable is active
- **Ignores source emissions when inner Observable is active**
- Dropped emissions are never processed (not queued, not retried)
- Never cancels inner Observables (unlike switchMap)
- Never runs inner Observables concurrently (like concatMap, unlike mergeMap)

**Completion semantics**:
- Waits for BOTH source AND current inner Observable to complete
- Source completion does NOT complete result if inner Observable is active
- Result completes when:
  1. Source has completed AND
  2. No inner Observable is active (or current one completes)
- If source completes while inner is active, waits for inner to complete
- Empty source (completes without emitting) causes immediate completion

**Error handling**:
- Any error from source propagates immediately to output
- Any error from current inner Observable propagates immediately
- Error stops processing (ignores future source emissions)
- No built-in error recovery mechanism
- Errors cancel current inner subscription and unsubscribe from source

**Backpressure**:
- Natural backpressure through ignoring mechanism
- Fast source + slow inner Observable = most emissions ignored
- **No memory accumulation** - dropped emissions are gone forever
- Ideal for user actions that shouldn't queue (button clicks, form submits)
- Prevents resource exhaustion from rapid events

**Ignoring vs Queueing**:
```typescript
// exhaustMap: Ignores (drops) source emissions
source$.pipe(
  exhaustMap(processItem)
);
// Fast clicks → Only first click processed, others ignored

// concatMap: Queues source emissions
source$.pipe(
  concatMap(processItem)
);
// Fast clicks → All clicks queued and processed sequentially
// Can lead to memory buildup and delayed responses
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
 *   - Only emits from accepted (not ignored) source values
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
 *   - Ignoring behavior is type-transparent (doesn't affect types)
 */

// Example: Type-safe form submission prevention
import { fromEvent } from 'rxjs';
import { exhaustMap, map } from 'rxjs/operators';

interface FormData {
  username: string;
  email: string;
}

interface SubmitResult {
  success: boolean;
  userId?: number;
  error?: string;
}

const submitButton = document.getElementById('submit') as HTMLButtonElement;
const form = document.getElementById('userForm') as HTMLFormElement;

const submit$ = fromEvent(submitButton, 'click');

// Type: Observable<SubmitResult>
const submission$ = submit$.pipe(
  map(() => ({
    username: (form.elements.namedItem('username') as HTMLInputElement).value,
    email: (form.elements.namedItem('email') as HTMLInputElement).value
  })),
  exhaustMap((formData: FormData) => 
    submitForm(formData) // Returns Observable<SubmitResult>
  )
);

submission$.subscribe(result => {
  // TypeScript knows: result is SubmitResult
  if (result.success) {
    console.log('User created:', result.userId);
  } else {
    console.log('Error:', result.error);
  }
});

function submitForm(data: FormData): Observable<SubmitResult> {
  return ajax.post<SubmitResult>('/api/users', data).pipe(
    map(response => response.response)
  );
}

// Example: Promise handling with type preservation
async function saveSettings(settings: any): Promise<{ saved: boolean; timestamp: Date }> {
  // Simulate API call
  await new Promise(resolve => setTimeout(resolve, 1000));
  return { saved: true, timestamp: new Date() };
}

const saveButton$ = fromEvent(document.getElementById('save')!, 'click');

// Type: Observable<{ saved: boolean; timestamp: Date }>
const saves$ = saveButton$.pipe(
  exhaustMap(() => saveSettings({ theme: 'dark' }))
);

saves$.subscribe(result => {
  console.log('Saved at:', result.timestamp); // Type-safe access
});

// Example: Union type handling
const action$ = fromEvent(button, 'click');

// Type: Observable<string | number>
const result$ = action$.pipe(
  exhaustMap(() => 
    Math.random() > 0.5
      ? of('success')
      : of(42)
  )
);

result$.subscribe(value => {
  if (typeof value === 'string') {
    console.log(value.toUpperCase());
  } else {
    console.log(value.toFixed(2));
  }
});
```

## Examples

### Basic Usage - Prevent Double-Click Form Submission
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const submitButton = document.getElementById('submit') as HTMLButtonElement;
const submit$ = fromEvent(submitButton, 'click');

const submission$ = submit$.pipe(
  exhaustMap(() => {
    console.log('Submitting form...');
    return ajax.post('/api/submit', { data: 'form data' }).pipe(
      map(response => response.response)
    );
  })
);

submission$.subscribe({
  next: result => {
    console.log('Form submitted successfully:', result);
    showSuccessMessage();
  },
  error: err => {
    console.error('Submission failed:', err);
    showErrorMessage();
  }
});

// User behavior:
// Click 1: Starts submission
// Click 2 (during submission): IGNORED
// Click 3 (during submission): IGNORED
// Submission completes
// Click 4: Starts new submission
// Click 5 (during submission): IGNORED

// Prevents duplicate submissions automatically!

function showSuccessMessage() {
  console.log('✓ Success!');
}

function showErrorMessage() {
  console.log('✗ Error!');
}
```

### Common Pattern - Login Request with Spam Prevention
```typescript
import { fromEvent } from 'rxjs';
import { exhaustMap, map, tap, catchError, finalize } from 'rxjs/operators';

interface LoginCredentials {
  username: string;
  password: string;
}

interface LoginResult {
  success: boolean;
  token?: string;
  error?: string;
}

const loginForm = document.getElementById('loginForm') as HTMLFormElement;
const loginButton = document.getElementById('login') as HTMLButtonElement;
const loginClick$ = fromEvent(loginButton, 'click');

let isLoading = false;

const login$ = loginClick$.pipe(
  tap(() => {
    isLoading = true;
    updateButtonState('Logging in...', true);
  }),
  map(() => ({
    username: (loginForm.elements.namedItem('username') as HTMLInputElement).value,
    password: (loginForm.elements.namedItem('password') as HTMLInputElement).value
  })),
  exhaustMap((credentials: LoginCredentials) => 
    performLogin(credentials).pipe(
      catchError((error): Observable<LoginResult> => {
        console.error('Login error:', error);
        return of({
          success: false,
          error: error.message || 'Login failed'
        });
      }),
      finalize(() => {
        isLoading = false;
        updateButtonState('Login', false);
      })
    )
  )
);

login$.subscribe(result => {
  if (result.success) {
    console.log('Login successful!');
    localStorage.setItem('token', result.token!);
    navigateToDashboard();
  } else {
    console.log('Login failed:', result.error);
    showErrorMessage(result.error!);
  }
});

function performLogin(credentials: LoginCredentials): Observable<LoginResult> {
  console.log('Attempting login for:', credentials.username);
  return ajax.post<LoginResult>('/api/login', credentials).pipe(
    map(response => response.response),
    delay(1000) // Simulate network delay
  );
}

function updateButtonState(text: string, disabled: boolean) {
  loginButton.textContent = text;
  loginButton.disabled = disabled;
}

function navigateToDashboard() {
  console.log('Navigating to dashboard...');
}

// Behavior:
// User clicks Login → Request starts, button disabled
// User frantically clicks 10 more times → All ignored!
// Request completes → Button enabled
// User clicks Login again → New request starts
```

### Edge Cases - Completion and Error Scenarios
```typescript
import { of, throwError, interval, EMPTY } from 'rxjs';
import { exhaustMap, take, delay } from 'rxjs/operators';

// Edge case 1: Source completes while inner is active
const sourceCompletesEarly$ = interval(100).pipe(
  take(3), // Emits 0, 1, 2, then completes
  exhaustMap(n => {
    console.log(`Processing: ${n}`);
    return of(n * 10).pipe(delay(500));
  })
);

sourceCompletesEarly$.subscribe({
  next: v => console.log('Result:', v),
  complete: () => console.log('Complete')
});

// Output:
// Processing: 0
// (1 and 2 ignored - inner still active)
// Result: 0
// Complete (waits for inner to finish)

// Edge case 2: Error in inner Observable
const withError$ = fromEvent(button, 'click').pipe(
  exhaustMap(() => {
    console.log('Starting operation');
    return throwError(() => new Error('Operation failed')).pipe(
      delay(100)
    );
  })
);

withError$.subscribe({
  next: v => console.log('Value:', v),
  error: err => console.log('Error:', err.message)
});

// First click: Starts operation, then errors
// Further clicks: IGNORED (stream already errored out)

// Edge case 3: Empty inner Observable
const empty$ = fromEvent(button, 'click').pipe(
  exhaustMap((event, index) => {
    console.log(`Click ${index}`);
    return index === 0 ? EMPTY : of('Processed');
  })
);

empty$.subscribe({
  next: v => console.log('Result:', v),
  complete: () => console.log('Complete')
});

// Click 1: EMPTY completes immediately, no output
// Click 2: Immediately processed (no inner active)
// Result: Processed

// Edge case 4: Very fast inner Observable (synchronous)
const sync$ = fromEvent(button, 'click').pipe(
  exhaustMap(() => {
    console.log('Sync operation');
    return of(1, 2, 3); // Synchronous
  })
);

sync$.subscribe(v => console.log('Value:', v));

// Each click is processed because synchronous Observable
// completes before next click can arrive
// Output per click: Value: 1, Value: 2, Value: 3

// Edge case 5: Source never emits
const never$ = NEVER.pipe(
  exhaustMap(() => of('never happens'))
);

never$.subscribe({
  next: v => console.log('Value:', v),
  complete: () => console.log('Complete')
});
// Output: (nothing - source never emits)
```

### Advanced Pattern - Refresh Button with Cooldown
```typescript
import { fromEvent, timer } from 'rxjs';
import { exhaustMap, tap, finalize, catchError } from 'rxjs/operators';

interface DataRefreshResult {
  timestamp: Date;
  data: any[];
  cached: boolean;
}

const refreshButton = document.getElementById('refresh') as HTMLButtonElement;
const refresh$ = fromEvent(refreshButton, 'click');

let lastRefreshTime: Date | null = null;

const refreshData$ = refresh$.pipe(
  tap(() => {
    console.log('Refresh initiated');
    showLoadingSpinner();
    disableRefreshButton();
  }),
  exhaustMap(() => 
    fetchFreshData().pipe(
      catchError(error => {
        console.error('Refresh failed:', error);
        return of({
          timestamp: new Date(),
          data: getCachedData(),
          cached: true
        });
      }),
      finalize(() => {
        hideLoadingSpinner();
        enableRefreshButton();
      })
    )
  )
);

refreshData$.subscribe(result => {
  lastRefreshTime = result.timestamp;
  
  if (result.cached) {
    console.log('Using cached data (refresh failed)');
    showWarning('Could not fetch fresh data, showing cached version');
  } else {
    console.log('Fresh data loaded');
  }
  
  updateUI(result.data);
  updateLastRefreshTime(result.timestamp);
});

function fetchFreshData(): Observable<DataRefreshResult> {
  console.log('Fetching fresh data from API...');
  return ajax.getJSON<any[]>('/api/data').pipe(
    delay(2000), // Simulate slow API
    map(data => ({
      timestamp: new Date(),
      data,
      cached: false
    }))
  );
}

function getCachedData(): any[] {
  return [{ id: 1, cached: true }];
}

function showLoadingSpinner() {
  console.log('🔄 Loading...');
}

function hideLoadingSpinner() {
  console.log('✓ Loading complete');
}

function disableRefreshButton() {
  refreshButton.disabled = true;
  refreshButton.textContent = 'Refreshing...';
}

function enableRefreshButton() {
  refreshButton.disabled = false;
  refreshButton.textContent = 'Refresh';
}

function updateUI(data: any[]) {
  console.log('UI updated with', data.length, 'items');
}

function updateLastRefreshTime(time: Date) {
  console.log('Last refresh:', time.toLocaleTimeString());
}

function showWarning(message: string) {
  console.warn('⚠️', message);
}

// Behavior:
// Click 1: Fetch starts (2s operation)
// Click 2 (at 0.5s): IGNORED
// Click 3 (at 1.0s): IGNORED
// Click 4 (at 1.5s): IGNORED
// Fetch completes (at 2.0s)
// Click 5: New fetch starts
// Perfect for preventing API spam!
```

### Advanced Pattern - Polling with Manual Trigger
```typescript
import { merge, fromEvent, interval } from 'rxjs';
import { exhaustMap, tap, switchMap, startWith } from 'rxjs/operators';

interface PollingConfig {
  autoRefreshEnabled: boolean;
  intervalMs: number;
}

const manualRefresh$ = fromEvent(
  document.getElementById('manualRefresh')!,
  'click'
).pipe(tap(() => console.log('Manual refresh triggered')));

const config: PollingConfig = {
  autoRefreshEnabled: true,
  intervalMs: 5000
};

// Auto-refresh every 5 seconds
const autoRefresh$ = interval(config.intervalMs).pipe(
  tap(() => console.log('Auto refresh triggered'))
);

// Combine manual and auto refresh
const refresh$ = merge(
  manualRefresh$,
  config.autoRefreshEnabled ? autoRefresh$ : EMPTY
).pipe(
  startWith(null), // Initial load
  exhaustMap(() => {
    console.log('Fetching data...');
    return ajax.getJSON('/api/status').pipe(
      tap(data => console.log('Data received:', data)),
      catchError(error => {
        console.error('Fetch failed:', error);
        return EMPTY;
      })
    );
  })
);

refresh$.subscribe(data => {
  updateStatusDisplay(data);
});

function updateStatusDisplay(data: any) {
  console.log('Status updated:', data);
}

// Behavior:
// t=0s:    Initial load starts
// t=2s:    Manual click → IGNORED (initial load still active)
// t=3s:    Initial load completes
// t=5s:    Auto refresh starts
// t=6s:    Manual click → IGNORED (auto refresh active)
// t=7s:    Auto refresh completes
// t=10s:   Auto refresh starts
// t=11s:   Manual click → IGNORED
// Pattern: Never overlapping requests, regardless of trigger source
```

## Common Pitfalls

### Anti-pattern 1: Using exhaustMap When You Need All Results
```typescript
// ❌ INCORRECT: Processing collection but losing items
import { from, interval } from 'rxjs';
import { exhaustMap, take } from 'rxjs/operators';

const items = [1, 2, 3, 4, 5];

// Process items from array
const processed$ = from(items).pipe(
  exhaustMap(item => {
    console.log('Processing:', item);
    return of(item * 10).pipe(delay(100));
  })
);

processed$.subscribe(result => {
  console.log('Result:', result);
});

// Output:
// Processing: 1
// Result: 10
// (Items 2,3,4,5 are IGNORED because they arrive while processing 1!)

// Only first item processed! This is almost never what you want.

// ✅ CORRECT: Use mergeMap or concatMap for collections
import { mergeMap, concatMap } from 'rxjs/operators';

// All items, concurrent processing
const allProcessed$ = from(items).pipe(
  mergeMap(item => {
    console.log('Processing:', item);
    return of(item * 10).pipe(delay(100));
  })
);
// Output: All 5 items processed

// All items, sequential processing
const sequential$ = from(items).pipe(
  concatMap(item => {
    console.log('Processing:', item);
    return of(item * 10).pipe(delay(100));
  })
);
// Output: All 5 items processed in order

// WHY: exhaustMap is for USER EVENTS, not data processing
// WHEN TO USE exhaustMap: Button clicks, form submissions, manual triggers
// WHEN NOT TO USE: Array/collection processing, data streams
```

### Anti-pattern 2: Not Indicating Ignored Actions to Users
```typescript
// ❌ INCORRECT: Silently ignoring user actions
import { fromEvent } from 'rxjs';
import { exhaustMap } from 'rxjs/operators';

const saveButton = document.getElementById('save') as HTMLButtonElement;
const save$ = fromEvent(saveButton, 'click');

const saves$ = save$.pipe(
  exhaustMap(() => performSave())
);

saves$.subscribe(result => {
  console.log('Saved:', result);
});

function performSave(): Observable<any> {
  return ajax.post('/api/save', {}).pipe(delay(2000));
}

// Problem: User clicks frantically
// No feedback that clicks are being ignored
// User thinks app is broken!

// ✅ CORRECT: Provide UI feedback for ignored actions
const betterSaves$ = save$.pipe(
  tap(() => {
    // Check if already saving
    if (saveButton.disabled) {
      showToast('Save already in progress, please wait...');
      return;
    }
    
    // Disable button and show feedback
    saveButton.disabled = true;
    saveButton.textContent = 'Saving...';
  }),
  exhaustMap(() => 
    performSave().pipe(
      finalize(() => {
        saveButton.disabled = false;
        saveButton.textContent = 'Save';
      })
    )
  )
);

betterSaves$.subscribe(result => {
  console.log('Saved:', result);
  showToast('Saved successfully!');
});

function showToast(message: string) {
  console.log('Toast:', message);
  // In real app: show UI toast notification
}

// WHY: Users need feedback when their actions are ignored
// SOLUTION: Disable buttons, show loading states, provide toast notifications
// UX PRINCIPLE: Make system state visible to users
```

### Anti-pattern 3: Using exhaustMap for Search/Type-ahead
```typescript
// ❌ INCORRECT: User typing is ignored, poor search experience
import { fromEvent } from 'rxjs';
import { map, exhaustMap, debounceTime } from 'rxjs/operators';

const searchInput = document.getElementById('search') as HTMLInputElement;
const search$ = fromEvent(searchInput, 'input');

const results$ = search$.pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300),
  exhaustMap(query => performSearch(query))
);

results$.subscribe(results => {
  displayResults(results);
});

function performSearch(query: string): Observable<any[]> {
  console.log('Searching for:', query);
  return ajax.getJSON(`/api/search?q=${query}`).pipe(delay(500));
}

function displayResults(results: any[]) {
  console.log('Results:', results);
}

// Problem:
// User types: "re" → Search starts (500ms)
// User types: "rea" → IGNORED (search in progress)
// User types: "reac" → IGNORED
// User types: "react" → IGNORED
// Original search completes with results for "re"
// User sees outdated results for "re" not "react"!

// ✅ CORRECT: Use switchMap for search (cancel outdated searches)
import { switchMap } from 'rxjs/operators';

const betterResults$ = search$.pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300),
  switchMap(query => performSearch(query)) // Cancels previous search
);

betterResults$.subscribe(results => {
  displayResults(results);
});

// Behavior:
// User types: "re" → Search starts
// User types: "rea" → Previous search CANCELLED, new search starts
// User types: "react" → Previous search CANCELLED, new search starts
// Search for "react" completes
// User sees current results!

// WHY: Search should show latest query results, not first query
// WHEN TO USE exhaustMap: Prevent duplicate submissions (save, submit, login)
// WHEN TO USE switchMap: Latest-wins scenarios (search, autocomplete, navigation)
```

### Anti-pattern 4: Confusing exhaustMap with throttle
```typescript
// ❌ INCORRECT: Thinking exhaustMap is the same as throttle
import { fromEvent, interval } from 'rxjs';
import { exhaustMap, throttleTime } from 'rxjs/operators';

const clicks$ = fromEvent(button, 'click');

// These are NOT the same!

// exhaustMap: Ignores while inner Observable is active
const withExhaustMap$ = clicks$.pipe(
  exhaustMap(() => timer(1000)) // 1 second operation
);
// Click pattern: 1___2_3_4___5___6
// Result:        ✓   ✗ ✗ ✗   ✓   ✓
// Only clicks when idle are processed

// throttleTime: Emits first, then ignores for duration
const withThrottle$ = clicks$.pipe(
  throttleTime(1000) // 1 second cooldown
);
// Click pattern: 1___2_3_4___5___6
// Result:        ✓   ✗ ✗ ✗   ✓   ✗
// First in each period emits, regardless of what's happening

// ✅ CORRECT: Understanding the difference

// Use exhaustMap when:
// - Operation takes variable time
// - Must prevent overlapping operations
// - Need to wait for async completion
const saveOperation$ = clicks$.pipe(
  exhaustMap(() => saveToServer()) // Wait for save to complete
);

// Use throttleTime when:
// - Just need rate limiting
// - Don't care about async operations
// - Want predictable time-based throttling
const scrollTracking$ = fromEvent(window, 'scroll').pipe(
  throttleTime(100), // At most once per 100ms
  map(() => window.scrollY)
);

// WHY: Different use cases, different behaviors
// KEY: exhaustMap depends on inner Observable duration
//      throttleTime depends only on time, not operations
```

### Performance: Excessive Ignoring
**When this matters**: 
- User performs valid actions that get ignored
- Important events are dropped
- Poor user experience from ignored inputs

**What to do**:
```typescript
// Consider if exhaustMap is right operator
// Ask: Should this action be ignored or queued?

// Ignored (exhaustMap): Form submission, login, save
fromEvent(saveButton, 'click').pipe(
  exhaustMap(() => save())
);

// Queued (concatMap): Sequential operations, all must complete
fromEvent(processButton, 'click').pipe(
  concatMap(() => processItem())
);

// Latest-only (switchMap): Search, navigation
fromEvent(searchInput, 'input').pipe(
  switchMap(query => search(query))
);

// Provide user feedback for ignored actions
fromEvent(button, 'click').pipe(
  tap(() => {
    if (isProcessing) {
      showToast('Operation in progress...');
    }
  }),
  exhaustMap(() => operation())
);
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`mergeMap`**: Concurrent version - all inner Observables run simultaneously. Use for independent parallel operations where all results matter.
- **`switchMap`**: Cancelling version - cancels previous inner Observable. Use for user-driven actions where only latest result matters (search, navigation).
- **`concatMap`**: Sequential version - queues and processes one at a time. Use when order matters or operations are dependent.
- **`exhaustAll`**: Flattens Observable<Observable<T>> using exhaust strategy. Use when you already have nested Observables.

**Complementary Operators**:
- **`tap`**: Debug and provide feedback → `exhaustMap(x => doWork(x).pipe(tap(...)))`
- **`finalize`**: Cleanup after operation → `exhaustMap(x => work(x).pipe(finalize(...)))`
- **`catchError`**: Handle errors gracefully → `exhaustMap(x => work(x).pipe(catchError(...)))`
- **`retry`**: Retry failed operations → `exhaustMap(x => work(x).pipe(retry(3)))`
- **`debounceTime`**: Combine with exhaust for rate limiting → `debounceTime(300), exhaustMap(...)`

**Alternatives by Use Case**:

| Use Case | Instead of exhaustMap | Use This | Why |
|----------|----------------------|----------|-----|
| Process all items | `from(items).pipe(exhaustMap(...))` | `mergeMap(...)` or `concatMap(...)` | Need all results |
| Latest search results | `search$.pipe(exhaustMap(...))` | `switchMap(...)` | Cancel outdated searches |
| Sequential workflow | Ordered operations | `concatMap(...)` | Guarantee order, process all |
| Simple rate limiting | Time-based throttling | `throttleTime(...)` | Don't need async completion |
| Prevent duplicate submit | `click$.pipe(exhaustMap(save))` | Use exhaustMap ✓ | Correct choice |
| Login spam prevention | `login$.pipe(exhaustMap(...))` | Use exhaustMap ✓ | Correct choice |
| Refresh button cooldown | `refresh$.pipe(exhaustMap(...))` | Use exhaustMap ✓ | Correct choice |

**Comparison Table**:

| Operator | Concurrency | On New Emission | Use When |
|----------|-------------|-----------------|----------|
| `exhaustMap` | 1 (ignore new) | Ignore if busy | Prevent duplicates, spam protection |
| `switchMap` | 1 (cancel prev) | Cancel previous | Latest-only (search, navigation) |
| `concatMap` | 1 (queue) | Queue for later | Order matters, process all |
| `mergeMap` | Unlimited | Process all | Independent parallel ops |

**Common Patterns**:

```typescript
// Pattern 1: Form submission prevention
fromEvent(submitButton, 'click').pipe(
  exhaustMap(() => submitForm())
);

// Pattern 2: Login with spam prevention
fromEvent(loginButton, 'click').pipe(
  exhaustMap(() => performLogin())
);

// Pattern 3: Refresh with cooldown
fromEvent(refreshButton, 'click').pipe(
  exhaustMap(() => fetchFreshData())
);

// Pattern 4: Save with duplicate prevention
fromEvent(saveButton, 'click').pipe(
  exhaustMap(() => saveData())
);

// Pattern 5: API call with protection
fromEvent(fetchButton, 'click').pipe(
  exhaustMap(() => fetchFromAPI())
);
```

**When NOT to Use exhaustMap**:
```typescript
// ❌ Don't use for data streams
from(array).pipe(exhaustMap(...))  // Most items ignored!

// ❌ Don't use for search
searchInput$.pipe(exhaustMap(...))  // User sees old results!

// ❌ Don't use for navigation
route$.pipe(exhaustMap(...))  // Page changes ignored!

// ❌ Don't use for real-time updates
updates$.pipe(exhaustMap(...))  // Miss important updates!

// ✅ Do use for user actions that shouldn't duplicate
clicks$.pipe(exhaustMap(...))  // Perfect!
```

**Migration Notes**:
```typescript
// RxJS 5 (deprecated resultSelector)
source$.pipe(
  exhaustMap(
    outer => inner$,
    (outer, inner) => ({ outer, inner })
  )
)

// RxJS 6+ (current)
source$.pipe(
  exhaustMap(outer => 
    inner$.pipe(
      map(inner => ({ outer, inner }))
    )
  )
)
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/exhaustMap](https://rxjs.dev/api/operators/exhaustMap)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/exhaustMap.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/exhaustMap.ts)
- **RxJS Team Guide**: "Choosing the right flattening operator"
- **Best Practices**: "Preventing duplicate operations with exhaustMap"

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: 
- **Pattern**: Ignoring Composition Strategy (First-Wins Flattening)
- **Cognitive Load**: High (4/5) - Requires understanding when to ignore vs queue vs cancel
- **Usage Frequency**: Medium (3/5) - Less common than merge/switch/concat but essential for specific use cases
- **Composability**: Medium (3/5) - Very specific use case, limited general applicability

**Problem Domain**: 
User-initiated actions that should not duplicate or overlap, where subsequent attempts during execution should be ignored (not queued, not cancelled). Classic examples: form submissions, login attempts, save operations, refresh buttons, API mutations.

**When to Teach**: 
After students understand all three primary flattening operators (mergeMap, switchMap, concatMap). exhaustMap is the "fourth operator" that completes the flattening strategy toolkit. Teach through the problem of duplicate form submissions, which clearly demonstrates why ignoring (not queueing or cancelling) is the right approach.

**Common Misconceptions**:
1. **"exhaustMap is just throttle for Observables"** - No, it ignores based on inner Observable completion, not time
2. **"Ignored emissions are queued"** - No, they're dropped forever (that's concatMap)
3. **"exhaustMap is rarely useful"** - No, it's essential for preventing duplicate mutations
4. **"exhaustMap is the same as debounce"** - No, debounce delays, exhaust ignores
5. **"You can use switchMap instead"** - No, cancelling (switch) is very different from ignoring (exhaust)

**Teaching Progression**:
1. Start with the problem: double-click on submit button
2. Show why mergeMap creates duplicates (both requests execute)
3. Show why switchMap is wrong (cancels first submission!)
4. Show why concatMap queues (both execute eventually)
5. Introduce exhaustMap as "ignore while busy" solution
6. Demonstrate with marble diagrams showing ignored emissions
7. Practice with form submission, login, save operations
8. Emphasize importance of UI feedback for ignored actions
9. Compare all four flattening strategies side-by-side

**Canonical Use Cases**:
- **Form submissions**: Prevent double-submit
- **Login/authentication**: Prevent login spam
- **Save operations**: Prevent duplicate saves
- **Refresh buttons**: Prevent API spam
- **Mutation APIs**: Any operation that shouldn't duplicate
- **Download buttons**: Prevent duplicate downloads
- **Payment processing**: Critical - never duplicate!

**Anti-use Cases** (when NOT to use):
- Data stream processing (use mergeMap or concatMap)
- Search/typeahead (use switchMap)
- Navigation (use switchMap)
- Array/collection operations (use mergeMap or concatMap)
- Real-time updates (use switchMap or mergeMap)
- Polling (usually switchMap or mergeMap)

**The Flattening Strategy Decision Tree**:
```
Question: What should happen when source emits while inner Observable is active?

1. "Process new emission in parallel" → mergeMap
2. "Cancel previous, process new" → switchMap
3. "Queue new, process after previous completes" → concatMap
4. "Ignore new, keep processing current" → exhaustMap
```

**UX Considerations**:
- Always provide visual feedback for ignored actions
- Disable buttons during operation
- Show loading spinners or progress indicators
- Display toast notifications for ignored clicks
- Make system state visible to users
- Consider accessibility (screen readers should announce state)

**Debugging Tips**:
```typescript
// Add logging to see ignored emissions
source$.pipe(
  tap(x => console.log('Source emitted:', x)),
  exhaustMap(x => 
    inner(x).pipe(
      tap(() => console.log('Inner started for:', x)),
      finalize(() => console.log('Inner completed for:', x))
    )
  ),
  tap(result => console.log('Result emitted:', result))
)

// You'll see source emissions that don't result in inner starts (ignored!)
```

**Memory Safety**:
- exhaustMap is memory-safe (O(1) memory)
- No queueing = no memory buildup
- Unlike concatMap which can have unbounded queue
- Perfect for high-frequency user events

**Performance Characteristics**:
- Best case: All emissions processed (slow source, fast inner)
- Worst case: Most emissions ignored (fast source, slow inner)
- Typical case for UI: ~10-50% of rapid clicks processed
- No performance overhead from queueing or cancellation
