# combineLatest

## Identity
- **Name**: combineLatest
- **Category**: Combination Operators
- **Type**: Static creation operator / Pipeable combination operator
- **Import**: 
  ```typescript
  // As creation operator (recommended)
  import { combineLatest } from 'rxjs';
  
  // As pipeable operator (deprecated in RxJS 7+)
  import { combineLatest } from 'rxjs/operators';
  ```
- **Signature**: 
  ```typescript
  // Creation operator - array of observables
  function combineLatest<T extends readonly unknown[]>(
    sources: readonly [...ObservableInputTuple<T>]
  ): Observable<T>;
  
  // Creation operator - dictionary of observables
  function combineLatest<T extends Record<string, ObservableInput<any>>>(
    sources: T
  ): Observable<{ [K in keyof T]: ObservedValueOf<T[K]> }>;
  
  // Creation operator - variadic arguments with project function
  function combineLatest<T extends readonly unknown[], R>(
    ...sources: [...ObservableInputTuple<T>, (...values: T) => R]
  ): Observable<R>;
  
  // Pipeable operator (deprecated)
  function combineLatest<T, R>(
    ...observables: Array<ObservableInput<any> | ((...values: Array<any>) => R)>
  ): OperatorFunction<T, R>;
  ```

## Functional Specification

**Input**: Multiple Observable sources (2 to N observables) of types T₁, T₂, ..., Tₙ

**Output**: Observable<[T₁, T₂, ..., Tₙ]> emitting arrays of the latest values from all sources

**Transformation**: When any source Observable emits, combineLatest collects the most recent value from each source and emits the combined array. Requires at least one emission from every source before first output.

**Mathematical representation**:
```
Let S₁, S₂, ..., Sₙ be source observables
Let vᵢⱼ be the j-th emission from Sᵢ
Let latest(Sᵢ) be the most recent emission from Sᵢ

Output emits [latest(S₁), latest(S₂), ..., latest(Sₙ)] whenever any Sᵢ emits
```

**Invariants**:
- **Completeness requirement**: All sources must emit at least once before any output
- **Latest value semantics**: Only the most recent value from each source is retained
- **Synchronous combination**: Combination occurs immediately upon any source emission
- **Order preservation**: Output array order matches source array order
- **Memory bounded**: Stores exactly one value per source (O(n) space where n = number of sources)
- **Eager evaluation**: Subscribes to all sources immediately

## Marble Diagram

```
Source A: --1-----2--------3-----|
Source B: ----a--------b---------|
Source C: ------x--y-------------|
            combineLatest([A, B, C])
Result:   ------[1,a,x]
                  -[2,a,x]
                  --[2,a,y]
                  ---------[2,b,y]
                  ----------[3,b,y]|

Legend:
  - : time unit (10ms)
  1,2,3 : values from Source A
  a,b : values from Source B
  x,y : values from Source C
  [1,a,x] : combined array emission
  | : completion
  ^ : subscription point
```

**Key observation**: No emission until all three sources have emitted at least once (at time unit 6).

## Behavioral Characteristics

**Subscription**: 
- Subscribes to all source Observables immediately upon subscription
- Creates independent subscriptions to each source
- All sources are active simultaneously (eager, not lazy)

**Completion semantics**:
- Completes only when **all** source Observables complete
- If any source never completes, result never completes
- If a source completes after emitting values, its last value continues to be used in combinations
- Empty Observables (complete without emitting) cause the result to complete immediately without emitting

**Error handling**:
- Any error from any source immediately propagates to output
- First error wins (if multiple sources error simultaneously)
- No error recovery mechanism
- Unsubscribes from all sources when any source errors

**Backpressure**:
- No built-in backpressure handling
- Stores latest value from each source (bounded memory)
- Fast producers overwrite previous values (lossy)
- Emission rate equals the sum of all source emission rates after initial combination

**Hot vs. Cold**:
- Creates new subscriptions to cold sources (may replay values)
- Shares hot sources naturally
- For sharing cold sources, use `share()` or `shareReplay()` on sources before combining

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T extends readonly unknown[] - Tuple type of all source emission types
 *   R - Result type when using projection function
 * 
 * Input Types:
 *   - Array form: readonly [...ObservableInputTuple<T>]
 *   - Dictionary form: Record<string, ObservableInput<any>>
 *   - Variadic form: Individual ObservableInput arguments
 * 
 * Output Type:
 *   - Array form: Observable<T> (tuple of source types)
 *   - Dictionary form: Observable<{ [K in keyof T]: ObservedValueOf<T[K]> }>
 *   - With projection: Observable<R>
 * 
 * Type Narrowing:
 *   - Preserves exact tuple types from input array
 *   - Dictionary form preserves key names and individual types
 *   - Projection function must accept all source types in order
 *   - Infers literal types when sources emit literals
 * 
 * Type Safety:
 *   - Compile-time verification of source count and types
 *   - Type-safe destructuring of result arrays
 *   - Prevents accidental index out-of-bounds (tuple types)
 *   - Generic constraints ensure ObservableInput compatibility
 *   - Dictionary form ensures type safety for named properties
 */

// Example: Type preservation
const num$ = of(1, 2, 3);           // Observable<number>
const str$ = of('a', 'b');          // Observable<string>
const bool$ = of(true, false);      // Observable<boolean>

// Result type: Observable<[number, string, boolean]>
const combined$ = combineLatest([num$, str$, bool$]);

// Type-safe destructuring
combined$.subscribe(([n, s, b]) => {
  n.toFixed(2);      // n is number
  s.toUpperCase();   // s is string
  b.valueOf();       // b is boolean
});

// Dictionary form provides named access
const namedCombined$ = combineLatest({
  count: num$,
  label: str$,
  active: bool$
});
// Type: Observable<{ count: number; label: string; active: boolean }>
```

## Examples

### Basic Usage - Array Form
```typescript
import { combineLatest, of, interval } from 'rxjs';
import { map, take } from 'rxjs/operators';

// Combine timer with static values
const timer$ = interval(1000).pipe(take(3));  // 0, 1, 2
const name$ = of('Alice');
const age$ = of(30);

const profile$ = combineLatest([timer$, name$, age$]);

profile$.subscribe(([tick, name, age]) => {
  console.log(`[${tick}] ${name} is ${age} years old`);
});

// Output (at 1-second intervals):
// [0] Alice is 30 years old
// [1] Alice is 30 years old
// [2] Alice is 30 years old
```

### Common Pattern - Form Field Validation
```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { map } from 'rxjs/operators';

interface FormState {
  username: string;
  email: string;
  password: string;
}

interface ValidationResult {
  isValid: boolean;
  errors: string[];
}

// Form field observables
const username$ = new BehaviorSubject('');
const email$ = new BehaviorSubject('');
const password$ = new BehaviorSubject('');

// Combine all fields for validation
const formState$ = combineLatest({
  username: username$,
  email: email$,
  password: password$
}).pipe(
  map((form): ValidationResult => {
    const errors: string[] = [];
    
    if (form.username.length < 3) {
      errors.push('Username must be at least 3 characters');
    }
    
    if (!form.email.includes('@')) {
      errors.push('Email must be valid');
    }
    
    if (form.password.length < 8) {
      errors.push('Password must be at least 8 characters');
    }
    
    return {
      isValid: errors.length === 0,
      errors
    };
  })
);

// Usage
formState$.subscribe(validation => {
  console.log('Form valid:', validation.isValid);
  console.log('Errors:', validation.errors);
});

username$.next('bob');      // Triggers validation
email$.next('bob@mail.com'); // Triggers validation
password$.next('secret123'); // Triggers validation
// Output: Form valid: true, Errors: []
```

### Edge Cases - Empty Observables and Error Handling
```typescript
import { combineLatest, of, EMPTY, throwError, timer } from 'rxjs';
import { take } from 'rxjs/operators';

// Edge case 1: Empty observable causes immediate completion
const empty$ = combineLatest([
  of(1, 2, 3),
  EMPTY,  // Completes without emitting
  of('a', 'b')
]);

empty$.subscribe({
  next: v => console.log('Value:', v),
  complete: () => console.log('Completed without emitting!')
});
// Output: "Completed without emitting!"

// Edge case 2: Error from any source propagates
const error$ = combineLatest([
  timer(100, 100).pipe(take(5)),
  throwError(() => new Error('Something went wrong')),
  of('will never emit')
]);

error$.subscribe({
  next: v => console.log('Value:', v),
  error: err => console.log('Error caught:', err.message)
});
// Output: "Error caught: Something went wrong"
// (Errors immediately, timer never gets to emit)

// Edge case 3: Source that never emits blocks all output
const never$ = new Observable(() => {
  // Never emits, never completes
});

const blocked$ = combineLatest([
  of(1, 2, 3),
  never$,  // Blocks forever
  of('a', 'b')
]);

blocked$.subscribe({
  next: v => console.log('Value:', v),
  complete: () => console.log('Complete')
});
// Output: (nothing - waits forever for never$ to emit)

// Edge case 4: Synchronous sources emit in predictable order
const sync$ = combineLatest([
  of(1, 2, 3),
  of('a', 'b'),
  of(true, false)
]);

sync$.subscribe(v => console.log(v));
// Output:
// [3, 'b', false]  <- Only one emission with final values
// (All sources complete synchronously, only final combination emits)
```

### Advanced Pattern - Dependent Observables
```typescript
import { combineLatest, BehaviorSubject } from 'rxjs';
import { switchMap, map } from 'rxjs/operators';

// User selects a category and a sort order
const category$ = new BehaviorSubject<string>('electronics');
const sortBy$ = new BehaviorSubject<'price' | 'rating'>('price');

// Fetch products based on both selections
const products$ = combineLatest([category$, sortBy$]).pipe(
  // switchMap cancels previous fetch if parameters change
  switchMap(([category, sort]) => 
    fetchProducts(category, sort)
  )
);

function fetchProducts(category: string, sort: string): Observable<Product[]> {
  // Simulated API call
  return of([
    { name: 'Laptop', category, price: 999, rating: 4.5 },
    { name: 'Mouse', category, price: 29, rating: 4.8 }
  ]).pipe(
    map(products => 
      sort === 'price' 
        ? products.sort((a, b) => a.price - b.price)
        : products.sort((a, b) => b.rating - a.rating)
    )
  );
}

products$.subscribe(products => {
  console.log('Products:', products);
});

// User interactions
category$.next('computers');  // Triggers new fetch
sortBy$.next('rating');       // Triggers new fetch with new sort
```

## Common Pitfalls

### Anti-pattern 1: Forgetting Initial Emission Requirement
```typescript
// ❌ INCORRECT: Assuming immediate emission
import { Subject, combineLatest } from 'rxjs';

const a$ = new Subject<number>();
const b$ = new Subject<string>();

combineLatest([a$, b$]).subscribe(
  value => console.log('Got:', value)
);

a$.next(1);  // Nothing happens - b$ hasn't emitted yet!
// User thinks it's broken, but it's working as designed

// ✅ CORRECT: Use BehaviorSubject or startWith for initial values
import { BehaviorSubject } from 'rxjs';
import { startWith } from 'rxjs/operators';

const a$ = new BehaviorSubject(0);  // Has initial value
const b$ = new Subject<string>().pipe(startWith(''));  // Provides initial value

combineLatest([a$, b$]).subscribe(
  value => console.log('Got:', value)
);
// Immediately emits: Got: [0, '']

a$.next(1);  // Emits: Got: [1, '']

// WHY: combineLatest requires all sources to emit before producing output
// WHEN TO USE: BehaviorSubject when you have a meaningful default
//              startWith when you need to kick off the combination
```

### Anti-pattern 2: Excessive Re-computation
```typescript
// ❌ INCORRECT: Expensive computation on every emission
import { combineLatest, interval } from 'rxjs';
import { map } from 'rxjs/operators';

const fastStream$ = interval(100);  // Emits every 100ms
const slowStream$ = interval(5000); // Emits every 5s

combineLatest([fastStream$, slowStream$]).pipe(
  map(([fast, slow]) => {
    // Expensive computation runs every 100ms!
    return computeExpensiveResult(fast, slow);
  })
).subscribe();

function computeExpensiveResult(a: number, b: number): number {
  // Simulated expensive operation
  let result = 0;
  for (let i = 0; i < 1000000; i++) {
    result += Math.sqrt(a * b * i);
  }
  return result;
}

// ✅ CORRECT: Debounce or throttle fast sources
import { debounceTime, throttleTime } from 'rxjs/operators';

combineLatest([
  fastStream$.pipe(throttleTime(1000)),  // Limit to once per second
  slowStream$
]).pipe(
  map(([fast, slow]) => computeExpensiveResult(fast, slow))
).subscribe();

// WHY: combineLatest emits on EVERY source emission after initial combination
// PERFORMANCE: Fast emitters can trigger expensive downstream operations unnecessarily
// SOLUTION: Rate-limit fast sources or use distinctUntilChanged to skip redundant computations
```

### Anti-pattern 3: Memory Leaks with Infinite Observables
```typescript
// ❌ INCORRECT: No completion strategy for infinite sources
import { combineLatest, interval } from 'rxjs';

function createDashboard() {
  const clock$ = interval(1000);      // Never completes
  const metrics$ = interval(5000);    // Never completes
  const alerts$ = interval(10000);    // Never completes
  
  return combineLatest([clock$, metrics$, alerts$]);
}

// Component mounts
const subscription = createDashboard().subscribe(data => {
  updateUI(data);
});

// Component unmounts, but subscription continues!
// Subscription still active, memory leak!

// ✅ CORRECT: Always unsubscribe or use takeUntil
import { Subject, takeUntil } from 'rxjs';

function createDashboard(destroy$: Subject<void>) {
  const clock$ = interval(1000);
  const metrics$ = interval(5000);
  const alerts$ = interval(10000);
  
  return combineLatest([clock$, metrics$, alerts$]).pipe(
    takeUntil(destroy$)  // Completes when component destroys
  );
}

// Component mounts
const destroy$ = new Subject<void>();
createDashboard(destroy$).subscribe(data => {
  updateUI(data);
});

// Component unmounts
destroy$.next();
destroy$.complete();

// WHY: combineLatest only completes when ALL sources complete
// MEMORY: Infinite sources mean subscriptions never clean up
// SOLUTION: Use takeUntil, take, or manual unsubscribe
```

### Anti-pattern 4: Incorrect Source Ordering Assumptions
```typescript
// ❌ INCORRECT: Assuming sources emit in predictable order
import { combineLatest, of, delay } from 'rxjs';

const first$ = of('first').pipe(delay(100));
const second$ = of('second').pipe(delay(50));
const third$ = of('third').pipe(delay(10));

combineLatest([first$, second$, third$]).subscribe(
  ([a, b, c]) => {
    // Expecting a='first', b='second', c='third'
    console.log(a, b, c);
  }
);
// Output: "first second third" 
// (But only because all complete before we observe)

// Misunderstanding: thinking combineLatest preserves emission order
// Reality: it preserves SOURCE order in array, not emission timing

// ✅ CORRECT: Understanding position !== emission order
combineLatest({
  userInput: first$,
  apiResponse: second$,
  cacheData: third$
}).subscribe(result => {
  // result.userInput is from first$ (regardless of when it emitted)
  // result.apiResponse is from second$ (regardless of when it emitted)
  // result.cacheData is from third$ (regardless of when it emitted)
});

// WHY: Array position determines result position, not emission timing
// CLARITY: Use dictionary form for named access when order is confusing
```

### Performance: High-Frequency Combinations
**When this matters**: 
- Combining multiple high-frequency streams (>10 Hz)
- Large number of sources (>10 observables)
- Heavy downstream processing

**What to do**:
```typescript
// Use auditTime or debounceTime to reduce emission rate
combineLatest([...manySources]).pipe(
  auditTime(100),  // At most one emission per 100ms
  // or
  debounceTime(100)  // Wait for 100ms of silence
);

// Use distinctUntilChanged to skip redundant values
combineLatest([...sources]).pipe(
  distinctUntilChanged((prev, curr) => 
    JSON.stringify(prev) === JSON.stringify(curr)
  )
);

// For large arrays, consider custom comparison
combineLatest([...sources]).pipe(
  distinctUntilChanged((prev, curr) => 
    prev.every((val, idx) => val === curr[idx])
  )
);
```

## Related Operators

**Same Category (Combination)**:
- **`withLatestFrom`**: Similar but only emits when the source (first) observable emits, using latest values from others as context. Use when you have a primary stream and auxiliary streams.
- **`zip`**: Combines sources by index position (1st with 1st, 2nd with 2nd). Use when you need pairwise alignment.
- **`forkJoin`**: Waits for all sources to complete, then emits one array of final values. Use for parallel async operations (like Promise.all).
- **`merge`**: Flattens all source emissions into single stream. Use when you want all events, not combinations.
- **`concat`**: Subscribes to sources sequentially. Use when order of completion matters.
- **`race`**: Emits from whichever source emits first, unsubscribes from others. Use for timeout patterns or redundant sources.

**Complementary Operators**:
- **`map`**: combineLatest(...).pipe(map(...)) for transforming combined values
- **`filter`**: combineLatest(...).pipe(filter(...)) to skip invalid combinations
- **`switchMap`**: combineLatest([a$, b$]).pipe(switchMap(...)) for dependent async operations
- **`distinctUntilChanged`**: Prevent redundant emissions from fast sources
- **`debounceTime` / `auditTime`**: Rate-limit high-frequency combinations
- **`startWith`**: Provide initial values for Subject sources

**Alternatives by Use Case**:

| Use Case | Instead of combineLatest | Use This | Why |
|----------|-------------------------|----------|-----|
| One primary stream + context | `combineLatest([main$, ctx$])` | `main$.pipe(withLatestFrom(ctx$))` | Only emits when main$ emits |
| Wait for all to complete | `combineLatest` with take(1) | `forkJoin` | Clearer intent, completes automatically |
| Process items pairwise | `combineLatest` with complex logic | `zip` | Maintains index alignment |
| Merge all events | `combineLatest` then flatten | `merge` | Direct flattening |
| React to any change | Already using `combineLatest` | ✓ Correct choice | This is the right operator |

**Migration from Deprecated API**:
```typescript
// RxJS 6 (deprecated)
import { combineLatest } from 'rxjs/operators';
source$.pipe(combineLatest(other$))

// RxJS 7+ (current)
import { combineLatest } from 'rxjs';
combineLatest([source$, other$])
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/combineLatest](https://rxjs.dev/api/index/function/combineLatest)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/combinelatest.html](http://reactivex.io/documentation/operators/combinelatest.html)
- **TC39 Observable Proposal**: Combination semantics discussion
- **Academic Foundation**: Functional Reactive Programming (FRP) - Continuous time-varying values
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/combineLatest.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/combineLatest.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: 
- **Pattern**: Parallel Composition Strategy
- **Cognitive Load**: Medium (3/5) - Requires understanding latest-value semantics and completion rules
- **Usage Frequency**: Very High (5/5) - One of the most common combination operators
- **Composability**: High (5/5) - Works well with most operators in pipelines

**Problem Domain**: 
Synchronizing multiple independent data streams into a unified view where changes to any input should trigger a recalculation of the combined output.

**When to Teach**: 
After covering basic operators (map, filter) and before advanced patterns like switchMap. Essential for form handling, state management, and coordinated UI updates.
