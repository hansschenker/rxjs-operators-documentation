# zip — Advanced Patterns

For `zip` fundamentals see the core [zip](./zip) doc. This page covers correlated data streams, animation sequencing, paging, and the comparison with `combineLatest` and `forkJoin`.

---

## The Core Contract

`zip` pairs values by **index position** — the Nth emission from source A is paired with the Nth emission from source B, regardless of timing. This makes it fundamentally different from `combineLatest` (latest values) and `forkJoin` (final values).

```
A: ---1-------2-------3---|
B: -----a---b---c---------|

zip(A, B):
   -----[1,a]-[2,b]-[3,c]-|
```

The first value from A pairs with the first from B. Neither emits until both have emitted their Nth value.

---

## When to Use `zip`

Use `zip` when values are **semantically paired** — the first value from one stream belongs with the first value from another, not the latest.

```typescript
// ✓ USE zip: sequential numbered results that pair naturally
const questions$ = this.api.getQuestions();     // emits Q1, Q2, Q3...
const answers$   = userAnswers$;                // emits A1, A2, A3...
zip(questions$, answers$).subscribe(([question, answer]) => {
  grade(question, answer); // Q1 with A1, Q2 with A2, guaranteed
});

// ✗ DON'T USE zip for "latest state from multiple sources" → use combineLatest
// ✗ DON'T USE zip for "wait for all to finish" → use forkJoin
```

---

## Pattern 1: Sequential Animation Frames

Pair animation steps with timing signals:

```typescript
import { zip, interval, of } from 'rxjs';
import { map } from 'rxjs/operators';

const steps$ = of('fadeIn', 'slideUp', 'highlight', 'fadeOut');
const timing$ = interval(500); // one step every 500ms

zip(steps$, timing$).pipe(
  map(([step]) => step) // discard index, keep step name
).subscribe(step => animateStep(step));
// fadeIn at 500ms, slideUp at 1000ms, highlight at 1500ms, fadeOut at 2000ms
```

---

## Pattern 2: Pairing Request with Response Metadata

```typescript
import { zip } from 'rxjs';
import { map } from 'rxjs/operators';

// When you have a stream of requests and a stream of responses
// and they correspond 1-to-1 in order:
const requestIds$  = requestStream$.pipe(map(r => r.id));
const responses$   = responseStream$; // arrives in same order

zip(requestIds$, responses$).pipe(
  map(([id, response]) => ({ requestId: id, data: response.data, duration: response.ms }))
).subscribe(logRequestMetrics);
```

---

## Pattern 3: Interleaving Two Streams

Alternate emissions from two sources:

```typescript
import { zip, merge, of } from 'rxjs';
import { map, mergeAll } from 'rxjs/operators';

// Interleave two arrays: [1,2,3] and [A,B,C] → 1,A,2,B,3,C
const numbers$ = of(1, 2, 3);
const letters$ = of('A', 'B', 'C');

zip(numbers$, letters$).pipe(
  map(([n, l]) => of(n, l)), // each pair → a mini Observable of two values
  mergeAll()                 // flatten: 1,A,2,B,3,C
).subscribe(console.log);
```

---

## Pattern 4: Paged Data Correlation

Match page requests with page results:

```typescript
import { zip, Subject } from 'rxjs';
import { switchMap } from 'rxjs/operators';

const pageRequests$ = new Subject<number>(); // emits page numbers: 1, 2, 3...

// Pair each page number with its fetched data:
const pagedResults$ = zip(
  pageRequests$,
  pageRequests$.pipe(switchMap(page => this.api.getPage(page)))
).pipe(
  map(([pageNum, data]) => ({ page: pageNum, items: data.items, total: data.total }))
);

// Load next page:
pageRequests$.next(1);
pageRequests$.next(2);
```

---

## Pattern 5: Synchronising Multiple Async Operations Step-by-Step

Run operations in lockstep — each step waits for both to complete before proceeding:

```typescript
import { zip, defer, range } from 'rxjs';
import { concatMap, map } from 'rxjs/operators';

const uploadProgress$ = uploadQueue$.pipe(
  concatMap(file => this.upload.file(file))
);

const processingStatus$ = uploadQueue$.pipe(
  concatMap(file => this.processor.process(file))
);

// Pair each upload result with its processing result:
zip(uploadProgress$, processingStatus$).pipe(
  map(([upload, process]) => ({
    fileId: upload.id,
    uploadOk:   upload.success,
    processOk:  process.success,
    ready:      upload.success && process.success
  }))
).subscribe(updateFileStatus);
```

---

## Pattern 6: `zip` for Test Data Generation

```typescript
import { zip, from } from 'rxjs';
import { map } from 'rxjs/operators';

// Generate structured test fixtures by zipping arrays:
const names$   = from(['Alice', 'Bob', 'Carol', 'Dave']);
const scores$  = from([95, 82, 88, 76]);
const grades$  = from(['A', 'B', 'B', 'C']);

zip(names$, scores$, grades$).pipe(
  map(([name, score, grade]) => ({ name, score, grade }))
).subscribe(student => testData.push(student));
// { name:'Alice', score:95, grade:'A' }, ...
```

---

## `zip` vs `combineLatest` vs `forkJoin` — Decision Table

| Operator | Pairs by | Emits | Use when |
|---|---|---|---|
| `zip` | Index (N-th with N-th) | After Nth from each | Values are semantically paired by position |
| `combineLatest` | Latest values | Any source changes | Live state — react to any change |
| `forkJoin` | Final values | Once, when all complete | Parallel HTTP requests, one-shot ops |

```typescript
// zip — Q1 always answers Q1, Q2 answers Q2:
zip(questions$, answers$)

// combineLatest — always uses the most recent answer for any question:
combineLatest({ question: questions$, answer: latestAnswer$ })

// forkJoin — wait for ALL questions and ALL answers, emit once:
forkJoin({ questions: questionsArray$, answers: answersArray$ })
```

---

## Common Pitfalls

### `zip` Backs Up if One Source is Faster

```typescript
// ❌ Memory pressure: if A emits much faster than B,
//    A's values queue up in memory waiting for B's matching value
const fast$ = interval(10);   // emits every 10ms
const slow$ = interval(1000); // emits every 1s

zip(fast$, slow$).subscribe(console.log);
// After 1 second: fast$ has 100 buffered values waiting for slow$'s 1 value
// After 60s: 6000 buffered values — potential memory issue

// ✅ Only use zip when sources emit at similar rates, or use take() to bound:
zip(fast$.pipe(take(10)), slow$.pipe(take(10))).subscribe(console.log);
```

### Confusing `zip` with `combineLatest` for Latest-Value Scenarios

```typescript
// ❌ zip — waits for both to have emitted N times (1st+1st, 2nd+2nd)
// If config$ only ever emits once, zip only produces ONE result
zip(userActions$, config$).subscribe(([action, config]) => handle(action, config));
// Only fires once — for the first userAction paired with the first config emission

// ✅ combineLatest — uses latest config on every user action
combineLatest({ action: userActions$, config: config$ })
  .subscribe(({ action, config }) => handle(action, config));
// Fires on every action with the current config value
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: `zip` is the right tool only when values are semantically coupled by position — the N-th value from one stream belongs specifically with the N-th from another. If you want "latest from both," use `combineLatest`. If speeds differ significantly, `zip` creates a memory backpressure problem.
