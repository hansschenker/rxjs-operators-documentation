# RxJS Operators Documentation

> **The most comprehensive RxJS operator reference ever assembled.**
> 265 pages. All 17 operator categories. 90+ advanced deep-dives. 82 reference guides. TypeScript-first.

Built with [VitePress](https://vitepress.dev) and the **Eight-Policy Framework** — a formal specification standard that ensures every operator is documented with the same depth, structure, and production focus. Every page answers not just *"what does this operator do?"* but *"what is its full behavioral contract, where does it fail, and when should I reach for something else?"*

---

## Why This Exists

The official [rxjs.dev](https://rxjs.dev) documentation is an API reference. [learn-rxjs.io](https://www.learnrxjs.io) is an excellent introduction. [rxmarbles.com](https://rxmarbles.com) visualizes timing.

None of them answer the questions that come up in production:

- What happens to `switchMap` when the inner Observable errors but I don't catch it?
- Why does `distinct()` cause a memory leak in a long-running stream?
- When does `exhaustMap` permanently stop accepting new values, and how do I fix it?
- How do I choose between `debounceTime`, `throttleTime`, `auditTime`, and `sampleTime`?
- What is the behavioral difference between `share()` and `shareReplay(1)` on reconnect?

This reference is built for those moments — for engineers who already know *of* an operator and need to understand it *completely*.

---

## Scale

| Metric | Count |
|---|---|
| Operator documentation pages | **183** |
| Reference guides & pattern libraries | **82** |
| **Total pages** | **265** |
| Operator categories covered | **17 / 17** |
| Operators with advanced deep-dive docs | **90+** |
| Real-world domain pattern guides | **55+** |
| Anti-pattern examples with corrections | **270+** |
| Git commits | **107+** |

---

## The Eight-Policy Framework

Every operator document — without exception — covers all eight policies in order:

| # | Policy | What it answers |
|---|---|---|
| 1 | **Identity** | Name, category, TypeScript signature, import path |
| 2 | **Functional Specification** | Input/output types, transformation rules, mathematical invariants |
| 3 | **Marble Diagrams** | ASCII temporal visualization of every meaningful behavior variant |
| 4 | **Behavioral Characteristics** | Subscription mechanics, completion semantics, error propagation, backpressure, hot vs cold |
| 5 | **Type System Integration** | Generics, type narrowing, compile-time guarantees |
| 6 | **Practical Examples** | Minimum 3 complete runnable examples: basic → common pattern → edge case |
| 7 | **Common Pitfalls** | Anti-patterns with corrections using `❌ INCORRECT` / `✅ CORRECT` / `WHY:` format |
| 8 | **Related Operators** | Same-category alternatives, complementary operators, decision matrix |

Each doc also carries **educational metadata**: Cognitive Load (1–5), Usage Frequency (1–5), Composability Factor (1–5), and teaching sequence placement — so you know how much mental overhead to budget and where each operator belongs in a learning curriculum.

---

## Operator Coverage

### Transformation (34 docs)

The core of RxJS — every major flattening strategy, buffering, and projection operator.

| Operator(s) | Core | Advanced |
|---|---|---|
| `map` | ✅ | ✅ |
| `mergeMap` | ✅ | ✅ |
| `switchMap` | ✅ | ✅ |
| `concatMap` | ✅ | ✅ |
| `exhaustMap` | ✅ | ✅ |
| `groupBy` | ✅ | ✅ |
| `expand` | ✅ | ✅ |
| `pairwise` | ✅ | ✅ |
| `bufferTime` | ✅ | ✅ |
| `bufferCount`, `windowCount` | ✅ | ✅ |
| `buffer`, `window` | ✅ | ✅ |
| `bufferWhen`, `windowWhen` | ✅ | ✅ |
| `bufferToggle`, `windowToggle` | ✅ | ✅ |
| `windowTime` | ✅ | ✅ |
| `switchScan` | ✅ | ✅ |
| `mergeScan` | ✅ | ✅ |

### Filtering (26 docs)

When, how often, and whether values pass through.

| Operator(s) | Core | Advanced |
|---|---|---|
| `filter` | ✅ | ✅ |
| `debounceTime` | ✅ | ✅ |
| `throttleTime` | ✅ | ✅ |
| `distinctUntilChanged` | ✅ | ✅ |
| `distinctUntilKeyChanged` | ✅ | ✅ |
| `distinct` | ✅ | ✅ |
| `take`, `skip`, `takeLast`, `elementAt` | ✅ | ✅ |
| `takeUntil`, `takeWhile` | ✅ | ✅ each |
| `first`, `last` | ✅ | ✅ |
| `skipUntil`, `skipWhile` | ✅ | ✅ |
| `find`, `findIndex` | ✅ | ✅ |

### Creation (33 docs)

Every factory function for producing Observables.

| Operator(s) | Core | Advanced |
|---|---|---|
| `of`, `range` | ✅ | — |
| `from` | ✅ | ✅ |
| `generate`, `using` | ✅ | ✅ |
| `interval`, `timer` | ✅ | ✅ |
| `fromEvent` | ✅ | ✅ |
| `fromEventPattern` | ✅ | ✅ |
| `fromFetch` | ✅ | ✅ |
| `defer` | ✅ | ✅ |
| `EMPTY`, `NEVER` | ✅ | ✅ |
| `throwError` | ✅ | — |
| `ajax` | ✅ | ✅ |
| `webSocket` | ✅ | ✅ |
| `animationFrames` | ✅ | ✅ |
| `partition`, `iif` | ✅ | ✅ |
| `bindCallback`, `bindNodeCallback` | ✅ | ✅ |
| `scheduled` | ✅ | ✅ |

### Combination (18 docs)

Merging, joining, and coordinating multiple streams.

| Operator(s) | Core | Advanced |
|---|---|---|
| `combineLatest` | ✅ | ✅ |
| `merge`, `concat` | ✅ | ✅ each |
| `forkJoin` | ✅ | ✅ |
| `zip` | ✅ | ✅ |
| `race` | ✅ | ✅ |
| `withLatestFrom` | ✅ | ✅ |
| `startWith` | ✅ | ✅ |
| `combineLatestWith`, `mergeWith`, `concatWith` | ✅ | — |

### Error Handling (7 docs)

| Operator(s) | Core | Advanced |
|---|---|---|
| `catchError` | ✅ | ✅ |
| `retry` | ✅ | ✅ |
| `timeout` | ✅ | ✅ |
| `onErrorResumeNext` | ✅ | ✅ |

### Subjects (9 docs)

| Type | Core | Advanced |
|---|---|---|
| `Subject` | ✅ | ✅ |
| `BehaviorSubject` | ✅ | ✅ |
| `ReplaySubject` | ✅ | ✅ |
| `AsyncSubject` | ✅ | ✅ |

### Utility (22 docs)

| Operator(s) | Core | Advanced |
|---|---|---|
| `tap` | ✅ | ✅ |
| `finalize` | ✅ | ✅ |
| `delay`, `delayWhen` | ✅ | ✅ |
| `observeOn`, `subscribeOn` | ✅ | ✅ |
| `materialize`, `dematerialize` | ✅ | ✅ |
| `timeInterval`, `timestamp` | ✅ | ✅ |
| `repeat` | ✅ | ✅ |
| `schedulers` | ✅ | ✅ |
| `firstValueFrom`, `lastValueFrom` | ✅ | ✅ |
| `endWith`, `ignoreElements` | ✅ | ✅ |
| `deprecated operators` | ✅ | — |

### Higher-Order, Multicasting, Mathematical/Aggregate, Rate Limiting, Interop, Testing, Conditional, Connectable

All 17 categories fully covered with both core docs and advanced deep-dives. See the sidebar for complete listings.

---

## Reference Library (82 guides)

Beyond operator docs, the reference library covers production patterns organized by domain.

### Decision Guides — Start here when you're choosing

| Guide | Answers |
|---|---|
| [Flattening Strategy Guide](docs/operator-trees/flattening-strategy-guide.md) | `switchMap` vs `concatMap` vs `mergeMap` vs `exhaustMap` |
| [Subject Decision Guide](docs/operator-trees/subject-decision-guide.md) | Which Subject type for which use case |
| [Operator Selection Guide](docs/operator-trees/operator-selection-guide.md) | Match your intent to the right operator |
| [Rate-Limiting Guide](docs/operator-trees/rate-limiting-guide.md) | `debounce` vs `throttle` vs `audit` vs `sample` |
| [Error Handling Patterns](docs/operator-trees/error-handling-patterns.md) | Retry strategies, circuit breakers, fallback chains |
| [Multicasting Guide](docs/operator-trees/multicasting-guide.md) | `share` vs `shareReplay`, refCount vs reset semantics |
| [Higher-Order Observables](docs/operator-trees/higher-order-observables-guide.md) | When and how to flatten inner Observables |

### Architecture & Testing

[Pipeline Architecture](docs/operator-trees/pipeline-architecture-guide.md) ·
[State Management Patterns](docs/operator-trees/state-management-patterns.md) ·
[Concurrency Patterns](docs/operator-trees/concurrency-guide.md) ·
[Custom Operators Guide](docs/operator-trees/custom-operators-guide.md) ·
[Custom Operators Advanced](docs/operator-trees/rxjs-custom-operators-advanced.md) ·
[Subscription Management](docs/operator-trees/subscription-management-guide.md) ·
[Cold vs Hot Observables](docs/operator-trees/cold-vs-hot-guide.md) ·
[Marble Testing Advanced](docs/operator-trees/rxjs-marble-testing-advanced.md) ·
[Integration Testing](docs/operator-trees/rxjs-testing-integration.md) ·
[Debugging Guide](docs/operator-trees/debugging-guide.md) ·
[Performance Patterns](docs/operator-trees/performance-patterns-guide.md) ·
[State Machines](docs/operator-trees/rxjs-state-machines.md)

### Framework Integrations

[Angular + RxJS Patterns](docs/operator-trees/angular-rxjs-patterns.md) ·
[Angular Signals + RxJS](docs/operator-trees/angular-signals-rxjs.md) ·
[NgRx Effects Patterns](docs/operator-trees/ngrx-effects-patterns.md) ·
[RxJS in React](docs/operator-trees/rxjs-react-patterns.md) ·
[RxJS in Vue](docs/operator-trees/rxjs-vue-patterns.md) ·
[RxJS in Svelte](docs/operator-trees/rxjs-svelte-patterns.md) ·
[Node.js Patterns](docs/operator-trees/nodejs-rxjs-patterns.md) ·
[TypeScript + RxJS Guide](docs/operator-trees/typescript-rxjs-guide.md)

### Real-World Domain Patterns

[WebSocket Patterns](docs/operator-trees/rxjs-websocket-patterns.md) ·
[GraphQL Patterns](docs/operator-trees/rxjs-graphql-patterns.md) ·
[Authentication Patterns](docs/operator-trees/rxjs-authentication-patterns.md) ·
[Caching Patterns](docs/operator-trees/rxjs-caching-patterns.md) ·
[Real-Time Data Guide](docs/operator-trees/realtime-data-guide.md) ·
[Polling Patterns](docs/operator-trees/rxjs-polling-patterns.md) ·
[File Upload Patterns](docs/operator-trees/rxjs-file-upload-patterns.md) ·
[Form Validation Guide](docs/operator-trees/form-validation-guide.md) ·
[Optimistic UI Patterns](docs/operator-trees/rxjs-optimistic-ui-patterns.md) ·
[Undo/Redo Patterns](docs/operator-trees/rxjs-undo-redo-patterns.md) ·
[Data Synchronization](docs/operator-trees/rxjs-data-synchronization-patterns.md) ·
[Multi-Tab Patterns](docs/operator-trees/rxjs-multi-tab-patterns.md) ·
[Notification & Toast Patterns](docs/operator-trees/rxjs-notification-toast-patterns.md)

### Browser & Platform APIs

[Web Worker Patterns](docs/operator-trees/rxjs-web-worker-patterns.md) ·
[Service Worker Patterns](docs/operator-trees/rxjs-service-worker-patterns.md) ·
[IndexedDB Patterns](docs/operator-trees/rxjs-indexeddb-patterns.md) ·
[Accessibility Patterns](docs/operator-trees/rxjs-accessibility-patterns.md) ·
[Charts & Visualization](docs/operator-trees/rxjs-charts-visualization.md) ·
[Drag, Drop & Animation](docs/operator-trees/rxjs-drag-drop-animation.md) ·
[Virtual Scroll](docs/operator-trees/rxjs-virtual-scroll.md) ·
[Infinite Scroll & Pagination](docs/operator-trees/rxjs-infinite-scroll-pagination.md) ·
[Search & Autocomplete](docs/operator-trees/rxjs-search-autocomplete-patterns.md) ·
[Micro-Frontend Patterns](docs/operator-trees/rxjs-micro-frontend-patterns.md)

### Migration & Future

[RxJS 8 Preparation](docs/operator-trees/rxjs-rxjs8-preparation.md) ·
[RxJS Migration Guide](docs/operator-trees/rxjs-migration-guide.md) ·
[Promise Interop](docs/operator-trees/rxjs-promise-interop.md) ·
[Error Resilience Patterns](docs/operator-trees/rxjs-error-resilience-patterns.md) ·
[RxJS Best Practices](docs/operator-trees/rxjs-best-practices.md) ·
[RxJS Mental Models](docs/operator-trees/rxjs-mental-models.md)

---

## Learning Paths

### Beginner — "I'm new to RxJS"

1. **[RxJS Mental Models](docs/operator-trees/rxjs-mental-models.md)** — build the conceptual foundation first
2. `of`, `from`, `interval`, `fromEvent` — the four most common creation operators
3. `map`, `filter`, `tap` — basic transformation
4. **[Cold vs Hot Observables](docs/operator-trees/cold-vs-hot-guide.md)** — the most important concept to internalize early
5. **[Subscription Management](docs/operator-trees/subscription-management-guide.md)** — avoid leaks from day one
6. `debounceTime`, `distinctUntilChanged` — the two filtering operators you'll use most
7. `catchError`, `retry` — error handling basics
8. **[RxJS Cookbook](docs/operator-trees/rxjs-cookbook.md)** — common recipes ready to use

### Intermediate — "I use RxJS daily and want to go deeper"

1. **[Flattening Strategy Guide](docs/operator-trees/flattening-strategy-guide.md)** — the most consequential RxJS decision
2. `switchMap` advanced · `concatMap` advanced · `mergeMap` advanced · `exhaustMap` advanced
3. **[Multicasting Guide](docs/operator-trees/multicasting-guide.md)** — `shareReplay`, `share`, refCount semantics
4. **[Subject Decision Guide](docs/operator-trees/subject-decision-guide.md)**
5. `scan` advanced — state accumulation without external variables
6. **[Error Handling Patterns](docs/operator-trees/error-handling-patterns.md)** — exponential backoff, circuit breakers
7. **[Custom Operators Guide](docs/operator-trees/custom-operators-guide.md)**
8. **[Marble Testing Advanced](docs/operator-trees/rxjs-marble-testing-advanced.md)**

### Advanced — "I design systems built on RxJS"

1. **[Pipeline Architecture](docs/operator-trees/pipeline-architecture-guide.md)**
2. **[Concurrency Patterns](docs/operator-trees/concurrency-guide.md)**
3. **[State Machines](docs/operator-trees/rxjs-state-machines.md)**
4. **[Custom Operators Advanced](docs/operator-trees/rxjs-custom-operators-advanced.md)**
5. **[Performance Patterns](docs/operator-trees/performance-patterns-guide.md)**
6. **[TypeScript + RxJS Guide](docs/operator-trees/typescript-rxjs-guide.md)**
7. **[Integration Testing](docs/operator-trees/rxjs-testing-integration.md)**
8. **[RxJS 8 Preparation](docs/operator-trees/rxjs-rxjs8-preparation.md)**

### Domain-specific paths

| Context | Recommended path |
|---|---|
| Angular developer | Angular Standalone APIs → Signals ↔ RxJS Deep Interop → NgRx Effects → Enterprise Patterns |
| Real-time app | WebSocket Patterns → GraphQL Patterns → Data Synchronization → Multi-Tab Patterns |
| Node.js backend | Node.js Patterns → Async Iterable Advanced → Error Resilience Patterns |
| Testing focus | Marble Testing Advanced → Integration Testing → Debugging Guide |
| Performance work | Performance Patterns → Schedulers → observeOn/subscribeOn Advanced |

---

## What Makes This Different

### Anti-patterns are first-class content

Most docs show correct code. This library shows *why specific incorrect patterns fail* — the subtle invariant being violated, the failure mode it creates in production, and the corrected version:

```typescript
// ❌ INCORRECT — catching error outside exhaustMap locks it forever:
submit$.pipe(
  exhaustMap(data => this.api.save(data)),
  catchError(err => { showError(err); return EMPTY; })
  // After first error: EMPTY terminates the outer chain.
  // No further submit events will ever be processed.
)

// ✅ CORRECT — catch inside the inner Observable:
submit$.pipe(
  exhaustMap(data =>
    this.api.save(data).pipe(
      catchError(err => { showError(err); return EMPTY; })
      // EMPTY completes the inner Observable only.
      // exhaustMap's outer subscription stays alive.
    )
  )
)
// WHY: catchError on the outer pipe terminates the entire exhaustMap chain.
//      Catching inside the inner Observable terminates only that attempt,
//      leaving the outer subscription open for subsequent submit events.
```

### Marble diagrams for every behavioral variant

Not one generic diagram per operator — one diagram for each configuration that has meaningfully different behavior:

```
// throttleTime(300, { leading: true, trailing: false }) — default:
Source: --a-b-c-----------d-e--|
Result: --a-----------d--------|
           ^300ms window

// throttleTime(300, { leading: true, trailing: true }):
Source: --a-b-c-----------d-e--|
Result: --a-------c------d---e-|
                  ^ trailing value emitted at end of window
```

### Behavioral contracts, not descriptions

Each doc specifies the operator's contract in terms precise enough to reason about correctness:

> `first(predicate, defaultValue)` — emits the first value satisfying `predicate`, then completes. If the source completes without a matching value: emits `defaultValue` if provided; throws `EmptyError` if not. Never errors on the predicate itself — predicate exceptions propagate as errors.

### Advanced pattern docs extend fundamentals without repeating them

Each major operator has a companion `-advanced.md` that assumes the fundamentals and goes directly to production patterns: concurrent file processing with `mergeScan`, progressive search with accumulated results using `switchScan`, animation-frame-locked game loops with `animationFrames`, LRU-bounded deduplication for long-running streams.

### Educational metadata on every doc

```
Cognitive Load:   2/5  (one main caveat, otherwise predictable)
Usage Frequency:  4/5  (appears in most production Angular/React apps)
Composability:    5/5  (composes naturally with the entire operator set)
Teaching sequence: After debounceTime — introduce as the "suppress consecutive repeats" complement
```

---

## Running Locally

```bash
npm install
npm run docs:dev      # dev server at http://localhost:5173
npm run docs:build    # build static site to .vitepress/dist
npm run docs:preview  # preview the built site
```

Requirements: Node.js 18+

---

## Repository Structure

```
.
├── operators-claude/           # Operator documentation (183 pages)
│   ├── transformation/         # map, mergeMap, switchMap, groupBy, expand, buffer…
│   ├── filtering/              # filter, debounceTime, distinctUntilChanged, take…
│   ├── creation/               # of, from, ajax, webSocket, animationFrames…
│   ├── combination/            # combineLatest, forkJoin, merge, zip, race…
│   ├── error-handling/         # catchError, retry, timeout…
│   ├── subject/                # Subject, BehaviorSubject, ReplaySubject, AsyncSubject
│   ├── multicasting/           # share, shareReplay
│   ├── mathematical-aggregate/ # scan, reduce, toArray, min/max
│   ├── utility/                # tap, finalize, delay, schedulers, materialize…
│   ├── higher-order/           # mergeAll, concatAll, switchAll, exhaustAll
│   ├── rate-limiting/          # auditTime, sampleTime, throttle, debounce
│   ├── interop/                # async-iterable, ReadableStream
│   ├── testing/                # TestScheduler, debugging
│   ├── conditional/            # defaultIfEmpty, isEmpty, every, sequenceEqual
│   └── connectable/            # connectable, connect
│
├── docs/operator-trees/        # Reference guides & pattern libraries (82 pages)
│   ├── *-guide.md              # Decision guides
│   ├── rxjs-*-patterns.md      # Domain pattern libraries
│   └── *.md                    # Mental models, best practices, cookbooks
│
├── SKILL.md                    # The complete Eight-Policy Framework specification
└── .vitepress/config.mts       # Site config: nav, sidebar, theme
```

---

## Design Decisions

**Why VitePress?** Markdown-first, excellent code syntax highlighting, zero-config static output, and a sidebar structure that matches the way developers navigate reference documentation.

**Why separate `-advanced.md` files?** Core docs stay focused on the contract and fundamentals. Advanced docs assume that knowledge and go directly to production patterns without re-explaining the basics. A developer debugging a production WebSocket reconnection issue doesn't want to scroll past an explanation of what `webSocket()` is.

**Why the Eight-Policy Framework?** Consistency is the most underrated property of documentation. When every doc answers the same questions in the same order, readers build a mental index: they know exactly where to look for the behavioral contract, exactly where the pitfalls section is. The framework also makes gaps visible — if you can't fill out all eight policies for an operator, you don't fully understand it yet.

**Why 200+ anti-patterns?** The gap between "I understand this operator" and "I use this operator correctly in production" is almost always an anti-pattern. Most RxJS bugs in real codebases fall into a small set of recurring failure modes: memory leaks from unbounded `distinct()`, form submission locking from catching outside `exhaustMap`, parallel writes from using `switchMap` instead of `concatMap`. Making those failure modes explicit — with the *mechanism* of why they fail — is more valuable than another correct usage example.

---

## License

MIT — use freely, attribution appreciated.

---

<p align="center">
  <a href="https://www.anthropic.com/claude">
    <img src="https://www.anthropic.com/images/icons/claude-ai-icon.svg" alt="Claude AI" width="48" height="48" />
  </a>
  <br/>
  <em>Co-authored with <a href="https://www.anthropic.com/claude">Claude Sonnet 4.6</a> by Anthropic</em>
</p>

*Eight-Policy Framework specification: [SKILL.md](./SKILL.md)*  
*RxJS version compatibility: 7.x (RxJS 8 migration guide included)*
