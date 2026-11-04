---
layout: home

hero:
  name: RxJS Operators Documentation
  text: A formal reference for every RxJS operator
  tagline: "201 operator pages and 76 reference guides across 15 categories. Every operator page carries its signature, functional specification, marble diagrams, behavioral contract, runnable examples and the pitfalls that bite in production."
  actions:
    - theme: brand
      text: Browse Operators
      link: /operators-claude/transformation/map
    - theme: alt
      text: Reference Guides
      link: /docs/operator-trees/operator-selection-guide
    - theme: alt
      text: ← hansschenker.github.io
      link: https://hansschenker.github.io

features:
  - title: "Transformation"
    details: "37 pages — map, mapTo (deprecated), mergeMap, mergeMapTo (deprecated), switchMap …"
    link: /operators-claude/transformation/map
  - title: "Filtering"
    details: "27 pages — filter, debounceTime, distinctUntilChanged, take, takeUntil …"
    link: /operators-claude/filtering/filter
  - title: "Combination"
    details: "21 pages — combineLatest, startWith, forkJoin, merge, concat …"
    link: /operators-claude/combination/combineLatest-operator-documentation
  - title: "Creation"
    details: "34 pages — of, from, defer, interval, timer …"
    link: /operators-claude/creation/of
  - title: "Error Handling"
    details: "12 pages — catchError, retry, retryWhen (deprecated), timeout, timeoutWith (deprecated) …"
    link: /operators-claude/error-handling/catchError
  - title: "Multicasting"
    details: "4 pages — share, shareReplay"
    link: /operators-claude/multicasting/share
  - title: "Rate Limiting"
    details: "6 pages — auditTime / sampleTime, sample / audit, throttle / debounce"
    link: /operators-claude/rate-limiting/auditTime-sampleTime
  - title: "Higher-Order"
    details: "8 pages — mergeAll / concatAll / switchAll, exhaustAll / exhaustMap, combineLatestAll, zipAll"
    link: /operators-claude/higher-order/mergeAll-concatAll-switchAll
  - title: "Mathematical / Aggregate"
    details: "8 pages — scan, reduce, toArray / count, min / max"
    link: /operators-claude/mathematical-aggregate/scan
  - title: "Conditional"
    details: "6 pages — defaultIfEmpty / isEmpty / every / sequenceEqual, defaultIfEmpty, isEmpty, every, sequenceEqual"
    link: /operators-claude/conditional/conditional-operators
  - title: "Subject"
    details: "8 pages — Subject, BehaviorSubject, ReplaySubject, AsyncSubject"
    link: /operators-claude/subject/Subject
  - title: "Connectable Observable"
    details: "2 pages — connectable / connect"
    link: /operators-claude/connectable/connectable-connect
  - title: "Testing / Debugging"
    details: "4 pages — TestScheduler, Debugging Streams"
    link: /operators-claude/testing/TestScheduler
  - title: "Interop"
    details: "2 pages — Async Iterable / ReadableStream"
    link: /operators-claude/interop/async-iterable
  - title: "Utility"
    details: "22 pages — tap, finalize, delay / delayWhen, repeat, repeatWhen (deprecated) …"
    link: /operators-claude/utility/tap
  - title: "Reference Guides"
    details: "76 guides — decision guides, architecture, testing, framework integrations, domain patterns"
    link: /docs/operator-trees/operator-selection-guide
  - title: "Authoring Standard"
    details: "The eight-policy framework every operator page follows"
    link: /SKILL
---

## About This Documentation

This site is a complete, formal reference for the RxJS operator set, built with [VitePress](https://vitepress.dev) and written against the **eight-policy framework** defined in [SKILL.md](/SKILL). Where the official API reference tells you what an operator does, these pages tell you its full behavioral contract: when it subscribes, how it completes, what it does with errors, how it behaves under pressure, and which mistakes it invites.

| Metric | Count |
|---|---|
| Operator pages | **201** |
| Reference guides and pattern libraries | **76** |
| Operator categories | **15** |
| Documentation standard | Eight-Policy Framework v1.0 |
| RxJS version | 7.x (RxJS 8 migration guide included) |

### The eight policies

1. **Identity** — name, category, TypeScript signature, import path
2. **Functional Specification** — input and output types, transformation rules, invariants
3. **Marble Diagrams** — ASCII temporal visualisation of each meaningful behaviour variant
4. **Behavioral Characteristics** — subscription, completion, error propagation, backpressure, hot vs cold
5. **Type System Integration** — generics, type narrowing, compile-time guarantees
6. **Practical Examples** — at least three runnable examples, basic → common pattern → edge case
7. **Common Pitfalls** — anti-patterns with corrections in `❌ INCORRECT` / `✅ CORRECT` / `WHY:` form
8. **Related Operators** — same-category alternatives, complementary operators, decision tables

Pages marked **(advanced)** in the sidebar assume the core page and go straight to production patterns. Pages marked **(deprecated)** document operators that still ship in RxJS 7 but are removed in RxJS 8, with migration notes.

---

## Operators by Category

### Transformation (37 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [map](/operators-claude/transformation/map) | Synchronous projection operator (1-to-1 transformation) | [advanced](/operators-claude/transformation/map-advanced) |
| [mapTo (deprecated)](/operators-claude/transformation/mapTo) | Constant projection (1-to-1 transformation) — Deprecated in RxJS 7, removed in RxJS 8. Replaces every source value with the same fixed value |  |
| [mergeMap](/operators-claude/transformation/mergeMap) | Concurrent higher-order flattener — maps each value to an Observable and merges all active inner Observables | [advanced](/operators-claude/transformation/mergeMap-advanced) |
| [mergeMapTo (deprecated)](/operators-claude/transformation/mergeMapTo) | Concurrent higher-order flattener with a fixed inner Observable — ⚠️ **Deprecated since RxJS 7**, slated for removal in the next major release … |  |
| [switchMap](/operators-claude/transformation/switchMap) | Higher-order transformation operator (flattening strategy — cancelling) | [advanced](/operators-claude/transformation/switchMap-advanced) |
| [switchMapTo (deprecated)](/operators-claude/transformation/switchMapTo) | Cancelling higher-order flattener with a fixed inner Observable — ⚠️ **Deprecated since RxJS 7**, slated for removal in the next major release … |  |
| [concatMap](/operators-claude/transformation/concatMap) | Sequential higher-order flattener — queues inner Observables and subscribes one at a time, in source order | [advanced](/operators-claude/transformation/concatMap-advanced) |
| [concatMapTo (deprecated)](/operators-claude/transformation/concatMapTo) | Sequential higher-order flattener with a fixed inner Observable — ⚠️ **Deprecated since RxJS 7**, slated for removal in the next major release … |  |
| [exhaustMap](/operators-claude/transformation/exhaustMap) | Ignore-while-busy higher-order flattener — maps source emissions to inner Observables, silently dropping new emissions while an inner is active | [advanced](/operators-claude/transformation/exhaustMap-advanced) |
| [pairwise](/operators-claude/transformation/pairwise) | Sliding window of size 2 — emits `[previous, current]` on each emission after the first | [advanced](/operators-claude/transformation/pairwise-advanced) |
| [groupBy](/operators-claude/transformation/groupBy) | Partitioning — splits a flat stream into a stream of keyed inner Observables | [advanced](/operators-claude/transformation/groupBy-advanced) |
| [bufferTime](/operators-claude/transformation/bufferTime) | Time-window collector — accumulates source values into arrays over fixed time intervals | [advanced](/operators-claude/transformation/bufferTime-advanced) |
| [bufferCount](/operators-claude/transformation/bufferCount) | Count-based collector — accumulates source values into arrays of a fixed size |  |
| [buffer / window](/operators-claude/transformation/buffer-window) | Signal-driven buffering and windowing — the most flexible members of the buffer/window family. | [advanced](/operators-claude/transformation/buffer-window-advanced) |
| [bufferWhen / windowWhen](/operators-claude/transformation/bufferWhen-windowWhen) | Signal-driven buffer/window — collect emissions until a closing signal, then start fresh | [advanced](/operators-claude/transformation/bufferWhen-windowWhen-advanced) |
| [bufferToggle / windowToggle](/operators-claude/transformation/bufferToggle-windowToggle) | Open/close signal-driven buffer/window — collect emissions between an opening and closing signal | [advanced](/operators-claude/transformation/bufferToggle-windowToggle-advanced) |
| [windowCount](/operators-claude/transformation/windowCount) | Count-based window opener — emits inner `Observable<T>` windows of a fixed count |  |
| [switchScan](/operators-claude/transformation/switchScan) | Stateful `switchMap` — like `switchMap` but passes accumulated state to each projection, and new emissions cancel previous inner Observables | [advanced](/operators-claude/transformation/switchScan-advanced) |
| [mergeScan](/operators-claude/transformation/mergeScan) | Stateful `mergeMap` — accumulates state across concurrent inner Observables | [advanced](/operators-claude/transformation/mergeScan-advanced) |
| [expand](/operators-claude/transformation/expand) | Recursive projection — applies a function to each value and merges the results back into the stream, enabling tree traversal and recursive async … | [advanced](/operators-claude/transformation/expand-advanced) |
| [windowTime](/operators-claude/transformation/windowTime) | Time-window streaming — emits inner `Observable<T>` windows over fixed time intervals; the streaming counterpart to `bufferTime` | [advanced](/operators-claude/transformation/windowTime-advanced) |
| bufferCount / windowCount | Advanced patterns and production recipes | [advanced](/operators-claude/transformation/bufferCount-windowCount-advanced) |

### Filtering (27 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [filter](/operators-claude/filtering/filter) | Synchronous predicate filter (N-to-M transformation, M ≤ N) | [advanced](/operators-claude/filtering/filter-advanced) |
| [debounceTime](/operators-claude/filtering/debounceTime) | Time-based silence filter (emits after idle period) | [advanced](/operators-claude/filtering/debounceTime-advanced) |
| [distinctUntilChanged](/operators-claude/filtering/distinctUntilChanged) | Consecutive duplicate filter | [advanced](/operators-claude/filtering/distinctUntilChanged-advanced) |
| [take](/operators-claude/filtering/take) | Count-based completion — forwards the first N emissions from source, then completes |  |
| [takeUntil](/operators-claude/filtering/takeUntil) | Completion trigger — unsubscribes source when a notifier emits | [advanced](/operators-claude/filtering/takeUntil-advanced) |
| [throttleTime](/operators-claude/filtering/throttleTime) | Time-window rate limiter — forwards first emission of each time window, suppresses the rest | [advanced](/operators-claude/filtering/throttleTime-advanced) |
| [first / last](/operators-claude/filtering/first-last) | function first&lt;T, D = T&gt;( | [advanced](/operators-claude/filtering/first-last-advanced) |
| [single](/operators-claude/filtering/single) | Cardinality assertion — emits the one value (optionally matching a predicate) and errors if the count is not exactly one |  |
| [takeWhile](/operators-claude/filtering/takeWhile) | Predicate-driven completion — emits values while a condition holds, then completes | [advanced](/operators-claude/filtering/takeWhile-advanced) |
| [skipUntil / skipWhile](/operators-claude/filtering/skipUntil-skipWhile) | function skipUntil&lt;T&gt;(notifier: ObservableInput&lt;any&gt;): MonoTypeOperatorFunction&lt;T&gt; | [advanced](/operators-claude/filtering/skipUntil-skipWhile-advanced) |
| [distinctUntilKeyChanged](/operators-claude/filtering/distinctUntilKeyChanged) | Property-based duplicate suppressor — emits only when a specified key of the source object has changed from the previous emission | [advanced](/operators-claude/filtering/distinctUntilKeyChanged-advanced) |
| [find / findIndex](/operators-claude/filtering/find-findIndex) | function find&lt;T&gt;( | [advanced](/operators-claude/filtering/find-findIndex-advanced) |
| [distinct](/operators-claude/filtering/distinct) | Global uniqueness filter — emits values only if they have not been seen before in the entire stream | [advanced](/operators-claude/filtering/distinct-advanced) |
| [skip / takeLast / elementAt](/operators-claude/filtering/skip-takeLast-elementAt) | Three focused filtering operators for position-based value selection. |  |
| [skipLast](/operators-claude/filtering/skipLast) | Tail suppression — forwards every value except the final `skipCount`, delaying each emission by `skipCount` positions |  |
| take / skip | Advanced patterns and production recipes | [advanced](/operators-claude/filtering/take-skip-advanced) |

### Combination (21 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [combineLatest](/operators-claude/combination/combineLatest-operator-documentation) | Reactive combiner — emits a new array whenever any source emits, combining each source's latest value | [advanced](/operators-claude/combination/combineLatest-advanced) |
| [startWith](/operators-claude/combination/startWith) | Initial value prepender — emits one or more static values synchronously before forwarding source emissions | [advanced](/operators-claude/combination/startWith-advanced) |
| [forkJoin](/operators-claude/combination/forkJoin) | Parallel completion combinator — waits for all sources to complete, then emits their final values as a tuple or dictionary | [advanced](/operators-claude/combination/forkJoin-advanced) |
| [merge](/operators-claude/combination/merge) | Concurrent stream combinator — subscribes to all sources simultaneously and forwards each emission as it arrives | [advanced](/operators-claude/combination/merge-advanced) |
| [concat](/operators-claude/combination/concat) | Sequential stream combinator — subscribes to each source in order, only starting the next after the previous completes | [advanced](/operators-claude/combination/concat-advanced) |
| [withLatestFrom](/operators-claude/combination/withLatestFrom) | Trigger-driven snapshot — when source emits, samples the latest value from secondary Observables | [advanced](/operators-claude/combination/withLatestFrom-advanced) |
| [zip](/operators-claude/combination/zip) | Indexed pairing — combines values from multiple Observables by position (index), emitting a tuple when all sources have emitted their Nth value | [advanced](/operators-claude/combination/zip-advanced) |
| [race](/operators-claude/combination/race) | First-wins selector — subscribes to all sources, forwards emissions from whichever emits first, immediately unsubscribes from all others | [advanced](/operators-claude/combination/race-advanced) |
| [Pipeable Aliases](/operators-claude/combination/pipeable-aliases) | These are the **pipeable operator forms** of the corresponding creation functions. They exist so combination operators can be used inside a … |  |
| [combineLatestWith / mergeWith / concatWith](/operators-claude/combination/combineLatestWith-mergeWith) | RxJS 7 introduced **pipeable** forms of static combination operators. These are used inside `.pipe()` rather than as standalone functions, making … |  |
| [concatWith](/operators-claude/combination/concatWith) | Sequential append — after the source completes, subscribes to each additional source in turn, forwarding every emission in order without overlap |  |
| [zipWith](/operators-claude/combination/zipWith) | Index-aligned pairing — emits the Nth tuple only when the source and every other input have each produced their Nth value |  |
| [raceWith](/operators-claude/combination/raceWith) | First-wins selector — subscribes to the source and every contender at once, mirrors whichever delivers the first notification, and unsubscribes … |  |

### Creation (34 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [of](/operators-claude/creation/of) | Synchronous static emitter — emits each argument in order then completes |  |
| [from](/operators-claude/creation/from) | Universal Observable converter — wraps arrays, Promises, iterables, async iterables, and Observable-like objects into an Observable | [advanced](/operators-claude/creation/from-advanced) |
| [defer](/operators-claude/creation/defer) | Lazy Observable factory — creates a new Observable for each subscriber by calling a factory function at subscription time | [advanced](/operators-claude/creation/defer-advanced) |
| [interval](/operators-claude/creation/interval) | Time-based sequence generator — emits sequential integers at a fixed period |  |
| [timer](/operators-claude/creation/timer) | Delayed one-shot or repeating sequence — emits after an initial delay, optionally continuing at a fixed period |  |
| [fromEvent](/operators-claude/creation/fromEvent) | DOM/Node event adapter — wraps an event target's event listener in an Observable | [advanced](/operators-claude/creation/fromEvent-advanced) |
| [EMPTY / NEVER](/operators-claude/creation/EMPTY-NEVER) | const EMPTY: Observable&lt;never&gt; // singleton, pre-created | [advanced](/operators-claude/creation/EMPTY-NEVER-advanced) |
| [partition](/operators-claude/creation/partition) | Binary splitter — splits one Observable into two based on a predicate, returning a tuple `[matching$, nonMatching$]` |  |
| [iif](/operators-claude/creation/iif) | Lazy conditional Observable selection — picks between two Observables based on a condition evaluated at subscription time |  |
| [throwError](/operators-claude/creation/throwError) | Error factory — creates an Observable that immediately errors with a specified value on subscription |  |
| [range](/operators-claude/creation/range) | Synchronous integer sequence — emits a contiguous range of integers |  |
| [generate](/operators-claude/creation/generate) | Loop-based sequence generator — creates an Observable from a for-loop definition |  |
| [scheduled](/operators-claude/creation/scheduled) | Scheduler-injected creation — wraps any creation source with a specific scheduler | [advanced](/operators-claude/creation/scheduled-advanced) |
| [bindCallback / bindNodeCallback](/operators-claude/creation/bindCallback-bindNodeCallback) | Wrap Node.js-style callback APIs as Observable factories. | [advanced](/operators-claude/creation/bindCallback-bindNodeCallback-advanced) |
| [fromEventPattern](/operators-claude/creation/fromEventPattern) | Custom event bridge — wraps any add/remove listener API as a cold, cancellable Observable | [advanced](/operators-claude/creation/fromEventPattern-advanced) |
| [using](/operators-claude/creation/using) | Resource factory — creates a disposable resource tied to an Observable's lifetime |  |
| [ajax](/operators-claude/creation/ajax) | HTTP client — creates an Observable that sends an HTTP request on subscription and emits the response | [advanced](/operators-claude/creation/ajax-advanced) |
| [fromFetch](/operators-claude/creation/fromFetch) | Fetch wrapper — wraps the native `fetch` API as a cold, cancellable Observable | [advanced](/operators-claude/creation/fromFetch-advanced) |
| [webSocket](/operators-claude/creation/webSocket) | WebSocket adapter — creates a bidirectional Observable/Subject over a WebSocket connection | [advanced](/operators-claude/creation/webSocket-advanced) |
| [animationFrames](/operators-claude/creation/animationFrames) | Animation frame stream — emits on every `requestAnimationFrame` tick with elapsed time | [advanced](/operators-claude/creation/animationFrames-advanced) |
| interval / timer | Advanced patterns and production recipes | [advanced](/operators-claude/creation/interval-timer-advanced) |
| partition / iif | Advanced patterns and production recipes | [advanced](/operators-claude/creation/partition-iif-advanced) |
| generate / using | Advanced patterns and production recipes | [advanced](/operators-claude/creation/generate-using-advanced) |

### Error Handling (12 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [catchError](/operators-claude/error-handling/catchError) | Error recovery — intercepts errors and returns a replacement Observable | [advanced](/operators-claude/error-handling/catchError-advanced) |
| [retry](/operators-claude/error-handling/retry) | Automatic resubscription on error — transparently re-executes the source Observable on failure | [advanced](/operators-claude/error-handling/retry-advanced) |
| [retryWhen (deprecated)](/operators-claude/error-handling/retryWhen) | Notifier-driven resubscription on error — Deprecated in RxJS 7, removed in RxJS 8. Hands the stream of source errors to a user function whose … |  |
| [timeout](/operators-claude/error-handling/timeout) | Time-bound guard — errors if no value (or completion) arrives within a specified window | [advanced](/operators-claude/error-handling/timeout-advanced) |
| [timeoutWith (deprecated)](/operators-claude/error-handling/timeoutWith) | Time-bound fallback switch — Deprecated in RxJS 7, removed in RxJS 8. Mirrors the source until a time limit is exceeded, then switches to a … |  |
| [onErrorResumeNext](/operators-claude/error-handling/onErrorResumeNext) | Error-ignoring sequencer — continues to the next source when any source errors or completes | [advanced](/operators-claude/error-handling/onErrorResumeNext-advanced) |
| [onErrorResumeNextWith](/operators-claude/error-handling/onErrorResumeNextWith) | Error-ignoring sequencer — the pipeable form of `onErrorResumeNext`; subscribes to the source and then to each further source in order, treating … |  |
| [throwIfEmpty](/operators-claude/error-handling/throwIfEmpty) | Empty-stream guard — forwards all values unchanged, but converts an empty completion into an error |  |

### Multicasting (4 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [share](/operators-claude/multicasting/share) | Reference-counted multicast — converts a cold Observable to hot, sharing one execution among all subscribers; tears down when all subscribers … | [advanced](/operators-claude/multicasting/share-advanced) |
| [shareReplay](/operators-claude/multicasting/shareReplay) | Multicast with replay buffer — shares one source subscription among all subscribers and replays buffered emissions to late subscribers | [advanced](/operators-claude/multicasting/shareReplay-advanced) |

### Rate Limiting (6 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [auditTime / sampleTime](/operators-claude/rate-limiting/auditTime-sampleTime) | function auditTime&lt;T&gt;( | [advanced](/operators-claude/rate-limiting/auditTime-sampleTime-advanced) |
| [sample / audit](/operators-claude/rate-limiting/sample-audit) | Two Observable-driven rate-limiting operators: `sample` (clock-based snapshot) and `audit` (trailing-edge with reactive trigger). | [advanced](/operators-claude/rate-limiting/sample-audit-advanced) |
| [throttle / debounce](/operators-claude/rate-limiting/throttle-debounce) | Observable-driven counterparts to `throttleTime` and `debounceTime`. Where the `*Time` variants accept a fixed millisecond value, these accept a … | [advanced](/operators-claude/rate-limiting/throttle-debounce-advanced) |

### Higher-Order (8 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [mergeAll / concatAll / switchAll](/operators-claude/higher-order/mergeAll-concatAll-switchAll) | These three operators flatten an `Observable<Observable<T>>` (a higher-order Observable) into a plain `Observable<T>`. They are the `*All` … | [advanced](/operators-claude/higher-order/mergeAll-concatAll-switchAll-advanced) |
| [exhaustAll / exhaustMap](/operators-claude/higher-order/exhaustAll) | Ignore-while-busy flattening — ignores new inner Observables while one is already active | [advanced](/operators-claude/higher-order/exhaustAll-advanced) |
| [combineLatestAll](/operators-claude/higher-order/combineLatestAll) | Higher-order flattener — collects all inner Observables, then combines their latest values reactively | [advanced](/operators-claude/higher-order/combineLatestAll-advanced) |
| [zipAll](/operators-claude/higher-order/zipAll) | Higher-order zip — collects all inner Observables, then combines their emissions by index | [advanced](/operators-claude/higher-order/zipAll-advanced) |

### Mathematical / Aggregate (8 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [scan](/operators-claude/mathematical-aggregate/scan) | Running accumulator — emits each intermediate accumulated value (streaming `Array.reduce`) | [advanced](/operators-claude/mathematical-aggregate/scan-advanced) |
| [reduce](/operators-claude/mathematical-aggregate/reduce) | Terminal accumulator — accumulates all source values into a single result, emits once on source completion | [advanced](/operators-claude/mathematical-aggregate/reduce-advanced) |
| [toArray / count](/operators-claude/mathematical-aggregate/toArray-count) | function toArray&lt;T&gt;(): OperatorFunction&lt;T, T[]&gt; | [advanced](/operators-claude/mathematical-aggregate/toArray-count-advanced) |
| [min / max](/operators-claude/mathematical-aggregate/min-max) | function min&lt;T&gt;(comparer?: (x: T, y: T) =&gt; number): OperatorFunction&lt;T, T&gt; | [advanced](/operators-claude/mathematical-aggregate/min-max-advanced) |

### Conditional (6 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [defaultIfEmpty / isEmpty / every / sequenceEqual](/operators-claude/conditional/conditional-operators) | If the source Observable completes without emitting any `next` value, `defaultIfEmpty` emits `defaultValue` and then completes. If the source … |  |
| [defaultIfEmpty](/operators-claude/conditional/defaultIfEmpty) | Empty-stream fallback — emits a single default value only when the source completes without emitting |  |
| [isEmpty](/operators-claude/conditional/isEmpty) | Emptiness test — emits one boolean stating whether the source completed without ever emitting |  |
| [every](/operators-claude/conditional/every) | Universal-quantifier reduction — emits one boolean stating whether all source values satisfy a predicate |  |
| [sequenceEqual](/operators-claude/conditional/sequenceEqual) | Two-stream equality test — emits one boolean stating whether the source and a comparison stream emit the same values in the same order |  |
| Conditional Operators | Advanced patterns and production recipes | [advanced](/operators-claude/conditional/conditional-operators-advanced) |

### Subject (8 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [Subject](/operators-claude/subject/Subject) | Multicast bridge — simultaneously an Observable (subscribe) and an Observer (next/error/complete) | [advanced](/operators-claude/subject/Subject-advanced) |
| [BehaviorSubject](/operators-claude/subject/BehaviorSubject) | Stateful multicast Observable — always holds a current value and replays it to each new subscriber | [advanced](/operators-claude/subject/BehaviorSubject-advanced) |
| [ReplaySubject](/operators-claude/subject/ReplaySubject) | Buffered multicast Subject — replays the last `bufferSize` emissions to each new subscriber | [advanced](/operators-claude/subject/ReplaySubject-advanced) |
| [AsyncSubject](/operators-claude/subject/AsyncSubject) | Completion-gated emitter — emits only the final value, and only when the source completes | [advanced](/operators-claude/subject/AsyncSubject-advanced) |

### Connectable Observable (2 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [connectable / connect](/operators-claude/connectable/connectable-connect) | The modern RxJS 7 API for controlled multicasting — replacing the deprecated `publish()` and `multicast()`. | [advanced](/operators-claude/connectable/connectable-connect-advanced) |

### Testing / Debugging (4 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [TestScheduler](/operators-claude/testing/TestScheduler) | Virtual time scheduler with marble syntax — enables deterministic, synchronous testing of time-based Observables | [advanced](/operators-claude/testing/TestScheduler-advanced) |
| [Debugging Streams](/operators-claude/testing/debugging-operators) | A practical guide to understanding what's happening in an Observable pipeline. | [advanced](/operators-claude/testing/debugging-operators-advanced) |

### Interop (2 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [Async Iterable / ReadableStream](/operators-claude/interop/async-iterable) | RxJS 7 supports `AsyncIterable` natively as an input to `from()`. This bridges the gap between the two major async abstractions in modern JavaScript: |  |
| Async Iterable | Advanced patterns and production recipes | [advanced](/operators-claude/interop/async-iterable-advanced) |

### Utility (22 pages)

| Operator | What it does | Deep dive |
|---|---|---|
| [tap](/operators-claude/utility/tap) | Transparent side-effect observer (pass-through) | [advanced](/operators-claude/utility/tap-advanced) |
| [finalize](/operators-claude/utility/finalize) | Teardown side-effect runner — executes a callback when the Observable terminates for any reason |  |
| [delay / delayWhen](/operators-claude/utility/delay-delayWhen) | function delay&lt;T&gt;( | [advanced](/operators-claude/utility/delay-delayWhen-advanced) |
| [repeat](/operators-claude/utility/repeat) | Completion-driven resubscription — resubscribes to the source when it completes, creating a repeating sequence | [advanced](/operators-claude/utility/repeat-advanced) |
| [repeatWhen (deprecated)](/operators-claude/utility/repeatWhen) | Notifier-driven resubscription on completion — Deprecated in RxJS 7, removed in RxJS 8. Hands a stream of source completions to a user function … |  |
| [materialize / dematerialize](/operators-claude/utility/materialize-dematerialize) | function materialize&lt;T&gt;(): OperatorFunction&lt;T, Notification&lt;T&gt;&gt; | [advanced](/operators-claude/utility/materialize-dematerialize-advanced) |
| [observeOn / subscribeOn](/operators-claude/utility/observeOn-subscribeOn) | Two scheduler-control operators that determine WHERE work executes in RxJS. | [advanced](/operators-claude/utility/observeOn-subscribeOn-advanced) |
| [timeInterval / timestamp](/operators-claude/utility/timeInterval-timestamp) | Two utility operators that annotate stream emissions with timing metadata. | [advanced](/operators-claude/utility/timeInterval-timestamp-advanced) |
| [endWith / ignoreElements](/operators-claude/utility/endWith-ignoreElements) | Emits all source values, then emits the provided `values` in order, then completes. The complement of `startWith`. | [advanced](/operators-claude/utility/endWith-ignoreElements-advanced) |
| [firstValueFrom / lastValueFrom](/operators-claude/utility/firstValueFrom-lastValueFrom) | Observable → Promise converters — bridge between reactive and async/await code | [advanced](/operators-claude/utility/firstValueFrom-lastValueFrom-advanced) |
| [Schedulers](/operators-claude/utility/schedulers) | Execution context controllers — determine when work is dispatched | [advanced](/operators-claude/utility/schedulers-advanced) |
| [Deprecated Operators](/operators-claude/utility/deprecated-operators) | These operators are deprecated in RxJS 7 and will be removed in RxJS 8. Each entry explains the reason and the correct modern replacement. |  |
| tap / finalize | Advanced patterns and production recipes | [advanced](/operators-claude/utility/tap-finalize-advanced) |

---

## Reference Guides

### Decision guides — start here when choosing

[Flattening Strategy Guide](/docs/operator-trees/flattening-strategy-guide) ·
[Operator Selection Guide](/docs/operator-trees/operator-selection-guide) ·
[Subject Decision Guide](/docs/operator-trees/subject-decision-guide) ·
[Rate-Limiting Guide](/docs/operator-trees/rate-limiting-guide) ·
[Error Handling Patterns](/docs/operator-trees/error-handling-patterns) ·
[Multicasting Guide](/docs/operator-trees/multicasting-guide) ·
[Higher-Order Observables](/docs/operator-trees/higher-order-observables-guide) ·
[RxJS Mental Models](/docs/operator-trees/rxjs-mental-models) ·
[Cold vs Hot Observables](/docs/operator-trees/cold-vs-hot-guide) ·
[RxJS Best Practices](/docs/operator-trees/rxjs-best-practices) ·
[RxJS Cookbook](/docs/operator-trees/rxjs-cookbook) ·
[Error Handling Cookbook](/docs/operator-trees/error-handling-cookbook)

### Architecture, state and testing

[Pipeline Architecture](/docs/operator-trees/pipeline-architecture-guide) ·
[State Management](/docs/operator-trees/state-management-patterns) ·
[Store from Scratch](/docs/operator-trees/rxjs-store-from-scratch) ·
[State Machines](/docs/operator-trees/rxjs-state-machines) ·
[Concurrency Patterns](/docs/operator-trees/concurrency-guide) ·
[Operator Composition Patterns](/docs/operator-trees/rxjs-operator-composition-patterns) ·
[Custom Operators Guide](/docs/operator-trees/custom-operators-guide) ·
[Custom Operators (Advanced)](/docs/operator-trees/rxjs-custom-operators-advanced) ·
[Subscription Management](/docs/operator-trees/subscription-management-guide) ·
[NgRx Effects (Advanced)](/docs/operator-trees/rxjs-effect-patterns-advanced) ·
[Enterprise Patterns](/docs/operator-trees/rxjs-enterprise-patterns) ·
[Microservices Patterns](/docs/operator-trees/rxjs-microservices-patterns) ·
[Testing Patterns](/docs/operator-trees/testing-patterns-guide) ·
[Marble Testing (Advanced)](/docs/operator-trees/rxjs-marble-testing-advanced) ·
[Marble Testing Patterns](/docs/operator-trees/rxjs-testing-marble-patterns) ·
[Integration Testing](/docs/operator-trees/rxjs-testing-integration) ·
[Debugging Guide](/docs/operator-trees/debugging-guide) ·
[DevTools & Extensions](/docs/operator-trees/rxjs-devtools-extensions) ·
[Performance Patterns](/docs/operator-trees/performance-patterns-guide) ·
[Performance Profiling](/docs/operator-trees/rxjs-performance-profiling)

### Framework integrations

[Angular + RxJS Patterns](/docs/operator-trees/angular-rxjs-patterns) ·
[Angular Standalone APIs](/docs/operator-trees/rxjs-angular-standalone) ·
[Zoneless Angular](/docs/operator-trees/rxjs-zoneless-angular) ·
[Angular Signals + RxJS](/docs/operator-trees/angular-signals-rxjs) ·
[Signals ↔ RxJS Deep Interop](/docs/operator-trees/rxjs-signals-interop-deep) ·
[NgRx Effects Patterns](/docs/operator-trees/ngrx-effects-patterns) ·
[NgRx ComponentStore](/docs/operator-trees/rxjs-ngrx-component-store) ·
[Reactive Forms Advanced](/docs/operator-trees/rxjs-reactive-forms-advanced) ·
[RxJS in React](/docs/operator-trees/rxjs-react-patterns) ·
[RxJS in Vue](/docs/operator-trees/rxjs-vue-patterns) ·
[RxJS in Svelte](/docs/operator-trees/rxjs-svelte-patterns) ·
[Node.js Patterns](/docs/operator-trees/nodejs-rxjs-patterns) ·
[Server-Side Rendering (SSR)](/docs/operator-trees/rxjs-server-side-rendering) ·
[TypeScript + RxJS Guide](/docs/operator-trees/typescript-rxjs-guide) ·
[TypeScript Type Guards](/docs/operator-trees/rxjs-typescript-type-guards)

### Real-world domain patterns

[WebSocket Patterns](/docs/operator-trees/rxjs-websocket-patterns) ·
[GraphQL Patterns](/docs/operator-trees/rxjs-graphql-patterns) ·
[HTTP Interceptor Patterns](/docs/operator-trees/rxjs-http-interceptor-patterns) ·
[Authentication Patterns](/docs/operator-trees/rxjs-authentication-patterns) ·
[Caching Patterns](/docs/operator-trees/rxjs-caching-patterns) ·
[Real-Time Data Guide](/docs/operator-trees/realtime-data-guide) ·
[Polling Patterns](/docs/operator-trees/rxjs-polling-patterns) ·
[Data Synchronization](/docs/operator-trees/rxjs-data-synchronization-patterns) ·
[Multi-Tab Patterns](/docs/operator-trees/rxjs-multi-tab-patterns) ·
[File Upload Patterns](/docs/operator-trees/rxjs-file-upload-patterns) ·
[Form Validation Guide](/docs/operator-trees/form-validation-guide) ·
[Optimistic UI Patterns](/docs/operator-trees/rxjs-optimistic-ui-patterns) ·
[Undo/Redo Patterns](/docs/operator-trees/rxjs-undo-redo-patterns) ·
[Notification & Toast Patterns](/docs/operator-trees/rxjs-notification-toast-patterns) ·
[Error Boundary Patterns](/docs/operator-trees/rxjs-error-boundary-patterns) ·
[Error Resilience Patterns](/docs/operator-trees/rxjs-error-resilience-patterns)

### Browser and platform APIs

[Web Worker Patterns](/docs/operator-trees/rxjs-web-worker-patterns) ·
[Service Worker Patterns](/docs/operator-trees/rxjs-service-worker-patterns) ·
[IndexedDB Patterns](/docs/operator-trees/rxjs-indexeddb-patterns) ·
[Accessibility Patterns](/docs/operator-trees/rxjs-accessibility-patterns) ·
[Charts & Visualization](/docs/operator-trees/rxjs-charts-visualization) ·
[Drag, Drop & Animation](/docs/operator-trees/rxjs-drag-drop-animation) ·
[Virtual Scroll](/docs/operator-trees/rxjs-virtual-scroll) ·
[Infinite Scroll & Pagination](/docs/operator-trees/rxjs-infinite-scroll-pagination) ·
[Search & Autocomplete](/docs/operator-trees/rxjs-search-autocomplete-patterns) ·
[Micro-Frontend Patterns](/docs/operator-trees/rxjs-micro-frontend-patterns)

### Migration and interop

[RxJS 8 Preparation](/docs/operator-trees/rxjs-rxjs8-preparation) ·
[RxJS Migration Guide](/docs/operator-trees/rxjs-migration-guide) ·
[Promise Interop](/docs/operator-trees/rxjs-promise-interop)

---

## Learning Paths

### Beginner — new to RxJS

1. [RxJS Mental Models](/docs/operator-trees/rxjs-mental-models) — build the conceptual foundation first
2. [`of`](/operators-claude/creation/of), [`from`](/operators-claude/creation/from), `interval / timer`, [`fromEvent`](/operators-claude/creation/fromEvent) — the creation operators you will use every day
3. [`map`](/operators-claude/transformation/map), [`filter`](/operators-claude/filtering/filter), [`tap`](/operators-claude/utility/tap) — basic transformation
4. [Cold vs Hot Observables](/docs/operator-trees/cold-vs-hot-guide) — the most important concept to internalise early
5. [Subscription Management](/docs/operator-trees/subscription-management-guide) — avoid leaks from day one
6. [`debounceTime`](/operators-claude/filtering/debounceTime), [`distinctUntilChanged`](/operators-claude/filtering/distinctUntilChanged) — the two filtering operators you will use most
7. [`catchError`](/operators-claude/error-handling/catchError), [`retry`](/operators-claude/error-handling/retry) — error handling basics
8. [RxJS Cookbook](/docs/operator-trees/rxjs-cookbook) — common recipes ready to use

### Intermediate — using RxJS daily

1. [Flattening Strategy Guide](/docs/operator-trees/flattening-strategy-guide) — the most consequential RxJS decision
2. [`switchMap`](/operators-claude/transformation/switchMap), [`concatMap`](/operators-claude/transformation/concatMap), [`mergeMap`](/operators-claude/transformation/mergeMap), [`exhaustMap`](/operators-claude/transformation/exhaustMap) and their advanced companions
3. [Multicasting Guide](/docs/operator-trees/multicasting-guide) — `share`, `shareReplay`, refCount semantics
4. [Subject Decision Guide](/docs/operator-trees/subject-decision-guide)
5. [`scan`](/operators-claude/mathematical-aggregate/scan) — state accumulation without external variables
6. [Error Handling Patterns](/docs/operator-trees/error-handling-patterns) — exponential backoff, circuit breakers
7. [Custom Operators Guide](/docs/operator-trees/custom-operators-guide)
8. [Marble Testing (Advanced)](/docs/operator-trees/rxjs-marble-testing-advanced)

### Advanced — designing systems on RxJS

1. [Pipeline Architecture](/docs/operator-trees/pipeline-architecture-guide)
2. [Concurrency Patterns](/docs/operator-trees/concurrency-guide)
3. [State Machines](/docs/operator-trees/rxjs-state-machines)
4. [Custom Operators (Advanced)](/docs/operator-trees/rxjs-custom-operators-advanced)
5. [Performance Patterns](/docs/operator-trees/performance-patterns-guide)
6. [TypeScript + RxJS Guide](/docs/operator-trees/typescript-rxjs-guide)
7. [Integration Testing](/docs/operator-trees/rxjs-testing-integration)
8. [RxJS 8 Preparation](/docs/operator-trees/rxjs-rxjs8-preparation)

### Domain-specific paths

| Context | Recommended path |
|---|---|
| Angular developer | [Angular Standalone APIs](/docs/operator-trees/rxjs-angular-standalone) → [Signals ↔ RxJS Deep Interop](/docs/operator-trees/rxjs-signals-interop-deep) → [NgRx Effects Patterns](/docs/operator-trees/ngrx-effects-patterns) → [Enterprise Patterns](/docs/operator-trees/rxjs-enterprise-patterns) |
| Real-time app | [WebSocket Patterns](/docs/operator-trees/rxjs-websocket-patterns) → [GraphQL Patterns](/docs/operator-trees/rxjs-graphql-patterns) → [Data Synchronization](/docs/operator-trees/rxjs-data-synchronization-patterns) → [Multi-Tab Patterns](/docs/operator-trees/rxjs-multi-tab-patterns) |
| Node.js backend | [Node.js Patterns](/docs/operator-trees/nodejs-rxjs-patterns) → [Error Resilience Patterns](/docs/operator-trees/rxjs-error-resilience-patterns) |
| Testing focus | [Marble Testing (Advanced)](/docs/operator-trees/rxjs-marble-testing-advanced) → [Integration Testing](/docs/operator-trees/rxjs-testing-integration) → [Debugging Guide](/docs/operator-trees/debugging-guide) |
| Performance work | [Performance Patterns](/docs/operator-trees/performance-patterns-guide) → [Performance Profiling](/docs/operator-trees/rxjs-performance-profiling) |

---

## Documentation Conventions

**Marble diagrams**

```
Source:   --a--b--c--|
          operator(config)
Result:   --A--B--C--|

-   one unit of time        a,b,c   emitted values
|   successful completion   #       error
^   subscription point     (ab)    synchronous emissions
```

**Educational metadata** — every core page ends with a strategy classification:

- **Cognitive Load** 1–5: how much mental overhead the operator demands (1 = trivial, 5 = expert)
- **Usage Frequency** 1–5: how often it appears in production code (1 = rare, 5 = essential)
- **Composability** 1–5: how naturally it combines with the rest of the operator set
- **Teaching sequence**: prerequisites, what it unlocks, and what it is commonly learned with

---

## Contributing

New operator pages follow the template in [SKILL.md](/SKILL), live in `operators-claude/<category>/<operatorName>.md`, and need a sidebar entry in `.vitepress/config.mts`. Every page must cover all eight policies, include at least three runnable examples with expected output, and document at least two pitfalls. Source: [github.com/hansschenker/rxjs-operators-documentation](https://github.com/hansschenker/rxjs-operators-documentation).

## Related Resources

- **RxJS official documentation**: [rxjs.dev](https://rxjs.dev)
- **ReactiveX**: [reactivex.io](http://reactivex.io)
- **RxJS source**: [github.com/ReactiveX/rxjs](https://github.com/ReactiveX/rxjs)
- **Learn RxJS**: [learnrxjs.io](https://www.learnrxjs.io)
- **RxJS Marbles**: [rxmarbles.com](https://rxmarbles.com)

---

**Last updated**: November 2026 · **Documentation standard**: Eight-Policy Framework v1.0 · **RxJS**: 7.x
