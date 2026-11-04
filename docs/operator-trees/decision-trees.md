# Decision Trees

The Operator Trees give every RxJS behavior coordinates: a root behavior plus one variant per axis. This page turns that into a procedure. Start from a requirement, answer the axis questions in order, and the coordinates name the operator. It reads the [Value-based](./value-tree.md) and [Time-based](./time-tree.md) trees from the requirement side; the notation is on the [Legend](./legend.md).

The [Operator Selection Guide](./operator-selection-guide.md) answers the same question by use case ("search box", "auto-save", "polling"). Use it when you recognise your situation; use these trees when you do not. The trees can also be walked interactively at [netxpert.ch/rxjs-operator-trees](https://netxpert.ch/rxjs-operator-trees/) (the concrete-behavior selector, and the reverse lookup under `#/op/<name>`).

---

## Start here

Three questions locate the family. Everything after that is choosing variants.

```text
How many sources are involved?
│
├── ONE source
│   │
│   ├── Do I want fewer, different or summarised values?
│   │       → Fewer, different, or summarised values
│   │
│   ├── Do values arrive faster than I can use them?
│   │       → Values arrive too fast
│   │
│   ├── Does each value start asynchronous work?
│   │       → Each value starts asynchronous work
│   │
│   ├── Is the problem how the stream ENDS (error, completion, silence)?
│   │       → When the stream ends badly
│   │
│   └── Do many subscribers share one expensive execution?
│           → Many subscribers, one expensive source
│
├── SEVERAL sources
│       → Several streams
│
└── NO source yet
        → Where does the Observable come from
```

If a requirement touches two branches (a search box filters values *and* starts work per value), each branch contributes one operator to the pipe. The worked examples at the end show that composition.

---

## Fewer, different, or summarised values

The value-based families with a single source: Selection, Transformation, Accumulation, Interrogation.

```text
What should the output be?
│
├── Some of the source values, unchanged            → SELECTION
│   │
│   ├── 1. Criterion: what decides whether a value survives?
│   │   ├── a count of values                 take / skip / takeLast / skipLast
│   │   ├── a predicate on the value          filter / takeWhile / skipWhile
│   │   ├── uniqueness                        distinct / distinctUntilChanged
│   │   ├── an index                          elementAt
│   │   └── another Observable                takeUntil / skipUntil
│   │
│   ├── 2. Polarity: keep what matches, or drop it?
│   │
│   ├── 3. Anchor: front of the sequence, back, or everywhere?
│   │       front  → may complete early        back → must wait for completion
│   │
│   └── 4. Empty policy: if nothing was selected…
│       ├── …that is normal                   complete silently   take(1)
│       ├── …use a fallback                   default             first(p, d) / defaultIfEmpty(d)
│       └── …that is a bug                    error               first() / throwIfEmpty()
│
├── One output per input, computed from it         → TRANSFORMATION
│       map(project)   (mapTo / pluck are map presets, deprecated)
│
├── A running or final summary of all values        → ACCUMULATION
│   ├── Cadence: every value, or only at completion?
│   │       every          scan          at completion   reduce
│   ├── Seed: explicit, or the first value?
│   └── Function: free, or a preset?
│           toArray · count · max · min
│
└── A single verdict about the sequence            → INTERROGATION
    ├── do all match?        every        ├── is it empty?       isEmpty
    ├── does any match?      find         ├── equal to another?  sequenceEqual
    └── exactly one match?   single       (answered as early as logic allows)
```

The empty-policy question is the one most often skipped. `take(1)`, `first()` and `find(p)` select the same value and differ only there: silent, error, `undefined`. Decide what "nothing" means before choosing.

Leaves:
[take](/operators-claude/filtering/take) ·
[skip / takeLast / elementAt](/operators-claude/filtering/skip-takeLast-elementAt) ·
[skipLast](/operators-claude/filtering/skipLast) ·
[filter](/operators-claude/filtering/filter) ·
[takeWhile](/operators-claude/filtering/takeWhile) ·
[skipUntil / skipWhile](/operators-claude/filtering/skipUntil-skipWhile) ·
[takeUntil](/operators-claude/filtering/takeUntil) ·
[distinct](/operators-claude/filtering/distinct) ·
[distinctUntilChanged](/operators-claude/filtering/distinctUntilChanged) ·
[first / last](/operators-claude/filtering/first-last) ·
[defaultIfEmpty](/operators-claude/conditional/defaultIfEmpty) ·
[throwIfEmpty](/operators-claude/error-handling/throwIfEmpty) ·
[map](/operators-claude/transformation/map) ·
[scan](/operators-claude/mathematical-aggregate/scan) ·
[reduce](/operators-claude/mathematical-aggregate/reduce) ·
[toArray / count](/operators-claude/mathematical-aggregate/toArray-count) ·
[min / max](/operators-claude/mathematical-aggregate/min-max) ·
[every](/operators-claude/conditional/every) ·
[find / findIndex](/operators-claude/filtering/find-findIndex) ·
[isEmpty](/operators-claude/conditional/isEmpty) ·
[sequenceEqual](/operators-claude/conditional/sequenceEqual) ·
[single](/operators-claude/filtering/single)

---

## Values arrive too fast

Apply the fire-hose test: **under sustained input, what should the output look like?** The answer picks the family; the suffix (`...Time` or not) only picks fixed versus dynamic duration.

```text
Under a fire-hose, the output should be…
│
├── a bounded steady drip, one value per window      → RATE-LIMITING (lossy)
│   │
│   ├── Who opens the window?
│   │   ├── a source value opens a suppression window         THROTTLE
│   │   │     Edge: emit at the opening edge   v------       leading (default)
│   │   │           at the closing edge        ------v       trailing
│   │   │           both                       v------v
│   │   ├── a source value opens a window; emit the LATEST
│   │   │     value when it closes             ------v       AUDIT
│   │   └── an independent clock or notifier takes a
│   │         snapshot of the latest NEW value ---|---|       SAMPLE
│   │
│   └── Duration: fixed → throttleTime / auditTime / sampleTime
│                 dynamic → throttle(fn) / audit(fn) / sample(notifier$)
│
├── nothing until the input pauses, then the latest    → DEBOUNCE   (timing)
│       Silence: fixed → debounceTime(ms)   dynamic → debounce(fn)
│
├── the same fire-hose, only later                     → DELAY      (timing)
│       Displacement: fixed → delay(ms | date)   per value → delayWhen(fn)
│
└── every value, batched per window (lossless)         → GROUPING by duration
        bufferTime(span) / windowTime(span)
        auditTime(t)  ≈  bufferTime(t) + last value of each batch
```

Two consequences worth remembering: the rate limiters and `debounce` are the overlap policy applied to a pending value (`throttle` drops the newcomer, `debounce` kills the previous), and `debounce` emits nothing at all while the fire-hose lasts.

Leaves and background:
[throttleTime](/operators-claude/filtering/throttleTime) ·
[throttle / debounce](/operators-claude/rate-limiting/throttle-debounce) ·
[auditTime / sampleTime](/operators-claude/rate-limiting/auditTime-sampleTime) ·
[sample / audit](/operators-claude/rate-limiting/sample-audit) ·
[debounceTime](/operators-claude/filtering/debounceTime) ·
[delay / delayWhen](/operators-claude/utility/delay-delayWhen) ·
[bufferTime](/operators-claude/transformation/bufferTime) ·
[windowTime](/operators-claude/transformation/windowTime) ·
[Time-based trees](./time-tree.md#rate-limiting) ·
[Rate-Limiting Guide](./rate-limiting-guide.md)

---

## Each value starts asynchronous work

The Flattening family. Every source value maps to an inner Observable; the only real decision is the overlap policy.

```text
A new inner Observable arrives while a previous one is still live. It should…
│
├── run alongside the live one                        merge      → mergeMap
│     How many may run at once?
│     ├── unbounded                                   mergeMap(f)
│     └── at most n                                   mergeMap(f, n)
│
├── wait until the live one completes (keep order)    concat     → concatMap
│     (= mergeMap(f, 1); unbounded queue while waiting)
│
├── cancel the live one and replace it                switch     → switchMap
│     (the only policy that unsubscribes live work)
│
└── be ignored because one is already live            exhaust    → exhaustMap
      (the only policy that never subscribes it)

Then: what is the input shape?
├── values that you project into Observables          *Map    mergeMap / concatMap / switchMap / exhaustMap
├── an Observable of Observables already              *All    mergeAll / concatAll / switchAll / exhaustAll
├── state must be carried across inners               mergeScan / switchScan
└── each output must be fed back in as input          expand(f, concurrency?)
```

Two memory aids from the recurring axes: `switchMap : debounce :: exhaustMap : throttle`, and "does order matter, and may work be lost?" separates concat (order kept, nothing lost) from switch (latest only, work discarded).

Leaves and background:
[mergeMap](/operators-claude/transformation/mergeMap) ·
[concatMap](/operators-claude/transformation/concatMap) ·
[switchMap](/operators-claude/transformation/switchMap) ·
[exhaustMap](/operators-claude/transformation/exhaustMap) ·
[mergeAll / concatAll / switchAll](/operators-claude/higher-order/mergeAll-concatAll-switchAll) ·
[exhaustAll](/operators-claude/higher-order/exhaustAll) ·
[mergeScan](/operators-claude/transformation/mergeScan) ·
[switchScan](/operators-claude/transformation/switchScan) ·
[expand](/operators-claude/transformation/expand) ·
[Flattening Strategy Guide](./flattening-strategy-guide.md)

---

## Several streams

```text
What should ONE output contain?
│
├── One value from EACH source (one slot per source)      → COMBINING
│   │
│   ├── 1. Emission trigger: which event fires an output?
│   │   ├── any source emits                              combineLatest
│   │   ├── only my primary source emits                  withLatestFrom
│   │   ├── a full tuple, one new value per source        zip
│   │   └── all sources have completed                    forkJoin
│   │
│   └── 2. Slot alignment: which value fills each slot?
│       ├── the latest snapshot                           combineLatest / withLatestFrom
│       ├── the n-th value of each (index-aligned queue)  zip        ← unbounded buffering
│       └── the final value                               forkJoin
│
├── Values from ALL sources, interleaved, unchanged      → overlap policy on whole sources
│   ├── as they come                                      merge
│   └── one source after the other completes              concat
│
└── Values from ONE source only; the others are dropped   → SOURCE SELECTION
    ├── whichever notifies first (next, error or complete) race
    └── decided by a condition at subscribe time          iif

Then: the input form
├── all sources known up front                           merge(a$, b$)  combineLatest([...])  race(a$, b$)
├── companions join a pipe                               mergeWith  concatWith  zipWith  combineLatestWith  raceWith
└── a stream of streams                                  mergeAll  concatAll  zipAll  combineLatestAll
```

Check the empty policy of `forkJoin` before using it: any input that completes without a value empties the whole join.

Leaves and background:
[combineLatest](/operators-claude/combination/combineLatest-operator-documentation) ·
[withLatestFrom](/operators-claude/combination/withLatestFrom) ·
[zip](/operators-claude/combination/zip) ·
[forkJoin](/operators-claude/combination/forkJoin) ·
[merge](/operators-claude/combination/merge) ·
[concat](/operators-claude/combination/concat) ·
[race](/operators-claude/combination/race) ·
[iif](/operators-claude/creation/iif) ·
[combineLatestWith / mergeWith](/operators-claude/combination/combineLatestWith-mergeWith) ·
[concatWith](/operators-claude/combination/concatWith) ·
[zipWith](/operators-claude/combination/zipWith) ·
[raceWith](/operators-claude/combination/raceWith) ·
[combineLatestAll](/operators-claude/higher-order/combineLatestAll) ·
[zipAll](/operators-claude/higher-order/zipAll) ·
[Higher-Order Observables Guide](./higher-order-observables-guide.md)

---

## When the stream ends badly

Three families react to how a stream ends: Resubscription tries the **same** source again, Substitution continues with a **different** one, and the Watchdog decides that **silence** counts as failure.

```text
What went wrong, and what should happen next?
│
├── The source ERRORED or COMPLETED, and I want the SAME source again   → RESUBSCRIPTION
│   ├── Trigger:   error → retry          complete → repeat
│   ├── Count:     forever → retry() / repeat()     n times → retry(n) / repeat(n)
│   ├── Delay before resubscribing:
│   │       none   ·   fixed { delay: ms }   ·   dynamic { delay: (err, count) => notifier$ }
│   └── (retry only) reset the counter after a success?   resetOnSuccess
│
├── The source ERRORED, and I want to continue with ANOTHER Observable   → SUBSTITUTION
│   ├── a replacement chosen from the error        catchError((err, caught$) => fallback$)
│   ├── the source itself (retry-equivalent)       catchError((_, caught$) => caught$)
│   └── the next source on ANY terminal,
│       errors swallowed                           onErrorResumeNextWith(...)
│
└── The source went SILENT for too long                                   → WATCHDOG
    ├── Scope:    first value only { first }   ·   every value { each }
    ├── Deadline: relative ms   ·   absolute Date
    └── On breach: error (TimeoutError)   ·   substitute { with: () => fallback$ }
```

`timeout({ with })` is `catchError` for time instead of for errors; the two families meet there.

Leaves and background:
[retry](/operators-claude/error-handling/retry) ·
[repeat](/operators-claude/utility/repeat) ·
[catchError](/operators-claude/error-handling/catchError) ·
[onErrorResumeNextWith](/operators-claude/error-handling/onErrorResumeNextWith) ·
[timeout](/operators-claude/error-handling/timeout) ·
[Time-based trees: Watchdog](./time-tree.md#14-watchdog) ·
[Error Handling Patterns](./error-handling-patterns.md)

---

## Many subscribers, one expensive source

The Sharing family. Every member keeps at most one live subscription to the source and multiplexes it; the choices are what the connector remembers, who connects, and when the state resets.

```text
Several subscribers should share ONE execution of the source.
│
├── 1. Connector: what does a late subscriber receive?
│   ├── nothing before it arrived                 Subject            share()
│   ├── the latest N values                       ReplaySubject(N)   shareReplay(N)
│   └── a custom subject                          share({ connector: () => new BehaviorSubject(x) })
│
├── 2. Connection: who starts the source?
│   ├── the first subscriber, automatically       share / shareReplay
│   └── my code, explicitly                       connectable(src) + connect()
│
└── 3. Reset: when does the shared state go away?   each true | false | () => notifier$
    ├── when the last subscriber leaves           resetOnRefCountZero
    ├── when the source completes                 resetOnComplete
    └── when the source errors                    resetOnError
         (grace period = dynamic variant: resetOnRefCountZero: () => timer(1000))
```

`shareReplay(1)` without `refCount: true` is the coordinate `resetOnRefCountZero: false`: the source stays alive after the last subscriber leaves. Choose it on purpose or not at all.

Leaves and background:
[share](/operators-claude/multicasting/share) ·
[shareReplay](/operators-claude/multicasting/shareReplay) ·
[connectable / connect](/operators-claude/connectable/connectable-connect) ·
[Multicasting Guide](./multicasting-guide.md) ·
[Subject Decision Guide](./subject-decision-guide.md)

---

## Where does the Observable come from

The Creation family. Nothing happens until subscribe, and the only question is the origin of the values.

```text
Where do the values come from?
│
├── literal values I have now                     of(a, b, c)
├── an existing array, iterable, Promise,         from(input)
│     or observable-like
├── a generated sequence                          range(start, count) · generate(...)
├── a clock                                       interval(ms) · timer(due, period?) · animationFrames()
├── an event source                               fromEvent(target, name) · fromEventPattern(add, remove)
├── a callback-style API                          bindCallback · bindNodeCallback
├── an external resource                          ajax(...) · fromFetch(...) · webSocket(...) · using(...)
├── a decision made per subscriber                defer(() => source)
└── no values at all, only a terminal
    ├── complete immediately                      EMPTY
    ├── never terminate                           NEVER
    └── error immediately                         throwError(() => err)

Scheduling: default, or scheduled(input, scheduler)
```

`iif` is not here: its values come from the two given Observables, so it is source selection (see Several streams).

Leaves:
[of](/operators-claude/creation/of) ·
[from](/operators-claude/creation/from) ·
[range](/operators-claude/creation/range) ·
[generate](/operators-claude/creation/generate) ·
[interval](/operators-claude/creation/interval) ·
[timer](/operators-claude/creation/timer) ·
[animationFrames](/operators-claude/creation/animationFrames) ·
[fromEvent](/operators-claude/creation/fromEvent) ·
[fromEventPattern](/operators-claude/creation/fromEventPattern) ·
[bindCallback / bindNodeCallback](/operators-claude/creation/bindCallback-bindNodeCallback) ·
[ajax](/operators-claude/creation/ajax) ·
[fromFetch](/operators-claude/creation/fromFetch) ·
[webSocket](/operators-claude/creation/webSocket) ·
[using](/operators-claude/creation/using) ·
[defer](/operators-claude/creation/defer) ·
[EMPTY / NEVER](/operators-claude/creation/EMPTY-NEVER) ·
[throwError](/operators-claude/creation/throwError) ·
[scheduled](/operators-claude/creation/scheduled)

---

## Worked examples

Each example walks the trees once per requirement clause. The pipe is the list of coordinates read top to bottom.

### A search box

```text
requirement    wait until typing pauses 300 ms, then request,
               abandon the previous request when a new one starts

coordinates    DEBOUNCE      invariant: restarting silence, trailing emit
               + fixed 300ms                      → debounceTime(300)
               SELECTION     uniqueness × adjacent → distinctUntilChanged()
               FLATTENING    kill-previous policy  → switchMap(query => http$)
```

```ts
search$ = input$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(toSearchRequest),
);
```

### A save button that must not double-submit

```text
requirement    one click starts a save; clicks while a save is in flight
               are ignored; a failed save is retried twice with a pause;
               after that the error is shown and the button keeps working

coordinates    FLATTENING      drop-newcomer policy           → exhaustMap(() => save$)
               RESUBSCRIPTION  error × count 2 × fixed delay  → retry({ count: 2, delay: 1000 })
               SUBSTITUTION    replacement from the error,
                               applied INSIDE the inner so the
                               outer click stream survives     → catchError(err => of(failed(err)))
```

```ts
saved$ = click$.pipe(
  exhaustMap(() =>
    save(form).pipe(
      retry({ count: 2, delay: 1000 }),
      catchError(err => of({ status: 'failed', err })),
    ),
  ),
);
```

Putting `catchError` outside `exhaustMap` would substitute the whole click stream and end it after the first failure; the inner placement is the coordinate that keeps the button alive.

### A dashboard that needs the latest of three feeds

```text
requirement    render whenever any of three feeds updates, but only once
               every feed has produced a value; never more than once per
               animation frame; late subscribers get the current state

coordinates    COMBINING    any source × latest snapshot       → combineLatest([a$, b$, c$])
               RATE-LIMIT   audit: latest value at window close,
                            window = one frame                  → auditTime(0, animationFrameScheduler)
               SHARING      ReplaySubject(1) × auto connect ×
                            reset when the last viewer leaves   → shareReplay({ bufferSize: 1, refCount: true })
```

```ts
view$ = combineLatest([prices$, orders$, alerts$]).pipe(
  auditTime(0, animationFrameScheduler),
  map(([prices, orders, alerts]) => buildViewModel(prices, orders, alerts)),
  shareReplay({ bufferSize: 1, refCount: true }),
);
```

`combineLatest` waiting for every slot once is its invariant, not an option; `auditTime` rather than `throttleTime` because the dashboard wants the latest state at the end of the frame, not the first state at its start; `refCount: true` because the reset axis defaults to keeping the feeds alive forever.
