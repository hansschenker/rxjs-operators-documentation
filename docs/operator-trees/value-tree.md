# Value-based Operator Trees

The Operator Trees describe every RxJS behavior as **one root behavior, an INVARIANT branch, and a VARIANTS branch** of independent axes. This page holds the trees whose decision moments are defined by **values**: a predicate, a count, a sequence position, an external notifier, a key, or the end of the sequence. No clock takes part in their invariant.

Families whose identity contains a clock — throttle, audit, sample, debounce, delay, the duration-bounded buffers and windows, and the watchdog — are on the [Time-based page](./time-tree.md). The split is the catalog's fire-hose test: a value-based family does the same thing however fast the source fires; a time-based family changes what it emits.

How to read a tree in one sentence: the INVARIANT lines are always true for every member, each VARIANT axis is one independent choice, and a concrete operator is the invariant plus one variant per axis. The notation, the coordinate syntax and the seven recurring axes are explained on the [Legend page](./legend.md). The same trees can be explored interactively at [netxpert.ch/rxjs-operator-trees](https://netxpert.ch/rxjs-operator-trees/) (hash routes such as `#/family/selection`).

Section numbers follow the catalog, so §5 (Rate-Limiting and Timing) and §14 (Watchdog) are intentionally absent here.

---

## 1. Selection

```text
SELECTION
│
├── INVARIANT
│   ├── every output value is a source value, passed through unchanged
│   ├── relative source order is preserved
│   ├── the output sequence is a sub-sequence of the input sequence
│   └── the operator never creates values
│
└── VARIANTS
    ├── 1. Criterion — what decides whether a value survives
    │   ├── count
    │   ├── predicate
    │   ├── uniqueness
    │   ├── index / position
    │   └── external notifier
    ├── 2. Polarity — keep or drop what the criterion selects
    │   ├── keep
    │   └── drop
    ├── 3. Anchor — where in the sequence the criterion applies
    │   ├── front        (leading region)
    │   ├── back         (trailing region)
    │   └── everywhere
    └── 4. Empty policy — what happens when nothing was selected
        ├── complete silently
        ├── emit a default value
        └── error
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `take(n)` | count × keep × front · silent | [take](/operators-claude/filtering/take) |
| `takeLast(n)` | count × keep × back · silent | [takeLast](/operators-claude/filtering/skip-takeLast-elementAt) |
| `skip(n)` | count × drop × front | [skip](/operators-claude/filtering/skip-takeLast-elementAt) |
| `skipLast(n)` | count × drop × back | [skipLast](/operators-claude/filtering/skipLast) |
| `filter(p)` | predicate × keep × everywhere | [filter](/operators-claude/filtering/filter) |
| `takeWhile(p, inclusive?)` | predicate × keep × front | [takeWhile](/operators-claude/filtering/takeWhile) |
| `skipWhile(p)` | predicate × drop × front | [skipWhile](/operators-claude/filtering/skipUntil-skipWhile) |
| `takeUntil(stop$)` | notifier × keep × front | [takeUntil](/operators-claude/filtering/takeUntil) |
| `skipUntil(start$)` | notifier × drop × front | [skipUntil](/operators-claude/filtering/skipUntil-skipWhile) |
| `distinct(key?, flush$?)` | uniqueness × keep × everywhere | [distinct](/operators-claude/filtering/distinct) |
| `distinctUntilChanged(cmp?)` | uniqueness × keep × everywhere | [distinctUntilChanged](/operators-claude/filtering/distinctUntilChanged) |
| `distinctUntilKeyChanged(k)` | uniqueness × keep × everywhere | [distinctUntilKeyChanged](/operators-claude/filtering/distinctUntilKeyChanged) |
| `first(p?, default?)` | predicate / position × keep × front · default or error | [first](/operators-claude/filtering/first-last) |
| `last(p?, default?)` | predicate / position × keep × back · default or error | [last](/operators-claude/filtering/first-last) |
| `elementAt(i, default?)` | index × keep · default or error | [elementAt](/operators-claude/filtering/skip-takeLast-elementAt) |
| `ignoreElements()` | predicate (always false) × drop × everywhere | [ignoreElements](/operators-claude/utility/endWith-ignoreElements) |
| `defaultIfEmpty(d)` | the empty-policy axis alone: default | [defaultIfEmpty](/operators-claude/conditional/defaultIfEmpty) |
| `throwIfEmpty(f)` | the empty-policy axis alone: error | [throwIfEmpty](/operators-claude/error-handling/throwIfEmpty) |

**Sub-axes of uniqueness**

```text
uniqueness
├── comparison scope
│   ├── entire history        distinct
│   └── adjacent values only  distinctUntilChanged
├── compared key
│   ├── the value itself
│   └── selected key          keySelector / distinctUntilKeyChanged
└── memory reset
    ├── never
    └── on notifier           distinct(key, flush$)
```

**What the tree explains**

- `first()` and `take(1)` differ in exactly one coordinate, the empty policy (error versus complete silently). The selection itself is identical.
- Back-anchored variants must buffer until completion. `takeLast`, `skipLast` and `last` cannot emit before the source completes; that follows from the `back` anchor, not from an implementation detail.
- Front-anchored keep variants complete early. Once `take(n)` or `first()` has what it needs, nothing further can be selected, so the output completes and the source is unsubscribed.
- The `inclusive` flag of `takeWhile` is a boundary policy: does the value that fails the predicate still belong to the selected region?

### Source Selection

`race` shares Selection's geometry one level up: it keeps one **source** and drops the rest.

```text
SOURCE SELECTION
│
├── INVARIANT
│   ├── competes several whole sources; exactly one source survives
│   ├── the winner's values pass through unchanged
│   └── losing sources are unsubscribed, or never subscribed at all
│
└── VARIANTS
    ├── 1. Decision — what picks the winning source
    │   ├── first notification wins    race(a$, b$) / raceWith(b$)
    │   └── condition at subscribe     iif(() => cond, then$, else$)
    └── 2. Input form
        ├── static creation            race(...), iif(...)
        └── *With operator suffix      raceWith(...)
```

| Operator | Coordinates | Reference |
|---|---|---|
| `race(a$, b$)` | first notification × static | [race](/operators-claude/combination/race) |
| `raceWith(b$)` | first notification × *With | [raceWith](/operators-claude/combination/raceWith) |
| `iif(cond, a$, b$)` | condition at subscribe × static | [iif](/operators-claude/creation/iif) |

- In `race` the first notification of **any** kind wins: a source that errors before anyone emits errors the race, and an early completion completes it. "First to emit a value" is a common misreading.
- `iif` is reclassified out of Creation. Its values come from the two given Observables, which fails the Creation invariant; behaviorally it is source selection decided by a predicate at subscribe time.
- `condition × *With` is a hole: there is no `iifWith`. Compose it with `defer` when needed.

---

## 2. Transformation

```text
TRANSFORMATION
│
├── INVARIANT
│   ├── exactly one output value per source value (1:1 cardinality)
│   ├── output timing equals source timing
│   └── each output is a pure projection of (value, index)
│
└── VARIANTS
    └── 1. Projection target
        ├── computed value        map(project)
        ├── constant              map(() => c)      (mapTo — deprecated)
        └── property path         map(v => v.k)     (pluck — deprecated)
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `map(project)` | computed value | [map](/operators-claude/transformation/map) |
| `mapTo(c)` | constant (deprecated) | [mapTo](/operators-claude/transformation/mapTo) |
| `pluck(...keys)` | property path (deprecated) | [deprecated operators](/operators-claude/utility/deprecated-operators) |
| `timestamp()` | preset projection `v => { value, timestamp }` | [timestamp](/operators-claude/utility/timeInterval-timestamp) |
| `timeInterval()` | preset projection `v => { value, interval }` | [timeInterval](/operators-claude/utility/timeInterval-timestamp) |

**What the tree explains**

- `map` is almost pure invariant: the family's whole behavior is fixed and the only variant is the projection function itself. That is why `map` feels simple; it has no hidden policies.
- `expand` does not belong here. It feeds its output back as input and subscribes to inner Observables, which satisfies the Flattening invariant, not this one. See §6.

---

## 3. Accumulation

```text
ACCUMULATION
│
├── INVARIANT
│   ├── holds an accumulator for the lifetime of the subscription
│   ├── every source value causes a transition  acc = f(acc, value, index)
│   └── outputs derive from the accumulator
│
└── VARIANTS
    ├── 1. Emission cadence
    │   ├── every transition       scan
    │   └── only at completion     reduce
    ├── 2. Seed
    │   ├── explicit seed value
    │   └── first source value acts as seed
    └── 3. Accumulation function
        ├── free                   scan(f) / reduce(f)
        └── preset
            ├── append to array    toArray
            ├── counter            count
            └── comparator keep    max / min
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `scan(f, seed?)` | every transition × free | [scan](/operators-claude/mathematical-aggregate/scan) |
| `reduce(f, seed?)` | at completion × free | [reduce](/operators-claude/mathematical-aggregate/reduce) |
| `toArray()` | at completion × preset: append | [toArray](/operators-claude/mathematical-aggregate/toArray-count) |
| `count(p?)` | at completion × preset: counter | [count](/operators-claude/mathematical-aggregate/toArray-count) |
| `max(cmp?)` / `min(cmp?)` | at completion × preset: comparator keep | [min / max](/operators-claude/mathematical-aggregate/min-max) |

**What the tree explains**

- `reduce` = `scan` + `last`. The cadence axis is the only difference, and composition makes it literal.
- `toArray()` is `reduce((acc, v) => [...acc, v], [])`: a preset, not a new behavior.
- `reduce` on an empty source without a seed errors, the same empty-policy dimension that separates `first()` from `take(1)` in Selection.

---

## 4. Grouping

```text
GROUPING
│
├── INVARIANT
│   ├── every source value is routed into one or more groups, unchanged
│   ├── the output emits the groups themselves
│   └── a partition rule decides group membership
│
└── VARIANTS
    ├── 1. Materialization
    │   ├── array                buffer family     Observable<T[]>
    │   └── inner Observable     window family     Observable<Observable<T>>
    ├── 2. Partition rule
    │   ├── temporal boundaries  buffer* / window*
    │   └── value key            groupBy
    ├── 3. Boundary trigger              (boundary-partitioned only)
    │   ├── count
    │   ├── duration             → Time-based page
    │   └── notifier
    ├── 4. Boundary determination
    │   ├── fixed / external
    │   └── dynamic / selected
    └── 5. Topology
        ├── contiguous
        ├── overlapping
        └── gapped
```

The duration trigger (`bufferTime`, `windowTime`) is the same tree entered through a clock; it is covered under [Lossless rate limiting](./time-tree.md#lossless-rate-limiting) on the Time-based page.

**Mapping** (buffer side; `window*` mirrors 1:1 with inner-Observable materialization)

| Operator | Coordinates | Reference |
|---|---|---|
| `bufferCount(n)` | array × count × fixed × contiguous | [bufferCount](/operators-claude/transformation/bufferCount) |
| `bufferCount(n, m)`, m < n | array × count × fixed × overlapping | [bufferCount](/operators-claude/transformation/bufferCount) |
| `bufferCount(n, m)`, m > n | array × count × fixed × gapped | [bufferCount](/operators-claude/transformation/bufferCount) |
| `windowCount(n, m?)` | inner Observable × count × fixed × same topology rule | [windowCount](/operators-claude/transformation/windowCount) |
| `buffer(boundary$)` / `window(boundary$)` | notifier × external × contiguous | [buffer / window](/operators-claude/transformation/buffer-window) |
| `bufferWhen(sel)` / `windowWhen(sel)` | notifier × dynamic × contiguous | [bufferWhen / windowWhen](/operators-claude/transformation/bufferWhen-windowWhen) |
| `bufferToggle(open$, sel)` / `windowToggle` | notifier × dynamic × overlapping or gapped | [bufferToggle / windowToggle](/operators-claude/transformation/bufferToggle-windowToggle) |
| `groupBy(key, opts)` | inner Observable × value key | [groupBy](/operators-claude/transformation/groupBy) |
| `partition(p)` | key-partitioned with a boolean key, exactly two Observables | [partition](/operators-claude/creation/partition) |
| `pairwise()` | `bufferCount(2, 1)` materialized as a tuple | [pairwise](/operators-claude/transformation/pairwise) |

**Key-partitioned grouping**

```text
groupBy(keySelector, options)
├── key            always dynamic/selected (the keySelector)
├── group value    identity | element selector
├── group lifetime duration selector  → dynamic close, group may reopen
└── connector      subject factory    → overlaps the Sharing axes (§8)
```

**What the tree explains**

- The second argument of `bufferCount` and `bufferTime` is the topology axis in disguise: comparing it with the first argument yields contiguous, overlapping or gapped. The tree turns a numeric relationship into a named behavioral choice.
- `bufferToggle` is the only boundary variant with independent open and close boundaries, which is exactly why it alone reaches the overlapping and gapped topologies with notifiers.
- `groupBy` shares the invariant but swaps the partition rule: membership by key, not by time region, so its groups interleave instead of tiling the timeline.

---

## 6. Flattening / Concurrency

```text
FLATTENING
│
├── INVARIANT
│   ├── each source value maps to an inner Observable
│   ├── output values are inner values, unchanged
│   └── the operator manages the lifetime of inner subscriptions
│
└── VARIANTS
    ├── 1. Overlap policy — a new inner arrives while another is live
    │   ├── run alongside        merge
    │   ├── queue until a slot   concat      (= concurrency limit 1)
    │   ├── kill the previous    switch
    │   └── drop the newcomer    exhaust
    ├── 2. Concurrency limit               (alongside/queue only)
    │   ├── unbounded            mergeMap(f)
    │   └── n slots              mergeMap(f, n)
    ├── 3. Input form
    │   ├── project, then flatten    *Map
    │   └── source is higher-order   *All
    ├── 4. State across inners
    │   ├── stateless            *Map / *All
    │   └── accumulating         mergeScan / switchScan
    └── 5. Recursion
        └── output re-enters as input      expand(f, concurrency?)
```

**The 4 × 2 grid**

```text
              project + flatten    higher-order source
alongside     mergeMap             mergeAll
queued        concatMap            concatAll
kill prev     switchMap            switchAll
drop new      exhaustMap           exhaustAll
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `mergeMap(f, n?)` | alongside × unbounded or n × *Map × stateless | [mergeMap](/operators-claude/transformation/mergeMap) |
| `concatMap(f)` | queue (= alongside × 1) × *Map × stateless | [concatMap](/operators-claude/transformation/concatMap) |
| `switchMap(f)` | kill previous × *Map × stateless | [switchMap](/operators-claude/transformation/switchMap) |
| `exhaustMap(f)` | drop newcomer × *Map × stateless | [exhaustMap](/operators-claude/transformation/exhaustMap) |
| `mergeAll(n?)` / `concatAll()` / `switchAll()` | same policies × *All | [mergeAll / concatAll / switchAll](/operators-claude/higher-order/mergeAll-concatAll-switchAll) |
| `exhaustAll()` | drop newcomer × *All | [exhaustAll](/operators-claude/higher-order/exhaustAll) |
| `mergeScan(f, seed, n?)` | alongside × accumulating | [mergeScan](/operators-claude/transformation/mergeScan) |
| `switchScan(f, seed)` | kill previous × accumulating | [switchScan](/operators-claude/transformation/switchScan) |
| `expand(f, n?)` | alongside × recursion | [expand](/operators-claude/transformation/expand) |
| `concatMapTo` / `mergeMapTo` / `switchMapTo` | fixed inner (deprecated) | [concatMapTo](/operators-claude/transformation/concatMapTo) · [mergeMapTo](/operators-claude/transformation/mergeMapTo) · [switchMapTo](/operators-claude/transformation/switchMapTo) |

**What the tree explains**

- The four overlap policies answer one question: what happens to a new inner Observable while a previous inner is still live. Alongside, queued, replaced, or dropped.
- `concatMap(f)` ≡ `mergeMap(f, 1)`. Queueing is not a separate root behavior; it is the alongside policy with a concurrency limit of one.
- `expand` is `mergeMap` plus the recursion variant: every output is projected again. Its optional second argument is the same concurrency-limit axis.
- Cancellation lives here: `switch` is the only policy that unsubscribes a live inner, and `exhaust` is the only one that never subscribes to a value's inner at all.

---

## 7. Combining

```text
COMBINING
│
├── INVARIANT
│   ├── subscribes to several sources at once
│   ├── each output combines one value per source (one slot per source)
│   └── source values fill slots unchanged
│
└── VARIANTS
    ├── 1. Emission trigger — which event fires an output
    │   ├── any source emits             combineLatest
    │   ├── only the primary emits       withLatestFrom
    │   ├── a full tuple is available    zip
    │   └── all sources complete         forkJoin
    └── 2. Slot alignment — which value fills each slot
        ├── latest snapshot              combineLatest / withLatestFrom
        ├── index-aligned queue          zip
        └── final value                  forkJoin
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `combineLatest([...])` | any source × latest snapshot | [combineLatest](/operators-claude/combination/combineLatest-operator-documentation) |
| `combineLatestWith(b$)` / `combineLatestAll()` | same × *With / *All | [combineLatestWith](/operators-claude/combination/combineLatestWith-mergeWith) · [combineLatestAll](/operators-claude/higher-order/combineLatestAll) |
| `withLatestFrom(b$)` | primary × latest snapshot | [withLatestFrom](/operators-claude/combination/withLatestFrom) |
| `zip(a$, b$)` / `zipWith(b$)` / `zipAll()` | full tuple × index-aligned | [zip](/operators-claude/combination/zip) · [zipWith](/operators-claude/combination/zipWith) · [zipAll](/operators-claude/higher-order/zipAll) |
| `forkJoin([...])` | all complete × final value | [forkJoin](/operators-claude/combination/forkJoin) |
| `merge` / `mergeWith` · `concat` / `concatWith` | interleaving without slots (see §6 overlap policy and §16 input form) | [merge](/operators-claude/combination/merge) · [mergeWith](/operators-claude/combination/combineLatestWith-mergeWith) · [concat](/operators-claude/combination/concat) · [concatWith](/operators-claude/combination/concatWith) |

**What the tree explains**

- `withLatestFrom` is `combineLatest` with a demoted trigger: the same latest-snapshot alignment, but only one source may fire an emission. One coordinate of difference, not a different concept.
- `zip`'s index alignment forces unbounded buffering of the faster source, visible from the coordinate alone.
- `forkJoin` is the completion corner of the space: trigger and alignment both degenerate to "the end".
- `race` does not belong here. It emits the winning source's values unchanged and drops the other sources entirely, one slot total rather than one slot per source. Its tree is Source Selection in §1.

---

## 8. Sharing

```text
SHARING
│
├── INVARIANT
│   ├── at most one live subscription to the source at a time
│   ├── values are multiplexed unchanged to every subscriber
│   └── a subject-like connector sits between source and subscribers
│
└── VARIANTS
    ├── 1. Connector / replay
    │   ├── none                Subject           share()
    │   ├── latest N            ReplaySubject     shareReplay(n)
    │   └── custom              share({ connector: () => subject })
    ├── 2. Connection
    │   ├── on first subscriber     share / refCount
    │   └── manual                  connectable(source) + .connect()
    └── 3. Reset policies — each: on | off | dynamic
        ├── resetOnRefCountZero    true | false | () => notifier$
        ├── resetOnComplete        true | false | () => notifier$
        └── resetOnError           true | false | () => notifier$
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `share()` | Subject × on first subscriber × all resets on | [share](/operators-claude/multicasting/share) |
| `shareReplay(n)` | ReplaySubject(n) × on first subscriber × resetOnError only | [shareReplay](/operators-claude/multicasting/shareReplay) |
| `share({ connector, resetOn… })` | custom connector × chosen resets | [share](/operators-claude/multicasting/share) |
| `connectable(src)` + `connect()` | chosen connector × manual connection | [connectable / connect](/operators-claude/connectable/connectable-connect) |
| `publish` / `multicast` / `refCount` | legacy spellings of the same coordinates (deprecated) | [deprecated operators](/operators-claude/utility/deprecated-operators) |

**What the tree explains**

- `shareReplay(n)` is not a distinct behavior. It is `share` with a `ReplaySubject(n)` connector and specific reset defaults: `resetOnError: true`, `resetOnComplete: false`, `resetOnRefCountZero: false` unless `refCount: true`.
- The dynamic reset variant enables grace periods: `resetOnRefCountZero: () => timer(1000)` keeps the source alive for one second after the last subscriber leaves. That is the recurring fixed/dynamic axis applied to teardown instead of to values.
- The classic "shareReplay never unsubscribes the source" pitfall is simply the coordinate `resetOnRefCountZero: false` made visible.

---

## 9. Resubscription

```text
RESUBSCRIPTION
│
├── INVARIANT
│   ├── reacts to a terminal notification from the source
│   ├── may subscribe to the SAME source again
│   └── values before the terminal pass through unchanged
│
└── VARIANTS
    ├── 1. Terminal trigger
    │   ├── error        retry
    │   └── complete     repeat
    ├── 2. Count
    │   ├── infinite               retry() / repeat()
    │   └── fixed n                retry(n) / repeat(n)
    ├── 3. Delay before resubscribing      → Time-based page for the duration variants
    │   ├── none
    │   ├── fixed                  { delay: ms }
    │   └── dynamic                { delay: (error, count) => notifier$ }
    └── 4. Counter reset                    (retry only)
        └── resetOnSuccess    true | false
```

The delay axis is the recurring determination axis; its fixed and dynamic variants are discussed alongside the other durations on the [Time-based page](./time-tree.md#timing).

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `retry(n?)` / `retry({ count, delay, resetOnSuccess })` | error × count × delay × reset | [retry](/operators-claude/error-handling/retry) |
| `repeat(n?)` / `repeat({ count, delay })` | complete × count × delay | [repeat](/operators-claude/utility/repeat) |
| `retryWhen(notifier)` | error × dynamic delay (deprecated spelling) | [retryWhen](/operators-claude/error-handling/retryWhen) |
| `repeatWhen(notifier)` | complete × dynamic delay (deprecated spelling) | [repeatWhen](/operators-claude/utility/repeatWhen) |

### Substitution

`catchError` shares the trigger but not the invariant: it **replaces** the source with a different Observable instead of resubscribing to the same one.

```text
SUBSTITUTION
│
├── INVARIANT
│   ├── reacts to an error from the source
│   └── continues with a REPLACEMENT Observable
│
└── VARIANTS
    └── 1. Replacement
        ├── selected from the error      catchError((err, caught$) => fallback$)
        └── retry-equivalent             catchError((_, caught$) => caught$)
```

| Operator | Coordinates | Reference |
|---|---|---|
| `catchError(sel)` | error × selected replacement | [catchError](/operators-claude/error-handling/catchError) |
| `onErrorResumeNext(...)` / `onErrorResumeNextWith(...)` | substitute on any terminal, swallowing errors | [onErrorResumeNext](/operators-claude/error-handling/onErrorResumeNext) · [onErrorResumeNextWith](/operators-claude/error-handling/onErrorResumeNextWith) |

**What the tree explains**

- The escape hatch `caught$` shows the family boundary precisely: resubscription is substitution where the replacement is the source itself.
- Count, delay and `resetOnSuccess` are the only things that separate the members of the resubscription family; the trigger decides whether you are looking at `retry` or `repeat`.

---

## 10. Creation

```text
CREATION
│
├── INVARIANT
│   ├── there is no upstream source Observable
│   ├── values originate from non-observable material
│   └── nothing happens until subscribe (one execution per subscriber)
│
└── VARIANTS
    ├── 1. Origin
    │   ├── literal values        of(a, b, c)
    │   ├── existing collection   from(iterable | promise | observable-like)
    │   ├── iteration             range(start, count), generate(...)
    │   ├── clock                 interval, timer, animationFrames   → Time-based page
    │   ├── event binding         fromEvent(target, name), fromEventPattern(add, remove)
    │   ├── lazy factory          defer(() => source)
    │   └── external resource     ajax(...), fromFetch(...), webSocket(...), using(...)
    ├── 2. Termination preset — origins with zero values
    │   ├── complete immediately  EMPTY
    │   ├── never terminate       NEVER
    │   └── error immediately     throwError(() => err)
    └── 3. Scheduling
        ├── default (synchronous where possible)
        └── explicit scheduler    scheduled(input, scheduler)
```

The clock origins (`interval`, `timer`, `animationFrames`) are listed with the other clock-driven behavior on the [Time-based page](./time-tree.md#timing).

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `of(a, b, c)` | literal values | [of](/operators-claude/creation/of) |
| `from(input)` | existing collection | [from](/operators-claude/creation/from) |
| `range(start, count)` | iteration | [range](/operators-claude/creation/range) |
| `generate(...)` | iteration | [generate](/operators-claude/creation/generate) |
| `fromEvent(target, name)` | event binding | [fromEvent](/operators-claude/creation/fromEvent) |
| `fromEventPattern(add, remove)` | event binding | [fromEventPattern](/operators-claude/creation/fromEventPattern) |
| `defer(factory)` | lazy factory | [defer](/operators-claude/creation/defer) |
| `ajax(...)` / `fromFetch(...)` / `webSocket(...)` | external resource | [ajax](/operators-claude/creation/ajax) · [fromFetch](/operators-claude/creation/fromFetch) · [webSocket](/operators-claude/creation/webSocket) |
| `using(factory, source)` | external resource with teardown | [using](/operators-claude/creation/using) |
| `bindCallback` / `bindNodeCallback` | callback API as origin | [bindCallback](/operators-claude/creation/bindCallback-bindNodeCallback) |
| `EMPTY` / `NEVER` | termination preset: complete / never | [EMPTY / NEVER](/operators-claude/creation/EMPTY-NEVER) |
| `throwError(factory)` | termination preset: error | [throwError](/operators-claude/creation/throwError) |
| `scheduled(input, scheduler)` | explicit scheduler | [scheduled](/operators-claude/creation/scheduled) |

**What the tree explains**

- `defer` is the determination axis applied to creation itself: any fixed origin becomes dynamic, selected per subscriber, by wrapping it in a factory.
- `iif` does not belong here. Its values come from the two given Observables, failing this family's invariant; it is source selection decided at subscribe time (§1).
- `EMPTY`, `NEVER` and `throwError` are not special operators. They are the three degenerate corners of the origin axis where only the terminal notification remains.

---

## 11. Side-effect

```text
SIDE-EFFECT
│
├── INVARIANT
│   ├── values pass through unchanged
│   ├── timing passes through unchanged
│   └── the operator only observes — it never influences the stream
│
└── VARIANTS
    └── 1. Lifecycle hook
        ├── next / error / complete    tap(fn) / tap({ next, error, complete })
        ├── subscribe / unsubscribe    tap({ subscribe, unsubscribe })
        └── teardown, any reason       finalize(fn)
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `tap(observer)` | next / error / complete / subscribe / unsubscribe hooks | [tap](/operators-claude/utility/tap) |
| `finalize(fn)` | teardown for any reason | [finalize](/operators-claude/utility/finalize) |

**What the tree explains**

- This is the only family whose entire dataflow behavior is invariant. The variants merely choose where to look, never what happens.
- `finalize` is `tap` for the end of the subscription regardless of how it ended: complete, error or unsubscribe. One hook unifies three exits.

---

## 12. Interrogation

```text
INTERROGATION
│
├── INVARIANT
│   ├── consumes the sequence to answer a single question
│   ├── emits exactly one answer, then completes
│   └── answers as early as logically possible
│
└── VARIANTS
    ├── 1. Question
    │   ├── do ALL values match?       every(p)
    │   ├── does ANY value match?      find(p) / findIndex(p)
    │   ├── is the sequence empty?     isEmpty()
    │   ├── equal to another one?      sequenceEqual(other$)
    │   └── exactly one match?         single(p?)
    └── 2. Answer content
        ├── boolean                    every, isEmpty, sequenceEqual
        ├── the matching value         find, single
        └── the matching index         findIndex
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `every(p)` | all match × boolean | [every](/operators-claude/conditional/every) |
| `find(p)` / `findIndex(p)` | any match × value / index | [find / findIndex](/operators-claude/filtering/find-findIndex) |
| `isEmpty()` | empty? × boolean | [isEmpty](/operators-claude/conditional/isEmpty) |
| `sequenceEqual(other$, cmp?)` | equal? × boolean | [sequenceEqual](/operators-claude/conditional/sequenceEqual) |
| `single(p?)` | exactly one × value | [single](/operators-claude/filtering/single) |

**What the tree explains**

- The decision point is not an axis; it is derived from the question's logic. Universal claims (`every`, `isEmpty`, `sequenceEqual`) can only be confirmed at completion but refuted by the earliest counterexample. Existential claims (`find`) confirm at the first match.
- `find(p)` and `first(p)` differ only in empty policy: `find` emits `undefined`, `first` errors. The same single-coordinate difference as `first()` versus `take(1)`.
- `single` adds a cardinality assertion: it errors when more than one value matches, which no Selection operator does.

---

## 13. Injection

```text
INJECTION
│
├── INVARIANT
│   ├── all source values pass through unchanged
│   └── extra given values are inserted at a sequence edge
│
└── VARIANTS
    └── 1. Edge
        ├── leading     startWith(a, b, ...)   before the source's values
        └── trailing    endWith(a, b, ...)     after the source completes
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `startWith(...values)` | leading edge | [startWith](/operators-claude/combination/startWith) |
| `endWith(...values)` | trailing edge | [endWith](/operators-claude/utility/endWith-ignoreElements) |

**What the tree explains**

- The recurring edge axis applied to inserting rather than selecting: `startWith` and `endWith` are to injection what `take` and `takeLast` are to selection.
- `endWith` values emit only on completion. An errored source never reaches its trailing edge.

---

## 15. Reification

```text
REIFICATION
│
├── INVARIANT
│   └── moves the stream between the value channel and the
│       notification channel — no information is added or lost
│
└── VARIANTS
    └── 1. Direction
        ├── lift      materialize()     next/error/complete → Notification values
        └── lower     dematerialize()   Notification values → next/error/complete
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `materialize()` | lift | [materialize](/operators-claude/utility/materialize-dematerialize) |
| `dematerialize()` | lower | [dematerialize](/operators-claude/utility/materialize-dematerialize) |

**What the tree explains**

- After `materialize()`, errors and completions are ordinary values, so every other family (Selection, Grouping, Accumulation, …) can operate on them. Reification is the escape hatch that makes terminals first-class.

---

## 16. Placements into existing trees

Operators that need no new family; the trees above already contain their coordinates.

| Operator | Family | Coordinates | Reference |
|---|---|---|---|
| `pairwise` | Grouping | `bufferCount(2, 1)` materialized as a tuple | [pairwise](/operators-claude/transformation/pairwise) |
| `partition` | Grouping | key-partitioned with a boolean key, exactly two Observables | [partition](/operators-claude/creation/partition) |
| `ignoreElements` | Selection | keep nothing (predicate false × everywhere); only the terminal survives | [ignoreElements](/operators-claude/utility/endWith-ignoreElements) |
| `timestamp` / `timeInterval` | Transformation | `map` presets | [timeInterval / timestamp](/operators-claude/utility/timeInterval-timestamp) |
| `toArray` / `count` / `max` / `min` | Accumulation | presets of the accumulation-function axis | [toArray / count](/operators-claude/mathematical-aggregate/toArray-count) · [min / max](/operators-claude/mathematical-aggregate/min-max) |
| `defaultIfEmpty(d)` | Selection | the empty-policy axis standalone: default | [defaultIfEmpty](/operators-claude/conditional/defaultIfEmpty) |
| `throwIfEmpty(f)` | Selection | the empty-policy axis standalone: error | [throwIfEmpty](/operators-claude/error-handling/throwIfEmpty) |
| `onErrorResumeNext` | Substitution | substitute on any terminal, swallowing errors | [onErrorResumeNext](/operators-claude/error-handling/onErrorResumeNext) |
| `observeOn` / `subscribeOn` | meta-axis | execution-context scheduling, orthogonal to every family | [observeOn / subscribeOn](/operators-claude/utility/observeOn-subscribeOn) |

**The input-form axis for multi-source behavior**

Join creation functions, `*With` operators and `*All` operators are the same coordinates reached through different input forms, the axis already seen in Flattening (§6, `*Map` versus `*All`).

```text
                static creation        operator suffix        higher-order
merge           merge(a$, b$)          mergeWith(b$)          mergeAll()
concat          concat(a$, b$)         concatWith(b$)         concatAll()
zip             zip(a$, b$)            zipWith(b$)            zipAll()
combineLatest   combineLatest([...])   combineLatestWith(b$)  combineLatestAll()
race            race(a$, b$)           raceWith(b$)           —
forkJoin        forkJoin([...])        —                      —
```

The empty cells are API-surface holes the table makes visible. See [Pipeable Aliases](/operators-claude/combination/pipeable-aliases) for the `*With` forms side by side.

---

## The recurring axes in this page

Seven variant axes recur across unrelated families; the [Legend](./legend.md#the-seven-recurring-axes) defines each one. Where they surface among the value-based trees:

| Axis | Appears in |
|---|---|
| Determination (fixed vs dynamic) | Grouping boundary determination · Sharing reset policies · Resubscription delay · Creation (`defer`) · Selection uniqueness key |
| Trigger kind (count / notifier / predicate) | Selection criterion · Grouping boundary trigger (the duration variant lives on the Time-based page) |
| Edge (leading vs trailing) | Selection anchor (`take` / `takeLast`, `first` / `last`) · Injection (`startWith` / `endWith`) |
| Latest-value memory | Combining slot alignment (`combineLatest`, `withLatestFrom`) · Sharing connector (`shareReplay(1)`) |
| Overlap policy | Flattening (`merge` / `concat` / `switch` / `exhaust`) |
| Input form | Flattening (`*Map` / `*All`) · Combining · Source Selection · §16 table |
| Empty policy | Selection (`first` / `take(1)`, `elementAt`) · Accumulation (`reduce` without seed) · Interrogation (`find` / `first(p)`) · `defaultIfEmpty` / `throwIfEmpty` |

Most value-based operators are one root behavior plus two or three answers to these questions. That is why the catalog stays small while the API surface looks large.
