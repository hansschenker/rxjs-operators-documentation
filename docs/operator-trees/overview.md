# Operator Trees: Overview

RxJS exports well over a hundred operator names. Learned as a flat list they look unrelated. Learned as **Operator Trees** they collapse into fifteen root behaviors, each with a handful of independent choices, and most of the names turn out to be the same choice made in a different family.

This section is the map. The operator pages elsewhere on this site are the territory: each one gives the full eight-policy contract of a single operator. The trees tell you where that operator sits among its relatives, and which single coordinate separates it from the operator next door.

## The idea in one picture

Every family is a root behavior with exactly two top branches:

```text
ROOT BEHAVIOR
│
├── INVARIANT      ← what every member of the family always does
│   └── ...
│
└── VARIANTS       ← independent axes; each axis offers explicit variants
    ├── 1. Axis
    │   ├── variant
    │   └── variant
    └── 2. Axis
        └── ...
```

Three reading rules carry the whole method:

1. **The INVARIANT branch is the identity of the family.** If a behavior does not satisfy every invariant line, it does not belong to this family.
2. **Each VARIANT axis is an independent dimension.** A concrete behavior picks exactly one variant per axis:

   ```text
   Concrete Behavior = INVARIANT + (one variant per axis)
   ```

3. **The official RxJS names are mappings onto those coordinates.** Some coordinates have a name, some have a configuration flag, and some have nothing at all. The empty ones are holes in the API surface, and the tree makes them visible.

The classic example is throttle. The API shows two names, `throttle` and `throttleTime`, and a configuration object. The tree shows two axes:

```text
THROTTLE
│
├── INVARIANT
│   ├── a source value opens a throttle window
│   ├── during the window further source values are suppressed
│   └── the window does not restart on suppressed values
│
└── VARIANTS
    ├── 1. Duration
    │   ├── fixed        throttleTime(ms)
    │   └── dynamic      throttle(v => duration$)
    └── 2. Edge
        ├── leading             v------
        ├── trailing            ------v
        └── leading + trailing  v------v

2 durations × 3 edges = 6 concrete throttle behaviors
```

Six behaviors hide behind two names. That is the general situation: the API surface looks large because names and configuration flags encode coordinates, and the trees decode them.

## Trees classify, policies specify

The method has two complementary halves.

```text
                  RxJS BEHAVIOR
                       │
          ┌────────────┴────────────┐
          │                         │
     FAMILY TREES                POLICIES
          │                         │
 classify behavior             specify behavior
          │                         │
 root behavior                Source
 variant axes                 Trigger
 variants                     Value
 concrete behavior            Cardinality
 API mapping                  Time
                              Concurrency
                              Cancellation
                              Termination
          │                         │
          └────────────┬────────────┘
                       │
              COMPLETE DESCRIPTION
              OF OPERATOR BEHAVIOR
```

**Family Trees tell you which behavior you have. Policies tell you exactly what that behavior does.** On this site the policy half is the [eight-policy operator page](/SKILL): identity, functional specification, marble diagrams, behavioral characteristics, type system, examples, pitfalls, related operators. Use the two together:

- Start in a tree to find the family and the coordinates that match your requirement.
- Open the operator page named by that coordinate for the full contract, the marble diagrams, and the pitfalls.
- When two operators look interchangeable, compare their coordinates. They usually differ in exactly one.

## The fifteen root behaviors

| # | Root behavior | Invariant in one line | Where | Examples |
|---|---|---|---|---|
| 1 | **Selection** | output is a sub-sequence of the input, values unchanged, order preserved | [Value-based](./value-tree.md#_1-selection) | [`take`](/operators-claude/filtering/take), [`filter`](/operators-claude/filtering/filter), [`distinct`](/operators-claude/filtering/distinct), [`first`](/operators-claude/filtering/first-last) |
| | *sibling: Source Selection* | several whole sources compete, exactly one survives | [Value-based](./value-tree.md#source-selection) | [`race`](/operators-claude/combination/race), `iif` |
| 2 | **Transformation** | one output per input, same timing, pure projection of (value, index) | [Value-based](./value-tree.md#_2-transformation) | [`map`](/operators-claude/transformation/map) |
| 3 | **Accumulation** | an accumulator lives for the subscription; outputs derive from it | [Value-based](./value-tree.md#_3-accumulation) | [`scan`](/operators-claude/mathematical-aggregate/scan), [`reduce`](/operators-claude/mathematical-aggregate/reduce), [`toArray`](/operators-claude/mathematical-aggregate/toArray-count) |
| 4 | **Grouping** | every value is routed into one or more groups; the groups are emitted | [Value-based](./value-tree.md#_4-grouping) · [Time-based](./time-tree.md#lossless-rate-limiting) | [`bufferCount`](/operators-claude/transformation/bufferCount), [`windowTime`](/operators-claude/transformation/windowTime), [`groupBy`](/operators-claude/transformation/groupBy) |
| 5 | **Rate-Limiting and Timing** | values untouched; a temporal policy bounds the rate or decides when delivery happens | [Time-based](./time-tree.md) | [`throttleTime`](/operators-claude/filtering/throttleTime), [`auditTime`](/operators-claude/rate-limiting/auditTime-sampleTime), [`debounceTime`](/operators-claude/filtering/debounceTime), [`delay`](/operators-claude/utility/delay-delayWhen) |
| 6 | **Flattening / Concurrency** | each value maps to an inner Observable whose values are forwarded | [Value-based](./value-tree.md#_6-flattening-concurrency) | [`mergeMap`](/operators-claude/transformation/mergeMap), [`switchMap`](/operators-claude/transformation/switchMap), [`concatMap`](/operators-claude/transformation/concatMap), [`exhaustMap`](/operators-claude/transformation/exhaustMap) |
| 7 | **Combining** | several sources, one slot per source, each output combines one value per slot | [Value-based](./value-tree.md#_7-combining) | [`combineLatest`](/operators-claude/combination/combineLatest-operator-documentation), [`zip`](/operators-claude/combination/zip), [`forkJoin`](/operators-claude/combination/forkJoin) |
| 8 | **Sharing** | at most one live source subscription; a connector multiplexes to subscribers | [Value-based](./value-tree.md#_8-sharing) | [`share`](/operators-claude/multicasting/share), [`shareReplay`](/operators-claude/multicasting/shareReplay) |
| 9 | **Resubscription** | reacts to a terminal notification by subscribing to the same source again | [Value-based](./value-tree.md#_9-resubscription) | [`retry`](/operators-claude/error-handling/retry), [`repeat`](/operators-claude/utility/repeat) |
| | *sibling: Substitution* | reacts to an error by continuing with a replacement Observable | [Value-based](./value-tree.md#substitution) | [`catchError`](/operators-claude/error-handling/catchError) |
| 10 | **Creation** | no upstream source; values originate from non-observable material | [Value-based](./value-tree.md#_10-creation) | `of`, `from`, `interval`, `fromEvent`, `defer` |
| 11 | **Side-effect** | values and timing pass through; the operator only observes | [Value-based](./value-tree.md#_11-side-effect) | [`tap`](/operators-claude/utility/tap), [`finalize`](/operators-claude/utility/finalize) |
| 12 | **Interrogation** | consumes the sequence to answer one question, then completes | [Value-based](./value-tree.md#_12-interrogation) | [`every`](/operators-claude/conditional/every), [`find`](/operators-claude/filtering/find-findIndex), [`isEmpty`](/operators-claude/conditional/isEmpty), [`single`](/operators-claude/filtering/single) |
| 13 | **Injection** | source values pass through; extra values are inserted at an edge | [Value-based](./value-tree.md#_13-injection) | [`startWith`](/operators-claude/combination/startWith), [`endWith`](/operators-claude/utility/endWith-ignoreElements) |
| 14 | **Watchdog** | values pass through while a deadline is met | [Time-based](./time-tree.md#_14-watchdog) | [`timeout`](/operators-claude/error-handling/timeout) |
| 15 | **Reification** | moves the stream between the value channel and the notification channel | [Value-based](./value-tree.md#_15-reification) | [`materialize`](/operators-claude/utility/materialize-dematerialize) |

Operators that need no family of their own (`pairwise`, `partition`, `ignoreElements`, `timestamp`, `defaultIfEmpty`, `throwIfEmpty`, `onErrorResumeNext`, `observeOn`, `subscribeOn`, and more) are placed into these trees as presets or standalone axes. See [Placements](./value-tree.md#_16-placements-into-existing-trees).

## Value-based and time-based

The fifteen roots are presented on two pages, split by one question: **does a clock take part in the decision?**

- [**Value-based Trees**](./value-tree.md) cover the families whose decision moment is defined by values, counts, sequence position, a notifier, or the end of the sequence. Nothing in their invariants mentions time.
- [**Time-based Trees**](./time-tree.md) cover the families where a duration or clock is part of the invariant or of the axis being chosen: the five temporal families, the lossless rate limiters, the watchdog, and the places where time appears as an axis inside other families.

The temporal page also carries the test that separates rate limiting from timing. Under a sustained fire-hose of input, `throttle`, `audit` and `sample` emit a steady bounded drip; `debounce` emits nothing until the input pauses; `delay` reproduces the fire-hose later. That behavioral difference, not the shared `...Time` suffix, is the category boundary.

## Seven questions that recur everywhere

The strongest result of the catalog is that a small set of variant axes recurs across unrelated families. Learn an axis once and it explains dozens of operators.

```text
1. Determination        fixed vs dynamic/selected
2. Trigger kind         count / duration / notifier / predicate
3. Edge                 leading vs trailing
4. Latest-value memory  one-slot cache, conflation on purpose
5. Overlap policy       alongside / queue / kill previous / drop newcomer
6. Input form           static creation / *With / *All / *Map
7. Empty policy         silent / default / error
```

Most operators are one root behavior plus two or three answers to these questions. The [Legend](./legend.md#the-seven-recurring-axes) defines each axis; the [Decision Trees](./decision-trees.md) ask them in order.

## What the trees reclassify

Seeing operators as coordinates moves a few of them out of the folder the official documentation keeps them in:

- **`expand` is flattening, not transformation.** It subscribes to inner Observables and feeds its output back as input, which is `mergeMap` plus a recursion variant.
- **`iif` is source selection, not creation.** Its values come from the two given Observables, which fails the creation invariant.
- **`race` is source selection, not combining.** It keeps one whole source and drops the rest: one slot total, not one slot per source.
- **`concatMap(f)` is `mergeMap(f, 1)`.** Queueing is the alongside policy with a concurrency limit of one.
- **`reduce` is `scan` plus `last`**, and `toArray`, `count`, `min` and `max` are presets of the accumulation function.
- **`shareReplay(n)` is `share` with a ReplaySubject connector** and particular reset defaults. The classic "source never unsubscribes" pitfall is the coordinate `resetOnRefCountZero: false` made visible.
- **`first()` is `take(1)` plus `throwIfEmpty()`.** The two differ in exactly one coordinate, the empty policy, which also separates `find` from `first(p)` and `last` from `takeLast(1)`.

## A worked example

A search box: wait until typing pauses for 300 ms, then request, and abandon the previous request when a new one starts.

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

Each operator in the pipe is one tree, one invariant, and a handful of chosen variants. Nothing has to be memorised as a special case.

## Pages in this section

| Page | Use it when |
|---|---|
| [Legend](./legend.md) | You need the notation: how to read a tree, coordinates, temporal glyphs, the seven axes |
| [Value-based Trees](./value-tree.md) | You are choosing or comparing operators that decide by values, counts, position, or completion |
| [Time-based Trees](./time-tree.md) | A clock is involved: rate limiting, debouncing, delaying, timing out, batching by time |
| [Decision Trees](./decision-trees.md) | You have a requirement and want to walk from it to an operator one question at a time |
| [A4 Edition](./a4-edition.md) | You want the whole classification as printable tables |

The same model drives an interactive explorer at [netxpert.ch/rxjs-operator-trees](https://netxpert.ch/rxjs-operator-trees/): a family explorer, a concrete-behavior selector, reverse lookup by operator name, the recurring-axes view, and two-layer temporal diagrams in which the five timing families and `bufferTime` run the same input scenario side by side. Its source, including the trees as typed data, is at [github.com/hansschenker/rxjs-operator-trees](https://github.com/hansschenker/rxjs-operator-trees).

## Extending the catalog

The founding method gives a seven-step template for analysing any operator family. Use it when a new operator appears, or when an existing one seems to resist the trees.

1. **Identify the root behavior.** What does this operator fundamentally do to values over time?
2. **Identify the independent variant axes.** Which semantic choices can vary on their own?
3. **List the variants on each axis.**
4. **Generate the valid concrete behaviors** as root × axis × axis. Not every combination has to exist; the grid reveals implemented, invalid, redundant, and missing ones.
5. **Map each valid behavior to the official API**, name, overload, or configuration flag. The behavioral model comes first; the exported function is the implementation mapping.
6. **Add an explicit temporal visualisation where relevant**, showing timers, gates, and windows, not only values.
7. **Describe the chosen behavior with the policies**: source, trigger, value, cardinality, time, concurrency, cancellation, termination. On this site that is the operator page.

If a configuration option changes the qualitative behavior of an operator, the trees treat it as a variant, not as a tuning parameter. Throttle's `leading` and `trailing` decide which values survive and where they are emitted, so they are an axis.
