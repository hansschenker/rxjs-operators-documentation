# Operator Trees: Legend

Everything on the tree pages uses the same small notation. This page defines it once: the shape of a tree, how coordinates are written, the temporal glyphs, the seven recurring axes, and how the vocabulary lines up with the eight-policy operator pages.

## Reading a tree

```text
ROOT BEHAVIOR                      the family's name
│
├── INVARIANT                      what is ALWAYS true for every member
│   ├── invariant line
│   └── invariant line
│
└── VARIANTS                       independent dimensions of choice
    ├── 1. Axis name               numbered in the order coordinates are written
    │   ├── variant                one choice on this axis      API name or flag
    │   └── variant
    └── 2. Axis name
        └── ...
```

- **INVARIANT** is the family's identity. A behavior that fails one invariant line belongs to a different family, however similar its name. The invariants are what distinguish `throttle` from `debounce`; their variant branches are nearly identical.
- **VARIANTS** are independent. A concrete behavior picks exactly one variant per axis, and the axes do not constrain each other unless the tree says so with a note such as `(alongside/queue only)`.
- **Axis numbers** fix the order in which coordinates are written on the mapping lines below each tree.
- **Sub-axes** refine a variant that itself has structure. Uniqueness in Selection is one variant on the criterion axis and has its own small tree: comparison scope, compared key, memory reset.
- **Sibling roots** share the geometry of a family but differ in the invariant. Source Selection sits next to Selection, Substitution next to Resubscription. They get their own tree under the family they resemble.
- **Placements** are operators that need no family of their own. They are presets or standalone axes of an existing tree, listed in a placement table rather than drawn.
- **Where an axis sits is itself information.** Throttle offers the edge as a variant; audit and debounce have the trailing edge baked into their invariant. The same choice can be a variant in one family and an invariant in another.

## Coordinates

A concrete behavior is written as one variant per axis, in axis order, joined with `×`:

```text
take(n)                    count      × keep × front        complete silently
takeLast(n)                count      × keep × back         complete silently
first(p?, default?)        predicate/position × keep × front   default | error
bufferCount(n, m), m < n   count      × fixed × overlapping
```

Conventions used on the mapping lines and in the matrices:

```text
a × b × c      one variant per axis, in the numbered axis order
{a | b}        the operator covers several variants of that axis, chosen per call
               by an argument or a configuration flag
—              an empty cell: a coordinate with no RxJS name (a hole in the API)
≡              behaviorally equivalent       concatMap(f) ≡ mergeMap(f, 1)
≈              equivalent up to details      auditTime(t) ≈ bufferTime(t) + last value of each batch
(deprecated)   the name still ships in RxJS 7 but its coordinate is reached
               another way; the operator page carries the migration
```

When two operators look interchangeable, write out both coordinates. They usually differ in exactly one, and that one is the thing to check before swapping them: `first()` and `take(1)` differ only in the empty policy, `withLatestFrom` and `combineLatest` only in the emission trigger.

## Temporal glyphs

Ordinary marble diagrams show the observable layer: values occurring over time. Timing families also need the **operator-control layer**: the timer, gate, silence interval, or window that decides when a value may pass. The tree pages draw that layer with a small set of glyphs.

```text
v------      leading        emit at the opening edge, then a window
------v      trailing       a window first, then emit at the closing edge
v------v     both edges     emit at the front, then the latest again at the back
---|---|     periodic       an independent clock decides when to look
   v   v                    (a snapshot is taken at each tick)
v---→---v    displacement   the same value, delivered later
v-----?      dynamic        a duration Observable decides where the boundary is
```

The suffix `Time` only says that a parameter is a duration or period. What that duration means is decided by the root behavior:

```text
throttleTime   ignore for        a suppression window opened by a value
debounceTime   silent for        a restarting silence timer
auditTime      fixed window      a non-restarting window, emit at close
sampleTime     periodic take     an independent clock takes snapshots
delay          shifted by        pure displacement, nothing dropped
```

On the operator pages the observable layer uses the usual marble notation:

```text
-        one unit of time            a,b,c    emitted values
|        successful completion       #        error
^        subscription point          (ab)     synchronous emissions
```

## The seven recurring axes

A small set of variant axes recurs across unrelated families. Each entry gives the question the axis asks and its variants; the family pages show where each one appears.

| Axis | Question it asks | Variants |
|---|---|---|
| **1. Determination** | Is the parameter decided once, statically, or computed per value or event? | fixed · dynamic/selected |
| **2. Trigger kind** | What kind of event marks the decision moment? | count · duration · notifier · predicate |
| **3. Edge** | Which end of the interval does the behavior act on? | leading · trailing · both |
| **4. Latest-value memory** | How much is remembered: one conflated slot, or everything? | one-slot cache (conflating) · queue (buffering) |
| **5. Overlap policy** | New work arrives while old work is live. What happens? | alongside · queue · kill previous · drop newcomer |
| **6. Input form** | Which starting shape reaches this behavior? | static creation · `*With` · `*All` · `*Map` |
| **7. Empty policy** | What does "produced nothing" mean at completion? | silent · default · error |

What each axis explains, in one line:

- **Determination.** Every `xxxTime` operator is the fixed variant of `xxx`; dynamic subsumes fixed, so `throttle(() => timer(ms))` is `throttleTime(ms)`. The axis reaches creation (`defer`) and teardown (`share({ resetOnRefCountZero: () => timer(1000) })`).
- **Trigger kind.** Notifier is the universal trigger: `buffer(interval(ms))` is `bufferTime(ms)` and `takeUntil(timer(ms))` is the missing `takeTime`. Predicate needs the values, which is why it exists in Selection but not in Sample.
- **Edge.** Trailing requires waiting: `takeLast` buffers until completion, `endWith` can never fire on an errored source, throttle's trailing edge holds a pending value. Leading acts immediately, which is why `take` can complete early.
- **Latest-value memory.** `audit`, `debounce`, `sample`, `combineLatest`, `withLatestFrom`, `shareReplay(1)` keep one slot and are lossy but always current. `zip` and `concatMap` keep unbounded queues. The axis predicts memory behavior under a fast producer.
- **Overlap policy.** The same four answers appear in flattening (`mergeMap`, `concatMap`, `switchMap`, `exhaustMap`) and in timing: `debounce` kills the previous pending value, `throttle` drops newcomers. Only *kill previous* unsubscribes live work; only *drop newcomer* never starts it.
- **Input form.** The root word names the behavior, the suffix names the entry point: `merge(a$, b$)`, `mergeWith(b$)`, `mergeAll()`, `mergeMap(f)` are one behavior reached four ways. Missing cells (`raceAll`, an operator form of `forkJoin`, `iifWith`) are holes.
- **Empty policy.** `defaultIfEmpty` and `throwIfEmpty` are the axis as standalone operators, which gives the identity `first() ≡ take(1) + throwIfEmpty()`.

The emblematic matrix, one axis spanning four families:

```text
              count            duration          notifier        predicate
selection     take(n)          —                 takeUntil(n$)   takeWhile(p)
buffer        bufferCount(n)   bufferTime(ms)    buffer(n$)      —
window        windowCount(n)   windowTime(ms)    window(n$)      —
sample        —                sampleTime(ms)    sample(n$)      —
```

Every `—` is a behavior RxJS does not name. There is no `bufferWhile(p)` and no `takeTime(ms)`; both are composed.

## Trees and the eight policies

The trees classify; the operator pages specify. The founding method names eight policy dimensions for the specification half. They map onto the sections of every operator page on this site roughly as follows:

| Policy dimension | Question | Operator page section |
|---|---|---|
| Source | What does the operator subscribe to, and when? | Behavioral Characteristics → Subscription |
| Trigger | Which event causes an output? | Functional Specification → Transformation |
| Value | What is emitted, and of which type? | Functional Specification → Output · Type System Integration |
| Cardinality | How many outputs per input: 1:1, 1:n, n:1? | Functional Specification → Invariants |
| Time | When are outputs delivered relative to inputs? | Marble Diagram · Behavioral Characteristics |
| Concurrency | How many inner subscriptions may be live at once? | Behavioral Characteristics → Backpressure |
| Cancellation | What is unsubscribed, and when? | Behavioral Characteristics → Subscription |
| Termination | How do completion and error propagate? | Behavioral Characteristics → Completion semantics · Error handling |

A tree coordinate tells you which page to open. The page tells you what that coordinate does.

## Vocabulary

| Term | Meaning |
|---|---|
| Root behavior | What an operator family fundamentally does to values over time |
| Invariant | A property every member of the family always has |
| Variant axis | One independent behavioral dimension that can vary |
| Variant | One choice on a variant axis |
| Concrete behavior | One valid combination of variants across the axes |
| Mapping | The RxJS name, overload, or configuration flag that implements a concrete behavior |
| Hole | A coordinate with no mapping; the behavior must be composed |
| Sibling root | A tree that shares a family's geometry but has a different invariant |
| Placement | An operator that is a preset or standalone axis of an existing tree |
| Fire-hose test | What happens under sustained input: bounded drip (rate limiting), silence until quiet (debounce), or shifted copy (delay) |
| Lossy / lossless | Rate limiting that keeps one value per window versus batching every value |
| Conflation | Keeping only the latest value in a one-slot cache, on purpose |
| Overlap policy | What happens to new work while previous work is live |
| Operator-control layer | The timers, gates, and windows a timing operator maintains, drawn above the value timeline |
