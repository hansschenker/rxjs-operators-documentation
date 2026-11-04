# Time-based Operator Trees

The [Operator Trees](./overview.md) describe every RxJS family as one **INVARIANT** branch (what every member always does) plus **VARIANT** axes (independent choices, one variant per axis). This page collects the trees in which **time is part of the behavior itself**: either a clock or a duration belongs to the family's invariant (throttle, debounce, delay, timeout), or the axis being chosen is a temporal one (a duration trigger, a delay before resubscribing, a scheduler). Families that decide by **values** — selection, transformation, accumulation, combining — live on the companion page, [Value-based Operator Trees](./value-tree.md).

Two rules from the founding method apply to everything below.

**The fire-hose test.** Ask what the operator does under sustained, faster-than-you-can-handle input. If the output is a *bounded drip* (at most one emission per window or period), the family is **rate-limiting**. If the output is not bounded but merely *placed in time* (silence until quiescence, or the same fire-hose shifted later), the family is **timing**. The shared `...Time` suffix says nothing about this boundary; the invariant does.

**Show the operator-control layer.** Ordinary marbles show only values. A timing explanation must also show the timers, gates, silence intervals and windows the operator maintains — the *operator-control layer* — otherwise leading and trailing edges, restarting timers and sampling ticks are invisible. The glyphs used on this page are explained in the [legend](./legend.md#temporal-glyphs):

```text
v------      leading        emit, then window
------v      trailing       window, then emit
v------v     both edges
---|---|     periodic       an independent clock decides
v---→---v    displacement   same value, delivered later
```

The interactive version of this page renders each of the five temporal families as a two-layer diagram (values plus timers), all over the **same input scenario**, at [netxpert.ch/rxjs-operator-trees](https://netxpert.ch/rxjs-operator-trees/) — hash routes `#/family/throttle`, `#/family/audit`, `#/family/sample`, `#/family/debounce`, `#/family/delay`, and `#/family/grouping` for the lossless limiters.

---

## Rate-Limiting

```text
RATE-LIMITING — bounded output under sustained input
│
├── INVARIANT
│   ├── values are never transformed
│   └── the output rate is bounded: at most one emission per
│       window / period, no matter how fast the source fires
│
└── SUB-ROOTS
    ├── throttle     suppression window, edge choice        → steady drip
    ├── audit        non-restarting window, trailing emit   → steady drip
    └── sample       independent trigger takes snapshots    → steady drip
```

The three sub-roots share the invariant and differ by **who opens the window and which edge emits**. Each is its own tree below.

### 5.1 Throttle

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
```

Glyph: `v------` (default), `------v`, or `v------v` — 2 durations × 3 edges = **6 concrete throttle behaviors**; the default is `{ leading: true, trailing: false }`.

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `throttleTime(ms)` | fixed × leading | [throttleTime](/operators-claude/filtering/throttleTime) |
| `throttleTime(ms, { trailing: true })` | fixed × trailing | [throttleTime](/operators-claude/filtering/throttleTime) |
| `throttleTime(ms, { leading: true, trailing: true })` | fixed × both | [throttleTime](/operators-claude/filtering/throttleTime) |
| `throttle(v => d$)` | dynamic × leading | [throttle](/operators-claude/rate-limiting/throttle-debounce) |
| `throttle(v => d$, { trailing: true })` | dynamic × trailing | [throttle](/operators-claude/rate-limiting/throttle-debounce) |

**What the tree explains**

- `leading` and `trailing` decide *which values survive and where they emit relative to the window*. They are behavioral policies, not tuning parameters — the tree makes them visible where the config object hides them.
- Throttle offers the edge as a **choice**; audit and debounce have trailing baked into their invariant. Where an axis sits in the tree (VARIANTS vs INVARIANT) is itself informative.
- A suppressed value never restarts the window — that line of the invariant is what separates throttle from debounce.

### 5.2 Audit

```text
AUDIT
│
├── INVARIANT
│   ├── a source value opens a non-restarting window
│   ├── the latest value during the window is remembered
│   └── that latest value emits when the window closes    ------v
│
└── VARIANTS
    └── 1. Duration
        ├── fixed        auditTime(ms)
        └── dynamic      audit(v => duration$)
```

Glyph: `------v` — always trailing.

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `auditTime(ms)` | fixed | [auditTime](/operators-claude/rate-limiting/auditTime-sampleTime) |
| `audit(v => d$)` | dynamic | [audit](/operators-claude/rate-limiting/sample-audit) |

**What the tree explains**

- Audit is throttle's window with the **latest-value memory** axis added: a one-slot cache, overwritten by newer values, read when the window closes. Intermediate values are conflated on purpose.
- The trailing edge is a *consequence* of the invariant ("emit the latest at close"), not a choice — which is why audit has no edge axis.

### 5.3 Sample

```text
SAMPLE
│
├── INVARIANT
│   ├── a trigger, independent of source values, requests a snapshot
│   ├── the snapshot is the latest NEW source value
│   └── no new value since the last trigger → no emission
│
└── VARIANTS
    └── 1. Trigger
        ├── periodic clock       sampleTime(period)    ---|---|---
        └── external notifier    sample(notifier$)
```

Glyph: `---|---|---` with `v` under each tick that found a new value.

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `sampleTime(period)` | periodic clock | [sampleTime](/operators-claude/rate-limiting/auditTime-sampleTime) |
| `sample(notifier$)` | external notifier | [sample](/operators-claude/rate-limiting/sample-audit) |

**What the tree explains**

- Sample is the only rate limiter whose window is **not opened by a source value**: the trigger is independent, so a quiet source simply produces no snapshots.
- "Latest **new** value" is the invariant line that distinguishes sample from a plain periodic read of a cache: a tick with nothing new emits nothing.
- The trigger axis has no `predicate` variant by design — the trigger must be independent of values, so the value-level trigger kind cannot appear here (see [Trigger kind](./legend.md#the-seven-recurring-axes)).

---

## Timing

```text
TIMING — temporal placement, no bounded cadence
│
├── INVARIANT
│   ├── values are never transformed
│   └── a temporal policy decides WHEN delivery happens
│
└── SUB-ROOTS
    ├── debounce     restarting silence, trailing emit      → silence until quiescence
    └── delay        pure temporal displacement             → fire-hose, shifted
```

Under a fire-hose, `debounce` emits **nothing** until the input pauses, and `delay` reproduces the fire-hose later. Neither bounds the output rate — that is why they are not rate limiters, whatever their suffix suggests.

### 5.4 Debounce

```text
DEBOUNCE
│
├── INVARIANT
│   ├── every source value starts a silence timer
│   ├── the next value restarts the timer and replaces the pending value
│   └── the pending value emits after uninterrupted silence    ------v
│
└── VARIANTS
    └── 1. Silence duration
        ├── fixed        debounceTime(ms)
        └── dynamic      debounce(v => duration$)
```

Glyph: `------v` — the window *restarts* on every value, which the static glyph cannot show; the two-layer diagram does.

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `debounceTime(ms)` | fixed | [debounceTime](/operators-claude/filtering/debounceTime) |
| `debounce(v => d$)` | dynamic | [debounce](/operators-claude/rate-limiting/throttle-debounce) |

**What the tree explains**

- Debounce is the **kill-previous** overlap policy applied to a silence timer: a new value cancels the pending timer *and* the pending value. Throttle, by contrast, is **drop-newcomer**.
- "Silent for" is the right reading of its `Time` parameter — not "ignore for" (throttle) and not "fixed window" (audit).

### 5.5 Delay

```text
DELAY
│
├── INVARIANT
│   ├── every value survives — nothing competes, nothing is dropped
│   └── delivery is displaced later in time    v------→------v
│
└── VARIANTS
    ├── 1. Displacement
    │   ├── fixed relative duration     delay(ms)
    │   ├── fixed absolute date         delay(date)
    │   └── dynamic per value           delayWhen(v => duration$)
    └── 2. Subscription start
        ├── immediate
        └── after notifier              (subscriptionDelay — deprecated)
```

Glyph: `v---→---v` — the same value, later.

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `delay(ms)` | fixed relative | [delay](/operators-claude/utility/delay-delayWhen) |
| `delay(date)` | fixed absolute | [delay](/operators-claude/utility/delay-delayWhen) |
| `delayWhen(v => d$)` | dynamic per value | [delayWhen](/operators-claude/utility/delay-delayWhen) |

**What the tree explains**

- Delay is the only temporal family with **no memory and no competition**: every value survives, so it cannot bound a fire-hose — it only shifts it.
- `delayWhen` with per-value durations can **reorder** values; the invariant promises displacement, not order preservation.

---

## The five temporal families side by side

```text
              category        window restarts?  who triggers?     what survives?    glyph
throttle      rate-limiting   no                source value      edge-dependent    v--- / ---v / v---v
audit         rate-limiting   no                source value      latest, at close  ------v
sample        rate-limiting   (no window)       independent       latest new        ---|---|---
                                                clock/notifier                         v   v
debounce      timing          yes               source value      latest, after     ------v
                                                                  full silence
delay         timing          (no window)       source value      everything        v---→---v
```

The invariant branch is what distinguishes them; the variant branches are nearly identical (`fixed` vs `dynamic` duration everywhere). **The temporal families differ by invariant, not by variant.**

---

## Lossless rate limiting

`bufferTime(t)` and `windowTime(t)` emit at the same bounded cadence as the lossy limiters, but **batch every value instead of picking one**:

```text
auditTime(t)  ≈  bufferTime(t) + last value of each batch

lossy     rate-limiting    throttle / audit / sample    keep one, drop the rest
lossless  rate-limiting    bufferTime / windowTime      keep all, in batches
```

They nevertheless stay in the **Grouping** family, because they share its full coordinate system — materialization, partition rule, boundary trigger, determination, topology — with members that are not rate limiters at all (`groupBy`, `partition`). Time enters Grouping through the **duration** variant of the boundary-trigger axis:

```text
bufferTime(span)              duration × fixed    × contiguous
bufferTime(span, i), i < span duration × fixed    × overlapping
bufferTime(span, i), i > span duration × fixed    × gapped
```

The **second argument is the topology axis in disguise**: comparing it to the span yields contiguous, overlapping or gapped windows. `windowTime` mirrors the mapping 1:1 with inner Observables instead of arrays.

| Operator | Coordinates | Reference |
|---|---|---|
| `bufferTime(span, interval?)` | array × duration × fixed × topology by argument | [bufferTime](/operators-claude/transformation/bufferTime) |
| `windowTime(span, interval?)` | inner Observable × duration × fixed × topology by argument | [windowTime](/operators-claude/transformation/windowTime) |
| `bufferWhen(() => closing$)` | array × notifier × dynamic × contiguous | [bufferWhen](/operators-claude/transformation/bufferWhen-windowWhen) |

---

## 14. Watchdog

```text
WATCHDOG
│
├── INVARIANT
│   ├── values pass through unchanged while the deadline is met
│   └── a temporal deadline is armed at subscribe and after values
│
└── VARIANTS
    ├── 1. Scope
    │   ├── first value only     timeout({ first })
    │   └── every value          timeout({ each })
    │
    ├── 2. Deadline
    │   ├── relative duration    timeout(ms) / { first: ms, each: ms }
    │   └── absolute date        timeout(date) / { first: date }
    │
    └── 3. On breach
        ├── error                TimeoutError            (default)
        └── substitute           timeout({ with: () => fallback$ })
                                 (legacy: timeoutWith)
```

**Mapping**

| Operator | Coordinates | Reference |
|---|---|---|
| `timeout(ms)` | each × relative × error | [timeout](/operators-claude/error-handling/timeout) |
| `timeout({ first: ms })` | first × relative × error | [timeout](/operators-claude/error-handling/timeout) |
| `timeout({ each: ms, with: () => f$ })` | each × relative × substitute | [timeout](/operators-claude/error-handling/timeout) |
| `timeoutWith(ms, f$)` *(deprecated)* | each × relative × substitute | [timeoutWith](/operators-claude/error-handling/timeoutWith) |

**What the tree explains**

- The `with` variant connects Watchdog to **Substitution**: breaching a deadline substitutes a replacement Observable — `timeout({ with })` is [`catchError`](/operators-claude/error-handling/catchError) for *time* instead of for errors.
- Scope `first` vs `each` is Selection's front/everywhere **anchor** axis reappearing on the time dimension.

---

## Time on other axes

Time also appears as a *variant* inside families whose invariant is not temporal.

**Resubscription — delay before resubscribing.** `retry` and `repeat` carry a three-variant delay axis: none, fixed (`{ delay: ms }`), or dynamic (`{ delay: (error, count) => notifier$ }`). The dynamic variant is where exponential backoff lives. See [retry](/operators-claude/error-handling/retry) and [repeat](/operators-claude/utility/repeat); the deprecated notifier forms are [retryWhen](/operators-claude/error-handling/retryWhen) and [repeatWhen](/operators-claude/utility/repeatWhen).

**Creation — the clock origin.** `interval(ms)`, `timer(due, period?)` and `animationFrames()` are the *clock* variant of Creation's origin axis. `timer(due, period)` unifies two origins — a one-shot deadline that hands over to a clock — which is why `timer(0, ms)` is `interval(ms)` without the initial wait. See [interval](/operators-claude/creation/interval), [timer](/operators-claude/creation/timer), [animationFrames](/operators-claude/creation/animationFrames).

**Scheduling — a meta-axis.** `observeOn`, `subscribeOn` and `scheduled` choose the execution context in which notifications are delivered or subscriptions made. The axis is orthogonal to every family (it is the Policies framework's *Time* dimension), so it never appears inside a tree. See [observeOn / subscribeOn](/operators-claude/utility/observeOn-subscribeOn), [scheduled](/operators-claude/creation/scheduled) and [Schedulers](/operators-claude/utility/schedulers).

**Timing metadata.** `timestamp` and `timeInterval` are Transformation presets (`map` to `{ value, timestamp }` and `{ value, interval }`); they read the clock but do not alter timing. See [timeInterval / timestamp](/operators-claude/utility/timeInterval-timestamp).

**The missing `takeTime`.** The trigger-kind matrix has an empty cell: Selection has count, notifier and predicate triggers but no duration trigger. The behavior is composed — `takeUntil(timer(ms))` — because the notifier is the universal trigger that count and duration can always be encoded as. See [takeUntil](/operators-claude/filtering/takeUntil).

```text
              count            duration          notifier        predicate
selection     take(n)          —                 takeUntil(n$)   takeWhile(p)
buffer        bufferCount(n)   bufferTime(ms)    buffer(n$)      —
window        windowCount(n)   windowTime(ms)    window(n$)      —
sample        —                sampleTime(ms)    sample(n$)      —
```

---

## The ...Time suffix

Every `xxxTime` operator is the **fixed** variant of `xxx` on the recurring **Determination** axis (fixed vs dynamic/selected):

```text
throttle duration      throttleTime(ms)      throttle(v => d$)
audit duration         auditTime(ms)         audit(v => d$)
debounce silence       debounceTime(ms)      debounce(v => d$)
delay displacement     delay(ms)             delayWhen(v => d$)
buffer boundary        bufferTime(ms)        bufferWhen(() => c$)
```

- **Dynamic subsumes fixed**: `throttle(() => timer(ms))` ≡ `throttleTime(ms)`. The fixed variants are ergonomic presets, not separate behaviors.
- The suffix says *that* a duration is supplied, not *what it means*. The root behavior decides: throttle → *ignore for*, debounce → *silent for*, audit → *fixed window*, sample → *periodic take*.
- The axis reaches beyond values: `defer` applies it to creation, and `share({ resetOnRefCountZero: () => timer(1000) })` applies it to teardown — a grace period instead of a boolean.

---

## Timing operators as flattening policies

The recurring **overlap policy** axis — what happens when new work arrives while previous work is live — answers *alongside, queue, kill previous, drop newcomer*. In Flattening the work is an inner subscription; in the temporal families it is a timer holding a pending value. The temporal operators therefore **are** flattening policies applied to timer inners (up to completion details):

```text
debounceTime(t)         ≈  switchMap(v => timer(t).pipe(map(() => v)))
throttleTime(t) leading ≈  exhaustMap(v =>
                              concat(of(v), timer(t).pipe(ignoreElements())))
```

`switchMap : debounce :: exhaustMap : throttle` — one analogy that makes four operators predict each other. Cancellation lives on this axis: *kill previous* ([switchMap](/operators-claude/transformation/switchMap), debounce) is the only policy that unsubscribes live work; *drop newcomer* ([exhaustMap](/operators-claude/transformation/exhaustMap), throttle leading) the only one that never starts it.

---

**See also:** [Overview](./overview.md) · [Legend](./legend.md) · [Value-based Operator Trees](./value-tree.md) · [Decision Trees](./decision-trees.md) · [A4 Edition](./a4-edition.md) · [Rate-Limiting Guide](./rate-limiting-guide.md) · [Flattening Strategy Guide](./flattening-strategy-guide.md)
