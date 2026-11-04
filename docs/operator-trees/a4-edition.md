# Operator Trees — A4 Edition

<style>
@media print {
  .VPNav, .VPLocalNav, .VPSidebar, .VPDocFooter, .VPDocAsideOutline, .aside { display: none !important; }
  .VPDoc .container, .VPDoc .content, .VPDoc .content-container { max-width: 100% !important; padding: 0 !important; }
  .vp-doc table { font-size: 9pt; }
  .vp-doc h2 { page-break-after: avoid; }
}
</style>

Print this page with the browser (Ctrl+P / Cmd+P); the site chrome is hidden in print and the tables fit two A4 pages.

## The fifteen root behaviors

| Root | Invariant in one line | Variant axes | Operators |
|---|---|---|---|
| 1. Selection | outputs are source values, unchanged, in order, a sub-sequence | criterion · polarity · anchor · empty policy | `take` `takeLast` `skip` `skipLast` `filter` `takeWhile` `skipWhile` `takeUntil` `skipUntil` `distinct` `distinctUntilChanged` `distinctUntilKeyChanged` `first` `last` `elementAt` `ignoreElements` `defaultIfEmpty` `throwIfEmpty` |
| 1b. Source Selection | several whole sources compete, exactly one survives | decision · input form | `race` `raceWith` `iif` |
| 2. Transformation | 1:1 cardinality, source timing, pure projection of (value, index) | projection target | `map` `mapTo`* `pluck`* `timestamp` `timeInterval` |
| 3. Accumulation | holds an accumulator; every value transitions it; outputs derive from it | emission cadence · seed · accumulation function | `scan` `reduce` `toArray` `count` `max` `min` |
| 4. Grouping | every value routed into one or more groups unchanged; output emits the groups | materialization · partition rule · boundary trigger · determination · topology | `buffer` `bufferCount` `bufferTime` `bufferWhen` `bufferToggle` `window` `windowCount` `windowTime` `windowWhen` `windowToggle` `groupBy` `partition` `pairwise` |
| 5a. Rate-Limiting | values never transformed; output rate bounded under a fire-hose | sub-roots: throttle (duration · edge), audit (duration), sample (trigger) | `throttle` `throttleTime` `audit` `auditTime` `sample` `sampleTime` |
| 5b. Timing | values never transformed; a temporal policy decides *when* | sub-roots: debounce (silence duration), delay (displacement · subscription start) | `debounce` `debounceTime` `delay` `delayWhen` |
| 6. Flattening | each value maps to an inner Observable; outputs are inner values; inner lifetimes managed | overlap policy · concurrency limit · input form · state across inners · recursion | `mergeMap` `concatMap` `switchMap` `exhaustMap` `mergeAll` `concatAll` `switchAll` `exhaustAll` `mergeScan` `switchScan` `expand` `*MapTo`* |
| 7. Combining | several sources at once; one slot per source; slots filled unchanged | emission trigger · slot alignment · input form | `combineLatest` `withLatestFrom` `zip` `forkJoin` `combineLatestWith` `zipWith` `combineLatestAll` `zipAll` |
| 8. Sharing | at most one live source subscription; values multiplexed; a connector in between | connector/replay · connection · reset policies | `share` `shareReplay` `connectable` `connect` `publish`* `multicast`* `refCount`* |
| 9. Resubscription | reacts to a terminal; may subscribe to the *same* source again | terminal trigger · count · delay · counter reset | `retry` `repeat` `retryWhen`* `repeatWhen`* |
| 9b. Substitution | reacts to an error; continues with a *replacement* Observable | replacement | `catchError` `onErrorResumeNextWith` `onErrorResumeNext` |
| 10. Creation | no upstream source; values from non-observable material; lazy per subscriber | origin · termination preset · scheduling | `of` `from` `range` `generate` `interval` `timer` `animationFrames` `fromEvent` `fromEventPattern` `defer` `ajax` `fromFetch` `webSocket` `using` `EMPTY` `NEVER` `throwError` `scheduled` |
| 11. Side-effect | values and timing pass through unchanged; only observes | lifecycle hook | `tap` `finalize` |
| 12. Interrogation | consumes the sequence to answer one question; one answer, then completes, as early as possible | question · answer content | `every` `find` `findIndex` `isEmpty` `sequenceEqual` `single` |
| 13. Injection | all source values pass through; extra values inserted at a sequence edge | edge | `startWith` `endWith` |
| 14. Watchdog | values pass through while the deadline is met; deadline armed at subscribe and after values | scope · deadline · on breach | `timeout` `timeoutWith`* |
| 15. Reification | moves the stream between value channel and notification channel losslessly | direction | `materialize` `dematerialize` |
| — Meta-axis | execution-context scheduling, orthogonal to every family | — | `observeOn` `subscribeOn` |

\* deprecated

## The seven recurring axes

| Axis | Question | Variants |
|---|---|---|
| 1. Determination | decided up front, or computed per value/event? | fixed (`xxxTime`, literal) · dynamic/selected (`xxx(v => d$)`, selector, `defer`) |
| 2. Trigger kind | what kind of event marks the decision moment? | count · duration · notifier · predicate |
| 3. Edge | which end of the interval does the behavior act on? | leading `v------` · trailing `------v` · both `v------v` |
| 4. Latest-value memory | how much is remembered — one conflated slot? | one-slot cache (audit, debounce, sample, combineLatest, shareReplay(1)) vs queue memory (zip, concatMap) |
| 5. Overlap policy | new work during live work: both, wait, replace, drop? | alongside (merge) · queue (concat) · kill previous (switch, debounce) · drop newcomer (exhaust, throttle) |
| 6. Input form | which starting shape reaches this behavior? | static creation · `*With` · `*All` · `*Map` |
| 7. Empty policy | what does "produced nothing" mean at completion? | silent · default · error |

## Flattening: overlap policy × input form

| Overlap policy | project + flatten (`*Map`) | higher-order source (`*All`) | with state | temporal twin |
|---|---|---|---|---|
| alongside | `mergeMap(f, n?)` | `mergeAll(n?)` | `mergeScan` | — |
| queue | `concatMap` ≡ `mergeMap(f, 1)` | `concatAll` | — | — |
| kill previous | `switchMap` | `switchAll` | `switchScan` | `debounce` |
| drop newcomer | `exhaustMap` | `exhaustAll` | — | `throttle` (leading) |
| recursion | `expand(f, n?)` | — | — | — |

## The five temporal families

| Family | Category | Window restarts? | Who triggers? | What survives? | Glyph |
|---|---|---|---|---|---|
| throttle | rate-limiting | no | source value | edge-dependent | `v---` / `---v` / `v---v` |
| audit | rate-limiting | no | source value | latest, at close | `------v` |
| sample | rate-limiting | (no window) | independent clock/notifier | latest *new* | `---\|---\|---` |
| debounce | timing | yes | source value | latest, after full silence | `------v` |
| delay | timing | (no window) | source value | everything | `v---→---v` |

Lossless twins in Grouping: `bufferTime(t)` / `windowTime(t)` keep all values in batches; `auditTime(t)` ≈ `bufferTime(t)` + last of each batch.

## Trigger-kind matrix

| Family | count | duration | notifier | predicate |
|---|---|---|---|---|
| selection | `take(n)` | — (`takeUntil(timer(ms))`) | `takeUntil(n$)` | `takeWhile(p)` |
| buffer | `bufferCount(n)` | `bufferTime(ms)` | `buffer(n$)` | — |
| window | `windowCount(n)` | `windowTime(ms)` | `window(n$)` | — |
| sample | — | `sampleTime(ms)` | `sample(n$)` | — |

Empty cells are API holes; the notifier variant can always encode them.

## Input-form matrix

| Behavior | static creation | `*With` | `*All` | `*Map` |
|---|---|---|---|---|
| merge | `merge(a$, b$)` | `mergeWith(b$)` | `mergeAll()` | `mergeMap(f)` |
| concat | `concat(a$, b$)` | `concatWith(b$)` | `concatAll()` | `concatMap(f)` |
| zip | `zip(a$, b$)` | `zipWith(b$)` | `zipAll()` | — |
| combineLatest | `combineLatest([…])` | `combineLatestWith(b$)` | `combineLatestAll()` | — |
| race | `race(a$, b$)` | `raceWith(b$)` | — | — |
| forkJoin | `forkJoin([…])` | — | — | — |
| iif | `iif(c, a$, b$)` | — | — | — |

## Empty-policy pairs

| Silent | Default | Error | Same selection, one coordinate apart |
|---|---|---|---|
| `take(1)` | `first(p, default)` | `first()` | front anchor |
| `takeLast(1)` | `last(p, default)` | `last()` | back anchor |
| `find(p)` → `undefined` | — | `first(p)` | existential match |
| `elementAt(i, d)` | `elementAt(i, d)` | `elementAt(i)` | index |
| `reduce(f, seed)` | — | `reduce(f)` on empty | accumulation |
| `forkJoin` (any empty input) | — | — | combining |
| — | `defaultIfEmpty(d)` | `throwIfEmpty(f)` | standalone operators: `first()` ≡ `take(1)` + `throwIfEmpty()` |

## Temporal glyphs

| Glyph | Name | Meaning |
|---|---|---|
| `v------` | leading | emit, then window / suppression period |
| `------v` | trailing | window first, then emit |
| `v------v` | both edges | emit at front, window, emit latest at back |
| `---\|---\|---` | periodic | an independent clock decides when to look |
| `v---→---v` | displacement | the same value, delivered later |
| `v-----?` | dynamic boundary | a duration Observable decides where the window ends |

Reading rule: `Concrete Behavior = INVARIANT + (one variant per axis)`; operator names are mappings onto those coordinates.

[Back to the Operator Trees overview](./overview.md)
