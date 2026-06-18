# RxJS Operator Documentation

Complete reference for all RxJS pipeable operators, organized by category. Each page covers the operator's description, signature, parameters, return type, marble diagram, practical examples, common pitfalls, and related operators.

**Total operators documented**: 97

---

## Transformation Operators (26)

Operators that transform, project, or reshape the values emitted by a source Observable.

| Operator | Description |
|----------|-------------|
| [buffer](transformation/buffer.md) | Collect values into arrays, flushed by a notifier Observable |
| [bufferCount](transformation/bufferCount.md) | Collect values into fixed-size arrays |
| [bufferTime](transformation/bufferTime.md) | Collect values into arrays emitted on a time interval |
| [bufferToggle](transformation/bufferToggle.md) | Collect values into arrays opened and closed by Observables |
| [bufferWhen](transformation/bufferWhen.md) | Collect values into arrays with a dynamic closing selector |
| [concatMap](transformation/concatMap.md) | Project each value to an Observable, subscribe sequentially |
| [concatMapTo](transformation/concatMapTo.md) | *(Deprecated)* Map every value to the same inner Observable, subscribed sequentially |
| [exhaustMap](transformation/exhaustMap.md) | Project to inner Observable, ignoring new values while inner is active |
| [expand](transformation/expand.md) | Recursively project each value to an Observable and merge results |
| [groupBy](transformation/groupBy.md) | Group source values by a key into GroupedObservable streams |
| [map](transformation/map.md) | Apply a projection function to each emitted value |
| [mapTo](transformation/mapTo.md) | *(Deprecated)* Map every emitted value to a constant value |
| [mergeMap](transformation/mergeMap.md) | Project each value to an Observable, merge all concurrently |
| [mergeMapTo](transformation/mergeMapTo.md) | *(Deprecated)* Map every value to the same inner Observable, merged concurrently |
| [mergeScan](transformation/mergeScan.md) | Like scan but the accumulator returns an Observable |
| [pairwise](transformation/pairwise.md) | Emit pairs of consecutive values as `[previous, current]` |
| [reduce](transformation/reduce.md) | Apply an accumulator, emit only the final accumulated value |
| [scan](transformation/scan.md) | Apply an accumulator, emit each intermediate accumulated value |
| [switchMap](transformation/switchMap.md) | Project each value to an Observable, cancelling the previous inner Observable |
| [switchMapTo](transformation/switchMapTo.md) | *(Deprecated)* Map every value to the same inner Observable, switching on each |
| [switchScan](transformation/switchScan.md) | Like scan but the accumulator returns an Observable, switching on each |
| [window](transformation/window.md) | Collect values into nested Observables, split by a notifier |
| [windowCount](transformation/windowCount.md) | Collect values into nested Observables of fixed count |
| [windowTime](transformation/windowTime.md) | Collect values into nested Observables on a time interval |
| [windowToggle](transformation/windowToggle.md) | Collect values into nested Observables opened and closed by Observables |
| [windowWhen](transformation/windowWhen.md) | Collect values into nested Observables with a dynamic closing selector |

---

## Filtering Operators (27)

Operators that selectively pass or suppress values emitted by a source Observable.

| Operator | Description |
|----------|-------------|
| [audit](filtering/audit.md) | Emit the most recent value after a duration determined by another Observable |
| [auditTime](filtering/auditTime.md) | Emit the most recent value after a fixed duration |
| [debounce](filtering/debounce.md) | Emit a value only after a duration determined by another Observable |
| [debounceTime](filtering/debounceTime.md) | Emit a value only after a fixed quiet period |
| [distinct](filtering/distinct.md) | Emit only values that have never been emitted before |
| [distinctUntilChanged](filtering/distinctUntilChanged.md) | Emit only when the current value is different from the previous |
| [distinctUntilKeyChanged](filtering/distinctUntilKeyChanged.md) | Emit only when a specified key's value changes |
| [elementAt](filtering/elementAt.md) | Emit only the nth value emitted |
| [filter](filtering/filter.md) | Emit only values that pass a predicate function |
| [find](filtering/find.md) | Emit the first value matching a predicate, then complete |
| [findIndex](filtering/findIndex.md) | Emit the index of the first value matching a predicate, then complete |
| [first](filtering/first.md) | Emit only the first value (or first matching a predicate) |
| [ignoreElements](filtering/ignoreElements.md) | Suppress all `next` emissions, passing only `error` and `complete` |
| [last](filtering/last.md) | Emit only the last value (or last matching a predicate) |
| [sample](filtering/sample.md) | Emit the most recent value whenever a notifier Observable emits |
| [sampleTime](filtering/sampleTime.md) | Emit the most recent value on a fixed time interval |
| [single](filtering/single.md) | Emit exactly one value matching a predicate; error otherwise |
| [skip](filtering/skip.md) | Skip the first n emitted values |
| [skipLast](filtering/skipLast.md) | Skip the last n emitted values |
| [skipUntil](filtering/skipUntil.md) | Skip values until a notifier Observable emits |
| [skipWhile](filtering/skipWhile.md) | Skip values while a predicate is true |
| [take](filtering/take.md) | Emit only the first n values |
| [takeLast](filtering/takeLast.md) | Emit only the last n values |
| [takeUntil](filtering/takeUntil.md) | Complete when a notifier Observable emits |
| [takeWhile](filtering/takeWhile.md) | Complete when a predicate becomes false |
| [throttle](filtering/throttle.md) | Emit a value then ignore subsequent values for a duration determined by another Observable |
| [throttleTime](filtering/throttleTime.md) | Emit a value then ignore subsequent values for a fixed duration |

---

## Combination Operators (14)

Operators that join multiple Observables or prepend/append values to a stream.

| Operator | Description |
|----------|-------------|
| [combineLatestAll](combination/combineLatestAll.md) | Flatten an Observable-of-Observables using `combineLatest` semantics |
| [combineLatestWith](combination/combineLatestWith.md) | Combine with other Observables, emitting when any source emits (latest values) |
| [concatAll](combination/concatAll.md) | Flatten an Observable-of-Observables sequentially |
| [concatWith](combination/concatWith.md) | Append other Observables sequentially after the source completes |
| [endWith](combination/endWith.md) | Append specified values at the end of the sequence |
| [exhaustAll](combination/exhaustAll.md) | Flatten an Observable-of-Observables, ignoring inner Observables while one is active |
| [mergeAll](combination/mergeAll.md) | Flatten an Observable-of-Observables by merging all concurrently |
| [mergeWith](combination/mergeWith.md) | Merge other Observables into the source stream |
| [raceWith](combination/raceWith.md) | Mirror the first Observable to emit, ignoring the rest |
| [startWith](combination/startWith.md) | Prepend specified values to the beginning of the sequence |
| [switchAll](combination/switchAll.md) | Flatten an Observable-of-Observables, switching to each new inner Observable |
| [withLatestFrom](combination/withLatestFrom.md) | Combine with other Observables, emitting only when the source emits |
| [zipAll](combination/zipAll.md) | Flatten an Observable-of-Observables using `zip` semantics |
| [zipWith](combination/zipWith.md) | Combine with other Observables index-by-index |

---

## Error Handling Operators (4)

Operators that intercept and recover from errors in an Observable sequence.

| Operator | Description |
|----------|-------------|
| [catchError](error-handling/catchError.md) | Catch errors and recover with a fallback Observable |
| [onErrorResumeNextWith](error-handling/onErrorResumeNextWith.md) | Continue with next Observables regardless of errors or completion |
| [retry](error-handling/retry.md) | Resubscribe on error, optionally with a count and delay |
| [retryWhen](error-handling/retryWhen.md) | *(Deprecated)* Resubscribe based on a notifier Observable |

---

## Utility Operators (26)

Operators for side effects, timing, scheduling, multicasting, and aggregation.

| Operator | Description |
|----------|-------------|
| [connect](utility/connect.md) | Create a multicast Observable using a `Subject` factory |
| [count](utility/count.md) | Count the number of emissions; emit total on completion |
| [defaultIfEmpty](utility/defaultIfEmpty.md) | Emit a default value if the source completes without emitting |
| [delay](utility/delay.md) | Delay all emissions by a fixed duration or until a given Date |
| [delayWhen](utility/delayWhen.md) | Delay each emission by a duration determined per-value |
| [dematerialize](utility/dematerialize.md) | Convert `Notification` objects back into `next`/`error`/`complete` signals |
| [every](utility/every.md) | Emit `true` if all values pass a predicate; `false` on first failure |
| [finalize](utility/finalize.md) | Execute a callback on completion, error, or unsubscription |
| [isEmpty](utility/isEmpty.md) | Emit `true` if the source completes without emitting; `false` on first value |
| [materialize](utility/materialize.md) | Wrap `next`/`error`/`complete` signals into `Notification` objects |
| [max](utility/max.md) | Emit the maximum value after the source completes |
| [min](utility/min.md) | Emit the minimum value after the source completes |
| [observeOn](utility/observeOn.md) | Re-emit values on a specified scheduler |
| [repeat](utility/repeat.md) | Resubscribe after completion, optionally with a count and delay |
| [repeatWhen](utility/repeatWhen.md) | *(Deprecated)* Resubscribe based on a notifier Observable |
| [sequenceEqual](utility/sequenceEqual.md) | Emit `true` if two sequences emit the same values in the same order |
| [share](utility/share.md) | Multicast using a `Subject`, with configurable reset behaviour |
| [shareReplay](utility/shareReplay.md) | Multicast and replay the last N values to new subscribers |
| [subscribeOn](utility/subscribeOn.md) | Schedule the subscription on a specified scheduler |
| [tap](utility/tap.md) | Perform side effects for each emission without transforming values |
| [throwIfEmpty](utility/throwIfEmpty.md) | Throw an error if the source completes without emitting |
| [timeInterval](utility/timeInterval.md) | Wrap each value with the elapsed time since the previous emission |
| [timeout](utility/timeout.md) | Error if a value is not received within a specified duration |
| [timeoutWith](utility/timeoutWith.md) | *(Deprecated)* Switch to a fallback Observable on timeout |
| [timestamp](utility/timestamp.md) | Wrap each value with the time it was emitted |
| [toArray](utility/toArray.md) | Collect all values into an array, emitting it on completion |

---

## Deprecated Operators

The following operators are deprecated and will be removed in a future version of RxJS. Migration guidance is provided in each operator's documentation page.

| Deprecated Operator | Replacement |
|---------------------|-------------|
| `concatMapTo` | `concatMap(() => inner$)` |
| `mapTo` | `map(() => value)` |
| `mergeMapTo` | `mergeMap(() => inner$)` |
| `repeatWhen` | `repeat({ delay: () => notifier$ })` |
| `retryWhen` | `retry({ delay: (err) => notifier$ })` |
| `switchMapTo` | `switchMap(() => inner$)` |
| `timeoutWith` | `timeout({ with: () => fallback$ })` |
