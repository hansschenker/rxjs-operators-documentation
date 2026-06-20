[← hansschenker.github.io](https://hansschenker.github.io)

# RxJS Operator Documentation Index

A comprehensive, formal documentation collection for RxJS operators following the eight-policy framework.

## About This Documentation

All operators are documented using a systematic eight-policy framework that includes:

1. **Operator Identity** - Classification, categorization, and TypeScript signatures
2. **Functional Specification** - Mathematical/functional transformation definition
3. **Marble Diagrams** - Visual temporal behavior representation
4. **Behavioral Characteristics** - Subscription, completion, error handling, and backpressure
5. **Type System Integration** - TypeScript type safety and inference
6. **Practical Examples** - Basic usage, common patterns, and edge cases
7. **Common Pitfalls** - Anti-patterns with corrections
8. **Related Operators** - Ecosystem context and alternatives

See [SKILL.md](./SKILL.md) for the complete documentation standard.

## Documentation Status

**Total Operators Documented**: 2 / 100+

**Last Updated**: 2025-01-XX

---

## Operators by Category

### Combination Operators
Operators that combine multiple Observables into a single Observable.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| [combineLatest](./operators/combination/combineLatest.md) | ✅ Complete | 3/5 | 5/5 | Combines latest values from all sources, emits on any change |
| withLatestFrom | 📝 Planned | - | - | Combines with latest from other sources, emits only on primary |
| zip | 📝 Planned | - | - | Combines sources by index position |
| forkJoin | 📝 Planned | - | - | Waits for all to complete, emits final values |
| merge | 📝 Planned | - | - | Flattens multiple observables into one |
| concat | 📝 Planned | - | - | Subscribes to observables sequentially |
| race | 📝 Planned | - | - | Mirrors first observable to emit |
| partition | 📝 Planned | - | - | Splits source into two based on predicate |

### Transformation Operators
Operators that transform emissions from source Observables.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| map | 📝 Planned | 1/5 | 5/5 | Apply projection function to each value |
| [mergeMap](./operators/transformation/mergeMap.md) | ✅ Complete | 4/5 | 5/5 | Projects to Observable, merges concurrently (flatMap) |
| switchMap | 📝 Planned | 4/5 | 5/5 | Projects to Observable, cancels previous |
| concatMap | 📝 Planned | 4/5 | 4/5 | Projects to Observable, queues sequentially |
| exhaustMap | 📝 Planned | 4/5 | 3/5 | Projects to Observable, ignores while active |
| scan | 📝 Planned | 3/5 | 4/5 | Accumulator function (like reduce but emits intermediates) |
| reduce | 📝 Planned | 2/5 | 3/5 | Accumulator function, emits final result only |
| pluck | 📝 Planned | 1/5 | 3/5 | Extract nested property (deprecated in RxJS 8) |
| mapTo | 📝 Planned | 1/5 | 2/5 | Map to constant value |
| expand | 📝 Planned | 5/5 | 2/5 | Recursively projects to Observables |
| groupBy | 📝 Planned | 4/5 | 3/5 | Group emissions by key into separate Observables |
| window | 📝 Planned | 4/5 | 2/5 | Branch out emissions into nested Observables |
| windowCount | 📝 Planned | 4/5 | 2/5 | Window by count |
| windowTime | 📝 Planned | 4/5 | 2/5 | Window by time |
| windowToggle | 📝 Planned | 5/5 | 2/5 | Window with opening/closing notifiers |
| windowWhen | 📝 Planned | 5/5 | 2/5 | Window with dynamic closing |
| buffer | 📝 Planned | 3/5 | 3/5 | Buffer emissions until notifier emits |
| bufferCount | 📝 Planned | 2/5 | 3/5 | Buffer by count |
| bufferTime | 📝 Planned | 2/5 | 3/5 | Buffer by time |
| bufferToggle | 📝 Planned | 4/5 | 2/5 | Buffer with opening/closing notifiers |
| bufferWhen | 📝 Planned | 4/5 | 2/5 | Buffer with dynamic closing |
| pairwise | 📝 Planned | 2/5 | 3/5 | Emit current and previous values as pair |
| toArray | 📝 Planned | 1/5 | 3/5 | Collect all emissions into array |

### Filtering Operators
Operators that selectively emit values from source Observable.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| filter | 📝 Planned | 1/5 | 5/5 | Emit values that pass predicate test |
| first | 📝 Planned | 2/5 | 4/5 | Emit first value (or first matching predicate) |
| last | 📝 Planned | 2/5 | 3/5 | Emit last value (or last matching predicate) |
| take | 📝 Planned | 1/5 | 5/5 | Emit first N values |
| takeLast | 📝 Planned | 2/5 | 3/5 | Emit last N values |
| takeUntil | 📝 Planned | 3/5 | 5/5 | Emit until notifier emits |
| takeWhile | 📝 Planned | 2/5 | 4/5 | Emit while predicate is true |
| skip | 📝 Planned | 1/5 | 3/5 | Skip first N values |
| skipLast | 📝 Planned | 2/5 | 2/5 | Skip last N values |
| skipUntil | 📝 Planned | 3/5 | 3/5 | Skip until notifier emits |
| skipWhile | 📝 Planned | 2/5 | 3/5 | Skip while predicate is true |
| distinct | 📝 Planned | 3/5 | 3/5 | Emit only unique values |
| distinctUntilChanged | 📝 Planned | 2/5 | 5/5 | Emit only when different from previous |
| distinctUntilKeyChanged | 📝 Planned | 2/5 | 4/5 | Emit when specified key changes |
| elementAt | 📝 Planned | 1/5 | 2/5 | Emit value at specific index |
| single | 📝 Planned | 2/5 | 2/5 | Emit single value (error if more/less) |
| ignoreElements | 📝 Planned | 1/5 | 2/5 | Ignore all values, only forward completion/error |
| sample | 📝 Planned | 3/5 | 3/5 | Emit most recent value when notifier emits |
| sampleTime | 📝 Planned | 2/5 | 3/5 | Emit most recent value at intervals |
| throttle | 📝 Planned | 3/5 | 3/5 | Emit then ignore for duration from notifier |
| throttleTime | 📝 Planned | 2/5 | 4/5 | Emit then ignore for time duration |
| audit | 📝 Planned | 3/5 | 3/5 | Ignore then emit latest after notifier |
| auditTime | 📝 Planned | 2/5 | 4/5 | Ignore then emit latest after duration |
| debounce | 📝 Planned | 3/5 | 3/5 | Emit after silence period from notifier |
| debounceTime | 📝 Planned | 2/5 | 5/5 | Emit after silence period (time) |

### Error Handling Operators
Operators for handling errors in Observable sequences.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| catchError | 📝 Planned | 3/5 | 5/5 | Catch errors and return recovery Observable |
| retry | 📝 Planned | 2/5 | 4/5 | Retry failed Observable sequence |
| retryWhen | 📝 Planned | 4/5 | 3/5 | Retry with custom retry logic |
| throwError | 📝 Planned | 1/5 | 4/5 | Create Observable that errors immediately |
| onErrorResumeNext | 📝 Planned | 3/5 | 2/5 | Continue with next Observable on error |

### Utility Operators
Utility operators for various purposes.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| tap | 📝 Planned | 2/5 | 5/5 | Perform side effects without affecting stream |
| delay | 📝 Planned | 2/5 | 4/5 | Delay emissions by time |
| delayWhen | 📝 Planned | 4/5 | 2/5 | Delay emissions by Observable |
| timeout | 📝 Planned | 3/5 | 4/5 | Error if no emission within time |
| timeoutWith | 📝 Planned | 4/5 | 3/5 | Switch to fallback if no emission |
| finalize | 📝 Planned | 2/5 | 4/5 | Execute callback on completion or error |
| repeat | 📝 Planned | 2/5 | 3/5 | Repeat Observable sequence |
| repeatWhen | 📝 Planned | 4/5 | 2/5 | Repeat with custom logic |
| subscribeOn | 📝 Planned | 3/5 | 3/5 | Control scheduler for subscription |
| observeOn | 📝 Planned | 3/5 | 3/5 | Control scheduler for emissions |
| materialize | 📝 Planned | 4/5 | 2/5 | Convert to Notification objects |
| dematerialize | 📝 Planned | 4/5 | 2/5 | Convert from Notification objects |
| timestamp | 📝 Planned | 2/5 | 3/5 | Attach timestamp to emissions |
| timeInterval | 📝 Planned | 2/5 | 2/5 | Emit time between emissions |

### Conditional Operators
Operators that emit based on conditions.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| defaultIfEmpty | 📝 Planned | 2/5 | 3/5 | Emit default if source is empty |
| every | 📝 Planned | 2/5 | 2/5 | Emit true if all values pass predicate |
| find | 📝 Planned | 2/5 | 3/5 | Emit first value matching predicate |
| findIndex | 📝 Planned | 2/5 | 2/5 | Emit index of first match |
| isEmpty | 📝 Planned | 2/5 | 2/5 | Emit true if source is empty |
| sequenceEqual | 📝 Planned | 3/5 | 2/5 | Compare sequences for equality |

### Mathematical/Aggregate Operators
Operators that perform mathematical operations on sequences.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| count | 📝 Planned | 1/5 | 3/5 | Count total emissions |
| max | 📝 Planned | 2/5 | 2/5 | Emit maximum value |
| min | 📝 Planned | 2/5 | 2/5 | Emit minimum value |

### Creation Operators
Operators that create new Observables.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| of | 📝 Planned | 1/5 | 5/5 | Create Observable from values |
| from | 📝 Planned | 2/5 | 5/5 | Convert array/promise/iterable to Observable |
| fromEvent | 📝 Planned | 2/5 | 5/5 | Create Observable from DOM events |
| fromEventPattern | 📝 Planned | 3/5 | 2/5 | Create from arbitrary event API |
| interval | 📝 Planned | 1/5 | 4/5 | Emit sequence at intervals |
| timer | 📝 Planned | 2/5 | 4/5 | Emit after delay, optionally repeat |
| range | 📝 Planned | 1/5 | 3/5 | Emit range of numbers |
| defer | 📝 Planned | 3/5 | 3/5 | Create Observable on subscription |
| iif | 📝 Planned | 3/5 | 3/5 | Subscribe to one of two Observables based on condition |
| generate | 📝 Planned | 4/5 | 2/5 | Generate sequence with loop-like logic |
| EMPTY | 📝 Planned | 1/5 | 3/5 | Observable that completes immediately |
| NEVER | 📝 Planned | 1/5 | 2/5 | Observable that never emits or completes |
| ajax | 📝 Planned | 3/5 | 4/5 | Create Observable from AJAX request |

### Multicasting Operators
Operators for sharing execution among multiple subscribers.

| Operator | Status | Cognitive Load | Usage Frequency | Description |
|----------|--------|----------------|-----------------|-------------|
| share | 📝 Planned | 4/5 | 5/5 | Multicast with automatic reference counting |
| shareReplay | 📝 Planned | 4/5 | 5/5 | Multicast with replay buffer |
| multicast | 📝 Planned | 5/5 | 2/5 | Multicast using Subject |
| publish | 📝 Planned | 4/5 | 2/5 | Multicast using vanilla Subject |
| publishBehavior | 📝 Planned | 4/5 | 2/5 | Multicast using BehaviorSubject |
| publishLast | 📝 Planned | 4/5 | 2/5 | Multicast using AsyncSubject |
| publishReplay | 📝 Planned | 4/5 | 2/5 | Multicast using ReplaySubject |
| refCount | 📝 Planned | 4/5 | 2/5 | Auto-connect when subscribers present |
| connect | 📝 Planned | 4/5 | 2/5 | Connect to multicast source |

---

## Learning Paths

### Beginner Path
Start here if you're new to RxJS:

1. **Creation**: of, from, fromEvent
2. **Basic Transformation**: map, filter, tap
3. **Basic Combination**: combineLatest
4. **Timing**: debounceTime, throttleTime
5. **Completion**: take, takeUntil, first

### Intermediate Path
After mastering basics:

1. **Higher-Order**: mergeMap, switchMap, concatMap
2. **Advanced Filtering**: distinctUntilChanged, audit, sample
3. **Error Handling**: catchError, retry
4. **Combination**: withLatestFrom, zip, forkJoin
5. **Buffering**: buffer, bufferTime, window

### Advanced Path
For complex reactive patterns:

1. **Specialized Higher-Order**: exhaustMap, expand
2. **Custom Retry Logic**: retryWhen
3. **Grouping**: groupBy
4. **Multicasting**: share, shareReplay
5. **Advanced Timing**: delayWhen, timeoutWith, repeatWhen
6. **Schedulers**: subscribeOn, observeOn

---

## Quick Reference

### Most Commonly Used (Top 20)
1. map
2. filter
3. tap
4. switchMap
5. mergeMap
6. combineLatest
7. debounceTime
8. distinctUntilChanged
9. catchError
10. takeUntil
11. share
12. shareReplay
13. take
14. of
15. from
16. fromEvent
17. retry
18. first
19. concatMap
20. throttleTime

### By Flattening Strategy
- **Concurrent (Parallel)**: mergeMap - All inner Observables run simultaneously
- **Sequential (Ordered)**: concatMap - Wait for each to complete before next
- **Cancelling (Latest)**: switchMap - Cancel previous when new starts
- **Blocking (Ignore)**: exhaustMap - Ignore new while one is active

### By Rate Limiting Strategy
- **Debounce**: debounceTime - Wait for silence
- **Throttle**: throttleTime - Emit first, ignore rest for duration
- **Audit**: auditTime - Ignore, then emit latest
- **Sample**: sampleTime - Emit latest at intervals

---

## Documentation Conventions

### Status Icons
- ✅ **Complete** - Fully documented with all 8 policies
- 🔄 **In Progress** - Documentation started
- 📝 **Planned** - Scheduled for documentation
- ⚠️ **Deprecated** - Operator deprecated in RxJS 7+

### Cognitive Load Scale (1-5)
- **1** - Simple concept, minimal prerequisites
- **2** - Straightforward with basic understanding
- **3** - Moderate complexity, some prerequisites
- **4** - Advanced concept, multiple prerequisites
- **5** - Expert-level, deep understanding required

### Usage Frequency Scale (1-5)
- **1** - Rarely used, specialized cases
- **2** - Occasionally used
- **3** - Moderately common
- **4** - Frequently used
- **5** - Essential, used in most projects

---

## Contributing

To add documentation for a new operator:

1. Follow the template in [SKILL.md](./SKILL.md)
2. Save to appropriate category folder: `operators/[category]/[operatorName].md`
3. Update this index with status, cognitive load, and usage frequency
4. Ensure all 8 policies are comprehensively covered
5. Include minimum 3 practical examples
6. Document at least 2 common pitfalls

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed guidelines.

---

## Related Resources

- **RxJS Official Documentation**: [https://rxjs.dev](https://rxjs.dev)
- **ReactiveX Documentation**: [http://reactivex.io](http://reactivex.io)
- **RxJS GitHub Repository**: [https://github.com/ReactiveX/rxjs](https://github.com/ReactiveX/rxjs)
- **Learn RxJS**: [https://www.learnrxjs.io](https://www.learnrxjs.io)
- **RxJS Marbles**: [https://rxmarbles.com](https://rxmarbles.com)

---

**Last Updated**: December 2024  
**Documentation Standard**: Eight-Policy Framework v1.0  
**RxJS Version**: 7.x (compatible with 8.x)
