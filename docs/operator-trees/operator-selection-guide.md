# Operator Selection Guide

Quick-reference decision tables for the most common "which operator should I use?" questions.

---

## I want to transform each value

| Goal | Operator |
|---|---|
| Change each value | `map` |
| Change each value to an Observable, run all concurrently | `mergeMap` |
| Change each value to an Observable, cancel previous | `switchMap` |
| Change each value to an Observable, queue sequentially | `concatMap` |
| Change each value to an Observable, ignore new while busy | `exhaustMap` |
| Accumulate values into a running total | `scan` |
| Accumulate and emit only the final result | `reduce` |
| Pair each value with the previous | `pairwise` |
| Group values by a key | `groupBy` |

---

## I want to filter values

| Goal | Operator |
|---|---|
| Keep values matching a condition | `filter` |
| Take the first N values | `take(N)` |
| Take the first value | `first()` |
| Take the last value | `last()` |
| Take values until a condition is false | `takeWhile` |
| Take values until another Observable emits | `takeUntil` |
| Skip the first N values | `skip(N)` |
| Skip values while a condition is true | `skipWhile` |
| Skip values until another Observable emits | `skipUntil` |
| Skip consecutive duplicate values | `distinctUntilChanged` |
| Skip ALL duplicate values (lifetime) | `distinct` |
| Get a single value by index | `elementAt` |
| Find the first value matching a condition | `find` |

---

## I want to control timing / rate limit

| Goal | Operator |
|---|---|
| Wait until quiet period after last event | `debounceTime` |
| Emit at most once per time window (first) | `throttleTime` |
| Emit the latest value at end of time window | `auditTime` |
| Emit the latest value at a regular interval | `sampleTime` |
| Emit immediately, then ignore for N ms | `throttleTime({ leading: true, trailing: false })` |
| Add a fixed delay before each value | `delay` |
| Variable delay based on a function | `delayWhen` |
| Dynamic debounce window | `debounce` |

---

## I want to combine multiple Observables

| Goal | Operator |
|---|---|
| Combine latest value from each source | `combineLatest` |
| Use latest value from another stream as context | `withLatestFrom` |
| Wait for all to complete, emit combined last values | `forkJoin` |
| Emit values from all sources as they arrive | `merge` |
| Run sources sequentially (one completes, next starts) | `concat` |
| Pair N-th value from each source | `zip` |
| Forward only the first source to emit | `race` |
| Prepend values before source emits | `startWith` |
| Append values after source completes | `endWith` |

---

## I want to handle errors

| Goal | Operator |
|---|---|
| Replace error with a fallback Observable | `catchError` |
| Re-subscribe on error | `retry` |
| Re-subscribe on error with delay/backoff | `retry({ delay: fn })` |
| Error if no value within time limit | `timeout` |
| Continue to next Observable on error | `onErrorResumeNext` |

---

## I want to buffer / batch values

| Goal | Operator |
|---|---|
| Collect into arrays by time | `bufferTime` |
| Collect into arrays by count | `bufferCount` |
| Collect into arrays using a signal | `bufferWhen` |
| Collect into arrays between open/close signals | `bufferToggle` |
| Inner Observable per time window | `windowTime` |
| Inner Observable per N values | `windowCount` |
| Inner Observable per signal | `windowWhen` |
| Collect everything into one array | `toArray` |

---

## I want to create an Observable

| Goal | Operator |
|---|---|
| From a fixed list of values | `of` |
| From an array, Promise, or iterable | `from` |
| Emit a number sequence | `range` |
| Emit at regular intervals | `interval` |
| Emit once after a delay | `timer(delay)` |
| Emit at intervals after a delay | `timer(delay, interval)` |
| From a DOM event | `fromEvent` |
| From a fetch/HTTP request | `fromFetch` / `ajax` |
| Never emit, never complete | `NEVER` |
| Complete immediately without emitting | `EMPTY` |
| Error immediately | `throwError` |
| Defer creation until subscribe | `defer` |
| Choose between two Observables | `iif` |
| Split one Observable into two | `partition` |

---

## I want to share / multicast

| Goal | Operator |
|---|---|
| Share one subscription among many consumers | `share` |
| Share + replay last value to late subscribers | `shareReplay(1)` |
| Share + replay N values | `shareReplay(N)` |
| Multicast with manual connect/disconnect | `connectable` |
| Multiple branches from one subscription | `connect` |

---

## I want to manage subscriptions

| Goal | Operator |
|---|---|
| Unsubscribe when a notifier emits | `takeUntil` |
| Run code when subscription ends (any reason) | `finalize` |
| Run side effects without changing values | `tap` |
| Convert Observable to Promise | `firstValueFrom` / `lastValueFrom` |

---

## I want to work with higher-order Observables

| Goal | Operator | Concurrency |
|---|---|---|
| Flatten, run all concurrently | `mergeAll` / `mergeMap` | Unbounded |
| Flatten, queue sequentially | `concatAll` / `concatMap` | 1 at a time |
| Flatten, cancel previous | `switchAll` / `switchMap` | Latest only |
| Flatten, ignore while busy | `exhaustAll` / `exhaustMap` | 1 at a time, drop |
| Flatten with concurrency limit | `mergeMap(fn, N)` | N at a time |

---

## I'm in Angular and need to…

| Goal | Solution |
|---|---|
| Unsubscribe on component destroy | `takeUntilDestroyed(destroyRef)` |
| Use Observable in template | `async` pipe or `toSignal()` |
| Convert Observable to Signal | `toSignal(obs$, { initialValue })` |
| Convert Signal to Observable | `toObservable(signal)` |
| Fetch data on route change | `switchMap` on `route.params` |
| Prevent double-submit | `exhaustMap` from submit Subject |
| Async form validation | `switchMap` inside `AsyncValidatorFn` |

---

## I'm building a…

### Search box
```
debounceTime(300) → distinctUntilChanged() → filter(q => q.length >= 2)
→ switchMap(search) → catchError(() => of([]))
```

### Auto-save
```
valueChanges → debounceTime(1000) → distinctUntilChanged(deepEqual)
→ switchMap(save) → catchError(handleError)
```

### Polling
```
timer(0, interval) → switchMap(fetch) → catchError(() => of(fallback))
```
or
```
fetch$ → repeat({ delay: interval }) → catchError(handleError)
```

### Loading state
```
source$ → map(data => ({ status: 'success', data }))
→ catchError(err => of({ status: 'error', error: err }))
→ startWith({ status: 'loading' })
```

### Form submission (no double-submit)
```
submit$ → exhaustMap(() => api.submit(form.value).pipe(
  catchError(handleError),
  finalize(() => loading$.next(false))
))
```

### WebSocket with reconnect
```
webSocket(url) → retry({ delay: exponentialBackoff }) → share()
```

### Component view model (Angular)
```
combineLatest({ data: data$, filter: filter$.pipe(startWith('')) })
→ map(vm => computeViewModel(vm)) → shareReplay(1)
```
