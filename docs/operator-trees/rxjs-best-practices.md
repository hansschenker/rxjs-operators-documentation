# RxJS Best Practices

Consolidated rules and principles distilled from across this documentation. Use as a code review checklist or onboarding reference.

---

## 1. Subscription Management

- **Always unsubscribe** from infinite/hot Observables (`interval`, `fromEvent`, `webSocket`, `Subject`). Use `takeUntil(destroy$)`, `takeUntilDestroyed()` (Angular 16+), or `Subscription.add()`.
- **HTTP Observables are safe** — they complete after one response; no explicit cleanup needed unless you need to cancel in-flight requests.
- **Never subscribe inside `subscribe`**. Use flattening operators (`mergeMap`, `switchMap`, etc.) instead.
- **Use `async` pipe in Angular templates** — auto-subscribes and auto-cleans up.

```typescript
// ❌ Nested subscribe
outer$.subscribe(v => inner$(v).subscribe(result => use(result)));

// ✅ Flat with mergeMap
outer$.pipe(mergeMap(v => inner$(v))).subscribe(use);
```

---

## 2. Cold vs Hot

- **A cold Observable subscribed twice = executed twice** (two HTTP requests, two timers). Add `shareReplay(1)` to any cold Observable used in multiple places.
- **`shareReplay(1)` replays to late subscribers** — use it for HTTP responses and computed values.
- **`share()` has no replay** — use it for live event streams where late subscribers don't need past values.
- **`shareReplay({ bufferSize: 1, refCount: false })`** creates a permanent cache that never resets. Only use intentionally.

---

## 3. Flattening Operator Selection

| When | Use |
|---|---|
| Read data, cancel previous on new trigger | `switchMap` |
| Independent items, parallel OK | `mergeMap(proj, concurrentN)` |
| Order matters / sequential writes | `concatMap` |
| Ignore while active (form submit) | `exhaustMap` |
| Accumulate async state | `mergeScan` / `switchScan` |

- **`switchMap` for reads, `exhaustMap` for writes** — the single most important flattening rule.
- **Always specify `concurrent` in `mergeMap`** when processing a bounded collection. Default (unlimited) risks rate-limit violations and memory spikes.

---

## 4. Error Handling

- **`catchError` belongs inside the flattening operator's projection**, not outside it. Outside kills the stream on the first error; inside only kills that inner Observable.
- **`retry` must come before `catchError`** in the pipeline — otherwise `catchError` handles the error before `retry` sees it.
- **Always return an Observable from `catchError`** — `of(fallback)`, `EMPTY`, or `throwError(() => err)`.
- **Isolate inner Observable errors** in `mergeMap`/`switchMap` to prevent one failure from stopping the entire stream.

```typescript
// ❌ One error kills all future processing
ids$.pipe(
  mergeMap(id => api.get(id)),
  catchError(() => EMPTY)  // stream ends here forever
);

// ✅ Per-item isolation
ids$.pipe(
  mergeMap(id =>
    api.get(id).pipe(catchError(() => EMPTY))  // skips failed item
  )
);
```

---

## 5. Operators to Reach For First

| Task | Operator |
|---|---|
| Transform each value | `map` |
| Filter values | `filter` |
| HTTP / async operations | `switchMap` or `mergeMap` |
| Combine latest from N streams | `combineLatest({...})` |
| One-shot parallel requests | `forkJoin({...})` |
| Cache HTTP response | `shareReplay(1)` |
| Side effects without changing value | `tap` |
| Always run cleanup | `finalize` |
| Convert to Promise | `firstValueFrom` / `lastValueFrom` |
| Prevent duplicate emissions | `distinctUntilChanged` |

---

## 6. Marble Diagram Mental Model

When debugging, draw the marble diagram:
- `-` = time passing, `a`-`z` = values, `|` = complete, `#` = error
- Ask: "What does each operator do to the timing and values?"
- `switchMap`: previous inner cancelled on new outer emission
- `concatMap`: inner queued until previous completes
- `mergeMap`: all inners run concurrently
- `exhaustMap`: new outer dropped while inner is active

---

## 7. Performance

- **Debounce high-frequency events** (`mousemove`, `scroll`, `resize`, `input`) before expensive work.
- **Coalesce `combineLatest` bursts** with `debounceTime(0)` to avoid redundant renders during initialization.
- **`distinctUntilChanged` before `switchMap`** — don't re-fetch when the value hasn't changed.
- **Move DOM updates to animation frames** with `observeOn(animationFrameScheduler)`.

---

## 8. TypeScript

- **Prefer `OperatorFunction<T, R>`** for operators that change the type; `MonoTypeOperatorFunction<T>` for operators that preserve it.
- **Use dictionary form for `combineLatest`** — `combineLatest({ a: a$, b: b$ })` — for named keys instead of index-based destructuring.
- **Type the Subject explicitly** — `new Subject<UserEvent>()` not `new Subject()`.
- **Use `Observable<never>` for `ignoreElements`** output — signals to TypeScript that no values flow through.

---

## 9. Custom Operators

- **Extract repeated `pipe()` sequences** into named operators when they appear 3+ times or have a meaningful domain name.
- **Always forward `error` and `complete`** in custom `new Observable()` operators — missing either causes leaks or swallowed failures.
- **Return a teardown function** from `new Observable()` — omitting it causes resource leaks on unsubscription.

```typescript
// ❌ Missing teardown
new Observable(sub => {
  const id = setInterval(() => sub.next(Date.now()), 1000);
  // interval keeps running after unsubscribe!
});

// ✅ Teardown cleans up
new Observable(sub => {
  const id = setInterval(() => sub.next(Date.now()), 1000);
  return () => clearInterval(id);
});
```

---

## 10. Patterns to Avoid

| Anti-pattern | Problem | Fix |
|---|---|---|
| `subscribe()` inside `subscribe()` | Creates nested, unmanaged subscriptions | Use flattening operators |
| `catchError` outside flattening | Kills the entire stream on first error | Move inside the projection |
| `mergeMap` without `concurrent` on large collections | Memory/rate-limit spike | Add `concurrent` parameter |
| `lastValueFrom` on infinite Observable | Hangs forever | Use `firstValueFrom` or add `take(1)` |
| Cold Observable used twice without `shareReplay` | Duplicate execution | Add `shareReplay(1)` |
| `switchMap` for writes/mutations | Cancels in-flight mutations | Use `concatMap` or `exhaustMap` |
| `retry` after `catchError` | `retry` never sees errors | Put `retry` before `catchError` |
| Missing `takeUntil` on hot stream | Memory leak | Always terminate hot Observables |
| `new Subject()` without type | Loose typing | `new Subject<SpecificType>()` |
| `BehaviorSubject` exposed directly | Callers can call `.next()` | Expose via `.asObservable()` |

---

## 11. Code Review Checklist

When reviewing RxJS code, check for:

- [ ] Every `subscribe()` on a hot/infinite Observable has a corresponding cleanup
- [ ] `catchError` is inside flattening projections (not outside)
- [ ] Cold Observable used in 2+ places has `shareReplay(1)`
- [ ] `mergeMap` on a collection has a `concurrent` limit
- [ ] Mutations use `concatMap`/`exhaustMap`, not `switchMap`
- [ ] `lastValueFrom` sources are bounded (complete or have `take`)
- [ ] Custom `new Observable()` operators return a teardown function
- [ ] `retry` appears before `catchError` in the pipeline
- [ ] High-frequency DOM events are throttled/debounced before expensive work
- [ ] `BehaviorSubject`/`ReplaySubject` exposed as `Observable` via `asObservable()`
