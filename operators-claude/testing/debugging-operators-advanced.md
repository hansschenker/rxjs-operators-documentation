# Debugging RxJS Streams — Advanced Techniques

> **Cognitive Load**: 4/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
> **Teaching Sequence**: After basic `tap` debugging — systematic, production-grade stream diagnostics

---

## The Debugging Mental Model

RxJS pipelines fail in ways that differ from synchronous code:

| Problem type | Symptom | Root cause pattern |
|---|---|---|
| Silent no-op | Observable created, nothing happens | No subscriber, or unsubscribed too early |
| Missing emissions | Stream emits less than expected | Wrong operator choice, filtering too early |
| Wrong timing | Emissions arrive out of order | Scheduler mismatch, async/sync confusion |
| Memory leak | Performance degrades over time | Subscriptions never unsubscribed |
| Error swallowed | No output, no error | `catchError` returning `EMPTY`, or `onErrorResumeNext` |
| Cold/hot confusion | Duplicate side effects | Multiple subscribers to a cold Observable |

---

## Technique 1: Labeled tap Middleware

Build a reusable `debug` operator that can be toggled off in production.

```typescript
import { tap, MonoTypeOperatorFunction } from 'rxjs';

const DEBUG = process.env.NODE_ENV !== 'production';

function debug<T>(label: string, color = '#666'): MonoTypeOperatorFunction<T> {
  if (!DEBUG) return source$ => source$; // no-op in production

  return tap<T>({
    subscribe:  ()  => console.groupCollapsed(`%c[${label}] subscribed`, `color:${color}`),
    next:       v   => console.log(`[${label}] next:`, v),
    error:      e   => console.error(`[${label}] error:`, e),
    complete:   ()  => console.log(`[${label}] complete`),
    unsubscribe: () => console.log(`[${label}] unsubscribed`),
    finalize:   ()  => console.groupEnd(),
  });
}

// Usage:
userInput$.pipe(
  debug('raw-input', '#2196f3'),
  debounceTime(300),
  debug('debounced', '#4caf50'),
  switchMap(q => searchApi(q)),
  debug('results', '#ff9800'),
).subscribe(renderResults);
```

### Timestamped Debug Operator

```typescript
import { tap, timestamp, map, pipe } from 'rxjs';
import { MonoTypeOperatorFunction } from 'rxjs';

function debugTimed<T>(label: string): MonoTypeOperatorFunction<T> {
  let lastMs = Date.now();

  return tap<T>(value => {
    const now = Date.now();
    const delta = now - lastMs;
    lastMs = now;
    console.log(`[${label}] +${delta}ms`, value);
  });
}

stream$.pipe(
  debugTimed('source'),
  debounceTime(300),
  debugTimed('after-debounce'),  // shows actual debounce delay
).subscribe();
```

---

## Technique 2: Subscription Lifecycle Tracing

Diagnose "subscribed but never emits" and "subscribed multiple times" issues.

```typescript
import { defer, tap, finalize } from 'rxjs';

function traceSubscription<T>(
  source$: Observable<T>,
  label: string,
): Observable<T> {
  let subCount = 0;

  return defer(() => {
    subCount++;
    const id = subCount;
    console.log(`[${label}#${id}] SUBSCRIBED (total active: ${subCount})`);

    return source$.pipe(
      finalize(() => {
        subCount--;
        console.log(`[${label}#${id}] FINALIZED (total active: ${subCount})`);
      }),
    );
  });
}

// Detect cold Observable being subscribed multiple times:
const sharedData$ = traceSubscription(http.get('/api/data'), 'http');

// If you see multiple SUBSCRIBED logs, you have a multicasting problem:
sharedData$.subscribe(a => useA(a));
sharedData$.subscribe(b => useB(b));
// [http#1] SUBSCRIBED — two HTTP calls!
// [http#2] SUBSCRIBED — need shareReplay(1)
```

---

## Technique 3: materialize for Full Notification Capture

`materialize()` converts the Observable notification stream into a stream of `Notification` objects — useful for capturing the full history including errors as values.

```typescript
import { materialize, dematerialize } from 'rxjs/operators';
import { Notification } from 'rxjs';

// Capture entire stream history for post-hoc analysis
function captureStream<T>(source$: Observable<T>): Observable<T> {
  const history: Notification<T>[] = [];

  return source$.pipe(
    materialize(),
    tap(notification => history.push(notification)),
    dematerialize(),
    finalize(() => {
      console.table(history.map(n => ({
        kind: n.kind,           // 'N' | 'E' | 'C'
        value: n.value,
        error: n.error?.message,
      })));
    }),
  );
}

// Non-destructive error logging — log error then re-throw
function logErrors<T>(label: string): MonoTypeOperatorFunction<T> {
  return source$ => source$.pipe(
    materialize(),
    tap(n => {
      if (n.kind === 'E') console.error(`[${label}] stream error:`, n.error);
    }),
    dematerialize(),
  );
}

riskyStream$.pipe(
  logErrors('risky'),
  catchError(err => fallback$),
).subscribe();
```

---

## Technique 4: Detecting Memory Leaks

```typescript
import { Subject, interval } from 'rxjs';
import { takeUntil, scan, tap } from 'rxjs/operators';

// Global subscription counter for leak detection
let activeSubscriptions = 0;

function trackSubscriptions<T>(label: string): MonoTypeOperatorFunction<T> {
  return source$ => new Observable<T>(observer => {
    activeSubscriptions++;
    console.log(`[leak-detector] +1 → ${activeSubscriptions} active (${label})`);

    const sub = source$.subscribe(observer);

    return () => {
      sub.unsubscribe();
      activeSubscriptions--;
      console.log(`[leak-detector] -1 → ${activeSubscriptions} active (${label})`);
    };
  });
}

// Typical leak: subscribing in a loop without cleanup
function badComponent() {
  // ❌ Each render creates a new subscription, none are cleaned up
  interval(1000).pipe(
    trackSubscriptions('timer'),
  ).subscribe(updateUI);
}

function goodComponent(destroy$: Subject<void>) {
  // ✅ Subscription cleaned up on destroy
  interval(1000).pipe(
    trackSubscriptions('timer'),
    takeUntil(destroy$),
  ).subscribe(updateUI);
}

// Check for leaks periodically during development
if (process.env.NODE_ENV === 'development') {
  setInterval(() => {
    if (activeSubscriptions > 20) {
      console.warn(`⚠️ Potential leak: ${activeSubscriptions} active subscriptions`);
    }
  }, 5000);
}
```

---

## Technique 5: Operator-Level Performance Profiling

```typescript
import { tap } from 'rxjs/operators';
import { MonoTypeOperatorFunction } from 'rxjs';

function measureOperator<T>(label: string): MonoTypeOperatorFunction<T> {
  const times: number[] = [];
  let lastEmit = 0;

  return tap<T>(() => {
    const now = performance.now();
    if (lastEmit > 0) times.push(now - lastEmit);
    lastEmit = now;

    if (times.length % 100 === 0 && times.length > 0) {
      const avg = times.reduce((a, b) => a + b, 0) / times.length;
      const max = Math.max(...times);
      console.log(`[${label}] emissions: ${times.length}, avg gap: ${avg.toFixed(1)}ms, max: ${max.toFixed(1)}ms`);
    }
  });
}

// Find slow operators in a pipeline
source$.pipe(
  measureOperator('source'),
  heavyTransform(),
  measureOperator('after-transform'),   // compare gap widths to find bottleneck
  filter(meetsCondition),
  measureOperator('after-filter'),
).subscribe();
```

---

## Technique 6: RxJS Spy Integration Pattern

When `tap`-based debugging isn't enough, build a lightweight spy system:

```typescript
import { Observable, Subject } from 'rxjs';
import { tap, share } from 'rxjs/operators';

interface SpyEvent<T> {
  tag: string;
  kind: 'next' | 'error' | 'complete' | 'subscribe' | 'unsubscribe';
  value?: T;
  error?: unknown;
  timestamp: number;
}

class ObservableSpy {
  private events$ = new Subject<SpyEvent<unknown>>();
  readonly stream$ = this.events$.pipe(share());

  tag<T>(tagName: string): MonoTypeOperatorFunction<T> {
    return (source$: Observable<T>) => new Observable<T>(observer => {
      const emit = (kind: SpyEvent<T>['kind'], value?: T, error?: unknown) =>
        this.events$.next({ tag: tagName, kind, value, error, timestamp: Date.now() });

      emit('subscribe');
      const sub = source$.pipe(
        tap({
          next:     v => emit('next', v),
          error:    e => emit('error', undefined, e),
          complete: () => emit('complete'),
        })
      ).subscribe(observer);

      return () => {
        sub.unsubscribe();
        emit('unsubscribe');
      };
    });
  }
}

const spy = new ObservableSpy();

// Log all events from all tagged streams
spy.stream$.subscribe(event => {
  const { tag, kind, value, timestamp } = event;
  console.log(`[${new Date(timestamp).toISOString()}] ${tag} → ${kind}`, value ?? '');
});

// Apply to pipeline
userSearch$.pipe(
  spy.tag('search-input'),
  debounceTime(300),
  spy.tag('debounced'),
  switchMap(q => searchApi(q)),
  spy.tag('api-response'),
).subscribe(render);
```

---

## Technique 7: Diagnosing Cold vs Hot Confusion

```typescript
import { share, shareReplay, tap } from 'rxjs/operators';

// Symptom: side effect runs multiple times
// Diagnosis: cold Observable with multiple subscribers

const coldHttp$ = http.get('/api/data').pipe(
  tap(() => console.log('HTTP request fired')),  // should fire once
);

// ❌ Fires HTTP twice
coldHttp$.subscribe(a => useA(a));
coldHttp$.subscribe(b => useB(b));
// Console: "HTTP request fired" × 2

// ✅ Share the cold Observable — fires once
const hotHttp$ = coldHttp$.pipe(shareReplay(1));
hotHttp$.subscribe(a => useA(a));
hotHttp$.subscribe(b => useB(b));
// Console: "HTTP request fired" × 1

// Diagnostic helper: count subscriptions
function countSubscriptions<T>(source$: Observable<T>, label: string) {
  let count = 0;
  return source$.pipe(
    tap({ subscribe: () => console.warn(`[${label}] subscription #${++count}`) })
  );
}
```

---

## Technique 8: Error Source Tracing

```typescript
import { catchError, throwError } from 'rxjs';
import { tap } from 'rxjs/operators';

// Add stack context to errors for easier tracing
function traceErrors<T>(label: string): MonoTypeOperatorFunction<T> {
  return catchError(err => {
    const enhanced = new Error(
      `[${label}] caught: ${err?.message ?? err}`
    );
    enhanced.stack = `${enhanced.stack}\nCaused by: ${err?.stack ?? '(no stack)'}`;
    return throwError(() => enhanced);
  });
}

// Apply at each pipeline stage to pinpoint error origin
source$.pipe(
  traceErrors('source'),
  map(transform),
  traceErrors('after-transform'),
  switchMap(callApi),
  traceErrors('after-api'),
).subscribe({ error: e => console.error(e.stack) });
// Stack trace shows exactly which stage threw
```

---

## Common Debugging Mistakes

```typescript
// ❌ INCORRECT — adding tap after subscribe (never runs)
source$.subscribe(value => {
  // tap here would be inside subscribe — correct placement is in pipe()
});
source$.pipe(tap(console.log)); // not subscribed — tap never fires

// ✅ CORRECT — tap belongs in the pipe chain before subscribe
source$.pipe(
  tap(v => console.log('value:', v)),
).subscribe();


// ❌ INCORRECT — assuming no output means no emissions
source$.pipe(
  filter(v => v > 100), // if all values ≤ 100, nothing passes
).subscribe(console.log); // silence ≠ error

// ✅ CORRECT — add tap before the filter to verify source is emitting
source$.pipe(
  tap(v => console.log('before filter:', v)),
  filter(v => v > 100),
  tap(v => console.log('after filter:', v)),
).subscribe(console.log);


// ❌ INCORRECT — debugging an Observable that was never subscribed
const debug$ = source$.pipe(tap(console.log)); // cold — not yet running
// ... nothing logged because debug$ is never subscribed

// ✅ CORRECT — subscribe to observe
const debug$ = source$.pipe(tap(console.log));
debug$.subscribe(); // now tap fires
```

---

## Debugging Checklist

```
Stream emits nothing?
  □ Is it subscribed? (console.log(typeof stream$.subscribe))
  □ Check filter conditions — add tap before each filter
  □ Is source cold and producing values? Add tap at source
  □ Is takeUntil / take(0) completing it immediately?

Stream errors silently?
  □ Add .subscribe({ error: e => console.error(e) })
  □ Check for catchError(() => EMPTY) swallowing errors
  □ Check onErrorResumeNext (errors are always silent)

Side effect fires multiple times?
  □ Multiple subscribers to a cold Observable — add shareReplay(1)
  □ Component re-renders re-subscribing — add takeUntil(destroy$)

Wrong values emitted?
  □ Add tap at each stage to see values transform
  □ Check distinctUntilChanged — may suppress expected duplicates
  □ Check switchMap — cancels in-flight requests on new emission
```

---

## Related Operators

- **`tap`** — primary debugging tool; inspect without modifying the stream
- **`materialize`** / **`dematerialize`** — capture full notification history including errors as values
- **`finalize`** — guaranteed side effect on termination; pairs with tap for lifecycle tracing
- **`timeInterval`** / **`timestamp`** — add timing metadata to emissions
- **`TestScheduler`** — virtual time for deterministic timing tests
- **`share`** / **`shareReplay`** — fix cold/hot multicasting issues uncovered during debugging
