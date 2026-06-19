# RxJS DevTools, Spy Libraries & Custom Debug Operators

A practical guide to tooling for Observable stream inspection: RxJS Spy, custom operator instrumentation, browser DevTools integration, and production-safe diagnostics.

---

## The Debugging Tooling Landscape

| Tool | Type | Best for |
|---|---|---|
| `tap` + `console` | Built-in | Quick one-off inspection |
| Custom debug operators | Built-in | Team-wide reusable instrumentation |
| RxJS Spy | Library | Named stream tracking, pause/resume |
| Redux DevTools + NgRx | Library | State + action time-travel |
| Browser Performance API | Built-in | Timing and profiling |
| `materialize` logging | Built-in | Capturing full notification history |

---

## RxJS Spy

[rxjs-spy](https://github.com/cartant/rxjs-spy) adds named, inspectable streams to any RxJS pipeline. Install:

```bash
npm install rxjs-spy
```

### Setup

```typescript
import { create } from 'rxjs-spy';
import { tag } from 'rxjs-spy/operators';

// Initialize the spy (development only)
const spy = create();

// In production: no-op — spy is never created
if (process.env.NODE_ENV === 'development') {
  const spy = create();
  // Expose on window for DevTools console access
  (window as any).__rxSpy = spy;
}
```

### Tagging Streams

```typescript
import { tag } from 'rxjs-spy/operators';
import { interval, fromEvent } from 'rxjs';
import { switchMap, debounceTime } from 'rxjs/operators';

const search$ = fromEvent<InputEvent>(input, 'input').pipe(
  tag('search/input'),           // name this point in the pipeline
  debounceTime(300),
  tag('search/debounced'),
  switchMap(e => searchApi((e.target as HTMLInputElement).value)),
  tag('search/results'),
);

search$.subscribe(renderResults);
```

### Inspecting from Browser Console

```javascript
// In DevTools console:
__rxSpy.show()                     // list all tagged streams
__rxSpy.show('search/input')       // show specific stream's emissions
__rxSpy.pause('search/debounced')  // pause — hold values
__rxSpy.resume('search/debounced') // resume — flush held values
__rxSpy.log('search/results')      // log all emissions to console
__rxSpy.flush('search/debounced')  // flush one buffered value
__rxSpy.undo()                     // undo last spy operation
```

### Spy Plugins

```typescript
import { create, GraphPlugin, SnapshotPlugin, StackTracePlugin } from 'rxjs-spy';

const spy = create({
  plugins: [
    new GraphPlugin(),       // track subscription graph
    new SnapshotPlugin(),    // capture stream snapshots
    new StackTracePlugin(),  // capture subscription stack traces
  ],
});

// Inspect subscription graph
const graph = spy.find('search/results');
console.log(graph?.subscriptions); // who subscribed?

// Take a snapshot
const snapshot = spy.snapshot();
snapshot.observables.forEach(obs => {
  console.log(obs.tag, obs.subscriptions.length, 'subscribers');
});
```

---

## Custom Debug Operator Library

Build a team-wide debug operator set that works uniformly across the codebase.

```typescript
// debug-operators.ts
import { Observable, MonoTypeOperatorFunction } from 'rxjs';
import { tap, finalize, timestamp, map } from 'rxjs/operators';

const IS_DEV = process.env.NODE_ENV !== 'production';

// ─── Core debug operator ──────────────────────────────────────────────────────
export function debug<T>(
  label: string,
  options: {
    logNext?: boolean;
    logError?: boolean;
    logComplete?: boolean;
    logSubscribe?: boolean;
    logUnsubscribe?: boolean;
    color?: string;
  } = {},
): MonoTypeOperatorFunction<T> {
  if (!IS_DEV) return src$ => src$; // no-op in production

  const {
    logNext = true,
    logError = true,
    logComplete = true,
    logSubscribe = true,
    logUnsubscribe = true,
    color = '#9c27b0',
  } = options;

  const style = `color:${color};font-weight:bold`;

  return (source$: Observable<T>) =>
    new Observable<T>(observer => {
      if (logSubscribe) console.log(`%c▶ [${label}] subscribed`, style);

      const sub = source$.pipe(
        tap({
          next:     v => logNext     && console.log(`%c→ [${label}]`, style, v),
          error:    e => logError    && console.error(`%c✗ [${label}]`, style, e),
          complete: () => logComplete && console.log(`%c■ [${label}] complete`, style),
        }),
      ).subscribe(observer);

      return () => {
        if (logUnsubscribe) console.log(`%c⏹ [${label}] unsubscribed`, style);
        sub.unsubscribe();
      };
    });
}

// ─── Timing operator ──────────────────────────────────────────────────────────
export function debugTiming<T>(label: string): MonoTypeOperatorFunction<T> {
  if (!IS_DEV) return src$ => src$;

  return (source$: Observable<T>) => {
    let index = 0;
    const start = performance.now();

    return source$.pipe(
      tap(value => {
        const elapsed = (performance.now() - start).toFixed(1);
        console.log(`[${label}] #${index++} at +${elapsed}ms`, value);
      }),
      finalize(() => {
        const total = (performance.now() - start).toFixed(1);
        console.log(`[${label}] completed after ${total}ms, ${index} emissions`);
      }),
    );
  };
}

// ─── Count operator ───────────────────────────────────────────────────────────
export function debugCount<T>(label: string): MonoTypeOperatorFunction<T> {
  if (!IS_DEV) return src$ => src$;

  let count = 0;
  return tap<T>({
    next: () => console.log(`[${label}] emission #${++count}`),
    complete: () => console.log(`[${label}] total: ${count} emissions`),
  });
}

// ─── Breakpoint operator ──────────────────────────────────────────────────────
export function debugBreak<T>(
  label: string,
  condition: (v: T) => boolean = () => true,
): MonoTypeOperatorFunction<T> {
  if (!IS_DEV) return src$ => src$;

  return tap<T>(value => {
    if (condition(value)) {
      console.log(`[${label}] breakpoint hit:`, value);
      // eslint-disable-next-line no-debugger
      debugger; // pause in DevTools when condition is met
    }
  });
}
```

### Usage

```typescript
import { debug, debugTiming, debugBreak } from './debug-operators';

userSearch$.pipe(
  debug('search:input', { color: '#2196f3' }),
  debounceTime(300),
  debug('search:debounced', { logSubscribe: false }),
  switchMap(q => searchApi(q)),
  debugTiming('search:api'),
  debugBreak('search:results', r => r.length === 0), // break on empty results
).subscribe(renderResults);
```

---

## Browser Performance API Integration

Mark RxJS pipeline stages as performance entries — visible in Chrome DevTools Performance panel.

```typescript
import { tap, finalize } from 'rxjs/operators';
import { MonoTypeOperatorFunction } from 'rxjs';

export function perfMark<T>(label: string): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => {
    performance.mark(`rxjs:${label}:subscribe`);

    return source$.pipe(
      tap(() => performance.mark(`rxjs:${label}:next`)),
      finalize(() => {
        performance.mark(`rxjs:${label}:complete`);
        performance.measure(
          `rxjs:${label}`,
          `rxjs:${label}:subscribe`,
          `rxjs:${label}:complete`,
        );
      }),
    );
  };
}

// Usage — marks appear in DevTools Performance timeline
apiCall$.pipe(
  perfMark('user-api'),
  switchMap(user => loadPermissions(user.id)),
  perfMark('permissions-api'),
).subscribe();
```

---

## NgRx DevTools Integration

NgRx's Store DevTools extension provides time-travel debugging over dispatched actions and state snapshots.

```typescript
// app.config.ts
import { provideStoreDevtools } from '@ngrx/store-devtools';

export const appConfig: ApplicationConfig = {
  providers: [
    provideStore(reducers),
    provideEffects(effects),
    provideStoreDevtools({
      maxAge: 50,               // keep last 50 actions
      logOnly: !isDevMode(),    // restrict to log-only in production
      autoPause: true,          // pause when DevTools window is not open
      trace: true,              // capture stack traces for each action
      traceLimit: 75,
    }),
  ],
};

// Custom action metadata for better DevTools labels
export const loadUser = createAction(
  '[User API] Load User',     // prefix = feature, suffix = trigger
  props<{ userId: string }>()
);
// DevTools shows: "[User API] Load User" with { userId: '123' } payload
```

### Logging Middleware for Custom Stores

```typescript
// For non-NgRx stores, add a logging middleware
function createLoggingStore<S, A extends { type: string }>(
  reducer: (s: S, a: A) => S,
  initialState: S,
) {
  return new Store<S, A>(
    (state, action) => {
      const prevState = state;
      const nextState = reducer(state, action);

      if (process.env.NODE_ENV === 'development') {
        console.groupCollapsed(`%caction: ${action.type}`, 'color:#4caf50;font-weight:bold');
        console.log('%cprev state', 'color:#9e9e9e', prevState);
        console.log('%caction',     'color:#2196f3', action);
        console.log('%cnext state', 'color:#4caf50', nextState);
        console.groupEnd();
      }

      return nextState;
    },
    initialState,
  );
}
```

---

## Production-Safe Diagnostics

Debug operators should vanish in production. Use environment-gated sampling instead of full logging.

```typescript
import { tap, sample, interval } from 'rxjs';

const IS_PROD = process.env.NODE_ENV === 'production';

// Sample 1% of emissions in production for metric collection
export function sample1pct<T>(
  label: string,
  metricsService: MetricsService,
): MonoTypeOperatorFunction<T> {
  return tap<T>(() => {
    if (Math.random() < 0.01) { // 1% sampling
      metricsService.increment(`rxjs.emission.${label}`);
    }
  });
}

// Error rate tracking — always on, lightweight
export function trackErrors<T>(
  label: string,
  metricsService: MetricsService,
): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => source$.pipe(
    catchError(err => {
      metricsService.increment(`rxjs.error.${label}`);
      return throwError(() => err); // re-throw
    }),
  );
}

// Latency histogram — sample in production
export function trackLatency<T>(
  label: string,
  metricsService: MetricsService,
): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) => {
    const start = performance.now();
    return source$.pipe(
      tap({ complete: () => {
        const latency = performance.now() - start;
        metricsService.histogram(`rxjs.latency.${label}`, latency);
      }}),
    );
  };
}
```

---

## DevTools Integration Checklist

```
Development tooling:
  □ Install rxjs-spy for named stream inspection
  □ Build a shared debug operator module (debug, debugTiming, debugBreak)
  □ Gate all debug operators on IS_DEV / process.env check
  □ Use perfMark to add RxJS spans to Performance timeline
  □ Enable NgRx DevTools with trace: true for action stack traces

Production safety:
  □ Verify debug operators are no-ops in production builds
  □ Add error-rate tracking (always on, lightweight)
  □ Add latency sampling (1% in production)
  □ Ensure no console.log calls in production code paths
  □ Tree-shaking: import debug tools dynamically if needed
```

---

## Related Guides

- **[Debugging Streams (Advanced)](../operators-claude/testing/debugging-operators-advanced.md)** — operator-level techniques
- **[Testing Patterns](./testing-patterns-guide.md)** — TestScheduler and marble testing
- **[Performance Patterns](./performance-patterns-guide.md)** — profiling and optimization
- **[RxJS Marble Testing (Advanced)](./rxjs-marble-testing-advanced.md)** — deep marble testing
