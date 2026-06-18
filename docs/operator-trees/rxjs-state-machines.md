# State Machines with RxJS

Modeling finite state machines using `scan`, `BehaviorSubject`, and discriminated union actions — from simple toggles to complex multi-state workflows.

---

## Why RxJS for State Machines

A state machine is a function `(state, event) → nextState`. `scan` is exactly that:

```typescript
source$.pipe(scan((state, event) => transition(state, event), initialState))
```

This gives you:
- Reactive state that updates on events
- Time-travel debugging via `replay`
- Testability (pure functions)
- Composability with all RxJS operators

---

## Pattern 1: Simple Toggle Machine

```typescript
import { Subject } from 'rxjs';
import { scan, startWith, distinctUntilChanged } from 'rxjs/operators';

type ToggleState = 'on' | 'off';
type ToggleEvent = 'TOGGLE' | 'FORCE_ON' | 'FORCE_OFF';

const events$ = new Subject<ToggleEvent>();

const state$ = events$.pipe(
  scan((state: ToggleState, event: ToggleEvent): ToggleState => {
    switch (event) {
      case 'TOGGLE':    return state === 'on' ? 'off' : 'on';
      case 'FORCE_ON':  return 'on';
      case 'FORCE_OFF': return 'off';
    }
  }, 'off'),
  startWith('off' as ToggleState),
  distinctUntilChanged(),
  shareReplay(1)
);

state$.subscribe(s => updateToggleUI(s));

// Dispatch events:
toggleButton$.subscribe(() => events$.next('TOGGLE'));
```

---

## Pattern 2: Async Loading State Machine

The classic 4-state machine for data fetching:

```typescript
import { BehaviorSubject, Subject } from 'rxjs';
import { switchMap, map } from 'rxjs/operators';

type LoadState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error';   error: Error };

type LoadEvent<T> =
  | { type: 'FETCH' }
  | { type: 'SUCCESS'; data: T }
  | { type: 'FAILURE'; error: Error }
  | { type: 'RESET' };

function createLoadMachine<T>(
  fetch$: Observable<T>
): { state$: Observable<LoadState<T>>; dispatch: (e: LoadEvent<T>) => void } {
  const events$ = new Subject<LoadEvent<T>>();

  const state$ = events$.pipe(
    scan((state: LoadState<T>, event: LoadEvent<T>): LoadState<T> => {
      switch (event.type) {
        case 'FETCH':
          return state.status === 'loading' ? state : { status: 'loading' };
        case 'SUCCESS':
          return { status: 'success', data: event.data };
        case 'FAILURE':
          return { status: 'error', error: event.error };
        case 'RESET':
          return { status: 'idle' };
      }
    }, { status: 'idle' }),
    startWith({ status: 'idle' } as LoadState<T>),
    shareReplay(1)
  );

  // Side-effect: fetch when FETCH event dispatched:
  events$.pipe(
    filter(e => e.type === 'FETCH'),
    switchMap(() =>
      fetch$.pipe(
        map(data  => ({ type: 'SUCCESS', data } as LoadEvent<T>)),
        catchError(error => of({ type: 'FAILURE', error } as LoadEvent<T>))
      )
    )
  ).subscribe(e => events$.next(e));

  return { state$, dispatch: e => events$.next(e) };
}

// Usage:
const { state$, dispatch } = createLoadMachine(
  this.api.getUsers()
);

state$.subscribe(state => {
  switch (state.status) {
    case 'idle':    showIdle(); break;
    case 'loading': showSpinner(); break;
    case 'success': renderUsers(state.data); break;
    case 'error':   showError(state.error.message); break;
  }
});

loadButton$.subscribe(() => dispatch({ type: 'FETCH' }));
```

---

## Pattern 3: Wizard / Multi-Step Form Machine

```typescript
type WizardStep = 'personal' | 'address' | 'payment' | 'review' | 'submitted';

interface WizardState {
  step:      WizardStep;
  data:      Partial<FormData>;
  errors:    Record<string, string>;
  canGoBack: boolean;
}

type WizardEvent =
  | { type: 'NEXT';   stepData: Partial<FormData> }
  | { type: 'BACK' }
  | { type: 'SUBMIT' }
  | { type: 'SUBMIT_SUCCESS' }
  | { type: 'SUBMIT_FAILURE'; error: string }
  | { type: 'RESET' };

const STEPS: WizardStep[] = ['personal', 'address', 'payment', 'review', 'submitted'];

const wizardEvents$ = new Subject<WizardEvent>();

const wizardState$ = wizardEvents$.pipe(
  scan((state: WizardState, event: WizardEvent): WizardState => {
    const currentIndex = STEPS.indexOf(state.step);

    switch (event.type) {
      case 'NEXT': {
        const nextStep = STEPS[currentIndex + 1];
        if (!nextStep) return state;
        return {
          ...state,
          step:      nextStep,
          data:      { ...state.data, ...event.stepData },
          canGoBack: true,
          errors:    {}
        };
      }
      case 'BACK': {
        const prevStep = STEPS[currentIndex - 1];
        if (!prevStep) return state;
        return { ...state, step: prevStep, canGoBack: currentIndex - 1 > 0 };
      }
      case 'SUBMIT':
        return { ...state, step: 'review' };
      case 'SUBMIT_SUCCESS':
        return { ...state, step: 'submitted', canGoBack: false };
      case 'SUBMIT_FAILURE':
        return { ...state, errors: { submit: event.error } };
      case 'RESET':
        return { step: 'personal', data: {}, errors: {}, canGoBack: false };
    }
  }, { step: 'personal', data: {}, errors: {}, canGoBack: false }),
  startWith({ step: 'personal', data: {}, errors: {}, canGoBack: false } as WizardState),
  shareReplay(1)
);

// Derived observables from state:
const currentStep$ = wizardState$.pipe(
  map(s => s.step),
  distinctUntilChanged()
);

const progressPercent$ = wizardState$.pipe(
  map(s => (STEPS.indexOf(s.step) / (STEPS.length - 1)) * 100)
);
```

---

## Pattern 4: Traffic Light / Cyclic State Machine

```typescript
type LightState = 'red' | 'green' | 'yellow';

interface TrafficLight {
  color:        LightState;
  durationMs:   number;
  cycleCount:   number;
}

const TRANSITIONS: Record<LightState, { next: LightState; durationMs: number }> = {
  red:    { next: 'green',  durationMs: 5000 },
  green:  { next: 'yellow', durationMs: 4000 },
  yellow: { next: 'red',    durationMs: 1500 }
};

// Auto-advancing state machine using expand:
const trafficLight$ = of<TrafficLight>({ color: 'red', durationMs: 5000, cycleCount: 0 }).pipe(
  expand(state =>
    timer(state.durationMs).pipe(
      map(() => {
        const { next, durationMs } = TRANSITIONS[state.color];
        return {
          color:      next,
          durationMs,
          cycleCount: state.color === 'red' ? state.cycleCount + 1 : state.cycleCount
        };
      })
    )
  ),
  takeUntilDestroyed()
);

trafficLight$.subscribe(({ color, cycleCount }) => {
  updateTrafficLight(color);
  if (cycleCount % 10 === 0) console.log(`Cycle ${cycleCount} complete`);
});
```

---

## Pattern 5: Connection State Machine

Model a network connection with reconnection logic:

```typescript
type ConnectionState =
  | { status: 'disconnected' }
  | { status: 'connecting';  attempt: number }
  | { status: 'connected';   connectedAt: number }
  | { status: 'reconnecting'; attempt: number; lastConnectedAt: number }
  | { status: 'failed';       reason: string };

type ConnectionEvent =
  | { type: 'CONNECT' }
  | { type: 'CONNECTED' }
  | { type: 'DISCONNECTED'; clean: boolean }
  | { type: 'RECONNECT_ATTEMPT'; attempt: number }
  | { type: 'GIVE_UP'; reason: string };

const connectionEvents$ = new Subject<ConnectionEvent>();

const connectionState$ = connectionEvents$.pipe(
  scan((state: ConnectionState, event: ConnectionEvent): ConnectionState => {
    switch (event.type) {
      case 'CONNECT':
        return { status: 'connecting', attempt: 1 };
      case 'CONNECTED':
        return { status: 'connected', connectedAt: Date.now() };
      case 'DISCONNECTED':
        if (event.clean) return { status: 'disconnected' };
        const lastConnectedAt = state.status === 'connected' ? state.connectedAt : Date.now();
        return { status: 'reconnecting', attempt: 1, lastConnectedAt };
      case 'RECONNECT_ATTEMPT':
        return state.status === 'reconnecting'
          ? { ...state, attempt: event.attempt }
          : state;
      case 'GIVE_UP':
        return { status: 'failed', reason: event.reason };
    }
  }, { status: 'disconnected' }),
  startWith({ status: 'disconnected' } as ConnectionState),
  distinctUntilChanged((a, b) => a.status === b.status),
  shareReplay(1)
);

// Reconnection side-effect driven by state:
connectionState$.pipe(
  filter(s => s.status === 'reconnecting'),
  switchMap((s) => {
    const state = s as Extract<ConnectionState, { status: 'reconnecting' }>;
    return timer(Math.min(1000 * Math.pow(2, state.attempt - 1), 30_000)).pipe(
      tap(() => connectionEvents$.next({ type: 'RECONNECT_ATTEMPT', attempt: state.attempt + 1 })),
      switchMap(() => wsConnect$),
      take(1)
    );
  }),
  catchError(() => {
    connectionEvents$.next({ type: 'GIVE_UP', reason: 'Max retries exceeded' });
    return EMPTY;
  })
).subscribe();
```

---

## Pattern 6: State History for Debugging

Add time-travel to any state machine:

```typescript
import { scan, map, shareReplay } from 'rxjs/operators';

function withHistory<S>(
  state$: Observable<S>,
  maxHistory = 50
): Observable<{ current: S; history: S[]; canUndo: boolean }> {
  return state$.pipe(
    scan((acc, state) => {
      const history = [...acc.history, acc.current].slice(-maxHistory);
      return { current: state, history, canUndo: history.length > 0 };
    }, { current: null as unknown as S, history: [] as S[], canUndo: false }),
    shareReplay(1)
  );
}

const { state$, dispatch } = createLoadMachine(api.getUsers());

withHistory(state$).subscribe(({ current, history, canUndo }) => {
  renderState(current);
  updateDevTools(history); // show state history in dev tools panel
});
```

---

## Common Pitfalls

### Illegal State Transitions Causing Corruption

```typescript
// ❌ Allowing any event in any state — invalid transitions corrupt state:
scan((state, event) => {
  if (event.type === 'SUCCESS') return { status: 'success', data: event.data };
  // If SUCCESS fires during 'idle' state (before FETCH), state is corrupted
})

// ✅ Guard transitions by current state:
scan((state, event) => {
  if (event.type === 'SUCCESS' && state.status === 'loading') {
    return { status: 'success', data: event.data };
  }
  return state; // ignore invalid transitions silently
})
```

### Side Effects Inside `scan`

```typescript
// ❌ Side effects in the reducer — makes testing hard and breaks replay:
scan((state, event) => {
  if (event.type === 'FETCH') {
    api.fetchData().subscribe(data => events$.next({ type: 'SUCCESS', data })); // side effect!
  }
  return nextState;
})

// ✅ Keep scan pure; drive side effects from state changes separately:
state$.pipe(
  filter(s => s.status === 'loading'),
  switchMap(() => api.fetchData()),
  map(data => ({ type: 'SUCCESS', data }))
).subscribe(events$);
```
