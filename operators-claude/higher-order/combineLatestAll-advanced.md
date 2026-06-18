# combineLatestAll — Advanced Patterns

For fundamentals see the core [combineLatestAll](./combineLatestAll) doc. This page covers dynamic stream registration, multi-source dashboard patterns, coordinated state from variable-count streams, and comparisons with `mergeAll`, `zipAll`, and `forkJoin`.

---

## Mental Model

```typescript
import { combineLatestAll } from 'rxjs/operators';

// combineLatestAll — flatten an Observable<Observable<T>> using combineLatest semantics
// Waits for ALL inner Observables to emit at least once, then emits arrays on any change

// Equivalent to:
// combineLatest([inner1$, inner2$, inner3$])
// ...but the array of inner Observables is itself an Observable

of(
  interval(1000).pipe(take(3), map(i => `A${i}`)),
  interval(1500).pipe(take(3), map(i => `B${i}`))
).pipe(
  combineLatestAll()
).subscribe(console.log);
// ['A0','B0'], ['A1','B0'], ['A1','B1'], ['A2','B1'], ['A2','B2']
// ↑ every emission from any inner triggers a new combined array
```

**Key characteristic**: `combineLatestAll` buffers all inner Observables before subscribing to any of them — the outer Observable must **complete** before combination begins. This distinguishes it from `mergeAll` (subscribes eagerly) and is critical for correctness.

---

## Pattern 1: Dynamic Dashboard — Variable Number of Metric Streams

The primary use case: combine N streams where N is not known at compile time:

```typescript
import { combineLatestAll, map, startWith } from 'rxjs/operators';

interface Metric { name: string; value: number; unit: string; }

// User selects which metrics to show — array of active metric IDs varies:
function createDashboard$(
  activeMetricIds$: Observable<string[]>
): Observable<Metric[]> {
  return activeMetricIds$.pipe(
    switchMap(ids =>
      // Create one stream per metric ID, then combineLatestAll:
      from(ids).pipe(
        map(id => metricService.getMetric$(id).pipe(
          startWith({ name: id, value: 0, unit: '—' } as Metric) // prevent blocking
        )),
        toArray(), // collect all metric streams into an array
        mergeMap(streams$ => combineLatest(streams$)) // or streams$.pipe(combineLatestAll())
      )
    )
  );
}

// Full combineLatestAll version:
function createDashboardV2$(activeMetricIds$: Observable<string[]>): Observable<Metric[]> {
  return activeMetricIds$.pipe(
    switchMap(ids =>
      of(...ids.map(id =>
        metricService.getMetric$(id).pipe(
          startWith({ name: id, value: 0, unit: '—' } as Metric)
        )
      )).pipe(
        combineLatestAll()
      )
    )
  );
}

// Usage:
createDashboard$(selectedMetricIds$).pipe(
  takeUntilDestroyed()
).subscribe(metrics => renderDashboard(metrics));
```

---

## Pattern 2: Coordinating Parallel Async Initializations

Wait for all dynamic initializations to produce at least one value, then track updates:

```typescript
import { combineLatestAll, tap, map } from 'rxjs/operators';

interface FeatureState { featureId: string; status: 'loading' | 'ready' | 'error'; data: unknown }

// Initialize N features in parallel, track the combined state:
function initializeFeatures$(featureIds: string[]): Observable<FeatureState[]> {
  return from(featureIds.map(id =>
    featureRegistry.init$(id).pipe(
      map(data => ({ featureId: id, status: 'ready' as const, data })),
      startWith({ featureId: id, status: 'loading' as const, data: null }),
      catchError(err => of({ featureId: id, status: 'error' as const, data: err.message }))
    )
  )).pipe(
    combineLatestAll()
  );
}

// App bootstraps only when all features are past loading:
initializeFeatures$(['auth', 'config', 'permissions', 'featureFlags']).pipe(
  filter(states => states.every(s => s.status !== 'loading')),
  take(1) // one-shot: proceed once all are initialized
).subscribe(states => {
  const errors = states.filter(s => s.status === 'error');
  if (errors.length) reportInitErrors(errors);
  else               bootstrapApplication();
});
```

---

## Pattern 3: Multi-User Presence Aggregation

Combine live presence streams for a dynamic set of users:

```typescript
import { combineLatestAll, map, switchMap } from 'rxjs/operators';

interface PresenceInfo { userId: string; online: boolean; lastSeen: Date }

// Room membership changes dynamically — new members can join/leave
function roomPresence$(roomId: string): Observable<PresenceInfo[]> {
  return roomMembers$(roomId).pipe(
    // Create a presence stream per member:
    switchMap(memberIds =>
      of(...memberIds.map(userId =>
        userPresence$(userId).pipe(
          map(status => ({ userId, online: status.online, lastSeen: status.lastSeen })),
          startWith({ userId, online: false, lastSeen: new Date(0) })
        )
      )).pipe(combineLatestAll())
    ),
    map(presences => presences.sort((a, b) =>
      Number(b.online) - Number(a.online) // online users first
    ))
  );
}

const onlineCount$ = roomPresence$('room-42').pipe(
  map(presences => presences.filter(p => p.online).length),
  distinctUntilChanged()
);

onlineCount$.subscribe(n => updateOnlineBadge(n));
```

---

## Pattern 4: A/B Test Stream Aggregation

Collect results from a dynamic set of variant streams:

```typescript
import { combineLatestAll, map } from 'rxjs/operators';

interface ExperimentVariant { variantId: string; conversionRate: number; sampleSize: number }

function aggregateExperiments$(
  activeExperimentIds$: Observable<string[]>
): Observable<ExperimentVariant[]> {
  return activeExperimentIds$.pipe(
    switchMap(ids =>
      of(...ids.map(id =>
        experimentMetrics$(id).pipe(
          startWith({ variantId: id, conversionRate: 0, sampleSize: 0 })
        )
      )).pipe(
        combineLatestAll()
      )
    ),
    map(variants => variants.sort((a, b) => b.conversionRate - a.conversionRate))
  );
}

// Display winning variant:
aggregateExperiments$(activeExperiments$).pipe(
  map(variants => variants[0]), // highest conversion rate
  distinctUntilChanged((a, b) => a.variantId === b.variantId),
  takeUntilDestroyed()
).subscribe(winner => highlightWinningVariant(winner));
```

---

## `combineLatestAll` vs `mergeAll` vs `zipAll` vs `forkJoin`

```typescript
// combineLatestAll — latest from each, re-emits on any change
// Use when: each inner stream is live/ongoing, need synchronized latest values
streams$.pipe(combineLatestAll())
// Emits: [latest_A, latest_B, latest_C] on every change to any

// mergeAll — all values from all inners, no coordination
// Use when: inner streams are independent, order doesn't matter
streams$.pipe(mergeAll())
// Emits: individual values from any inner, as they arrive

// zipAll — one-to-one pairing by emission order
// Use when: streams emit at same pace, need nth-from-each-combined
streams$.pipe(zipAll())
// Emits: [first_A, first_B, first_C], then [second_A, second_B, second_C]...

// forkJoin equivalent (via combineLatestAll on completing streams)
// Use when: all inners complete, need final values combined
streams$.pipe(
  combineLatestAll(), // tracks latest
  last()             // only care about the final state when all complete
)
// Or: from([obs1$, obs2$, obs3$]).pipe(toArray(), mergeMap(arr => forkJoin(arr)))
```

---

## Common Pitfalls

### Outer Observable Must Complete Before Combination Begins

```typescript
// ❌ interval() never completes — combineLatestAll waits forever:
interval(1000).pipe(
  map(i => of(i)),
  combineLatestAll() // outer never completes → never starts combining
).subscribe(); // nothing emits

// ✅ Ensure outer completes (use take, toArray+mergeMap, or of()):
interval(1000).pipe(
  take(3),          // outer completes after 3 emissions
  map(i => timer(i * 500).pipe(map(() => i))),
  combineLatestAll()
).subscribe(console.log);
```

### Missing `startWith` Causes Blocking

```typescript
// ❌ If any inner never emits, combineLatestAll blocks forever:
const slow$  = timer(5000).pipe(map(() => 'slow'));
const fast$  = of('fast');

of(slow$, fast$).pipe(combineLatestAll()).subscribe(console.log);
// Nothing until slow$ emits at 5000ms

// ✅ Add startWith to each inner so combineLatestAll can fire immediately:
of(
  slow$.pipe(startWith(null)),
  fast$.pipe(startWith(null))
).pipe(
  combineLatestAll(),
  filter(values => values.every(v => v !== null)) // skip null-padded rows
).subscribe(console.log);
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `combineLatestAll` is `combineLatest()` for a dynamic, runtime-determined set of streams. Its single most important constraint — the outer Observable must complete before it starts — means it's almost always preceded by `toArray()` or used with `of(stream1$, stream2$, ...)`. The `startWith` guard on each inner stream prevents the common "one slow stream blocks the whole combination" failure mode.
