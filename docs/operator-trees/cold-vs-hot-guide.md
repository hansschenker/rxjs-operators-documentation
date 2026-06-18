# Cold vs Hot Observables

Understanding cold vs hot is the single most important concept for reasoning about RxJS behavior. Most subscription bugs — duplicate HTTP requests, missed events, unexpected sharing — trace back to this distinction.

---

## The Core Difference

| | Cold Observable | Hot Observable |
|---|---|---|
| **When does it start?** | On each `subscribe()` | Independently of subscribers |
| **Does each subscriber get its own execution?** | Yes | No — shares one execution |
| **What do late subscribers receive?** | Everything from the beginning | Only future emissions |
| **Analogy** | Netflix (each viewer starts from the beginning) | Live TV (you join mid-broadcast) |

---

## Cold Observables

A cold Observable **contains its own producer**. Each subscription creates a new, independent execution.

```typescript
import { Observable, ajax } from 'rxjs';

// Cold — each subscribe() creates a new HTTP request:
const user$ = ajax.getJSON('/api/user');

user$.subscribe(u => console.log('A:', u.name)); // request #1
user$.subscribe(u => console.log('B:', u.name)); // request #2 — independent!

// Cold — each subscription gets values from 0:
const counter$ = interval(1000);
counter$.subscribe(v => console.log('A:', v)); // 0, 1, 2, 3...
setTimeout(() => {
  counter$.subscribe(v => console.log('B:', v)); // 0, 1, 2, 3... (starts fresh)
}, 3000);
// A: 0, 1, 2, 3, 4...
// B:          0, 1... (starts from 0 regardless of A's position)
```

**Cold Observables include**: `ajax`, `http.get()`, `from([...])`, `of(...)`, `interval`, `timer`, `defer`, custom `new Observable(...)`.

---

## Hot Observables

A hot Observable has a **producer that exists outside** the Observable. Subscribers tap into an ongoing stream.

```typescript
import { fromEvent, Subject } from 'rxjs';

// Hot — DOM events exist independently of subscriptions:
const clicks$ = fromEvent(document, 'click');
clicks$.subscribe(e => console.log('A clicked'));
// 2 seconds later:
clicks$.subscribe(e => console.log('B clicked'));
// Both A and B receive the SAME clicks from now on
// B misses any clicks from the first 2 seconds

// Subject — classic hot Observable:
const subject = new Subject<number>();
subject.subscribe(v => console.log('A:', v));
subject.next(1); // A: 1
subject.subscribe(v => console.log('B:', v));
subject.next(2); // A: 2, B: 2 — B joined late, missed 1
```

**Hot Observables include**: `fromEvent`, `Subject` / `BehaviorSubject` / `ReplaySubject`, `webSocket`, `share()` / `shareReplay()` multicast results.

---

## Why It Matters: The Duplicate Request Problem

```typescript
// ❌ TWO HTTP REQUESTS — cold Observable subscribed twice
const user$ = this.http.get<User>('/api/me');

this.user$ = user$;  // template: {{ (user$ | async)?.name }}
this.role$ = user$.pipe(map(u => u.role)); // second subscription!

// ✅ ONE REQUEST — make it hot with shareReplay(1)
const user$ = this.http.get<User>('/api/me').pipe(shareReplay(1));
this.user$ = user$;
this.role$ = user$.pipe(map(u => u.role)); // shares the one request
```

---

## Making Cold Observables Hot

### `share()` — Ref-counted, no replay

```typescript
import { share } from 'rxjs/operators';

const hot$ = coldSource$.pipe(share());

hot$.subscribe(a => console.log('A:', a));
hot$.subscribe(b => console.log('B:', b));
// Both A and B receive the same emissions
// Source executes once, shared between subscribers
```

### `shareReplay(n)` — Ref-counted with replay buffer

```typescript
import { shareReplay } from 'rxjs/operators';

const config$ = loadConfig().pipe(shareReplay(1));

// Late subscriber immediately gets the last value:
config$.subscribe(render);
setTimeout(() => {
  config$.subscribe(renderSidebar); // gets current value immediately
}, 5000);
```

### `Subject` — Manual hot bridge

```typescript
import { Subject } from 'rxjs';

const subject = new Subject<Event>();

// Pipe cold Observable into hot Subject:
coldSource$.subscribe(subject);

// Multiple subscribers share the Subject:
subject.subscribe(a => handleA(a));
subject.subscribe(b => handleB(b));
```

---

## The Subscription Timing Problem

Late subscribers to hot Observables miss past emissions:

```typescript
const clicks$ = fromEvent(document, 'click').pipe(share());

// ❌ TIMING RACE — if click fires before second subscribe, B misses it
clicks$.subscribe(a => handleA(a)); // subscribes at t=0
// ... some work ...
clicks$.subscribe(b => handleB(b)); // subscribes at t=100ms — may miss clicks
```

**Solutions by use case**:

| Need | Solution |
|---|---|
| Late subscriber gets last value | `shareReplay(1)` |
| Late subscriber gets last N values | `shareReplay(n)` |
| Late subscriber gets a default | `share()` + `startWith(default)` |
| Late subscriber gets nothing (by design) | `share()` |

---

## Diagnosing Cold vs Hot

**Symptom: unexpected duplicate work** (two requests, two timers) → source is **cold**, subscribed multiple times. Fix: `shareReplay(1)`.

**Symptom: late subscriber misses values** → source is **hot**, or `share()` with no replay. Fix: `shareReplay(n)` or `BehaviorSubject`.

**Symptom: values arrive even with zero subscribers** → **hot** producer (event listener, Subject, timer that's already running). Check for unmanaged subscriptions.

---

## Cold vs Hot in Testing

```typescript
import { TestScheduler } from 'rxjs/testing';

testScheduler.run(({ cold, hot, expectObservable }) => {
  // cold() — each subscription starts at frame 0 of the marble:
  const cold$ = cold('--a--b--|');

  // hot() — subscription point marked with '^', values before ^ are "past":
  const hot$ = hot('--a--^--b--|'); // subscriber joins at ^, gets b but not a
  //                        ^ subscription point

  expectObservable(hot$).toBe('---b--|');
});
```

---

## Quick Reference

```typescript
// Is this cold or hot?
interval(1000)              // cold — each subscribe starts from 0
ajax.getJSON('/api')        // cold — each subscribe makes a new request
fromEvent(btn, 'click')     // hot — DOM event exists independently
new Subject()               // hot — multicast by nature
new BehaviorSubject(init)   // hot — has current value

// Make cold hot:
cold$.pipe(share())         // hot, no replay
cold$.pipe(shareReplay(1))  // hot, replays last value to late subscribers
```
