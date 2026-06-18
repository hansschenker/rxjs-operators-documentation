# withLatestFrom — Advanced Patterns

For fundamentals, see the core [withLatestFrom](./withLatestFrom) doc. This page covers the subscription timing problem, Angular patterns, and comparison with `combineLatestWith`.

---

## The Subscription Timing Problem

`withLatestFrom(other$)` subscribes to `other$` **when the outer Observable is subscribed**, not when the source emits. If `other$` hasn't emitted by the time the source emits, **the emission is silently dropped**.

```typescript
import { Subject } from 'rxjs';
import { withLatestFrom } from 'rxjs/operators';

const source$  = new Subject<string>();
const other$   = new Subject<number>();

source$.pipe(
  withLatestFrom(other$)
).subscribe(([s, o]) => console.log(s, o));

source$.next('A'); // ❌ DROPPED — other$ hasn't emitted yet!
other$.next(1);
source$.next('B'); // ✅ ['B', 1] — other$ now has a value
source$.next('C'); // ✅ ['C', 1] — other$ still has 1
other$.next(2);
source$.next('D'); // ✅ ['D', 2]
```

**This is intentional** — `withLatestFrom` means "combine with the latest value of `other$` if it has one." It's NOT like `combineLatest`, which waits for all sources.

---

## Fix 1: Seed `other$` with `startWith`

```typescript
import { startWith } from 'rxjs/operators';

const userSettings$ = this.settingsService.settings$; // may not have emitted

clicks$.pipe(
  withLatestFrom(userSettings$.pipe(startWith(DEFAULT_SETTINGS)))
).subscribe(([click, settings]) => handleClick(click, settings));
// DEFAULT_SETTINGS is used until userSettings$ emits its first value
```

---

## Fix 2: Use `combineLatestWith` When Both Must Have Emitted

```typescript
import { combineLatestWith } from 'rxjs/operators';

// If you NEED both streams to have a value before any emission:
formInput$.pipe(
  combineLatestWith(validationRules$) // waits for BOTH to emit
).subscribe(([input, rules]) => validate(input, rules));
```

**Rule**: Use `withLatestFrom` when `source$` drives the timing and `other$` is a "current state" lookup. Use `combineLatestWith` when any stream changing should trigger a new emission.

---

## Pattern 1: Snapshot Current State on User Action

```typescript
// "Take a photo" of current state when user acts
saveBtn$.pipe(
  withLatestFrom(this.store.select(selectCurrentDocument))
).subscribe(([_, doc]) => {
  this.api.save(doc).subscribe(); // saves the state at click time
});

// ✅ Correct — the button click drives timing, store state is a snapshot
// ❌ combineLatestWith would re-trigger on every store change (wrong)
```

---

## Pattern 2: Enrich Events with Context

```typescript
fromEvent<MouseEvent>(canvas, 'click').pipe(
  withLatestFrom(
    activeTool$,
    currentColor$,
    zoom$
  )
).subscribe(([event, tool, color, zoom]) => {
  const canvasCoords = screenToCanvas(event, zoom);
  applyTool(tool, canvasCoords, color);
});
```

---

## Pattern 3: Gate Actions on Permission State

```typescript
// Only proceed if user currently has permission
adminAction$.pipe(
  withLatestFrom(userPermissions$.pipe(startWith({ isAdmin: false }))),
  filter(([_, perms]) => perms.isAdmin),
  map(([action]) => action)
).subscribe(executeAdminAction);
```

---

## Pattern 4: Angular Route Guard with Store

```typescript
// In NgRx: check store state when route activates
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(private store: Store, private router: Router) {}

  canActivate(): Observable<boolean> {
    return this.store.select(selectAuthLoaded).pipe(
      filter(loaded => loaded),   // wait until auth state is loaded
      take(1),
      withLatestFrom(this.store.select(selectIsAuthenticated)),
      map(([_, isAuthenticated]) => {
        if (!isAuthenticated) this.router.navigate(['/login']);
        return isAuthenticated;
      })
    );
  }
}
```

---

## Pattern 5: Optimistic UI with Latest State

```typescript
deleteBtn$.pipe(
  withLatestFrom(selectedItem$),
  filter(([_, item]) => item !== null),
  map(([_, item]) => item!)
).subscribe(item => {
  // Optimistic delete using the item snapshot at click time
  this.items$.next(this.items$.value.filter(i => i.id !== item.id));
  this.api.delete(item.id).subscribe({
    error: () => this.items$.next([...this.items$.value, item]) // rollback
  });
});
```

---

## `withLatestFrom` vs `combineLatestWith` — Decision

| | `withLatestFrom(b$)` | `combineLatestWith(b$)` |
|---|---|---|
| Emits when | `source$` emits | Either source emits |
| Drops if `b$` hasn't emitted | Yes | No (waits) |
| `b$` drives emissions | No | Yes |
| "Current state" lookup | ✅ Perfect | ❌ Re-triggers on state changes |
| "Both must contribute" | ❌ Source-only driven | ✅ Both drive |

---

## Common Pitfalls

### Silent Drop on Cold `other$`

```typescript
// ❌ SILENT DROP — HttpClient returns cold Observable
clicks$.pipe(
  withLatestFrom(this.http.get('/api/config')) // cold — must subscribe first!
).subscribe(([click, config]) => use(click, config));
// config$ is cold — its value isn't ready at subscription time
// clicks before the HTTP response arrives are silently dropped

// ✅ Option 1: shareReplay so config is hot and always has a value
const config$ = this.http.get('/api/config').pipe(shareReplay(1));
clicks$.pipe(withLatestFrom(config$)).subscribe(([click, config]) => use(click, config));

// ✅ Option 2: switchMap — don't combine, sequence instead
clicks$.pipe(
  switchMap(click => this.http.get('/api/config').pipe(map(config => [click, config])))
).subscribe(([click, config]) => use(click, config));
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**The golden rule**: `withLatestFrom` = "when source fires, look up the current value of other$." If other$ might not have a value yet, seed it with `startWith`.
