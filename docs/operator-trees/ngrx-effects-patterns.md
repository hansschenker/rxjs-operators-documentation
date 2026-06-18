# NgRx Effects Patterns

How to choose the right flattening operator in NgRx Effects, handle errors, and compose effects cleanly.

---

## The Four Flattening Operators in Effects

The choice of flattening operator is the most important decision in any Effect.

```typescript
import { createEffect, Actions, ofType } from '@ngrx/effects';
import { mergeMap, switchMap, concatMap, exhaustMap } from 'rxjs/operators';

@Injectable()
export class ItemEffects {
  constructor(
    private actions$: Actions,
    private api: ApiService
  ) {}

  // mergeMap — parallel, order not guaranteed
  loadAll$ = createEffect(() => this.actions$.pipe(
    ofType(ItemActions.loadAll),
    mergeMap(() => this.api.getItems().pipe(
      map(items  => ItemActions.loadAllSuccess({ items })),
      catchError(err => of(ItemActions.loadAllFailure({ error: err.message })))
    ))
  ));

  // switchMap — cancel previous, keep only latest
  search$ = createEffect(() => this.actions$.pipe(
    ofType(ItemActions.search),
    switchMap(({ query }) => this.api.search(query).pipe(
      map(results => ItemActions.searchSuccess({ results })),
      catchError(err => of(ItemActions.searchFailure({ error: err.message })))
    ))
  ));

  // concatMap — queue, preserve order
  save$ = createEffect(() => this.actions$.pipe(
    ofType(ItemActions.save),
    concatMap(({ item }) => this.api.save(item).pipe(
      map(saved  => ItemActions.saveSuccess({ item: saved })),
      catchError(err => of(ItemActions.saveFailure({ error: err.message })))
    ))
  ));

  // exhaustMap — ignore while in-flight (ideal for login/submit)
  login$ = createEffect(() => this.actions$.pipe(
    ofType(AuthActions.login),
    exhaustMap(({ credentials }) => this.auth.login(credentials).pipe(
      map(user  => AuthActions.loginSuccess({ user })),
      catchError(err => of(AuthActions.loginFailure({ error: err.message })))
    ))
  ));
}
```

### Which to Use

| Action type | Operator | Reason |
|---|---|---|
| Load list / GET many | `mergeMap` | Independent; run in parallel |
| Search / filter | `switchMap` | Cancel stale searches |
| Create / Update / Delete | `concatMap` | Order matters; no concurrent mutations |
| Login / Submit form | `exhaustMap` | Ignore duplicate submits |
| Load by ID (latest wins) | `switchMap` | Cancel previous load if ID changes |

---

## Error Handling — The Critical Rule

**`catchError` MUST be inside the `mergeMap`/`switchMap` projection**, not outside it. An uncaught error in an Effect terminates the entire effect stream — the action will never be handled again.

```typescript
// ❌ EFFECT DIES on first error — all future actions ignored
loadItem$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.load),
  switchMap(({ id }) => this.api.getItem(id)),  // error propagates out
  map(item => ItemActions.loadSuccess({ item })),
  catchError(err => of(ItemActions.loadFailure({ error: err.message })))
  // ↑ This catchError REPLACES the entire effect Observable.
  // After one error, the effect completes and stops responding to actions.
));

// ✅ CORRECT — catchError inside projection restores stream
loadItem$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.load),
  switchMap(({ id }) =>
    this.api.getItem(id).pipe(
      map(item  => ItemActions.loadSuccess({ item })),
      catchError(err => of(ItemActions.loadFailure({ error: err.message })))
      // ↑ Only this inner Observable fails; outer stream continues
    )
  )
));
```

---

## Optimistic vs Pessimistic Updates

### Pessimistic (Default) — Wait for Server

```typescript
// Show loading → wait for API → update store → hide loading
updateItem$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.update),
  concatMap(({ item }) =>
    this.api.update(item).pipe(
      map(saved  => ItemActions.updateSuccess({ item: saved })),
      catchError(err => of(ItemActions.updateFailure({ item, error: err.message })))
    )
  )
));
```

### Optimistic — Update Store First, Rollback on Failure

```typescript
// Apply immediately → attempt API → rollback on failure
updateItem$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.updateOptimistic),
  // Store already updated by reducer (optimistic)
  concatMap(({ item, previousItem }) =>
    this.api.update(item).pipe(
      map(saved  => ItemActions.updateConfirmed({ item: saved })),
      catchError(err => of(ItemActions.updateRollback({
        item: previousItem,  // restore previous state
        error: err.message
      })))
    )
  )
));
```

---

## Dispatching Multiple Actions

```typescript
// Use EMPTY to dispatch nothing; of(...) for multiple
loadAndInit$ = createEffect(() => this.actions$.pipe(
  ofType(AppActions.init),
  switchMap(() =>
    this.api.getConfig().pipe(
      // Dispatch multiple actions from one effect:
      mergeMap(config => of(
        ConfigActions.set({ config }),
        FeatureActions.initialize({ features: config.features }),
        AppActions.initSuccess()
      )),
      catchError(err => of(AppActions.initFailure({ error: err.message })))
    )
  )
));
```

---

## Non-Dispatching Effects (Side Effects)

```typescript
// { dispatch: false } — effect runs but dispatches nothing
logActions$ = createEffect(() => this.actions$.pipe(
  tap(action => this.analytics.track(action.type))
), { dispatch: false });

// Navigate after success:
navigateAfterSave$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.saveSuccess),
  tap(({ item }) => this.router.navigate(['/items', item.id]))
), { dispatch: false });

// Show toast:
showError$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.loadFailure, ItemActions.saveFailure),
  tap(({ error }) => this.toast.error(error))
), { dispatch: false });
```

---

## Effect with Retry

```typescript
import { retry, timer } from 'rxjs';

loadWithRetry$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.loadRetryable),
  mergeMap(({ id }) =>
    this.api.getItem(id).pipe(
      retry({
        count: 3,
        delay: (_, attempt) => timer(500 * 2 ** attempt)
      }),
      map(item  => ItemActions.loadSuccess({ item })),
      catchError(err => of(ItemActions.loadFailure({ error: err.message })))
    )
  )
));
```

---

## Loading State in Store via Effects

Rather than tracking loading in the component, dispatch loading actions:

```typescript
// Actions: loadRequest, loadSuccess, loadFailure
// Reducer sets loading: true on loadRequest, false on success/failure

loadItems$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.loadRequest),
  switchMap(() =>
    this.api.getItems().pipe(
      map(items  => ItemActions.loadSuccess({ items })),
      catchError(err => of(ItemActions.loadFailure({ error: err.message })))
    )
  )
));

// Component just selects loading state from store:
// isLoading$ = this.store.select(selectItemsLoading);
```

---

## Composing Effects — Action Sequences

```typescript
// Effect that listens to another effect's output action:
loadThenSelect$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.loadSuccess),  // triggered when loadItems$ succeeds
  map(({ items }) =>
    items.length > 0
      ? ItemActions.selectFirst({ id: items[0].id })
      : ItemActions.noItemsFound()
  )
));
```

---

## Common Mistakes

### Missing `catchError` Inside Projection

See the critical rule above — always catch inside the flattening operator.

### Using `switchMap` for Mutations

```typescript
// ❌ Mutation with switchMap — cancels previous save if actions arrive quickly
saveItem$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.save),
  switchMap(({ item }) => this.api.save(item).pipe(...))
  // Second save dispatched quickly? First save HTTP request cancelled client-side
  // but may continue server-side → data integrity risk
));

// ✅ concatMap for mutations
saveItem$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.save),
  concatMap(({ item }) => this.api.save(item).pipe(...))
));
```

### Forgetting `{ dispatch: false }` on Side-Effect Effects

```typescript
// ❌ Missing { dispatch: false } — Effect returns Observable<Action>
// If tap() somehow returns a value, NgRx will try to dispatch it
navigate$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.saveSuccess),
  tap(() => this.router.navigate(['/items']))
)); // No { dispatch: false }!

// ✅ Always add { dispatch: false } for side effects
navigate$ = createEffect(() => this.actions$.pipe(
  ofType(ItemActions.saveSuccess),
  tap(() => this.router.navigate(['/items']))
), { dispatch: false });
```
