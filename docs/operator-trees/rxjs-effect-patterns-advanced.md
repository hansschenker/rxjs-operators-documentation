# NgRx Effects — Advanced Patterns

Deep patterns for NgRx Effects: orchestration, optimistic updates, effect composition, cancellation, and error containment.

---

## Effect Fundamentals Recap

An NgRx Effect is an Observable pipeline that:
1. Listens to the action stream via `Actions`
2. Performs a side effect (HTTP, WebSocket, localStorage, etc.)
3. Dispatches zero or more result actions

```typescript
@Injectable()
export class UserEffects {
  loadUser$ = createEffect(() =>
    this.actions$.pipe(
      ofType(UserActions.loadUser),
      switchMap(({ userId }) =>
        this.userService.getUser(userId).pipe(
          map(user => UserActions.loadUserSuccess({ user })),
          catchError(err => of(UserActions.loadUserFailure({ error: err.message }))),
        )
      ),
    )
  );

  constructor(private actions$: Actions, private userService: UserService) {}
}
```

---

## Pattern 1: Effect Orchestration — Sequential Actions

Chain multiple async operations where each depends on the previous result.

```typescript
@Injectable()
export class CheckoutEffects {
  // Orchestrate: validate cart → charge payment → create order → send email
  checkout$ = createEffect(() =>
    this.actions$.pipe(
      ofType(CheckoutActions.initiateCheckout),
      switchMap(({ cartId, paymentMethod }) =>
        this.cartService.validate(cartId).pipe(
          switchMap(cart =>
            this.paymentService.charge(cart.total, paymentMethod).pipe(
              map(charge => ({ cart, charge }))
            )
          ),
          switchMap(({ cart, charge }) =>
            this.orderService.create({ cart, charge }).pipe(
              map(order => ({ cart, charge, order }))
            )
          ),
          switchMap(({ order }) =>
            this.emailService.sendConfirmation(order.id).pipe(
              map(() => CheckoutActions.checkoutSuccess({ orderId: order.id })),
            )
          ),
          catchError(err => of(CheckoutActions.checkoutFailure({ error: err.message }))),
        )
      ),
    )
  );

  constructor(
    private actions$: Actions,
    private cartService: CartService,
    private paymentService: PaymentService,
    private orderService: OrderService,
    private emailService: EmailService,
  ) {}
}
```

---

## Pattern 2: Optimistic Updates

Dispatch the success action immediately (before the server confirms), then roll back on failure.

```typescript
import { Store } from '@ngrx/store';
import { withLatestFrom } from 'rxjs/operators';

@Injectable()
export class TodoEffects {
  // Optimistic: update UI immediately, roll back on error
  updateTodo$ = createEffect(() =>
    this.actions$.pipe(
      ofType(TodoActions.updateTodo),
      withLatestFrom(this.store.select(selectTodos)), // capture pre-update state
      mergeMap(([{ todo }, previousTodos]) =>
        this.todoService.update(todo).pipe(
          map(() => TodoActions.updateTodoConfirmed({ todo })),
          catchError(err =>
            // Roll back: restore previous state + show error
            of(
              TodoActions.updateTodoRollback({ previousTodos }),
              NotificationActions.showError({ message: 'Update failed — changes reverted' }),
            )
          ),
        )
      ),
    )
  );

  constructor(
    private actions$: Actions,
    private store: Store,
    private todoService: TodoService,
  ) {}
}

// Reducer handles all three action types:
// updateTodo        → apply optimistic change immediately
// updateTodoConfirmed → no-op (already applied)
// updateTodoRollback  → restore previous todos array
```

---

## Pattern 3: Parallel Effect Orchestration with forkJoin

Run independent async operations in parallel, dispatch when all complete.

```typescript
import { forkJoin } from 'rxjs';

@Injectable()
export class DashboardEffects {
  // Load all dashboard data in parallel — dispatch one success action
  loadDashboard$ = createEffect(() =>
    this.actions$.pipe(
      ofType(DashboardActions.loadDashboard),
      switchMap(({ userId }) =>
        forkJoin({
          user:          this.userService.getUser(userId),
          notifications: this.notificationService.getUnread(userId),
          metrics:       this.metricsService.getSummary(userId),
          recentOrders:  this.orderService.getRecent(userId, 5),
        }).pipe(
          map(data => DashboardActions.loadDashboardSuccess(data)),
          catchError(err => of(DashboardActions.loadDashboardFailure({ error: err.message }))),
        )
      ),
    )
  );

  constructor(
    private actions$: Actions,
    private userService: UserService,
    private notificationService: NotificationService,
    private metricsService: MetricsService,
    private orderService: OrderService,
  ) {}
}
```

---

## Pattern 4: Effect Cancellation with Race

Cancel a long-running operation when a cancel action arrives.

```typescript
import { race } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Injectable()
export class ReportEffects {
  // Generate report — cancellable by user
  generateReport$ = createEffect(() =>
    this.actions$.pipe(
      ofType(ReportActions.generateReport),
      switchMap(({ reportConfig }) => {
        const cancel$ = this.actions$.pipe(ofType(ReportActions.cancelReport));

        return this.reportService.generate(reportConfig).pipe(
          takeUntil(cancel$),  // cancel HTTP request on cancel action
          map(report => ReportActions.reportReady({ report })),
          catchError(err => of(ReportActions.reportFailed({ error: err.message }))),
        );
      }),
    )
  );

  // Alternatively: explicit race between result and cancellation
  generateWithRace$ = createEffect(() =>
    this.actions$.pipe(
      ofType(ReportActions.generateReport),
      switchMap(({ reportConfig }) =>
        race(
          this.reportService.generate(reportConfig).pipe(
            map(report => ReportActions.reportReady({ report })),
          ),
          this.actions$.pipe(
            ofType(ReportActions.cancelReport),
            map(() => ReportActions.reportCancelled()),
          ),
        ).pipe(
          catchError(err => of(ReportActions.reportFailed({ error: err.message }))),
        )
      ),
    )
  );

  constructor(private actions$: Actions, private reportService: ReportService) {}
}
```

---

## Pattern 5: Effect Composition — Effects Triggering Effects

Effects that dispatch actions consumed by other effects, forming a reactive chain.

```typescript
@Injectable()
export class AuthEffects {
  // Effect 1: Login → dispatches loadProfile action
  login$ = createEffect(() =>
    this.actions$.pipe(
      ofType(AuthActions.login),
      switchMap(({ credentials }) =>
        this.authService.login(credentials).pipe(
          map(session => AuthActions.loginSuccess({ session })),
          catchError(err => of(AuthActions.loginFailure({ error: err.message }))),
        )
      ),
    )
  );

  // Effect 2: Reacts to loginSuccess — loads user profile
  loadProfileOnLogin$ = createEffect(() =>
    this.actions$.pipe(
      ofType(AuthActions.loginSuccess),
      map(({ session }) => ProfileActions.loadProfile({ userId: session.userId })),
    )
  );

  // Effect 3: Reacts to loginSuccess — loads notifications
  loadNotificationsOnLogin$ = createEffect(() =>
    this.actions$.pipe(
      ofType(AuthActions.loginSuccess),
      map(({ session }) => NotificationActions.loadNotifications({ userId: session.userId })),
    )
  );

  constructor(private actions$: Actions, private authService: AuthService) {}
}
```

**Benefit**: Each effect has a single responsibility. Adding a new post-login action (e.g. load preferences) just adds a new effect — no modification to existing effects.

---

## Pattern 6: WebSocket Effect with Reconnection

```typescript
import { webSocket } from 'rxjs/webSocket';
import { retryWhen, delay, tap } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

@Injectable()
export class RealtimeEffects {
  private socket$ = webSocket<ServerEvent>('wss://api.example.com/events');

  // Connect on app init, auto-reconnect on error
  connectRealtimeFeed$ = createEffect(() =>
    this.actions$.pipe(
      ofType(AppActions.init),
      switchMap(() =>
        this.socket$.pipe(
          retryWhen(errors$ => errors$.pipe(
            tap(err => console.warn('WebSocket error, reconnecting...', err)),
            delay(3000),
          )),
          map(event => RealtimeActions.eventReceived({ event })),
          takeUntil(this.actions$.pipe(ofType(AppActions.destroy))),
        )
      ),
    )
  );

  // Route server events to domain actions
  routeEvents$ = createEffect(() =>
    this.actions$.pipe(
      ofType(RealtimeActions.eventReceived),
      map(({ event }) => {
        switch (event.type) {
          case 'order.updated':   return OrderActions.orderUpdated({ order: event.payload });
          case 'message.received': return MessageActions.messageReceived({ message: event.payload });
          default: return AppActions.unknownRealtimeEvent({ event });
        }
      }),
    )
  );

  constructor(private actions$: Actions) {}
}
```

---

## Pattern 7: Error Isolation — Effects That Never Die

An effect that errors terminates the effect stream. Protect with `catchError` inside the inner pipe.

```typescript
@Injectable()
export class ResilientEffects {
  // ❌ One HTTP error kills all future loadUser dispatches
  fragileLoad$ = createEffect(() =>
    this.actions$.pipe(
      ofType(UserActions.loadUser),
      switchMap(({ userId }) => this.userService.getUser(userId)),
      map(user => UserActions.loadUserSuccess({ user })),
      catchError(err => of(UserActions.loadUserFailure({ error: err.message }))),
      // ↑ catchError here means the outer pipe terminates — effect is dead
    )
  );

  // ✅ catchError inside switchMap — outer pipe survives errors
  resilientLoad$ = createEffect(() =>
    this.actions$.pipe(
      ofType(UserActions.loadUser),
      switchMap(({ userId }) =>
        this.userService.getUser(userId).pipe(
          map(user => UserActions.loadUserSuccess({ user })),
          catchError(err => of(UserActions.loadUserFailure({ error: err.message }))),
          // ↑ catchError here — only this inner Observable terminates
          //   the outer pipe continues listening for more loadUser actions
        )
      ),
    )
  );

  constructor(private actions$: Actions, private userService: UserService) {}
}
```

---

## Pattern 8: Non-Dispatching Effects

Effects that perform side effects without dispatching any action.

```typescript
@Injectable()
export class AnalyticsEffects {
  trackPageView$ = createEffect(
    () =>
      this.actions$.pipe(
        ofType(RouterActions.navigated),
        tap(({ url }) => this.analytics.trackPageView(url)),
      ),
    { dispatch: false }, // ← no action dispatched
  );

  savePreferences$ = createEffect(
    () =>
      this.actions$.pipe(
        ofType(PreferencesActions.update),
        debounceTime(500),
        tap(({ preferences }) => localStorage.setItem('prefs', JSON.stringify(preferences))),
      ),
    { dispatch: false },
  );

  constructor(
    private actions$: Actions,
    private analytics: AnalyticsService,
  ) {}
}
```

---

## Effect Operator Selection Guide

| Use case | Operator | Why |
|---|---|---|
| Cancel in-flight on new action | `switchMap` | New action cancels pending request |
| Allow concurrent operations | `mergeMap` | Each action runs independently |
| Queue — one at a time | `concatMap` | No cancellation, sequential |
| Ignore while busy | `exhaustMap` | Drop new actions while current runs |
| Cancel on explicit action | `takeUntil(actions$.pipe(ofType(...)))` | Reactive cancellation |
| Parallel independent calls | `forkJoin` inside `switchMap` | All must complete |

---

## Related Guides

- **[NgRx Effects Patterns](./ngrx-effects-patterns.md)** — foundational effects guide
- **[NgRx ComponentStore](./rxjs-ngrx-component-store.md)** — local state alternative
- **[Build a Store from Scratch](./rxjs-store-from-scratch.md)** — understand the primitives
- **[Optimistic UI Patterns](./rxjs-optimistic-ui-patterns.md)** — broader optimistic update patterns
