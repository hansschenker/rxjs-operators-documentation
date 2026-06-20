# mergeWith

**Category**: Combination  
**Import**: `import { mergeWith } from 'rxjs';`

## Description

`mergeWith` is the pipeable equivalent of the `merge` creation operator. It subscribes to the source Observable and all provided Observable inputs concurrently, forwarding every value emitted by any of them to the output stream in arrival order. The resulting Observable completes only after all participating sources complete. If any source errors, the error is immediately forwarded and all other subscriptions are cleaned up.

Unlike `combineLatestWith`, which emits arrays of latest values, `mergeWith` produces a flat stream of individual values from all sources interleaved by time. It is useful for merging multiple independent event streams into a single unified stream without caring about the relationships between the values.

## Signature

```typescript
function mergeWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| otherSources | `...ObservableInputTuple<A>` | One or more Observable inputs to merge with the source. |

## Return Type

`OperatorFunction<T, T | A[number]>` — An Observable that emits all values from the source and all provided Observables, interleaved in arrival order.

## Marble Diagram

```
Source A: --1-----3-----5--|
Source B: -----2-----4-----|
          mergeWith(B)
Output:   --1--2--3--4--5--|
```

## Examples

### Example 1: Unified event stream from multiple DOM event types

```typescript
import { fromEvent, map, mergeWith } from 'rxjs';

// Track any user interaction on the page for an idle-timeout feature
const clicks$ = fromEvent(document, 'click').pipe(map(() => 'click'));
const keydowns$ = fromEvent(document, 'keydown').pipe(map(() => 'keydown'));
const mousemoves$ = fromEvent(document, 'mousemove').pipe(map(() => 'mousemove'));

let lastActivity = Date.now();

clicks$.pipe(
  mergeWith(keydowns$, mousemoves$)
).subscribe(eventType => {
  lastActivity = Date.now();
  // Reset idle timeout whenever any interaction occurs
  resetIdleTimer();
});
```

### Example 2: Aggregating notifications from multiple WebSocket channels

```typescript
import { mergeWith, map } from 'rxjs';
import { webSocket } from 'rxjs/webSocket';

const orderUpdates$ = webSocket<OrderUpdate>('wss://api.example.com/orders').pipe(
  map(msg => ({ type: 'order', ...msg }))
);
const inventoryAlerts$ = webSocket<InventoryAlert>('wss://api.example.com/inventory').pipe(
  map(msg => ({ type: 'inventory', ...msg }))
);
const systemNotices$ = webSocket<SystemNotice>('wss://api.example.com/system').pipe(
  map(msg => ({ type: 'system', ...msg }))
);

orderUpdates$.pipe(
  mergeWith(inventoryAlerts$, systemNotices$)
).subscribe(notification => {
  displayNotification(notification);
});
```

### Example 3: Merging polling streams with user-triggered refresh

```typescript
import { interval, Subject, map, mergeWith, switchMap } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const autoRefresh$ = interval(60_000); // Poll every 60 seconds
const manualRefresh$ = new Subject<void>(); // Triggered by "Refresh" button

autoRefresh$.pipe(
  mergeWith(manualRefresh$), // Either timer or manual click triggers a refresh
  switchMap(() => ajax.getJSON<DashboardData>('/api/dashboard'))
).subscribe(data => updateDashboard(data));

// Wire up the refresh button
document.getElementById('refresh-btn')!.addEventListener('click', () => {
  manualRefresh$.next();
});
```

## Common Pitfalls

- **Output type is a union, not a tuple**: `mergeWith` emits `T | A[number]`, so the downstream subscriber must handle all possible value types. Add a `map` or type guard to narrow the type if the sources emit different shapes.
- **Completion requires all sources to complete**: The output stays open until every merged source completes. An infinite source like `interval` will keep the merged stream alive indefinitely. Be deliberate about completing merged streams using operators like `takeUntil` or `take`.
- **Error from any source terminates the whole stream**: A single failing source propagates its error and cancels all remaining subscriptions. Wrap individual sources in `catchError` before merging if you need fault isolation.

## Related Operators

- `merge` — creation operator equivalent; takes a static array of Observable inputs without needing a pipe source
- `concatWith` — subscribes to sources one at a time in sequence rather than concurrently
- `raceWith` — subscribes to all sources but mirrors only the first one to emit
- `combineLatestWith` — emits arrays of latest values rather than a flat interleaved stream
