# raceWith

**Category**: Combination  
**Import**: `import { raceWith } from 'rxjs';`

## Description

`raceWith` is the pipeable equivalent of the `race` creation operator. It subscribes to the source Observable and all provided Observable inputs simultaneously. The first source to emit a value, error, or completion "wins" the race — `raceWith` then mirrors that winning Observable exclusively and unsubscribes from all others. The output Observable follows the winning source for the rest of its lifetime.

If no additional sources are provided, `raceWith` passes the source through unchanged. `raceWith` is useful for implementing timeouts, fallbacks, and scenarios where you want the fastest available data source to take over.

## Signature

```typescript
function raceWith<T, A extends readonly unknown[]>(
  ...otherSources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| otherSources | `...ObservableInputTuple<A>` | One or more Observable inputs to race against the source. The first to emit determines the output. |

## Return Type

`OperatorFunction<T, T | A[number]>` — An Observable that mirrors whichever of the source or provided Observables emits first.

## Marble Diagram

```
Source A: --------1--2--3--|   (slow)
Source B: --a--b--c--------|   (fast — wins the race)
Source C: ----x--y---------|
          raceWith(B, C)
Output:   --a--b--c--------|   (A and C are unsubscribed immediately)
```

## Examples

### Example 1: Request with a timeout fallback

```typescript
import { timer, map, raceWith } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const request$ = ajax.getJSON<UserProfile>('/api/user/profile');
const timeout$ = timer(5000).pipe(
  map(() => ({ error: 'Request timed out', profile: null }))
);

request$.pipe(
  raceWith(timeout$)
).subscribe(result => {
  if ('error' in result) {
    console.warn(result.error);
    showCachedProfile();
  } else {
    renderProfile(result);
  }
});
```

### Example 2: Use the fastest of several CDN mirrors

```typescript
import { interval, map, raceWith } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const cdnMirrors = [
  'https://cdn1.example.com/asset.js',
  'https://cdn2.example.com/asset.js',
  'https://cdn3.example.com/asset.js',
];

const [primary, ...others] = cdnMirrors.map(url =>
  ajax({ url, responseType: 'text' }).pipe(
    map(response => response.response as string)
  )
);

primary.pipe(
  raceWith(...others)
).subscribe(scriptContent => {
  // Use whichever CDN responded first
  eval(scriptContent);
});
```

### Example 3: Cancel a pending operation when the user navigates away

```typescript
import { Subject, raceWith, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Emits when the user leaves the current route
const navigationAway$ = new Subject<void>();

function loadReportData(reportId: string) {
  const data$ = ajax.getJSON<ReportData>(`/api/reports/${reportId}`);
  const cancelled$ = navigationAway$.pipe(map(() => null));

  return data$.pipe(
    raceWith(cancelled$)
  );
}

loadReportData('q4-2025').subscribe(data => {
  if (data !== null) {
    renderReport(data);
  }
  // If null, user navigated away — silently do nothing
});
```

## Common Pitfalls

- **Losers are unsubscribed immediately**: As soon as one source emits, all other sources are unsubscribed. Any side effects initiated by those subscriptions (e.g. in-flight HTTP requests) may be abandoned mid-flight. Ensure sources handle cancellation gracefully.
- **An error on the winning source propagates**: If the first source to emit does so with an error, that error becomes the error of the output Observable. The other sources never get a chance to succeed.
- **Synchronous sources always win**: If two sources emit synchronously at the same tick, the first one in the argument list wins. Be careful when mixing synchronous and asynchronous sources.
- **Not a fallback on error**: `raceWith` picks the fastest emitter, not a fallback when one fails. For error fallback logic, use `catchError` with an alternative Observable.

## Related Operators

- `race` — creation operator equivalent; takes a static array of Observable inputs without a pipe source
- `mergeWith` — subscribes to all sources and emits from all of them, rather than picking the winner
- `combineLatestWith` — emits whenever any source emits but keeps all sources active
