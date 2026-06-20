# onErrorResumeNextWith

**Category**: Error Handling  
**Import**: `import { onErrorResumeNextWith } from 'rxjs';`

## Description

`onErrorResumeNextWith` subscribes to a sequence of observables one after another, moving to the next observable whenever the current one either completes *or* errors. Errors are silently swallowed — they never reach the subscriber's error handler. The result observable completes once all provided observables have been exhausted, regardless of how any of them ended.

This operator is a pipe-friendly version of the `onErrorResumeNext` creation function. Think of it as a more permissive `concatWith`: where `concatWith` requires each source to complete successfully before moving to the next, `onErrorResumeNextWith` moves on whether the source completed or errored. Because errors are discarded silently, use this operator only when you genuinely do not care about error details. If you need to inspect or react to specific errors, use `catchError` instead.

## Signature

```typescript
function onErrorResumeNextWith<T, A extends readonly unknown[]>(
  sources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>

function onErrorResumeNextWith<T, A extends readonly unknown[]>(
  ...sources: [...ObservableInputTuple<A>]
): OperatorFunction<T, T | A[number]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `sources` | `ObservableInput[]` (spread or array) | One or more observable inputs to subscribe to in sequence after the source ends (by completion or error). Can be passed as individual arguments or as a single array. |

## Return Type

`OperatorFunction<T, T | A[number]>` — an operator that returns an observable emitting all values from the source followed by all values from each provided source in order, continuing through errors silently, completing after the last source.

## Marble Diagram

```
Source:   --a--b--X
Next 1:             --c--d--|
Next 2:                      --e--|
          onErrorResumeNextWith(next1$, next2$)
Output:   --a--b--c--d--e--|
(X = error, silently skipped; output never errors)

Source:   --a--b--|
Next 1:             --c--X
Next 2:                   --d--|
          onErrorResumeNextWith(next1$, next2$)
Output:   --a--b--c--d--|
(error from next1$ also silently skipped)
```

## Examples

### Example 1: Fallback chain for a content API

```typescript
import { onErrorResumeNextWith } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs';

// Try primary API, then regional mirror, then static fallback — errors are
// silently ignored so the chain always advances to the next source.
const primaryApi$ = ajax.getJSON('/api/articles');
const mirrorApi$ = ajax.getJSON('https://mirror.example.com/api/articles');
const staticFallback$ = ajax.getJSON('/static/articles.json');

primaryApi$.pipe(
  map((response: any) => response.data),
  onErrorResumeNextWith(
    mirrorApi$.pipe(map((r: any) => r.data)),
    staticFallback$.pipe(map((r: any) => r.data))
  )
).subscribe({
  next: articles => console.log('Articles:', articles),
  // error callback will NEVER fire — all errors are silently consumed
  complete: () => console.log('Done')
});
```

### Example 2: Sequential data migrations that may partially fail

```typescript
import { from, onErrorResumeNextWith } from 'rxjs';
import { mergeMap, tap } from 'rxjs';

// Run three migration steps; if any step fails, continue with the rest
// (useful for best-effort data migrations where partial success is acceptable)
const migrationStep1$ = from(runMigration('add_index')).pipe(
  tap({ error: e => console.warn('Step 1 failed (non-fatal):', e.message) })
);
const migrationStep2$ = from(runMigration('backfill_nulls')).pipe(
  tap({ error: e => console.warn('Step 2 failed (non-fatal):', e.message) })
);
const migrationStep3$ = from(runMigration('rename_column')).pipe(
  tap({ error: e => console.warn('Step 3 failed (non-fatal):', e.message) })
);

migrationStep1$.pipe(
  onErrorResumeNextWith(migrationStep2$, migrationStep3$)
).subscribe({
  next: result => console.log('Step completed:', result),
  complete: () => console.log('All migration steps attempted')
  // No error handler needed — errors never surface here
});

function runMigration(name: string) {
  return Promise.resolve(`Migration '${name}' done`);
}
```

### Example 3: Preloading assets with graceful degradation

```typescript
import { from, onErrorResumeNextWith, toArray } from 'rxjs';
import { mergeMap, catchError, of } from 'rxjs';

// Load a list of image URLs; skip any that fail to load
const imageUrls = [
  'https://cdn.example.com/img/hero.jpg',
  'https://cdn.example.com/img/missing.jpg', // 404
  'https://cdn.example.com/img/footer.jpg'
];

// onErrorResumeNextWith works at the observable level; for per-item error
// tolerance within a single stream, use catchError per item:
const loadImage$ = (url: string) =>
  from(fetch(url)).pipe(
    catchError(() => of(null)) // returns null for failed images
  );

// To apply onErrorResumeNextWith across entirely separate streams:
const [img1$, img2$, img3$] = imageUrls.map(loadImage$);

img1$.pipe(
  onErrorResumeNextWith(img2$, img3$),
  toArray()
).subscribe(results => {
  const loaded = results.filter(Boolean).length;
  console.log(`Loaded ${loaded} of ${imageUrls.length} images`);
});
```

## Common Pitfalls

- **Errors are permanently discarded**: Unlike `catchError`, you have no access to the error value and no way to log or react to it from within `onErrorResumeNextWith`. If you need to inspect the error, use `catchError` and decide whether to continue from there. Silently swallowing errors can make debugging very difficult in production.

- **Confused with `catchError`**: `catchError` gives you the error and lets you return a single replacement observable. `onErrorResumeNextWith` works with a pre-defined list of fallback observables and does not expose the error at all. Choose `catchError` when error-specific logic matters; choose `onErrorResumeNextWith` when you want unconditional sequential execution.

- **Completion also advances the chain**: Unlike error-only operators, `onErrorResumeNextWith` advances to the next source on *both* complete and error. If your source completes normally and you did not intend to continue with the next source, this operator is the wrong choice — use `concatWith` instead for completion-only sequencing.

- **Empty source list is a no-op**: If no sources are passed, the operator behaves identically to the source observable with no error handling. Always provide at least one fallback source, or the operator adds no value.

## Related Operators

- `catchError` — intercepts errors and provides the error value to a selector function; use this when error details matter or when the fallback observable depends on the error
- `concatWith` — sequences observables like `onErrorResumeNextWith` but stops and propagates if any source errors; use when errors should not be ignored
- `retry` — resubscribes to the *same* source on error rather than moving to a new one; use when the same request should be retried
- `onErrorResumeNext` — the standalone creation function version of this operator; takes the source as its first argument rather than being used in a pipe
