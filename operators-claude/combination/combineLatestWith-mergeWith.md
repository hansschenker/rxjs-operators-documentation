# Pipeable Combination Aliases

## Identity

RxJS 7 introduced **pipeable** forms of static combination operators. These are used inside `.pipe()` rather than as standalone functions, making them composable with other operators in a chain.

| Pipeable operator | Equivalent static function |
|---|---|
| `combineLatestWith(...sources)` | `combineLatest([source, ...sources])` |
| `mergeWith(...sources)` | `merge(source, ...sources)` |
| `concatWith(...sources)` | `concat(source, ...sources)` |
| `zipWith(...sources)` | `zip(source, ...sources)` |
| `raceWith(...sources)` | `race(source, ...sources)` |
| `startWith(value)` | Already pipeable (unchanged) |
| `endWith(value)` | Already pipeable (unchanged) |

**Import**:
```typescript
import { combineLatestWith, mergeWith, concatWith, zipWith, raceWith } from 'rxjs/operators';
```

---

## `combineLatestWith`

```typescript
import { combineLatestWith } from 'rxjs/operators';

// Static form:
combineLatest([a$, b$, c$]).subscribe(([a, b, c]) => render(a, b, c));

// Pipeable form — composable inside a chain:
a$.pipe(
  map(normalizeA),
  combineLatestWith(b$.pipe(map(normalizeB)), c$),
  map(([a, b, c]) => merge(a, b, c))
).subscribe(render);
```

**When to prefer pipeable form**: When `a$` itself has upstream transformations and you want to keep the whole pipeline in one `.pipe()` chain. The static form is cleaner when all sources start from scratch.

```typescript
// Real-world: form validity + user permissions + feature flag
userInput$.pipe(
  debounceTime(300),
  combineLatestWith(permissions$, featureFlags$),
  map(([input, perms, flags]) => canSubmit(input, perms, flags))
).subscribe(enabled => submitBtn.disabled = !enabled);
```

---

## `mergeWith`

Merge another Observable's emissions into the current stream — concurrently, in arrival order.

```typescript
import { mergeWith } from 'rxjs/operators';
import { fromEvent } from 'rxjs';

// Combine click and keyboard events into one stream:
fromEvent<MouseEvent>(button, 'click').pipe(
  mergeWith(
    fromEvent<KeyboardEvent>(document, 'keydown').pipe(
      filter(e => e.key === 'Enter')
    )
  ),
  map(() => 'submit')
).subscribe(handleSubmit);

// Equivalent static form:
merge(
  fromEvent(button, 'click'),
  fromEvent(document, 'keydown').pipe(filter(e => e.key === 'Enter'))
).pipe(map(() => 'submit')).subscribe(handleSubmit);
```

---

## `concatWith`

Queue another Observable to start after the current one completes.

```typescript
import { concatWith } from 'rxjs/operators';
import { of } from 'rxjs';

// Intro animation → main content → outro animation
introAnimation$.pipe(
  concatWith(mainContent$, outroAnimation$)
).subscribe(renderFrame);

// HTTP sequence: load auth, then load user, then load preferences
getAuthToken$().pipe(
  concatWith(
    getUserProfile$(),
    getUserPreferences$()
  )
).subscribe(handleEach);

// Equivalent:
concat(getAuthToken$(), getUserProfile$(), getUserPreferences$()).subscribe(handleEach);
```

---

## `zipWith`

Pair emissions by index with another Observable — waits for both to emit before producing a pair.

```typescript
import { zipWith } from 'rxjs/operators';

// Pair questions with answers (index-aligned):
questions$.pipe(
  zipWith(answers$)
).subscribe(([question, answer]) => renderQA(question, answer));

// Equivalent:
zip(questions$, answers$).subscribe(([q, a]) => renderQA(q, a));
```

---

## `raceWith`

Subscribe to the first Observable to emit, ignore all others.

```typescript
import { raceWith } from 'rxjs/operators';

// Timeout race — whichever comes first:
apiCall$.pipe(
  raceWith(timer(5000).pipe(map(() => { throw new Error('timeout'); })))
).subscribe({
  next:  data => render(data),
  error: e    => showTimeout()
});
```

---

## Static vs Pipeable — When to Use Each

```typescript
// Prefer STATIC when all sources are independent and start at the same level:
combineLatest([userProfile$, userSettings$, featureFlags$])
  .subscribe(([profile, settings, flags]) => init(profile, settings, flags));

// Prefer PIPEABLE when one source is the "primary" and others augment it:
primaryData$.pipe(
  map(transform),            // transform primary first
  combineLatestWith(meta$),  // then combine with metadata
  filter(([data, meta]) => meta.enabled),
  map(([data]) => data)
).subscribe(render);
```

---

## Common Pitfalls

### Mixing Static and Pipeable Forms Inconsistently

```typescript
// ❌ CONFUSING — inconsistent style in same file
import { combineLatest } from 'rxjs';
import { mergeWith } from 'rxjs/operators';

// Static here...
const combined$ = combineLatest([a$, b$]);
// Pipeable there...
combined$.pipe(mergeWith(c$)).subscribe();

// ✅ CONSISTENT — pick one style per use case
// Static when sources are equal peers
const combined$ = combineLatest([a$, b$, c$]);
combined$.subscribe();

// Pipeable when there's a clear primary source
a$.pipe(combineLatestWith(b$, c$)).subscribe();
```

### `combineLatestWith` Requires All Sources to Have Emitted

```typescript
// ❌ NEVER EMITS if b$ or c$ haven't emitted yet
a$.pipe(combineLatestWith(b$, c$)).subscribe(render);
// Same rule as combineLatest — all sources need at least one value

// ✅ Seed with startWith to unblock:
a$.pipe(
  combineLatestWith(
    b$.pipe(startWith(defaultB)),
    c$.pipe(startWith(defaultC))
  )
).subscribe(render);
```

## Related Operators

- **`combineLatest`**: Static equivalent for `combineLatestWith`
- **`merge`**: Static equivalent for `mergeWith`
- **`concat`**: Static equivalent for `concatWith`
- **`zip`**: Static equivalent for `zipWith`
- **`withLatestFrom`**: Similar to `combineLatestWith` but only emits on the primary source
- **`startWith`**: Pipeable, no static equivalent needed

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 3/5 | **Composability**: 5/5
**Key teaching point**: Pipeable aliases exist purely for ergonomics when there's a clear "primary" Observable in the chain. Both forms are semantically identical.
