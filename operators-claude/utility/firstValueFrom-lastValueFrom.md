# firstValueFrom / lastValueFrom

## Identity

- **Names**: `firstValueFrom`, `lastValueFrom`
- **Category**: Utility / Promise Interop
- **Type**: Observable → Promise converters — bridge between reactive and async/await code
- **Import**:
  ```typescript
  import { firstValueFrom, lastValueFrom } from 'rxjs';
  ```
- **Signatures**:
  ```typescript
  function firstValueFrom<T>(
    source: Observable<T>,
    config?: FirstValueFromConfig<T>
  ): Promise<T>

  function lastValueFrom<T>(
    source: Observable<T>,
    config?: LastValueFromConfig<T>
  ): Promise<T>
  ```
- **Added**: RxJS 7.0 (replaces deprecated `toPromise()`)

## Functional Specification

| | `firstValueFrom` | `lastValueFrom` |
|---|---|---|
| Resolves with | First emitted value | Last emitted value (after completion) |
| Unsubscribes after | First value | Source completes |
| On empty source | Throws `EmptyError` | Throws `EmptyError` |
| Default value option | `config.defaultValue` | `config.defaultValue` |
| Source must complete? | No — unsubscribes after first | Yes — waits for `complete` |

**Why not `toPromise()`**: `toPromise()` returned `undefined` on empty sources — a silent failure. Both `firstValueFrom` and `lastValueFrom` throw `EmptyError` on empty, making the failure explicit. Use `config.defaultValue` to opt into the old silent behavior.

## Marble Diagrams

```
firstValueFrom(--a--b--c--|):
  Promise resolves with 'a', unsubscribes immediately
  source: --a--b--c--|
  result: --a (unsub ✂️)

lastValueFrom(--a--b--c--|):
  Waits for completion, resolves with 'c'
  source: --a--b--c--|
  result: ----------c (after |)

firstValueFrom(------|):   → EmptyError (unless defaultValue provided)
lastValueFrom(------|):    → EmptyError (unless defaultValue provided)
```

## Examples

### Basic Usage — HTTP Request in async/await
```typescript
import { firstValueFrom } from 'rxjs';
import { ajax } from 'rxjs/ajax';

async function getUser(id: string): Promise<User> {
  return firstValueFrom(
    ajax.getJSON<User>(`/api/users/${id}`)
  );
}

// Use like any async function:
const user = await getUser('123');
console.log(user.name);
```

### Common Pattern — lastValueFrom for Aggregating Streams
```typescript
import { lastValueFrom } from 'rxjs';
import { toArray } from 'rxjs/operators';

async function getAllItems(): Promise<Item[]> {
  // toArray() collects all values then completes — lastValueFrom gets the array
  return lastValueFrom(
    itemStream$.pipe(toArray())
  );
}

// Or for a scan accumulation:
async function getRunningTotal(): Promise<number> {
  return lastValueFrom(
    numbers$.pipe(
      scan((acc, n) => acc + n, 0)
    )
  );
}
```

### Common Pattern — Default Value on Empty Source
```typescript
import { firstValueFrom, lastValueFrom, EMPTY } from 'rxjs';

// Without default — throws EmptyError on empty source:
try {
  const val = await firstValueFrom(EMPTY);
} catch (e) {
  console.error(e); // EmptyError
}

// With default — never throws:
const val = await firstValueFrom(EMPTY, { defaultValue: null });
// val === null

const last = await lastValueFrom(EMPTY, { defaultValue: 'fallback' });
// last === 'fallback'
```

### Common Pattern — Angular Service Integration
```typescript
import { firstValueFrom } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class UserService {
  constructor(private http: HttpClient) {}

  // Angular HttpClient returns Observable — convert for async/await consumers
  async getUser(id: string): Promise<User> {
    return firstValueFrom(this.http.get<User>(`/api/users/${id}`));
  }

  async updateUser(id: string, data: Partial<User>): Promise<User> {
    return firstValueFrom(
      this.http.put<User>(`/api/users/${id}`, data)
    );
  }
}
```

### Edge Case — Infinite Observable with firstValueFrom
```typescript
import { firstValueFrom } from 'rxjs';
import { interval } from 'rxjs';

// Safe — firstValueFrom unsubscribes after first value
const firstTick = await firstValueFrom(interval(1000));
// firstTick === 0, interval is cleaned up

// ❌ WRONG — lastValueFrom on infinite stream hangs forever
const last = await lastValueFrom(interval(1000));
// Never resolves — interval never completes
```

## Common Pitfalls

### Using `lastValueFrom` on a Non-Completing Stream

```typescript
// ❌ HANGS FOREVER — WebSocket never completes on its own
const message = await lastValueFrom(webSocket$.pipe(
  filter(m => m.type === 'response')
));

// ✅ CORRECT — bound with take(1) so it completes
const message = await lastValueFrom(webSocket$.pipe(
  filter(m => m.type === 'response'),
  take(1)  // complete after first matching message
));

// Or use firstValueFrom — semantically clearer here:
const message = await firstValueFrom(webSocket$.pipe(
  filter(m => m.type === 'response')
));
// WHY: lastValueFrom waits for source completion. Infinite streams never
// complete, so the Promise never resolves. firstValueFrom unsubscribes
// after the first value — correct for "get the next matching message".
```

### Swallowing `EmptyError` Silently

```typescript
// ❌ SILENT BUG — undefined is indistinguishable from a real value
const user = await userStream$.toPromise(); // deprecated
// user might be undefined if stream was empty

// ✅ EXPLICIT — EmptyError is thrown and must be handled
try {
  const user = await firstValueFrom(userStream$);
} catch (e) {
  if (e instanceof EmptyError) {
    // stream was empty — handle explicitly
    return null;
  }
  throw e; // rethrow unexpected errors
}

// Or opt-in to a default:
const user = await firstValueFrom(userStream$, { defaultValue: null });
// WHY: EmptyError forces you to decide what "no value" means rather than
// silently receiving undefined and potentially crashing later.
```

## Related Operators

- **`toPromise()`**: Deprecated predecessor — returns `undefined` on empty; avoid in new code
- **`take(1)`**: Complete after first value — use inside `lastValueFrom` as an alternative to `firstValueFrom`
- **`from(promise)`**: Inverse direction — Promise → Observable
- **`defer(() => from(asyncFn()))`**: Lazy Promise-to-Observable for deferred execution

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 4/5 | **Composability**: N/A (terminal — produces Promise)
**Key teaching points**:
1. `firstValueFrom` = take first and clean up; `lastValueFrom` = wait for completion
2. Both throw `EmptyError` on empty — use `defaultValue` to opt into silent fallback
3. Never use `lastValueFrom` on non-completing streams — it hangs forever
