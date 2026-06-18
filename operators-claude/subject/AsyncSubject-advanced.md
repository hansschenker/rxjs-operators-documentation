# AsyncSubject — Advanced Patterns

For `AsyncSubject` fundamentals see the core [AsyncSubject](./AsyncSubject) doc. This page covers one-shot caching, lazy initialisation, request deduplication, and the key differences from `BehaviorSubject`, `ReplaySubject(1)`, and `shareReplay(1)`.

---

## What Makes `AsyncSubject` Unique

`AsyncSubject` only emits its **last value**, and only **when it completes**. Every subscriber — including those that subscribe *after* completion — receives that last value immediately.

```typescript
import { AsyncSubject } from 'rxjs';

const subject = new AsyncSubject<number>();

subject.subscribe(v => console.log('A:', v)); // subscribes before completion
subject.next(1);
subject.next(2);
subject.next(3);
// Nothing emitted yet...

subject.complete(); // ← triggers emission of last value (3) to A

subject.subscribe(v => console.log('B:', v)); // subscribes AFTER completion
// B: 3 — receives cached last value immediately
// Output: A: 3, B: 3
```

---

## Pattern 1: One-Shot HTTP Request Cache

Cache a request result so all callers — concurrent or later — share one fetch:

```typescript
import { AsyncSubject } from 'rxjs';

class SingletonRequest<T> {
  private subject: AsyncSubject<T> | null = null;

  get(source$: Observable<T>): Observable<T> {
    if (!this.subject) {
      this.subject = new AsyncSubject<T>();
      source$.subscribe(this.subject); // pipe source into subject
    }
    return this.subject.asObservable();
  }

  invalidate(): void {
    this.subject = null; // next get() triggers fresh request
  }
}

// Usage — config loads once, shared by all subscribers:
class ConfigService {
  private cache = new SingletonRequest<AppConfig>();

  getConfig(): Observable<AppConfig> {
    return this.cache.get(
      this.http.get<AppConfig>('/api/config')
    );
  }

  clearConfig(): void {
    this.cache.invalidate();
  }
}
```

---

## Pattern 2: Lazy Initialisation Gate

Block all dependents until an async resource is ready:

```typescript
import { AsyncSubject } from 'rxjs';

class DatabaseConnection {
  private ready$ = new AsyncSubject<IDBDatabase>();

  constructor() {
    const request = indexedDB.open('app-db', 1);
    request.onsuccess = () => {
      this.ready$.next(request.result);
      this.ready$.complete();
    };
    request.onerror = () => this.ready$.error(request.error);
  }

  // All callers wait here until DB is open:
  execute<T>(operation: (db: IDBDatabase) => Observable<T>): Observable<T> {
    return this.ready$.pipe(
      switchMap(db => operation(db))
    );
  }
}

// Usage:
const db = new DatabaseConnection();

// Multiple concurrent callers — only one open() call:
db.execute(db => queryUsers(db)).subscribe(renderUsers);
db.execute(db => querySettings(db)).subscribe(applySettings);
```

---

## Pattern 3: Parallel Init with `forkJoin`

Gate app bootstrap on multiple async inits completing:

```typescript
import { AsyncSubject, forkJoin } from 'rxjs';

class AppBootstrap {
  private authReady$   = new AsyncSubject<AuthState>();
  private configReady$ = new AsyncSubject<Config>();
  private i18nReady$   = new AsyncSubject<Translations>();

  readonly ready$ = forkJoin({
    auth:   this.authReady$,
    config: this.configReady$,
    i18n:   this.i18nReady$
  });

  initAuth(state: AuthState): void {
    this.authReady$.next(state);
    this.authReady$.complete();
  }

  initConfig(cfg: Config): void {
    this.configReady$.next(cfg);
    this.configReady$.complete();
  }

  initI18n(t: Translations): void {
    this.i18nReady$.next(t);
    this.i18nReady$.complete();
  }
}

// App starts rendering only once all three complete:
const bootstrap = new AppBootstrap();
bootstrap.ready$.subscribe(({ auth, config, i18n }) => {
  mountApp({ auth, config, i18n });
});

// Initialise in parallel — order doesn't matter:
fetchAuth().subscribe(s => bootstrap.initAuth(s));
fetchConfig().subscribe(c => bootstrap.initConfig(c));
fetchTranslations().subscribe(t => bootstrap.initI18n(t));
```

---

## Pattern 4: Promise-Like One-Shot Notification

Expose a one-time "done" signal to multiple consumers:

```typescript
import { AsyncSubject } from 'rxjs';

class FileProcessor {
  private done$ = new AsyncSubject<ProcessResult>();

  readonly result$ = this.done$.asObservable();

  async process(file: File): Promise<void> {
    try {
      const result = await this.heavyProcess(file);
      this.done$.next(result);
      this.done$.complete();
    } catch (err) {
      this.done$.error(err);
    }
  }
}

// Usage — multiple consumers register interest before processing starts:
const processor = new FileProcessor();

processor.result$.subscribe(r => updateProgressUI(r));
processor.result$.subscribe(r => sendToAnalytics(r));
processor.result$.subscribe(r => notifyUser(r));

processor.process(uploadedFile);
// All three subscribers receive result exactly once when done
```

---

## `AsyncSubject` vs `ReplaySubject(1)` vs `shareReplay(1)` vs `BehaviorSubject`

```typescript
// AsyncSubject — emits LAST value, only on completion:
const as = new AsyncSubject<number>();
as.next(1); as.next(2); as.complete();
as.subscribe(v => console.log(v)); // 2 (last value)
// ✓ Perfect for "fetch once, cache forever" (HTTP responses)
// ✗ Nothing emitted until complete; errors discard buffered values

// ReplaySubject(1) — emits LAST value to late subscribers, completes normally:
const rs = new ReplaySubject<number>(1);
rs.next(1); rs.next(2); rs.complete();
rs.subscribe(v => console.log(v)); // 2 (replays last)
// ✓ Can emit intermediate values before completion
// ✓ Works without completing (unlike AsyncSubject)

// shareReplay(1) — multicasts a source, replays 1 to late subscribers:
const sp$ = source$.pipe(shareReplay(1));
// ✓ Composable with any operator pipeline
// ✗ Subject to refCount/memory-leak concerns

// BehaviorSubject — always holds current value, emits to every new subscriber:
const bs = new BehaviorSubject<number>(0);
bs.next(1); bs.next(2);
bs.subscribe(v => console.log(v)); // 2 (current value)
// ✓ Represents ongoing state; getValue() available
// ✗ Never "completes" normally; not suited for one-shot requests
```

**Decision rule**: Use `AsyncSubject` when the observable represents a **one-time computation** (like an HTTP request) and you want every subscriber — past, present, or future — to receive the same final result exactly once.

---

## Common Pitfalls

### Forgetting to Complete — Nothing Ever Emits

```typescript
// ❌ AsyncSubject with next() but no complete():
const subject = new AsyncSubject<number>();
subject.next(42);
subject.subscribe(v => console.log(v)); // never fires!
// AsyncSubject buffers values but ONLY emits on complete()

// ✅ Always call complete() to trigger emission:
subject.next(42);
subject.complete(); // now emits 42 to all subscribers
```

### Error Discards All Buffered Values

```typescript
// ❌ Expecting buffered value on error:
const subject = new AsyncSubject<number>();
subject.next(42);
subject.error(new Error('fail'));
subject.subscribe({
  next:  v   => console.log('value:', v),  // never called
  error: err => console.log('error:', err) // called with Error('fail')
});
// The buffered 42 is discarded — error takes precedence

// ✅ If you need a fallback value on error, use catchError before subscribing:
subject.pipe(
  catchError(() => of(0))
).subscribe(v => console.log(v)); // 0 on error
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**When to reach for `AsyncSubject`**: The clearest signal is "I need `shareReplay(1)` but the source is imperative (not a cold Observable)." Any time you're wrapping a callback-based API or a Promise and want to share the result with multiple future subscribers, `AsyncSubject` is the lowest-overhead option.
