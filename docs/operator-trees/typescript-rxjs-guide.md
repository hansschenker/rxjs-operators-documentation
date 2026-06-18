# TypeScript + RxJS Guide

Type-safe Observable pipelines, generic operators, type narrowing, and common TypeScript patterns for RxJS code.

---

## 1. Type Inference in Pipelines

TypeScript infers types through `pipe()` automatically in most cases:

```typescript
import { of } from 'rxjs';
import { map, filter } from 'rxjs/operators';

// Types flow through the pipeline:
of(1, 2, 3)             // Observable<number>
  .pipe(
    map(x => x * 2),    // Observable<number>
    map(x => String(x)) // Observable<string>
  )
  .subscribe(s => s.toUpperCase()); // TypeScript knows s is string
```

When inference breaks down, annotate the first operator:

```typescript
source$.pipe(
  map((x): ProcessedItem => ({ ...x, processed: true })), // explicit return type
  filter(x => x.active)
)
```

---

## 2. Type-Safe `filter` with Type Guards

`filter` preserves the input type by default. Use a type predicate to narrow:

```typescript
import { filter, map } from 'rxjs/operators';

interface Cat  { type: 'cat';  meow: () => void; }
interface Dog  { type: 'dog';  bark: () => void; }
type Animal = Cat | Dog;

const animals$ = of<Animal>(
  { type: 'cat', meow: () => console.log('meow') },
  { type: 'dog', bark: () => console.log('woof') }
);

// ❌ No narrowing — result is Observable<Animal>
animals$.pipe(filter(a => a.type === 'cat')).subscribe(a => a.meow()); // ERROR: no meow on Animal

// ✅ Type predicate — result is Observable<Cat>
animals$.pipe(
  filter((a): a is Cat => a.type === 'cat')
).subscribe(cat => cat.meow()); // TypeScript knows cat is Cat
```

### Type-Safe null/undefined Filter

```typescript
import { filter } from 'rxjs/operators';

// Remove nulls and get Observable<T> (not Observable<T | null>):
function filterNull<T>() {
  return filter((x: T | null | undefined): x is T => x != null);
}

const maybeUsers$: Observable<User | null> = /* ... */;
const users$: Observable<User> = maybeUsers$.pipe(filterNull());
```

---

## 3. Generic Custom Operators

```typescript
import { OperatorFunction, MonoTypeOperatorFunction } from 'rxjs';
import { map, filter } from 'rxjs/operators';

// Generic transformation operator:
function pluckTyped<T, K extends keyof T>(key: K): OperatorFunction<T, T[K]> {
  return map(obj => obj[key]);
}

// Usage — fully typed:
interface User { id: string; name: string; age: number; }
users$.pipe(
  pluckTyped('name')  // Observable<string>
).subscribe(name => name.toUpperCase()); // TypeScript knows it's string

// Generic filter with discriminated union:
function ofType<T extends { type: string }, K extends T['type']>(
  ...types: K[]
): OperatorFunction<T, Extract<T, { type: K }>> {
  return filter((action): action is Extract<T, { type: K }> =>
    types.includes(action.type as K)
  );
}

// Usage in NgRx-style:
type Action =
  | { type: 'LOAD'; id: string }
  | { type: 'SAVE'; data: unknown }
  | { type: 'DELETE'; id: string };

actions$.pipe(
  ofType('LOAD', 'DELETE')  // Observable<{ type: 'LOAD'; id: string } | { type: 'DELETE'; id: string }>
).subscribe(action => console.log(action.id)); // id is always present
```

---

## 4. Typing `combineLatest` Results

```typescript
import { combineLatest } from 'rxjs';

// Dictionary form — cleaner and fully typed:
combineLatest({
  user:  userStream$,      // Observable<User>
  count: countStream$,     // Observable<number>
  flag:  featureStream$    // Observable<boolean>
}).subscribe(({ user, count, flag }) => {
  // user: User, count: number, flag: boolean — all inferred
});

// Array form — use tuple typing to preserve individual types:
combineLatest([
  of(1),      // Observable<number>
  of('hello') // Observable<string>
] as const).subscribe(([num, str]) => {
  // Without `as const`: [number | string, number | string] — wrong!
  // With `as const`: [number, string] — correct tuple
  console.log(num.toFixed(2)); // TypeScript knows num is number
});
```

---

## 5. Typing Subjects

```typescript
import { Subject, BehaviorSubject, ReplaySubject } from 'rxjs';

// Always type Subjects explicitly:
const events$   = new Subject<UserEvent>();           // Subject<UserEvent>
const selected$ = new BehaviorSubject<Item | null>(null); // BehaviorSubject<Item | null>
const history$  = new ReplaySubject<Action>(10);      // ReplaySubject<Action>

// Expose read-only Observable from services:
class EventService {
  private events$ = new Subject<UserEvent>();

  // External consumers get Observable<UserEvent>, cannot call .next()
  readonly events: Observable<UserEvent> = this.events$.asObservable();

  emit(event: UserEvent): void {
    this.events$.next(event);
  }
}
```

---

## 6. Typing `scan` Accumulator

`scan` often needs explicit typing when the accumulator type differs from the source:

```typescript
import { scan } from 'rxjs/operators';

interface State { count: number; items: string[] }

// Without explicit type — TypeScript may infer wrong accumulator type:
source$.pipe(
  scan((state: State, item: string) => ({    // annotate accumulator type
    count: state.count + 1,
    items: [...state.items, item]
  }), { count: 0, items: [] } as State)      // annotate seed type
)

// Or use the full generic form:
source$.pipe(
  scan<string, State>((state, item) => ({
    count: state.count + 1,
    items: [...state.items, item]
  }), { count: 0, items: [] })
)
```

---

## 7. Typing `switchMap` / `mergeMap` with Generics

```typescript
import { switchMap } from 'rxjs/operators';

// When the inner Observable type differs from outer:
interface UserId { id: string }
interface UserProfile { name: string; email: string }

const userId$: Observable<UserId> = /* ... */;

const profile$: Observable<UserProfile> = userId$.pipe(
  switchMap(({ id }): Observable<UserProfile> =>
    this.http.get<UserProfile>(`/api/users/${id}`)
  )
);
```

---

## 8. `firstValueFrom` / `lastValueFrom` Type Inference

```typescript
import { firstValueFrom, lastValueFrom } from 'rxjs';

// Type is inferred from the Observable:
const user: User = await firstValueFrom(userStream$);    // User
const items: Item[] = await lastValueFrom(
  itemStream$.pipe(toArray())
); // Item[]

// With defaultValue — union type:
const maybeUser: User | null = await firstValueFrom(
  userStream$,
  { defaultValue: null }
); // User | null
```

---

## 9. Type-Safe Error Handling

```typescript
import { catchError, throwError } from 'rxjs/operators';

// catchError callback receives unknown — always narrow:
source$.pipe(
  catchError((err: unknown) => {
    if (err instanceof HttpErrorResponse) {
      return of({ statusCode: err.status, message: err.message });
    }
    if (err instanceof Error) {
      return throwError(() => err); // re-throw typed Error
    }
    return throwError(() => new Error(String(err))); // normalize unknown
  })
)
```

---

## 10. Discriminated Union Actions with RxJS

```typescript
type AppAction =
  | { type: 'INCREMENT'; amount: number }
  | { type: 'DECREMENT'; amount: number }
  | { type: 'RESET' };

const actions$ = new Subject<AppAction>();

// Fully type-safe reducer with scan:
const state$ = actions$.pipe(
  scan((count: number, action: AppAction): number => {
    switch (action.type) {
      case 'INCREMENT': return count + action.amount; // amount: number ✓
      case 'DECREMENT': return count - action.amount; // amount: number ✓
      case 'RESET':     return 0;
    }
  }, 0)
);
```

---

## 11. `OperatorFunction` Composition

```typescript
import { pipe } from 'rxjs';
import { OperatorFunction, MonoTypeOperatorFunction } from 'rxjs';

// Compose operators into a reusable typed operator:
function searchPipeline(): MonoTypeOperatorFunction<string> {
  return pipe(
    debounceTime(300),
    distinctUntilChanged(),
    filter(q => q.length >= 2)
  );
}

function toUserResults(): OperatorFunction<string, User[]> {
  return pipe(
    switchMap(q => userApi.search(q)),
    catchError(() => of([] as User[]))
  );
}

// Compose pipelines:
searchInput$.pipe(
  searchPipeline(),   // string → string
  toUserResults()     // string → User[]
).subscribe(renderUsers);
```

---

## Common TypeScript Pitfalls

### `filter` Without Type Predicate Doesn't Narrow

```typescript
// ❌ Still Observable<string | null> after filter:
const obs$: Observable<string | null> = /* ... */;
obs$.pipe(filter(x => x !== null)).subscribe(x => x.toUpperCase()); // ERROR

// ✅ Type predicate narrows correctly:
obs$.pipe(filter((x): x is string => x !== null))
  .subscribe(x => x.toUpperCase()); // x is string ✓
```

### `combineLatest` Array Without `as const`

```typescript
// ❌ Loses tuple types — all inferred as (string | number)[]
combineLatest([of(1), of('a')]).subscribe(([n, s]) => n.toFixed()); // ERROR

// ✅ as const preserves tuple:
combineLatest([of(1), of('a')] as const).subscribe(([n, s]) => n.toFixed()); // ✓
```

### Typing `scan` Seed Too Narrowly

```typescript
// ❌ Seed inferred as { items: never[] } — can't push strings
source$.pipe(scan((acc, x) => ({ items: [...acc.items, x] }), { items: [] }));

// ✅ Annotate seed type explicitly:
source$.pipe(scan((acc, x) => ({ items: [...acc.items, x] }), { items: [] as string[] }));
```
