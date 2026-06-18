# GraphQL Patterns with RxJS

GraphQL subscriptions as Observables, mutation + refetch patterns, optimistic responses, and integrating Apollo/URQL with RxJS streams.

---

## GraphQL + RxJS: The Natural Fit

GraphQL has three operation types:
- **Query** — one-shot fetch → `Observable` that completes after one emission
- **Mutation** — one-shot write → `Observable` that completes after one emission  
- **Subscription** — live stream → `Observable` that never completes (until unsubscribed)

RxJS operators map perfectly onto all three.

---

## Pattern 1: Wrapping Apollo Client

```typescript
import { from, Observable } from 'rxjs';
import { ApolloClient, gql, DocumentNode, OperationVariables } from '@apollo/client';

class RxApolloClient {
  constructor(private client: ApolloClient<unknown>) {}

  query<T, V extends OperationVariables = OperationVariables>(
    query: DocumentNode,
    variables?: V
  ): Observable<T> {
    return from(
      this.client.query<{ data: T }>({ query, variables, fetchPolicy: 'network-only' })
    ).pipe(
      map(result => {
        if (result.errors?.length) throw new Error(result.errors[0].message);
        return result.data as unknown as T;
      })
    );
  }

  mutate<T, V extends OperationVariables = OperationVariables>(
    mutation: DocumentNode,
    variables?: V
  ): Observable<T> {
    return from(
      this.client.mutate<T>({ mutation, variables })
    ).pipe(
      map(result => {
        if (result.errors?.length) throw new Error(result.errors[0].message);
        return result.data!;
      })
    );
  }

  subscribe<T, V extends OperationVariables = OperationVariables>(
    query: DocumentNode,
    variables?: V
  ): Observable<T> {
    return new Observable<T>(subscriber => {
      const sub = this.client.subscribe<T>({ query, variables }).subscribe({
        next:     result => subscriber.next(result.data!),
        error:    err    => subscriber.error(err),
        complete: ()     => subscriber.complete()
      });
      return () => sub.unsubscribe();
    });
  }
}
```

---

## Pattern 2: Mutation + Cache Refetch

After a mutation, refetch queries that depend on the changed data:

```typescript
const ADD_COMMENT = gql`
  mutation AddComment($postId: ID!, $text: String!) {
    addComment(postId: $postId, text: $text) { id text author { name } }
  }
`;

const GET_COMMENTS = gql`
  query GetComments($postId: ID!) {
    comments(postId: $postId) { id text author { name } }
  }
`;

// Submit comment → refetch comments for the post:
submitComment$.pipe(
  exhaustMap(({ postId, text }) =>
    apollo.mutate<AddCommentMutation>(ADD_COMMENT, { postId, text }).pipe(
      switchMap(() => apollo.query<GetCommentsQuery>(GET_COMMENTS, { postId })),
      catchError(err => {
        showError(err.message);
        return EMPTY;
      })
    )
  ),
  takeUntilDestroyed()
).subscribe(data => updateCommentList(data.comments));
```

---

## Pattern 3: Live Subscription with Reconnect

GraphQL subscriptions over WebSocket — auto-reconnect on disconnect:

```typescript
const MESSAGES_SUBSCRIPTION = gql`
  subscription OnNewMessage($roomId: ID!) {
    messageAdded(roomId: $roomId) { id text sender { name } createdAt }
  }
`;

function roomMessages$(roomId: string): Observable<Message> {
  return defer(() =>
    apollo.subscribe<{ messageAdded: Message }>(
      MESSAGES_SUBSCRIPTION,
      { roomId }
    ).pipe(
      map(data => data.messageAdded),
      retryWhen(errors =>
        errors.pipe(
          scan((n, err) => { if (n >= 5) throw err; return n + 1; }, 0),
          delayWhen(n => timer(1000 * Math.pow(2, n)))
        )
      )
    )
  );
}

// Accumulate messages in state:
roomMessages$(currentRoomId).pipe(
  scan((messages: Message[], msg) => [...messages, msg], []),
  startWith([] as Message[]),
  takeUntilDestroyed()
).subscribe(messages => renderMessageList(messages));
```

---

## Pattern 4: Optimistic Mutation

Apply changes immediately, rollback on error:

```typescript
import { BehaviorSubject } from 'rxjs';

interface TodoState { todos: Todo[]; }

const state$ = new BehaviorSubject<TodoState>({ todos: [] });

const TOGGLE_TODO = gql`
  mutation ToggleTodo($id: ID!) {
    toggleTodo(id: $id) { id done }
  }
`;

function toggleTodo(id: string): void {
  const current = state$.getValue();

  // Optimistic update:
  state$.next({
    todos: current.todos.map(t => t.id === id ? { ...t, done: !t.done } : t)
  });

  apollo.mutate<ToggleTodoMutation>(TOGGLE_TODO, { id }).pipe(
    catchError(err => {
      // Rollback on error:
      state$.next(current);
      showError('Failed to update todo');
      return EMPTY;
    })
  ).subscribe(result => {
    // Confirm with server value:
    state$.next({
      todos: state$.getValue().todos.map(t =>
        t.id === id ? { ...t, done: result.toggleTodo.done } : t
      )
    });
  });
}
```

---

## Pattern 5: Subscription + Initial Query (Catch-Up)

Fetch initial data, then stream live updates:

```typescript
const GET_MESSAGES = gql`
  query GetMessages($roomId: ID!, $after: String) {
    messages(roomId: $roomId, after: $after) { id text createdAt }
  }
`;

function messagesWithCatchup$(roomId: string): Observable<Message[]> {
  return apollo.query<GetMessagesQuery>(GET_MESSAGES, { roomId }).pipe(
    map(data => data.messages),
    switchMap(initial => {
      const lastId = initial[initial.length - 1]?.id;

      return roomMessages$(roomId).pipe(
        // Only emit messages newer than our initial fetch:
        filter(msg => msg.id > (lastId ?? '')),
        scan((msgs, msg) => [...msgs, msg], initial),
        startWith(initial)
      );
    })
  );
}

messagesWithCatchup$(roomId).pipe(
  takeUntilDestroyed()
).subscribe(renderMessages);
```

---

## Pattern 6: Paginated Query with Load More

```typescript
const GET_USERS = gql`
  query GetUsers($cursor: String, $limit: Int!) {
    users(cursor: $cursor, limit: $limit) {
      items { id name email }
      nextCursor
      hasMore
    }
  }
`;

interface PagedState { items: User[]; cursor: string | null; hasMore: boolean; loading: boolean; }

const loadMore$ = new Subject<void>();

const users$ = loadMore$.pipe(
  startWith(null), // initial load
  scan((state: PagedState) => ({ ...state, loading: true }),
       { items: [], cursor: null, hasMore: true, loading: false }),
  switchMap(state =>
    apollo.query<GetUsersQuery>(GET_USERS, {
      cursor: state.cursor,
      limit:  20
    }).pipe(
      map(data => ({
        items:    [...state.items, ...data.users.items],
        cursor:   data.users.nextCursor,
        hasMore:  data.users.hasMore,
        loading:  false
      })),
      startWith({ ...state, loading: true }),
      catchError(() => of({ ...state, loading: false }))
    )
  ),
  shareReplay(1)
);

users$.pipe(takeUntilDestroyed()).subscribe(renderUserList);
loadMoreButton$.subscribe(() => {
  if (!users$.getValue()?.loading) loadMore$.next();
});
```

---

## Pattern 7: URQL Integration

URQL's `client.executeQuery` returns an AsyncIterable — convert to Observable:

```typescript
import { from } from 'rxjs';
import { Client, gql } from '@urql/core';

function urqlQuery$<T>(
  client: Client,
  query:  ReturnType<typeof gql>,
  variables?: object
): Observable<T> {
  return new Observable<T>(subscriber => {
    const sub = client
      .executeQuery({ query, variables: variables ?? {} })
      [Symbol.asyncIterator]();

    let cancelled = false;

    (async () => {
      try {
        for await (const result of { [Symbol.asyncIterator]: () => sub }) {
          if (cancelled) break;
          if (result.error) { subscriber.error(result.error); return; }
          subscriber.next(result.data as T);
        }
        subscriber.complete();
      } catch (err) {
        subscriber.error(err);
      }
    })();

    return () => { cancelled = true; };
  });
}
```

---

## Common Pitfalls

### Not Unsubscribing From GraphQL Subscriptions

```typescript
// ❌ Subscription keeps WebSocket connection open after component unmounts:
apollo.subscribe(MESSAGES_SUB).subscribe(msg => renderMessage(msg));

// ✅ Always tie subscription to component lifetime:
apollo.subscribe<{ messageAdded: Message }>(MESSAGES_SUB).pipe(
  map(d => d.messageAdded),
  takeUntilDestroyed()
).subscribe(renderMessage);
```

### Using `switchMap` for Mutations (Cancels In-Flight Writes)

```typescript
// ❌ switchMap cancels previous mutation when user saves quickly:
saveButton$.pipe(
  switchMap(data => apollo.mutate(SAVE_MUTATION, { data }))
)
// Two rapid saves: first write cancelled mid-flight — data may not persist

// ✅ exhaustMap (ignore during in-flight) or concatMap (queue):
saveButton$.pipe(
  exhaustMap(data => apollo.mutate(SAVE_MUTATION, { data }))
)
```
