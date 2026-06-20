import { defineConfig } from 'vitepress'

export default defineConfig({
  base: '/rxjs-operators-documentation/',
  ignoreDeadLinks: true,
  title: 'RxJS Operator Documentation',
  description: 'Comprehensive formal specifications for RxJS operators using the eight-policy framework',

  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      {
        text: 'Operators',
        items: [
          { text: 'Transformation', link: '/operators-claude/transformation/map' },
          { text: 'Filtering', link: '/operators-claude/filtering/filter' },
          { text: 'Combination', link: '/operators-claude/combination/combineLatest-operator-documentation' },
          { text: 'Creation', link: '/operators-claude/creation/EMPTY-NEVER' },
          { text: 'Error Handling', link: '/operators-claude/error-handling/catchError' },
          { text: 'Multicasting', link: '/operators-claude/multicasting/share' },
          { text: 'Rate Limiting', link: '/operators-claude/rate-limiting/auditTime-sampleTime' },
          { text: 'Higher-Order', link: '/operators-claude/higher-order/combineLatestAll' },
          { text: 'Mathematical / Aggregate', link: '/operators-claude/mathematical-aggregate/min-max' },
          { text: 'Conditional', link: '/operators-claude/conditional/conditional-operators' },
          { text: 'Subject', link: '/operators-claude/subject/AsyncSubject' },
          { text: 'Connectable Observable', link: '/operators-claude/connectable/connectable-connect' },
          { text: 'Testing / Debugging', link: '/operators-claude/testing/TestScheduler' },
          { text: 'Interop', link: '/operators-claude/interop/async-iterable' },
          { text: 'Utility', link: '/operators-claude/utility/finalize' },
        ]
      },
      { text: 'Reference', link: '/docs/operator-trees/overview' },
    ],

    sidebar: {
      '/operators-claude/transformation/': [
        {
          text: 'Transformation',
          items: [
            { text: 'map', link: '/operators-claude/transformation/map' },
            { text: 'mergeMap', link: '/operators-claude/transformation/mergeMap' },
            { text: 'switchMap', link: '/operators-claude/transformation/switchMap' },
            { text: 'concatMap', link: '/operators-claude/transformation/concatMap' },
            { text: 'exhaustMap', link: '/operators-claude/transformation/exhaustMap' },
            { text: 'pairwise', link: '/operators-claude/transformation/pairwise' },
            { text: 'groupBy', link: '/operators-claude/transformation/groupBy' },
            { text: 'bufferTime', link: '/operators-claude/transformation/bufferTime' },
            { text: 'bufferTime (advanced)', link: '/operators-claude/transformation/bufferTime-advanced' },
            { text: 'bufferCount', link: '/operators-claude/transformation/bufferCount' },
            { text: 'buffer / window', link: '/operators-claude/transformation/buffer-window' },
            { text: 'buffer / window (advanced)', link: '/operators-claude/transformation/buffer-window-advanced' },
            { text: 'bufferWhen / windowWhen', link: '/operators-claude/transformation/bufferWhen-windowWhen' },
            { text: 'bufferWhen / windowWhen (advanced)', link: '/operators-claude/transformation/bufferWhen-windowWhen-advanced' },
            { text: 'bufferToggle / windowToggle', link: '/operators-claude/transformation/bufferToggle-windowToggle' },
            { text: 'bufferToggle / windowToggle (advanced)', link: '/operators-claude/transformation/bufferToggle-windowToggle-advanced' },
            { text: 'windowCount', link: '/operators-claude/transformation/windowCount' },
            { text: 'switchScan', link: '/operators-claude/transformation/switchScan' },
            { text: 'switchScan (advanced)', link: '/operators-claude/transformation/switchScan-advanced' },
            { text: 'mergeScan', link: '/operators-claude/transformation/mergeScan' },
            { text: 'mergeScan (advanced)', link: '/operators-claude/transformation/mergeScan-advanced' },
            { text: 'expand', link: '/operators-claude/transformation/expand' },
            { text: 'expand (advanced)', link: '/operators-claude/transformation/expand-advanced' },
            { text: 'windowTime', link: '/operators-claude/transformation/windowTime' },
            { text: 'windowTime (advanced)', link: '/operators-claude/transformation/windowTime-advanced' },
            { text: 'groupBy (advanced)', link: '/operators-claude/transformation/groupBy-advanced' },
            { text: 'concatMap (advanced)', link: '/operators-claude/transformation/concatMap-advanced' },
            { text: 'switchMap (advanced)', link: '/operators-claude/transformation/switchMap-advanced' },
            { text: 'mergeMap (advanced)', link: '/operators-claude/transformation/mergeMap-advanced' },
            { text: 'bufferCount / windowCount (advanced)', link: '/operators-claude/transformation/bufferCount-windowCount-advanced' },
            { text: 'map (advanced)', link: '/operators-claude/transformation/map-advanced' },
            { text: 'pairwise (advanced)', link: '/operators-claude/transformation/pairwise-advanced' },
            { text: 'exhaustMap (advanced)', link: '/operators-claude/transformation/exhaustMap-advanced' },
          ]
        },
      ],

      '/operators-claude/filtering/': [
        {
          text: 'Filtering',
          items: [
            { text: 'filter', link: '/operators-claude/filtering/filter' },
            { text: 'debounceTime', link: '/operators-claude/filtering/debounceTime' },
            { text: 'distinctUntilChanged', link: '/operators-claude/filtering/distinctUntilChanged' },
            { text: 'take', link: '/operators-claude/filtering/take' },
            { text: 'takeUntil', link: '/operators-claude/filtering/takeUntil' },
            { text: 'throttleTime', link: '/operators-claude/filtering/throttleTime' },
            { text: 'first / last', link: '/operators-claude/filtering/first-last' },
            { text: 'takeWhile', link: '/operators-claude/filtering/takeWhile' },
            { text: 'skipUntil / skipWhile', link: '/operators-claude/filtering/skipUntil-skipWhile' },
            { text: 'distinctUntilKeyChanged', link: '/operators-claude/filtering/distinctUntilKeyChanged' },
            { text: 'distinctUntilKeyChanged (advanced)', link: '/operators-claude/filtering/distinctUntilKeyChanged-advanced' },
            { text: 'find / findIndex', link: '/operators-claude/filtering/find-findIndex' },
            { text: 'find / findIndex (advanced)', link: '/operators-claude/filtering/find-findIndex-advanced' },
            { text: 'distinct', link: '/operators-claude/filtering/distinct' },
            { text: 'skip / takeLast / elementAt', link: '/operators-claude/filtering/skip-takeLast-elementAt' },
            { text: 'take / skip (advanced)', link: '/operators-claude/filtering/take-skip-advanced' },
            { text: 'debounceTime (advanced)', link: '/operators-claude/filtering/debounceTime-advanced' },
            { text: 'throttleTime (advanced)', link: '/operators-claude/filtering/throttleTime-advanced' },
            { text: 'distinctUntilChanged (advanced)', link: '/operators-claude/filtering/distinctUntilChanged-advanced' },
            { text: 'takeUntil (advanced)', link: '/operators-claude/filtering/takeUntil-advanced' },
            { text: 'takeWhile (advanced)', link: '/operators-claude/filtering/takeWhile-advanced' },
            { text: 'filter (advanced)', link: '/operators-claude/filtering/filter-advanced' },
            { text: 'distinct (advanced)', link: '/operators-claude/filtering/distinct-advanced' },
            { text: 'first / last (advanced)', link: '/operators-claude/filtering/first-last-advanced' },
            { text: 'skipUntil / skipWhile (advanced)', link: '/operators-claude/filtering/skipUntil-skipWhile-advanced' },
          ]
        },
      ],

      '/operators-claude/combination/': [
        {
          text: 'Combination',
          items: [
            { text: 'combineLatest', link: '/operators-claude/combination/combineLatest-operator-documentation' },
            { text: 'startWith', link: '/operators-claude/combination/startWith' },
            { text: 'forkJoin', link: '/operators-claude/combination/forkJoin' },
            { text: 'merge', link: '/operators-claude/combination/merge' },
            { text: 'concat', link: '/operators-claude/combination/concat' },
            { text: 'withLatestFrom', link: '/operators-claude/combination/withLatestFrom' },
            { text: 'zip', link: '/operators-claude/combination/zip' },
            { text: 'race', link: '/operators-claude/combination/race' },
            { text: 'Pipeable Aliases', link: '/operators-claude/combination/pipeable-aliases' },
            { text: 'combineLatestWith / mergeWith / concatWith', link: '/operators-claude/combination/combineLatestWith-mergeWith' },
            { text: 'withLatestFrom (advanced)', link: '/operators-claude/combination/withLatestFrom-advanced' },
            { text: 'forkJoin (advanced)', link: '/operators-claude/combination/forkJoin-advanced' },
            { text: 'combineLatest (advanced)', link: '/operators-claude/combination/combineLatest-advanced' },
            { text: 'zip (advanced)', link: '/operators-claude/combination/zip-advanced' },
            { text: 'startWith (advanced)', link: '/operators-claude/combination/startWith-advanced' },
            { text: 'race (advanced)', link: '/operators-claude/combination/race-advanced' },
            { text: 'merge (advanced)', link: '/operators-claude/combination/merge-advanced' },
            { text: 'concat (advanced)', link: '/operators-claude/combination/concat-advanced' },
          ]
        },
      ],

      '/operators-claude/creation/': [
        {
          text: 'Creation',
          items: [
            { text: 'of', link: '/operators-claude/creation/of' },
            { text: 'from', link: '/operators-claude/creation/from' },
            { text: 'from (advanced)', link: '/operators-claude/creation/from-advanced' },
            { text: 'defer', link: '/operators-claude/creation/defer' },
            { text: 'defer (advanced)', link: '/operators-claude/creation/defer-advanced' },
            { text: 'interval', link: '/operators-claude/creation/interval' },
            { text: 'timer', link: '/operators-claude/creation/timer' },
            { text: 'interval / timer (advanced)', link: '/operators-claude/creation/interval-timer-advanced' },
            { text: 'fromEvent', link: '/operators-claude/creation/fromEvent' },
            { text: 'fromEvent (advanced)', link: '/operators-claude/creation/fromEvent-advanced' },
            { text: 'EMPTY / NEVER', link: '/operators-claude/creation/EMPTY-NEVER' },
            { text: 'EMPTY / NEVER (advanced)', link: '/operators-claude/creation/EMPTY-NEVER-advanced' },
            { text: 'partition', link: '/operators-claude/creation/partition' },
            { text: 'iif', link: '/operators-claude/creation/iif' },
            { text: 'partition / iif (advanced)', link: '/operators-claude/creation/partition-iif-advanced' },
            { text: 'throwError', link: '/operators-claude/creation/throwError' },
            { text: 'range', link: '/operators-claude/creation/range' },
            { text: 'generate', link: '/operators-claude/creation/generate' },
            { text: 'scheduled', link: '/operators-claude/creation/scheduled' },
            { text: 'scheduled (advanced)', link: '/operators-claude/creation/scheduled-advanced' },
            { text: 'bindCallback / bindNodeCallback', link: '/operators-claude/creation/bindCallback-bindNodeCallback' },
            { text: 'bindCallback / bindNodeCallback (advanced)', link: '/operators-claude/creation/bindCallback-bindNodeCallback-advanced' },
            { text: 'fromEventPattern', link: '/operators-claude/creation/fromEventPattern' },
            { text: 'fromEventPattern (advanced)', link: '/operators-claude/creation/fromEventPattern-advanced' },
            { text: 'using', link: '/operators-claude/creation/using' },
            { text: 'ajax', link: '/operators-claude/creation/ajax' },
            { text: 'ajax (advanced)', link: '/operators-claude/creation/ajax-advanced' },
            { text: 'fromFetch', link: '/operators-claude/creation/fromFetch' },
            { text: 'fromFetch (advanced)', link: '/operators-claude/creation/fromFetch-advanced' },
            { text: 'webSocket', link: '/operators-claude/creation/webSocket' },
            { text: 'webSocket (advanced)', link: '/operators-claude/creation/webSocket-advanced' },
            { text: 'animationFrames', link: '/operators-claude/creation/animationFrames' },
            { text: 'animationFrames (advanced)', link: '/operators-claude/creation/animationFrames-advanced' },
            { text: 'generate / using (advanced)', link: '/operators-claude/creation/generate-using-advanced' },
          ]
        },
      ],

      '/operators-claude/error-handling/': [
        {
          text: 'Error Handling',
          items: [
            { text: 'catchError', link: '/operators-claude/error-handling/catchError' },
            { text: 'catchError (advanced)', link: '/operators-claude/error-handling/catchError-advanced' },
            { text: 'retry', link: '/operators-claude/error-handling/retry' },
            { text: 'retry (advanced)', link: '/operators-claude/error-handling/retry-advanced' },
            { text: 'timeout', link: '/operators-claude/error-handling/timeout' },
            { text: 'timeout (advanced)', link: '/operators-claude/error-handling/timeout-advanced' },
            { text: 'onErrorResumeNext', link: '/operators-claude/error-handling/onErrorResumeNext' },
            { text: 'onErrorResumeNext (advanced)', link: '/operators-claude/error-handling/onErrorResumeNext-advanced' },
          ]
        },
      ],

      '/operators-claude/multicasting/': [
        {
          text: 'Multicasting',
          items: [
            { text: 'share', link: '/operators-claude/multicasting/share' },
            { text: 'share (advanced)', link: '/operators-claude/multicasting/share-advanced' },
            { text: 'shareReplay', link: '/operators-claude/multicasting/shareReplay' },
            { text: 'shareReplay (advanced)', link: '/operators-claude/multicasting/shareReplay-advanced' },
          ]
        },
      ],

      '/operators-claude/rate-limiting/': [
        {
          text: 'Rate Limiting',
          items: [
            { text: 'auditTime / sampleTime', link: '/operators-claude/rate-limiting/auditTime-sampleTime' },
            { text: 'auditTime / sampleTime (advanced)', link: '/operators-claude/rate-limiting/auditTime-sampleTime-advanced' },
            { text: 'sample / audit', link: '/operators-claude/rate-limiting/sample-audit' },
            { text: 'sample / audit (advanced)', link: '/operators-claude/rate-limiting/sample-audit-advanced' },
            { text: 'throttle / debounce', link: '/operators-claude/rate-limiting/throttle-debounce' },
            { text: 'throttle / debounce (advanced)', link: '/operators-claude/rate-limiting/throttle-debounce-advanced' },
          ]
        },
      ],

      '/operators-claude/higher-order/': [
        {
          text: 'Higher-Order',
          items: [
            { text: 'mergeAll / concatAll / switchAll', link: '/operators-claude/higher-order/mergeAll-concatAll-switchAll' },
            { text: 'mergeAll / concatAll / switchAll (advanced)', link: '/operators-claude/higher-order/mergeAll-concatAll-switchAll-advanced' },
            { text: 'exhaustAll / exhaustMap', link: '/operators-claude/higher-order/exhaustAll' },
            { text: 'exhaustAll / exhaustMap (advanced)', link: '/operators-claude/higher-order/exhaustAll-advanced' },
            { text: 'combineLatestAll', link: '/operators-claude/higher-order/combineLatestAll' },
            { text: 'combineLatestAll (advanced)', link: '/operators-claude/higher-order/combineLatestAll-advanced' },
            { text: 'zipAll', link: '/operators-claude/higher-order/zipAll' },
            { text: 'zipAll (advanced)', link: '/operators-claude/higher-order/zipAll-advanced' },
          ]
        },
      ],

      '/operators-claude/mathematical-aggregate/': [
        {
          text: 'Mathematical / Aggregate',
          items: [
            { text: 'scan', link: '/operators-claude/mathematical-aggregate/scan' },
            { text: 'scan (advanced)', link: '/operators-claude/mathematical-aggregate/scan-advanced' },
            { text: 'reduce', link: '/operators-claude/mathematical-aggregate/reduce' },
            { text: 'reduce (advanced)', link: '/operators-claude/mathematical-aggregate/reduce-advanced' },
            { text: 'toArray / count', link: '/operators-claude/mathematical-aggregate/toArray-count' },
            { text: 'toArray / count (advanced)', link: '/operators-claude/mathematical-aggregate/toArray-count-advanced' },
            { text: 'min / max', link: '/operators-claude/mathematical-aggregate/min-max' },
            { text: 'min / max (advanced)', link: '/operators-claude/mathematical-aggregate/min-max-advanced' },
          ]
        },
      ],

      '/operators-claude/conditional/': [
        {
          text: 'Conditional',
          items: [
            { text: 'defaultIfEmpty / isEmpty / every / sequenceEqual', link: '/operators-claude/conditional/conditional-operators' },
            { text: 'Conditional Operators (advanced)', link: '/operators-claude/conditional/conditional-operators-advanced' },
          ]
        },
      ],

      '/operators-claude/subject/': [
        {
          text: 'Subject',
          items: [
            { text: 'Subject', link: '/operators-claude/subject/Subject' },
            { text: 'Subject (advanced)', link: '/operators-claude/subject/Subject-advanced' },
            { text: 'BehaviorSubject', link: '/operators-claude/subject/BehaviorSubject' },
            { text: 'BehaviorSubject (advanced)', link: '/operators-claude/subject/BehaviorSubject-advanced' },
            { text: 'ReplaySubject', link: '/operators-claude/subject/ReplaySubject' },
            { text: 'ReplaySubject (advanced)', link: '/operators-claude/subject/ReplaySubject-advanced' },
            { text: 'AsyncSubject', link: '/operators-claude/subject/AsyncSubject' },
            { text: 'AsyncSubject (advanced)', link: '/operators-claude/subject/AsyncSubject-advanced' },
          ]
        },
      ],

      '/operators-claude/connectable/': [
        {
          text: 'Connectable Observable',
          items: [
            { text: 'connectable / connect', link: '/operators-claude/connectable/connectable-connect' },
            { text: 'connectable / connect (advanced)', link: '/operators-claude/connectable/connectable-connect-advanced' },
          ]
        },
      ],

      '/operators-claude/testing/': [
        {
          text: 'Testing / Debugging',
          items: [
            { text: 'TestScheduler', link: '/operators-claude/testing/TestScheduler' },
            { text: 'TestScheduler (advanced)', link: '/operators-claude/testing/TestScheduler-advanced' },
            { text: 'Debugging Streams', link: '/operators-claude/testing/debugging-operators' },
            { text: 'Debugging Streams (advanced)', link: '/operators-claude/testing/debugging-operators-advanced' },
          ]
        },
      ],

      '/operators-claude/interop/': [
        {
          text: 'Interop',
          items: [
            { text: 'Async Iterable / ReadableStream', link: '/operators-claude/interop/async-iterable' },
            { text: 'Async Iterable (advanced)', link: '/operators-claude/interop/async-iterable-advanced' },
          ]
        },
      ],

      '/operators-claude/utility/': [
        {
          text: 'Utility',
          items: [
            { text: 'tap', link: '/operators-claude/utility/tap' },
            { text: 'tap (advanced)', link: '/operators-claude/utility/tap-advanced' },
            { text: 'tap / finalize (advanced)', link: '/operators-claude/utility/tap-finalize-advanced' },
            { text: 'finalize', link: '/operators-claude/utility/finalize' },
            { text: 'delay / delayWhen', link: '/operators-claude/utility/delay-delayWhen' },
            { text: 'delay / delayWhen (advanced)', link: '/operators-claude/utility/delay-delayWhen-advanced' },
            { text: 'repeat', link: '/operators-claude/utility/repeat' },
            { text: 'repeat (advanced)', link: '/operators-claude/utility/repeat-advanced' },
            { text: 'materialize / dematerialize', link: '/operators-claude/utility/materialize-dematerialize' },
            { text: 'materialize / dematerialize (advanced)', link: '/operators-claude/utility/materialize-dematerialize-advanced' },
            { text: 'observeOn / subscribeOn', link: '/operators-claude/utility/observeOn-subscribeOn' },
            { text: 'observeOn / subscribeOn (advanced)', link: '/operators-claude/utility/observeOn-subscribeOn-advanced' },
            { text: 'timeInterval / timestamp', link: '/operators-claude/utility/timeInterval-timestamp' },
            { text: 'timeInterval / timestamp (advanced)', link: '/operators-claude/utility/timeInterval-timestamp-advanced' },
            { text: 'endWith / ignoreElements', link: '/operators-claude/utility/endWith-ignoreElements' },
            { text: 'endWith / ignoreElements (advanced)', link: '/operators-claude/utility/endWith-ignoreElements-advanced' },
            { text: 'firstValueFrom / lastValueFrom', link: '/operators-claude/utility/firstValueFrom-lastValueFrom' },
            { text: 'firstValueFrom / lastValueFrom (advanced)', link: '/operators-claude/utility/firstValueFrom-lastValueFrom-advanced' },
            { text: 'Schedulers', link: '/operators-claude/utility/schedulers' },
            { text: 'Schedulers (advanced)', link: '/operators-claude/utility/schedulers-advanced' },
            { text: 'Deprecated Operators', link: '/operators-claude/utility/deprecated-operators' },
          ]
        },
      ],

      '/docs/operator-trees/': [
        {
          text: 'Operator Trees',
          items: [
            { text: 'Overview', link: '/docs/operator-trees/overview' },
            { text: 'Flattening Strategy Guide', link: '/docs/operator-trees/flattening-strategy-guide' },
            { text: 'Subject Decision Guide', link: '/docs/operator-trees/subject-decision-guide' },
            { text: 'Error Handling Patterns', link: '/docs/operator-trees/error-handling-patterns' },
            { text: 'Multicasting Guide', link: '/docs/operator-trees/multicasting-guide' },
            { text: 'Rate-Limiting Guide', link: '/docs/operator-trees/rate-limiting-guide' },
            { text: 'Custom Operators Guide', link: '/docs/operator-trees/custom-operators-guide' },
            { text: 'Subscription Management', link: '/docs/operator-trees/subscription-management-guide' },
            { text: 'Cold vs Hot Observables', link: '/docs/operator-trees/cold-vs-hot-guide' },
            { text: 'Performance Patterns', link: '/docs/operator-trees/performance-patterns-guide' },
            { text: 'Angular + RxJS Patterns', link: '/docs/operator-trees/angular-rxjs-patterns' },
            { text: 'RxJS Cookbook', link: '/docs/operator-trees/rxjs-cookbook' },
            { text: 'Error Handling Cookbook', link: '/docs/operator-trees/error-handling-cookbook' },
            { text: 'Testing Patterns', link: '/docs/operator-trees/testing-patterns-guide' },
            { text: 'Node.js Patterns', link: '/docs/operator-trees/nodejs-rxjs-patterns' },
            { text: 'State Management', link: '/docs/operator-trees/state-management-patterns' },
            { text: 'NgRx Effects Patterns', link: '/docs/operator-trees/ngrx-effects-patterns' },
            { text: 'NgRx ComponentStore', link: '/docs/operator-trees/rxjs-ngrx-component-store' },
            { text: 'Enterprise Patterns', link: '/docs/operator-trees/rxjs-enterprise-patterns' },
            { text: 'RxJS Migration Guide', link: '/docs/operator-trees/rxjs-migration-guide' },
            { text: 'RxJS Best Practices', link: '/docs/operator-trees/rxjs-best-practices' },
            { text: 'Angular Signals + RxJS', link: '/docs/operator-trees/angular-signals-rxjs' },
            { text: 'TypeScript + RxJS Guide', link: '/docs/operator-trees/typescript-rxjs-guide' },
            { text: 'TypeScript Type Guards', link: '/docs/operator-trees/rxjs-typescript-type-guards' },
            { text: 'Real-Time Data Guide', link: '/docs/operator-trees/realtime-data-guide' },
            { text: 'Form Validation Guide', link: '/docs/operator-trees/form-validation-guide' },
            { text: 'RxJS in React', link: '/docs/operator-trees/rxjs-react-patterns' },
            { text: 'Debugging Guide', link: '/docs/operator-trees/debugging-guide' },
            { text: 'Pipeline Architecture', link: '/docs/operator-trees/pipeline-architecture-guide' },
            { text: 'Concurrency Patterns', link: '/docs/operator-trees/concurrency-guide' },
            { text: 'Operator Selection Guide', link: '/docs/operator-trees/operator-selection-guide' },
            { text: 'RxJS in Vue', link: '/docs/operator-trees/rxjs-vue-patterns' },
            { text: 'RxJS in Svelte', link: '/docs/operator-trees/rxjs-svelte-patterns' },
            { text: 'RxJS Mental Models', link: '/docs/operator-trees/rxjs-mental-models' },
            { text: 'Caching Patterns', link: '/docs/operator-trees/rxjs-caching-patterns' },
            { text: 'Higher-Order Observables', link: '/docs/operator-trees/higher-order-observables-guide' },
            { text: 'Infinite Scroll & Pagination', link: '/docs/operator-trees/rxjs-infinite-scroll-pagination' },
            { text: 'Search & Autocomplete', link: '/docs/operator-trees/rxjs-search-autocomplete-patterns' },
            { text: 'Drag, Drop & Animation', link: '/docs/operator-trees/rxjs-drag-drop-animation' },
            { text: 'Notification & Toast Patterns', link: '/docs/operator-trees/rxjs-notification-toast-patterns' },
            { text: 'Optimistic UI Patterns', link: '/docs/operator-trees/rxjs-optimistic-ui-patterns' },
            { text: 'Authentication Patterns', link: '/docs/operator-trees/rxjs-authentication-patterns' },
            { text: 'File Upload Patterns', link: '/docs/operator-trees/rxjs-file-upload-patterns' },
            { text: 'Polling Patterns', link: '/docs/operator-trees/rxjs-polling-patterns' },
            { text: 'Promise Interop', link: '/docs/operator-trees/rxjs-promise-interop' },
            { text: 'Error Resilience Patterns', link: '/docs/operator-trees/rxjs-error-resilience-patterns' },
            { text: 'Undo/Redo Patterns', link: '/docs/operator-trees/rxjs-undo-redo-patterns' },
            { text: 'Data Synchronization', link: '/docs/operator-trees/rxjs-data-synchronization-patterns' },
            { text: 'Multi-Tab Patterns', link: '/docs/operator-trees/rxjs-multi-tab-patterns' },
            { text: 'Marble Testing (Advanced)', link: '/docs/operator-trees/rxjs-marble-testing-advanced' },
            { text: 'Custom Operators (Advanced)', link: '/docs/operator-trees/rxjs-custom-operators-advanced' },
            { text: 'Web Worker Patterns', link: '/docs/operator-trees/rxjs-web-worker-patterns' },
            { text: 'State Machines', link: '/docs/operator-trees/rxjs-state-machines' },
            { text: 'IndexedDB Patterns', link: '/docs/operator-trees/rxjs-indexeddb-patterns' },
            { text: 'Charts & Visualization', link: '/docs/operator-trees/rxjs-charts-visualization' },
            { text: 'Micro-Frontend Patterns', link: '/docs/operator-trees/rxjs-micro-frontend-patterns' },
            { text: 'Virtual Scroll', link: '/docs/operator-trees/rxjs-virtual-scroll' },
            { text: 'Service Worker Patterns', link: '/docs/operator-trees/rxjs-service-worker-patterns' },
            { text: 'Accessibility Patterns', link: '/docs/operator-trees/rxjs-accessibility-patterns' },
            { text: 'GraphQL Patterns', link: '/docs/operator-trees/rxjs-graphql-patterns' },
            { text: 'Integration Testing', link: '/docs/operator-trees/rxjs-testing-integration' },
            { text: 'RxJS 8 Preparation', link: '/docs/operator-trees/rxjs-rxjs8-preparation' },
            { text: 'WebSocket Patterns', link: '/docs/operator-trees/rxjs-websocket-patterns' },
            { text: 'Operator Composition Patterns', link: '/docs/operator-trees/rxjs-operator-composition-patterns' },
            { text: 'Reactive Forms Advanced', link: '/docs/operator-trees/rxjs-reactive-forms-advanced' },
            { text: 'Signals ↔ RxJS Deep Interop', link: '/docs/operator-trees/rxjs-signals-interop-deep' },
            { text: 'Server-Side Rendering (SSR)', link: '/docs/operator-trees/rxjs-server-side-rendering' },
            { text: 'Angular Standalone APIs', link: '/docs/operator-trees/rxjs-angular-standalone' },
            { text: 'Performance Profiling', link: '/docs/operator-trees/rxjs-performance-profiling' },
            { text: 'Zoneless Angular', link: '/docs/operator-trees/rxjs-zoneless-angular' },
            { text: 'Error Boundary Patterns', link: '/docs/operator-trees/rxjs-error-boundary-patterns' },
            { text: 'HTTP Interceptor Patterns', link: '/docs/operator-trees/rxjs-http-interceptor-patterns' },
            { text: 'Store from Scratch', link: '/docs/operator-trees/rxjs-store-from-scratch' },
            { text: 'NgRx Effects (Advanced)', link: '/docs/operator-trees/rxjs-effect-patterns-advanced' },
            { text: 'Microservices Patterns', link: '/docs/operator-trees/rxjs-microservices-patterns' },
            { text: 'DevTools & Extensions', link: '/docs/operator-trees/rxjs-devtools-extensions' },
            { text: 'Marble Testing Patterns', link: '/docs/operator-trees/rxjs-testing-marble-patterns' },
            { text: 'Decision Trees', link: '/docs/operator-trees/decision-trees' },
            { text: 'Legend', link: '/docs/operator-trees/legend' },
            { text: 'Time-based', link: '/docs/operator-trees/time-tree' },
            { text: 'Value-based', link: '/docs/operator-trees/value-tree' },
            { text: 'A4 Edition', link: '/docs/operator-trees/a4-edition' },
          ]
        }
      ],
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/hansschenker/rxjs-operator-documentation' }
    ]
  }
})
