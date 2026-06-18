import { defineConfig } from 'vitepress'

export default defineConfig({
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
        ]
      },
      { text: 'Reference', link: '/docs/operator-trees/overview' },
    ],

    sidebar: {
      '/operators-claude/': [
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
            { text: 'bufferWhen / windowWhen', link: '/operators-claude/transformation/bufferWhen-windowWhen' },
            { text: 'bufferToggle / windowToggle', link: '/operators-claude/transformation/bufferToggle-windowToggle' },
            { text: 'windowCount', link: '/operators-claude/transformation/windowCount' },
            { text: 'switchScan', link: '/operators-claude/transformation/switchScan' },
            { text: 'switchScan (advanced)', link: '/operators-claude/transformation/switchScan-advanced' },
            { text: 'mergeScan', link: '/operators-claude/transformation/mergeScan' },
            { text: 'expand', link: '/operators-claude/transformation/expand' },
            { text: 'windowTime', link: '/operators-claude/transformation/windowTime' },
            { text: 'groupBy (advanced)', link: '/operators-claude/transformation/groupBy-advanced' },
            { text: 'concatMap (advanced)', link: '/operators-claude/transformation/concatMap-advanced' },
            { text: 'switchMap (advanced)', link: '/operators-claude/transformation/switchMap-advanced' },
            { text: 'mergeMap (advanced)', link: '/operators-claude/transformation/mergeMap-advanced' },
            { text: 'expand (advanced)', link: '/operators-claude/transformation/expand-advanced' },
            { text: 'windowTime (advanced)', link: '/operators-claude/transformation/windowTime-advanced' },
            { text: 'map (advanced)', link: '/operators-claude/transformation/map-advanced' },
            { text: 'pairwise (advanced)', link: '/operators-claude/transformation/pairwise-advanced' },
            { text: 'exhaustMap (advanced)', link: '/operators-claude/transformation/exhaustMap-advanced' },
          ]
        },
        {
          text: 'Utility',
          items: [
            { text: 'tap', link: '/operators-claude/utility/tap' },
            { text: 'finalize', link: '/operators-claude/utility/finalize' },
            { text: 'delay / delayWhen', link: '/operators-claude/utility/delay-delayWhen' },
            { text: 'repeat', link: '/operators-claude/utility/repeat' },
            { text: 'delay / delayWhen (advanced)', link: '/operators-claude/utility/delay-delayWhen-advanced' },
            { text: 'materialize / dematerialize', link: '/operators-claude/utility/materialize-dematerialize' },
            { text: 'materialize / dematerialize (advanced)', link: '/operators-claude/utility/materialize-dematerialize-advanced' },
            { text: 'observeOn / subscribeOn', link: '/operators-claude/utility/observeOn-subscribeOn' },
            { text: 'observeOn / subscribeOn (advanced)', link: '/operators-claude/utility/observeOn-subscribeOn-advanced' },
            { text: 'timeInterval / timestamp', link: '/operators-claude/utility/timeInterval-timestamp' },
            { text: 'timeInterval / timestamp (advanced)', link: '/operators-claude/utility/timeInterval-timestamp-advanced' },
            { text: 'endWith / ignoreElements', link: '/operators-claude/utility/endWith-ignoreElements' },
            { text: 'firstValueFrom / lastValueFrom', link: '/operators-claude/utility/firstValueFrom-lastValueFrom' },
            { text: 'Schedulers', link: '/operators-claude/utility/schedulers' },
            { text: 'repeat (advanced)', link: '/operators-claude/utility/repeat-advanced' },
          ]
        },
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
            { text: 'find / findIndex', link: '/operators-claude/filtering/find-findIndex' },
            { text: 'distinct', link: '/operators-claude/filtering/distinct' },
            { text: 'skip / takeLast / elementAt', link: '/operators-claude/filtering/skip-takeLast-elementAt' },
            { text: 'debounceTime (advanced)', link: '/operators-claude/filtering/debounceTime-advanced' },
            { text: 'throttleTime (advanced)', link: '/operators-claude/filtering/throttleTime-advanced' },
            { text: 'distinctUntilChanged (advanced)', link: '/operators-claude/filtering/distinctUntilChanged-advanced' },
            { text: 'takeUntil (advanced)', link: '/operators-claude/filtering/takeUntil-advanced' },
            { text: 'takeWhile (advanced)', link: '/operators-claude/filtering/takeWhile-advanced' },
            { text: 'filter (advanced)', link: '/operators-claude/filtering/filter-advanced' },
          ]
        },
        {
          text: 'Error Handling',
          items: [
            { text: 'catchError', link: '/operators-claude/error-handling/catchError' },
            { text: 'retry', link: '/operators-claude/error-handling/retry' },
            { text: 'timeout', link: '/operators-claude/error-handling/timeout' },
            { text: 'onErrorResumeNext', link: '/operators-claude/error-handling/onErrorResumeNext' },
            { text: 'catchError (advanced)', link: '/operators-claude/error-handling/catchError-advanced' },
            { text: 'retry (advanced)', link: '/operators-claude/error-handling/retry-advanced' },
            { text: 'timeout (advanced)', link: '/operators-claude/error-handling/timeout-advanced' },
          ]
        },
        {
          text: 'Higher-Order',
          items: [
            { text: 'mergeAll / concatAll / switchAll', link: '/operators-claude/higher-order/mergeAll-concatAll-switchAll' },
            { text: 'exhaustAll / exhaustMap', link: '/operators-claude/higher-order/exhaustAll' },
            { text: 'combineLatestAll', link: '/operators-claude/higher-order/combineLatestAll' },
            { text: 'zipAll', link: '/operators-claude/higher-order/zipAll' },
          ]
        },
        {
          text: 'Rate Limiting',
          items: [
            { text: 'auditTime / sampleTime', link: '/operators-claude/rate-limiting/auditTime-sampleTime' },
            { text: 'sample / audit', link: '/operators-claude/rate-limiting/sample-audit' },
            { text: 'throttle / debounce', link: '/operators-claude/rate-limiting/throttle-debounce' },
            { text: 'throttle / debounce (advanced)', link: '/operators-claude/rate-limiting/throttle-debounce-advanced' },
          ]
        },
        {
          text: 'Creation',
          items: [
            { text: 'of', link: '/operators-claude/creation/of' },
            { text: 'from', link: '/operators-claude/creation/from' },
            { text: 'defer', link: '/operators-claude/creation/defer' },
            { text: 'interval', link: '/operators-claude/creation/interval' },
            { text: 'timer', link: '/operators-claude/creation/timer' },
            { text: 'fromEvent', link: '/operators-claude/creation/fromEvent' },
            { text: 'EMPTY / NEVER', link: '/operators-claude/creation/EMPTY-NEVER' },
            { text: 'partition', link: '/operators-claude/creation/partition' },
            { text: 'iif', link: '/operators-claude/creation/iif' },
            { text: 'throwError', link: '/operators-claude/creation/throwError' },
            { text: 'range', link: '/operators-claude/creation/range' },
            { text: 'generate', link: '/operators-claude/creation/generate' },
            { text: 'scheduled', link: '/operators-claude/creation/scheduled' },
            { text: 'bindCallback / bindNodeCallback', link: '/operators-claude/creation/bindCallback-bindNodeCallback' },
            { text: 'fromEventPattern', link: '/operators-claude/creation/fromEventPattern' },
            { text: 'using', link: '/operators-claude/creation/using' },
            { text: 'ajax', link: '/operators-claude/creation/ajax' },
            { text: 'fromFetch', link: '/operators-claude/creation/fromFetch' },
            { text: 'webSocket', link: '/operators-claude/creation/webSocket' },
            { text: 'animationFrames', link: '/operators-claude/creation/animationFrames' },
            { text: 'defer (advanced)', link: '/operators-claude/creation/defer-advanced' },
            { text: 'interval / timer (advanced)', link: '/operators-claude/creation/interval-timer-advanced' },
            { text: 'fromEvent (advanced)', link: '/operators-claude/creation/fromEvent-advanced' },
            { text: 'fromFetch (advanced)', link: '/operators-claude/creation/fromFetch-advanced' },
            { text: 'webSocket (advanced)', link: '/operators-claude/creation/webSocket-advanced' },
          ]
        },
        {
          text: 'Subject',
          items: [
            { text: 'Subject', link: '/operators-claude/subject/Subject' },
            { text: 'BehaviorSubject', link: '/operators-claude/subject/BehaviorSubject' },
            { text: 'BehaviorSubject (advanced)', link: '/operators-claude/subject/BehaviorSubject-advanced' },
            { text: 'ReplaySubject', link: '/operators-claude/subject/ReplaySubject' },
            { text: 'ReplaySubject (advanced)', link: '/operators-claude/subject/ReplaySubject-advanced' },
            { text: 'AsyncSubject', link: '/operators-claude/subject/AsyncSubject' },
          ]
        },
        {
          text: 'Mathematical / Aggregate',
          items: [
            { text: 'scan', link: '/operators-claude/mathematical-aggregate/scan' },
            { text: 'reduce', link: '/operators-claude/mathematical-aggregate/reduce' },
            { text: 'toArray / count', link: '/operators-claude/mathematical-aggregate/toArray-count' },
            { text: 'min / max', link: '/operators-claude/mathematical-aggregate/min-max' },
            { text: 'scan (advanced)', link: '/operators-claude/mathematical-aggregate/scan-advanced' },
            { text: 'reduce (advanced)', link: '/operators-claude/mathematical-aggregate/reduce-advanced' },
            { text: 'toArray / count (advanced)', link: '/operators-claude/mathematical-aggregate/toArray-count-advanced' },
          ]
        },
        {
          text: 'Multicasting',
          items: [
            { text: 'shareReplay', link: '/operators-claude/multicasting/shareReplay' },
            { text: 'shareReplay (advanced)', link: '/operators-claude/multicasting/shareReplay-advanced' },
            { text: 'share', link: '/operators-claude/multicasting/share' },
            { text: 'share (advanced)', link: '/operators-claude/multicasting/share-advanced' },
          ]
        },
        {
          text: 'Utility (Advanced)',
          items: [
            { text: 'tap / finalize (advanced)', link: '/operators-claude/utility/tap-finalize-advanced' },
            { text: 'Deprecated Operators', link: '/operators-claude/utility/deprecated-operators' },
          ]
        },
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

        {
          text: 'Conditional',
          items: [
            { text: 'defaultIfEmpty / isEmpty / every / sequenceEqual', link: '/operators-claude/conditional/conditional-operators' },
          ]
        },
        {
          text: 'Connectable Observable',
          items: [
            { text: 'connectable / connect', link: '/operators-claude/connectable/connectable-connect' },
          ]
        },
        {
          text: 'Testing / Debugging',
          items: [
            { text: 'TestScheduler', link: '/operators-claude/testing/TestScheduler' },
            { text: 'Debugging Streams', link: '/operators-claude/testing/debugging-operators' },
          ]
        },
        {
          text: 'Interop',
          items: [
            { text: 'Async Iterable / ReadableStream', link: '/operators-claude/interop/async-iterable' },
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
            { text: 'RxJS Migration Guide', link: '/docs/operator-trees/rxjs-migration-guide' },
            { text: 'RxJS Best Practices', link: '/docs/operator-trees/rxjs-best-practices' },
            { text: 'Angular Signals + RxJS', link: '/docs/operator-trees/angular-signals-rxjs' },
            { text: 'TypeScript + RxJS Guide', link: '/docs/operator-trees/typescript-rxjs-guide' },
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
