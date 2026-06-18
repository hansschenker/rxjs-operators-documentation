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
            { text: 'bufferCount', link: '/operators-claude/transformation/bufferCount' },
            { text: 'windowCount', link: '/operators-claude/transformation/windowCount' },
            { text: 'expand', link: '/operators-claude/transformation/expand' },
            { text: 'windowTime', link: '/operators-claude/transformation/windowTime' },
          ]
        },
        {
          text: 'Utility',
          items: [
            { text: 'tap', link: '/operators-claude/utility/tap' },
            { text: 'finalize', link: '/operators-claude/utility/finalize' },
            { text: 'delay / delayWhen', link: '/operators-claude/utility/delay-delayWhen' },
            { text: 'repeat', link: '/operators-claude/utility/repeat' },
            { text: 'materialize / dematerialize', link: '/operators-claude/utility/materialize-dematerialize' },
            { text: 'observeOn / subscribeOn', link: '/operators-claude/utility/observeOn-subscribeOn' },
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
          ]
        },
        {
          text: 'Error Handling',
          items: [
            { text: 'catchError', link: '/operators-claude/error-handling/catchError' },
            { text: 'retry', link: '/operators-claude/error-handling/retry' },
            { text: 'timeout', link: '/operators-claude/error-handling/timeout' },
            { text: 'onErrorResumeNext', link: '/operators-claude/error-handling/onErrorResumeNext' },
          ]
        },
        {
          text: 'Higher-Order',
          items: [
            { text: 'mergeAll / concatAll / switchAll', link: '/operators-claude/higher-order/mergeAll-concatAll-switchAll' },
            { text: 'exhaustAll / exhaustMap', link: '/operators-claude/higher-order/exhaustAll' },
            { text: 'combineLatestAll', link: '/operators-claude/higher-order/combineLatestAll' },
          ]
        },
        {
          text: 'Rate Limiting',
          items: [
            { text: 'auditTime / sampleTime', link: '/operators-claude/rate-limiting/auditTime-sampleTime' },
            { text: 'sample / audit', link: '/operators-claude/rate-limiting/sample-audit' },
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
            { text: 'generate', link: '/operators-claude/creation/generate' },
            { text: 'ajax', link: '/operators-claude/creation/ajax' },
            { text: 'fromFetch', link: '/operators-claude/creation/fromFetch' },
            { text: 'webSocket', link: '/operators-claude/creation/webSocket' },
          ]
        },
        {
          text: 'Subject',
          items: [
            { text: 'Subject', link: '/operators-claude/subject/Subject' },
            { text: 'BehaviorSubject', link: '/operators-claude/subject/BehaviorSubject' },
            { text: 'ReplaySubject', link: '/operators-claude/subject/ReplaySubject' },
            { text: 'AsyncSubject', link: '/operators-claude/subject/AsyncSubject' },
          ]
        },
        {
          text: 'Mathematical / Aggregate',
          items: [
            { text: 'scan', link: '/operators-claude/mathematical-aggregate/scan' },
            { text: 'reduce', link: '/operators-claude/mathematical-aggregate/reduce' },
            { text: 'toArray / count', link: '/operators-claude/mathematical-aggregate/toArray-count' },
          ]
        },
        {
          text: 'Multicasting',
          items: [
            { text: 'shareReplay', link: '/operators-claude/multicasting/shareReplay' },
            { text: 'share', link: '/operators-claude/multicasting/share' },
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
          ]
        },
      ],

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
      ],

      '/docs/operator-trees/': [
        {
          text: 'Operator Trees',
          items: [
            { text: 'Overview', link: '/docs/operator-trees/overview' },
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
