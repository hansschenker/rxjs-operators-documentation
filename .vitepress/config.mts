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
          ]
        },
        {
          text: 'Utility',
          items: [
            { text: 'tap', link: '/operators-claude/utility/tap' },
            { text: 'finalize', link: '/operators-claude/utility/finalize' },
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
          ]
        },
        {
          text: 'Error Handling',
          items: [
            { text: 'catchError', link: '/operators-claude/error-handling/catchError' },
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
            { text: 'EMPTY / NEVER', link: '/operators-claude/creation/EMPTY-NEVER' },
          ]
        },
        {
          text: 'Subject',
          items: [
            { text: 'Subject', link: '/operators-claude/subject/Subject' },
            { text: 'BehaviorSubject', link: '/operators-claude/subject/BehaviorSubject' },
            { text: 'ReplaySubject', link: '/operators-claude/subject/ReplaySubject' },
          ]
        },
        {
          text: 'Mathematical / Aggregate',
          items: [
            { text: 'scan', link: '/operators-claude/mathematical-aggregate/scan' },
            { text: 'reduce', link: '/operators-claude/mathematical-aggregate/reduce' },
          ]
        },
        {
          text: 'Multicasting',
          items: [
            { text: 'shareReplay', link: '/operators-claude/multicasting/shareReplay' },
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
