#!/usr/bin/env node
/**
 * Generate the home page (root index.md) from the sidebar in .vitepress/config.mts
 * and the pages on disk.
 *
 *   node scripts/gen-home.mjs            dry run: prints stats only
 *   node scripts/gen-home.mjs --write    writes index.md
 *   npm run docs:home                    same as --write
 *
 * The sidebar is the source of truth for which pages exist and how they are
 * labelled; each operator row's description is the `**Type**:` line of the
 * page's Identity section. Run this after adding pages or editing the sidebar.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const CFG = path.join(ROOT, '.vitepress', 'config.mts')
const OUT = path.join(ROOT, 'index.md')
const WRITE = process.argv.includes('--write')

const cfg = fs.readFileSync(CFG, 'utf8')

// ---------- parse sidebar ----------
const keyRe = /^\s+'(\/(?:operators-claude|docs)\/[^']+\/)': \[/gm
const itemRe = /\{ text: '([^']*)', link: '([^']*)' \}/g
const keys = [...cfg.matchAll(keyRe)]
const groups = [] // { key, title, items: [[label, link], ...] }
keys.forEach((m, i) => {
  const end = i + 1 < keys.length ? keys[i + 1].index : cfg.length
  const seg = cfg.slice(m.index + m[0].length, end)
  const title = /text: '([^']*)'/.exec(seg)[1]
  const items = [...seg.matchAll(itemRe)].map((x) => [x[1], x[2]])
  groups.push({ key: m[1], title, items })
})

const opGroups = groups.filter((g) => g.key.startsWith('/operators-claude/'))
const guideItems = groups.filter((g) => g.key.startsWith('/docs/')).flatMap((g) => g.items)
const labelToLink = new Map()
for (const g of groups) for (const [label, link] of g.items) if (!labelToLink.has(label)) labelToLink.set(label, link)

// ---------- helpers ----------
const pagePath = (link) => path.join(ROOT, link.replace(/^\/+/, '') + '.md')
const exists = (link) => fs.existsSync(pagePath(link)) && fs.statSync(pagePath(link)).size > 0

function escapeCell(d) {
  // escape angle brackets outside backticks (VitePress compiles markdown as Vue templates)
  const parts = d.split('`')
  for (let i = 0; i < parts.length; i += 2) {
    parts[i] = parts[i].replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\{\{/g, '{ {')
  }
  return parts.join('`').replace(/\|/g, '\\|')
}

function describe(link) {
  const p = pagePath(link)
  if (!fs.existsSync(p)) return ''
  const t = fs.readFileSync(p, 'utf8')
  const m = /^\s*-\s*\*\*Type\*\*:\s*(.+)$/m.exec(t)
  let d = ''
  if (m) {
    d = m[1].trim()
  } else {
    for (const line of t.split('\n').slice(1)) {
      const l = line.trim()
      if (l && !'#[|`->*!'.includes(l[0]) && !l.startsWith('<')) {
        d = l
        break
      }
    }
  }
  d = d.replace(/\s+/g, ' ').trim()
  if (d.length > 150) {
    const cut = d.slice(0, 147)
    d = cut.slice(0, cut.lastIndexOf(' ')).replace(/[,;:]+$/, '') + ' …'
  }
  return escapeCell(d)
}

function h1(link) {
  const p = pagePath(link)
  if (!fs.existsSync(p)) return null
  for (const line of fs.readFileSync(p, 'utf8').split('\n')) {
    if (line.startsWith('# ')) return line.slice(2).trim()
  }
  return null
}

const yq = (s) => '"' + s.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"'

// ---------- counts ----------
function mdCount(sub, nonempty = false) {
  let n = 0
  const walk = (dir) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, e.name)
      if (e.isDirectory()) walk(full)
      else if (e.name.endsWith('.md') && e.name.toLowerCase() !== 'readme.md') {
        if (!nonempty || fs.statSync(full).size > 0) n++
      }
    }
  }
  walk(path.join(ROOT, sub))
  return n
}

const nOps = mdCount('operators-claude')
const nGuides = mdCount('docs/operator-trees', true)
const nCats = opGroups.length

// ---------- operator tables ----------
const missing = []
const opSections = []
const featureCards = []
for (const { title, items } of opGroups) {
  const core = items.filter(([t]) => !t.endsWith('(advanced)'))
  const adv = new Map(items.filter(([t]) => t.endsWith('(advanced)')).map(([t, l]) => [t.slice(0, -'(advanced)'.length).trim(), l]))
  const rows = []
  const used = new Set()
  for (const [t, l] of core) {
    if (!exists(l)) missing.push(l)
    const a = adv.get(t)
    if (a) used.add(t)
    rows.push([t, l, describe(l), a])
  }
  for (const [base, a] of adv) if (!used.has(base)) rows.push([base, null, 'Advanced patterns and production recipes', a])
  const lines = [`### ${title} (${items.length} pages)`, '', '| Operator | What it does | Deep dive |', '|---|---|---|']
  for (const [t, l, d, a] of rows) {
    const name = l ? `[${t}](${l})` : t
    const deep = a ? `[advanced](${a})` : ''
    lines.push(`| ${name} | ${d} | ${deep} |`)
  }
  opSections.push(lines.join('\n'))
  const sample = core.slice(0, 5).map(([t]) => t).join(', ') + (core.length > 5 ? ' …' : '')
  featureCards.push([title, `${items.length} pages — ${sample}`, core[0][1]])
}

// ---------- guides ----------
const GUIDE_GROUPS = [
  ['Operator Trees — the behavioral classification', [
    'overview', 'legend', 'value-tree', 'time-tree', 'decision-trees', 'a4-edition']],
  ['Decision guides — start here when choosing', [
    'flattening-strategy-guide', 'operator-selection-guide', 'subject-decision-guide', 'rate-limiting-guide',
    'error-handling-patterns', 'multicasting-guide', 'higher-order-observables-guide', 'rxjs-mental-models',
    'cold-vs-hot-guide', 'rxjs-best-practices', 'rxjs-cookbook', 'error-handling-cookbook']],
  ['Architecture, state and testing', [
    'pipeline-architecture-guide', 'state-management-patterns', 'rxjs-store-from-scratch', 'rxjs-state-machines',
    'concurrency-guide', 'rxjs-operator-composition-patterns', 'custom-operators-guide', 'rxjs-custom-operators-advanced',
    'subscription-management-guide', 'rxjs-effect-patterns-advanced', 'rxjs-enterprise-patterns', 'rxjs-microservices-patterns',
    'testing-patterns-guide', 'rxjs-marble-testing-advanced', 'rxjs-testing-marble-patterns', 'rxjs-testing-integration',
    'debugging-guide', 'rxjs-devtools-extensions', 'performance-patterns-guide', 'rxjs-performance-profiling']],
  ['Framework integrations', [
    'angular-rxjs-patterns', 'rxjs-angular-standalone', 'rxjs-zoneless-angular', 'angular-signals-rxjs',
    'rxjs-signals-interop-deep', 'ngrx-effects-patterns', 'rxjs-ngrx-component-store', 'rxjs-reactive-forms-advanced',
    'rxjs-react-patterns', 'rxjs-vue-patterns', 'rxjs-svelte-patterns', 'nodejs-rxjs-patterns',
    'rxjs-server-side-rendering', 'typescript-rxjs-guide', 'rxjs-typescript-type-guards']],
  ['Real-world domain patterns', [
    'rxjs-websocket-patterns', 'rxjs-graphql-patterns', 'rxjs-http-interceptor-patterns', 'rxjs-authentication-patterns',
    'rxjs-caching-patterns', 'realtime-data-guide', 'rxjs-polling-patterns', 'rxjs-data-synchronization-patterns',
    'rxjs-multi-tab-patterns', 'rxjs-file-upload-patterns', 'form-validation-guide', 'rxjs-optimistic-ui-patterns',
    'rxjs-undo-redo-patterns', 'rxjs-notification-toast-patterns', 'rxjs-error-boundary-patterns',
    'rxjs-error-resilience-patterns']],
  ['Browser and platform APIs', [
    'rxjs-web-worker-patterns', 'rxjs-service-worker-patterns', 'rxjs-indexeddb-patterns', 'rxjs-accessibility-patterns',
    'rxjs-charts-visualization', 'rxjs-drag-drop-animation', 'rxjs-virtual-scroll', 'rxjs-infinite-scroll-pagination',
    'rxjs-search-autocomplete-patterns', 'rxjs-micro-frontend-patterns']],
  ['Migration and interop', ['rxjs-rxjs8-preparation', 'rxjs-migration-guide', 'rxjs-promise-interop']],
]
const guideDir = path.join(ROOT, 'docs', 'operator-trees')
const guideFiles = fs.readdirSync(guideDir).filter((f) => f.endsWith('.md'))
const allGuides = guideFiles.filter((f) => fs.statSync(path.join(guideDir, f)).size > 0).map((f) => f.slice(0, -3)).sort()
const emptyGuides = guideFiles.filter((f) => fs.statSync(path.join(guideDir, f)).size === 0).map((f) => f.slice(0, -3)).sort()
const sidebarLabel = new Map()
for (const [t, l] of guideItems) sidebarLabel.set(l.slice(l.lastIndexOf('/') + 1), t)
const placed = new Set()
const guideSections = []
for (const [gtitle, stems] of GUIDE_GROUPS) {
  const links = []
  for (const stem of stems) {
    if (allGuides.includes(stem) && !placed.has(stem)) {
      placed.add(stem)
      const link = '/docs/operator-trees/' + stem
      const label = sidebarLabel.get(stem) || h1(link) || stem
      links.push(`[${label}](${link})`)
    }
  }
  if (links.length) guideSections.push(`### ${gtitle}\n\n${links.join(' ·\n')}`)
}
const rest = allGuides.filter((s) => !placed.has(s))
if (rest.length) {
  const links = rest.map((s) => `[${sidebarLabel.get(s) || h1('/docs/operator-trees/' + s) || s}](/docs/operator-trees/${s})`)
  guideSections.push(`### More guides\n\n${links.join(' ·\n')}`)
}

// ---------- learning paths ----------
const ops = (...names) =>
  names.map((n) => (labelToLink.has(n) ? `[\`${n}\`](${labelToLink.get(n)})` : `\`${n}\``)).join(', ')
const guide = (stem, label) =>
  `[${label || sidebarLabel.get(stem) || h1('/docs/operator-trees/' + stem) || stem}](/docs/operator-trees/${stem})`

const beginner = [
  `${guide('rxjs-mental-models')} — build the conceptual foundation first`,
  `${ops('of', 'from', 'interval / timer', 'fromEvent')} — the creation operators you will use every day`,
  `${ops('map', 'filter', 'tap')} — basic transformation`,
  `${guide('cold-vs-hot-guide')} — the most important concept to internalise early`,
  `${guide('subscription-management-guide')} — avoid leaks from day one`,
  `${ops('debounceTime', 'distinctUntilChanged')} — the two filtering operators you will use most`,
  `${ops('catchError', 'retry')} — error handling basics`,
  `${guide('rxjs-cookbook')} — common recipes ready to use`,
]
const intermediate = [
  `${guide('flattening-strategy-guide')} — the most consequential RxJS decision`,
  `${ops('switchMap', 'concatMap', 'mergeMap', 'exhaustMap')} and their advanced companions`,
  `${guide('multicasting-guide')} — \`share\`, \`shareReplay\`, refCount semantics`,
  guide('subject-decision-guide'),
  `${ops('scan')} — state accumulation without external variables`,
  `${guide('error-handling-patterns')} — exponential backoff, circuit breakers`,
  guide('custom-operators-guide'),
  guide('rxjs-marble-testing-advanced'),
]
const advanced = [
  guide('pipeline-architecture-guide'),
  guide('concurrency-guide'),
  guide('rxjs-state-machines'),
  guide('rxjs-custom-operators-advanced'),
  guide('performance-patterns-guide'),
  guide('typescript-rxjs-guide'),
  guide('rxjs-testing-integration'),
  guide('rxjs-rxjs8-preparation'),
]
const numbered = (items) => items.map((s, i) => `${i + 1}. ${s}`).join('\n')

// ---------- assemble ----------
const featuresYaml = featureCards.map(([title, details, link]) => `  - title: ${yq(title)}\n    details: ${yq(details)}\n    link: ${link}`)
featuresYaml.push(
  `  - title: ${yq('Operator Trees & Guides')}\n    details: ${yq(`${nGuides} pages — the operator-tree classification, decision guides, architecture, testing, framework integrations, domain patterns`)}\n    link: /docs/operator-trees/overview`,
)
featuresYaml.push(`  - title: ${yq('Authoring Standard')}\n    details: ${yq('The eight-policy framework every operator page follows')}\n    link: /SKILL`)

const tagline =
  `${nOps} operator pages and ${nGuides} reference guides across ${nCats} categories. Every operator page carries its signature, ` +
  'functional specification, marble diagrams, behavioral contract, runnable examples and the pitfalls that bite in production.'

const doc = []
doc.push('---')
doc.push('layout: home')
doc.push('')
doc.push('hero:')
doc.push('  name: RxJS Operators Documentation')
doc.push('  text: A formal reference for every RxJS operator')
doc.push(`  tagline: ${yq(tagline)}`)
doc.push('  actions:')
doc.push('    - theme: brand')
doc.push('      text: Browse Operators')
doc.push('      link: /operators-claude/transformation/map')
doc.push('    - theme: alt')
doc.push('      text: Operator Trees')
doc.push('      link: /docs/operator-trees/overview')
doc.push('    - theme: alt')
doc.push('      text: ← hansschenker.github.io')
doc.push('      link: https://hansschenker.github.io')
doc.push('')
doc.push('features:')
doc.push(...featuresYaml)
doc.push('---')
doc.push('')
doc.push('## About This Documentation')
doc.push('')
doc.push(
  'This site is a complete, formal reference for the RxJS operator set, built with [VitePress](https://vitepress.dev) ' +
    'and written against the **eight-policy framework** defined in [SKILL.md](/SKILL). Where the official API reference ' +
    'tells you what an operator does, these pages tell you its full behavioral contract: when it subscribes, how it completes, ' +
    'what it does with errors, how it behaves under pressure, and which mistakes it invites.',
)
doc.push('')
doc.push('| Metric | Count |')
doc.push('|---|---|')
doc.push(`| Operator pages | **${nOps}** |`)
doc.push(`| Reference guides and pattern libraries | **${nGuides}** |`)
doc.push(`| Operator categories | **${nCats}** |`)
doc.push('| Documentation standard | Eight-Policy Framework v1.0 |')
doc.push('| RxJS version | 7.x (RxJS 8 migration guide included) |')
doc.push('')
doc.push('### The eight policies')
doc.push('')
doc.push('1. **Identity** — name, category, TypeScript signature, import path')
doc.push('2. **Functional Specification** — input and output types, transformation rules, invariants')
doc.push('3. **Marble Diagrams** — ASCII temporal visualisation of each meaningful behaviour variant')
doc.push('4. **Behavioral Characteristics** — subscription, completion, error propagation, backpressure, hot vs cold')
doc.push('5. **Type System Integration** — generics, type narrowing, compile-time guarantees')
doc.push('6. **Practical Examples** — at least three runnable examples, basic → common pattern → edge case')
doc.push('7. **Common Pitfalls** — anti-patterns with corrections in `❌ INCORRECT` / `✅ CORRECT` / `WHY:` form')
doc.push('8. **Related Operators** — same-category alternatives, complementary operators, decision tables')
doc.push('')
doc.push(
  'Pages marked **(advanced)** in the sidebar assume the core page and go straight to production patterns. ' +
    'Pages marked **(deprecated)** document operators that still ship in RxJS 7 but are removed in RxJS 8, with migration notes.',
)
doc.push('')
doc.push('---')
doc.push('')
doc.push('## Operators by Category')
doc.push('')
doc.push(opSections.join('\n\n'))
doc.push('')
doc.push('---')
doc.push('')
doc.push('## Operator Trees and Reference Guides')
doc.push('')
doc.push(
  'The [Operator Trees](/docs/operator-trees/overview) classify the whole operator surface by behavior: each family is an ' +
    'invariant plus independent variant axes, and every RxJS name is a coordinate in that space. The guides below are ' +
    'organised by what you are trying to do.',
)
doc.push('')
doc.push(guideSections.join('\n\n'))
doc.push('')
doc.push('---')
doc.push('')
doc.push('## Learning Paths')
doc.push('')
doc.push('### Beginner — new to RxJS')
doc.push('')
doc.push(numbered(beginner))
doc.push('')
doc.push('### Intermediate — using RxJS daily')
doc.push('')
doc.push(numbered(intermediate))
doc.push('')
doc.push('### Advanced — designing systems on RxJS')
doc.push('')
doc.push(numbered(advanced))
doc.push('')
doc.push('### Domain-specific paths')
doc.push('')
doc.push('| Context | Recommended path |')
doc.push('|---|---|')
doc.push(`| Angular developer | ${guide('rxjs-angular-standalone')} → ${guide('rxjs-signals-interop-deep')} → ${guide('ngrx-effects-patterns')} → ${guide('rxjs-enterprise-patterns')} |`)
doc.push(`| Real-time app | ${guide('rxjs-websocket-patterns')} → ${guide('rxjs-graphql-patterns')} → ${guide('rxjs-data-synchronization-patterns')} → ${guide('rxjs-multi-tab-patterns')} |`)
doc.push(`| Node.js backend | ${guide('nodejs-rxjs-patterns')} → ${guide('rxjs-error-resilience-patterns')} |`)
doc.push(`| Testing focus | ${guide('rxjs-marble-testing-advanced')} → ${guide('rxjs-testing-integration')} → ${guide('debugging-guide')} |`)
doc.push(`| Performance work | ${guide('performance-patterns-guide')} → ${guide('rxjs-performance-profiling')} |`)
doc.push('')
doc.push('---')
doc.push('')
doc.push('## Documentation Conventions')
doc.push('')
doc.push('**Marble diagrams**')
doc.push('')
doc.push('```')
doc.push('Source:   --a--b--c--|')
doc.push('          operator(config)')
doc.push('Result:   --A--B--C--|')
doc.push('')
doc.push('-   one unit of time        a,b,c   emitted values')
doc.push('|   successful completion   #       error')
doc.push('^   subscription point     (ab)    synchronous emissions')
doc.push('```')
doc.push('')
doc.push('**Educational metadata** — every core page ends with a strategy classification:')
doc.push('')
doc.push('- **Cognitive Load** 1–5: how much mental overhead the operator demands (1 = trivial, 5 = expert)')
doc.push('- **Usage Frequency** 1–5: how often it appears in production code (1 = rare, 5 = essential)')
doc.push('- **Composability** 1–5: how naturally it combines with the rest of the operator set')
doc.push('- **Teaching sequence**: prerequisites, what it unlocks, and what it is commonly learned with')
doc.push('')
doc.push('---')
doc.push('')
doc.push('## Contributing')
doc.push('')
doc.push(
  'New operator pages follow the template in [SKILL.md](/SKILL), live in `operators-claude/<category>/<operatorName>.md`, ' +
    'and need a sidebar entry in `.vitepress/config.mts`; then run `npm run docs:home` to regenerate this page. Every page must cover all eight policies, include at least three ' +
    'runnable examples with expected output, and document at least two pitfalls. Source: ' +
    '[github.com/hansschenker/rxjs-operators-documentation](https://github.com/hansschenker/rxjs-operators-documentation).',
)
doc.push('')
doc.push('## Related Resources')
doc.push('')
doc.push('- **RxJS official documentation**: [rxjs.dev](https://rxjs.dev)')
doc.push('- **ReactiveX**: [reactivex.io](http://reactivex.io)')
doc.push('- **RxJS source**: [github.com/ReactiveX/rxjs](https://github.com/ReactiveX/rxjs)')
doc.push('- **Learn RxJS**: [learnrxjs.io](https://www.learnrxjs.io)')
doc.push('- **RxJS Marbles**: [rxmarbles.com](https://rxmarbles.com)')
doc.push('')
doc.push('---')
doc.push('')
doc.push('**Last updated**: November 2026 · **Documentation standard**: Eight-Policy Framework v1.0 · **RxJS**: 7.x')
doc.push('')

const text = doc.join('\n')
console.log(`operator pages: ${nOps} | guides: ${nGuides} (empty, skipped: ${emptyGuides.join(', ')}) | categories: ${nCats}`)
console.log(`sidebar links without a page on disk: ${missing.length}`)
for (const m of missing) console.log('   missing:', m)
console.log(`home page: ${(text.match(/\n/g) || []).length} lines, ${Buffer.byteLength(text, 'utf8')} bytes`)
if (missing.length) process.exitCode = 1
if (WRITE) {
  fs.writeFileSync(OUT, text, 'utf8')
  console.log('WROTE', OUT)
} else {
  console.log('(dry run — pass --write to write index.md)')
}
