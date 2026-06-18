# bindCallback / bindNodeCallback

Wrap Node.js-style callback APIs as Observable factories.

---

## `bindCallback`

### Identity
- **Import**: `import { bindCallback } from 'rxjs'`
- **Signature**:
  ```typescript
  function bindCallback(
    callbackFunc: (...args: any[]) => void,
    resultSelector?: (...args: any[]) => any,
    scheduler?: SchedulerLike
  ): (...args: any[]) => Observable<any>
  ```
- **Category**: Creation — converts a callback-style function into an Observable factory

### Functional Specification

`bindCallback(fn)` wraps a function that accepts a callback as its **last argument**. It returns a new function with the same signature minus the callback — when called, that function returns a cold Observable. The Observable emits the callback's arguments and completes.

```typescript
// Original callback API:
someLib.doWork(config, (result) => use(result));

// bindCallback equivalent:
const doWork$ = bindCallback(someLib.doWork.bind(someLib));
doWork$(config).subscribe(result => use(result));
```

### Examples

```typescript
import { bindCallback } from 'rxjs';

// Wrap fs.readFile (callback receives err + data — use bindNodeCallback instead)
// Wrap a browser API that uses plain callbacks:
const getLocation$ = bindCallback(
  navigator.geolocation.getCurrentPosition.bind(navigator.geolocation)
);

getLocation$().subscribe(position => {
  console.log(position.coords.latitude, position.coords.longitude);
});

// Wrap a custom callback util
function loadScript(url: string, cb: (script: HTMLScriptElement) => void) {
  const el = document.createElement('script');
  el.src = url;
  el.onload = () => cb(el);
  document.head.appendChild(el);
}

const loadScript$ = bindCallback(loadScript);
loadScript$('https://example.com/lib.js').subscribe(script => {
  console.log('loaded:', script.src);
});
```

---

## `bindNodeCallback`

### Identity
- **Import**: `import { bindNodeCallback } from 'rxjs'`
- **Signature**:
  ```typescript
  function bindNodeCallback(
    callbackFunc: (...args: any[]) => void,
    resultSelector?: (...args: any[]) => any,
    scheduler?: SchedulerLike
  ): (...args: any[]) => Observable<any>
  ```
- **Category**: Creation — wraps Node.js error-first `(err, result) => void` callbacks

### Functional Specification

`bindNodeCallback` is specifically for the **Node.js error-first convention**: the callback's first argument is an error (`null` on success). If `err` is non-null, the Observable errors. If `err` is null, the Observable emits the remaining callback arguments.

```typescript
// Node.js callback convention:
fs.readFile('/path', 'utf8', (err, data) => {
  if (err) throw err;
  use(data);
});

// bindNodeCallback:
const readFile$ = bindNodeCallback(fs.readFile);
readFile$('/path', 'utf8').subscribe(data => use(data));
// Errors automatically on non-null err argument
```

### Examples

```typescript
import { bindNodeCallback } from 'rxjs';
import * as fs from 'fs';
import * as dns from 'dns';

// Wrap Node.js fs
const readFile$  = bindNodeCallback(fs.readFile);
const writeFile$ = bindNodeCallback(fs.writeFile);

readFile$('./config.json', 'utf8').pipe(
  map(content => JSON.parse(content as string)),
  switchMap(config => writeFile$('./config.bak.json', JSON.stringify(config)))
).subscribe({
  complete: () => console.log('backup written'),
  error:    e  => console.error('failed:', e)
});

// Wrap Node.js dns.lookup
const lookup$ = bindNodeCallback(dns.lookup);
lookup$('example.com').subscribe(([address, family]) => {
  console.log(`${address} (IPv${family})`);
});
```

---

## `bindCallback` vs `bindNodeCallback`

| | `bindCallback` | `bindNodeCallback` |
|---|---|---|
| Callback convention | `(...results) => void` | `(err, ...results) => void` |
| Error handling | No error argument | First arg = error; non-null → Observable errors |
| Use for | Browser APIs, custom callbacks | Node.js `fs`, `dns`, `crypto`, etc. |

## Common Pitfall

```typescript
import { bindCallback } from 'rxjs';
import * as fs from 'fs';

// ❌ WRONG — using bindCallback for Node.js error-first callbacks
const readFile$ = bindCallback(fs.readFile);
readFile$('./file.txt', 'utf8').subscribe(([err, data]) => {
  // err arrives as part of the emission, not as an Observable error!
  if (err) console.error(err);
});

// ✅ CORRECT — use bindNodeCallback for error-first convention
import { bindNodeCallback } from 'rxjs';
const readFile$ = bindNodeCallback(fs.readFile);
readFile$('./file.txt', 'utf8').subscribe({
  next:  data  => use(data),
  error: err   => console.error(err) // err auto-routed to error channel
});

// WHY: bindCallback treats ALL callback arguments as next emissions.
// bindNodeCallback understands the (err, result) convention and routes
// the error to the Observable's error channel automatically.
```

## Modern Alternative

For most new Node.js code, `util.promisify` + `from()` is cleaner:

```typescript
import { promisify } from 'util';
import { from } from 'rxjs';
import * as fs from 'fs';

const readFile = promisify(fs.readFile);
const data$ = from(readFile('./config.json', 'utf8'));
// Observable<string | Buffer> — errors auto-routed
```

`bindNodeCallback` is most useful when working with legacy APIs that can't easily be promisified, or when you need RxJS-specific features (retry, timeout) directly on the Observable.

## References
- [bindCallback](https://rxjs.dev/api/index/function/bindCallback)
- [bindNodeCallback](https://rxjs.dev/api/index/function/bindNodeCallback)

---

**`bindCallback`** — Cognitive Load: 2/5 | Usage: 2/5 | Browser/custom callback APIs without error-first convention.
**`bindNodeCallback`** — Cognitive Load: 2/5 | Usage: 2/5 | Node.js error-first callbacks; prefer `from(promisify(fn)(...))` for modern code.
