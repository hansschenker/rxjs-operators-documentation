# File Upload Patterns with RxJS

Single file, multi-file, progress tracking, chunked uploads, and drag-and-drop integration.

---

## Pattern 1: Single File Upload with Progress

```typescript
import { Observable } from 'rxjs';
import { HttpClient, HttpEventType, HttpEvent } from '@angular/common/http';
import { map, filter } from 'rxjs/operators';

export interface UploadProgress {
  percent:  number;
  status:   'uploading' | 'done';
  url?:     string;
}

@Injectable({ providedIn: 'root' })
export class UploadService {
  upload(file: File): Observable<UploadProgress> {
    const form = new FormData();
    form.append('file', file);

    return this.http.post<{ url: string }>('/api/upload', form, {
      reportProgress: true,
      observe: 'events'
    }).pipe(
      map((event: HttpEvent<{ url: string }>): UploadProgress | null => {
        if (event.type === HttpEventType.UploadProgress) {
          return {
            percent: Math.round(100 * (event.loaded / (event.total ?? event.loaded))),
            status: 'uploading'
          };
        }
        if (event.type === HttpEventType.Response) {
          return { percent: 100, status: 'done', url: event.body?.url };
        }
        return null;
      }),
      filter((p): p is UploadProgress => p !== null)
    );
  }
}
```

---

## Pattern 2: Multi-File Upload — Parallel with Aggregate Progress

```typescript
import { forkJoin, combineLatest } from 'rxjs';
import { map, shareReplay, startWith } from 'rxjs/operators';

@Injectable()
export class MultiUploadService {
  uploadAll(files: File[]): Observable<{ overall: number; files: UploadProgress[] }> {
    const uploads = files.map(f =>
      this.upload(f).pipe(
        startWith({ percent: 0, status: 'uploading' as const }),
        shareReplay(1)
      )
    );

    return combineLatest(uploads).pipe(
      map(progresses => ({
        overall: Math.round(
          progresses.reduce((s, p) => s + p.percent, 0) / progresses.length
        ),
        files: progresses
      }))
    );
  }
}
```

---

## Pattern 3: Sequential Upload Queue

```typescript
import { from } from 'rxjs';
import { concatMap, scan } from 'rxjs/operators';

interface QueueResult { file: string; url: string; }

function uploadSequentially(files: File[]): Observable<QueueResult[]> {
  return from(files).pipe(
    concatMap(file =>
      this.upload(file).pipe(
        filter(p => p.status === 'done'),
        map(p => ({ file: file.name, url: p.url! }))
      )
    ),
    scan((results, result) => [...results, result], [] as QueueResult[])
  );
}
```

---

## Pattern 4: Chunked Upload (Large Files)

```typescript
import { from, concat } from 'rxjs';
import { concatMap, scan, map } from 'rxjs/operators';

const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB

function uploadInChunks(file: File): Observable<number> {
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

  const chunks$ = from(
    Array.from({ length: totalChunks }, (_, i) => ({
      index: i,
      blob:  file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE)
    }))
  );

  return chunks$.pipe(
    concatMap(({ index, blob }) => {
      const form = new FormData();
      form.append('chunk',      blob);
      form.append('chunkIndex', String(index));
      form.append('totalChunks', String(totalChunks));
      form.append('fileId',      file.name);
      return this.http.post('/api/upload/chunk', form).pipe(
        map(() => Math.round(((index + 1) / totalChunks) * 100))
      );
    })
  );
}
```

---

## Pattern 5: Drag-and-Drop File Upload

```typescript
import { fromEvent, merge } from 'rxjs';
import { map, filter, tap, switchMap } from 'rxjs/operators';

@Component({ ... })
export class DropZoneComponent {
  private dropZone = viewChild<ElementRef>('dropZone');

  ngAfterViewInit() {
    const el = this.dropZone().nativeElement;

    const dragover$ = fromEvent<DragEvent>(el, 'dragover').pipe(
      tap(e => { e.preventDefault(); el.classList.add('dragover'); })
    );
    const dragleave$ = fromEvent(el, 'dragleave').pipe(
      tap(() => el.classList.remove('dragover'))
    );
    const drop$ = fromEvent<DragEvent>(el, 'drop').pipe(
      tap(e => { e.preventDefault(); el.classList.remove('dragover'); }),
      map(e => Array.from(e.dataTransfer?.files ?? []))
    );

    merge(dragover$, dragleave$).subscribe();

    drop$.pipe(
      filter(files => files.length > 0),
      switchMap(files => this.uploadService.uploadAll(files))
    ).subscribe(progress => this.uploadState.set(progress));
  }
}
```

---

## Pattern 6: Upload with Retry and Cancellation

```typescript
import { Subject } from 'rxjs';
import { takeUntil, retry, catchError } from 'rxjs/operators';

@Injectable()
export class CancellableUploadService {
  private cancel$ = new Subject<void>();

  upload(file: File): Observable<UploadProgress> {
    return this.doUpload(file).pipe(
      retry(2),
      catchError(err => {
        logger.error('Upload failed after retries', err);
        return throwError(() => err);
      }),
      takeUntil(this.cancel$)
    );
  }

  cancel(): void { this.cancel$.next(); }
}
```

---

## Decision Table

| Scenario | Strategy | Key operators |
|---|---|---|
| Single file with progress | `HttpEventType` mapping | `map`, `filter` |
| Multiple files in parallel | Combined progress | `combineLatest`, `startWith` |
| Sequential queue | One at a time | `concatMap`, `scan` |
| Large file (> 100MB) | Chunked | `from(chunks)`, `concatMap` |
| Drag and drop | Drop event | `fromEvent`, `switchMap` |
| With cancel | Cancel Subject | `takeUntil` |
