import express from 'express';
import { CoachStore, SwimRecord } from './store';
import { automaticSwim, fetchDiaryPost, noteUrl, rankSwims, validDate } from './services/swimNotes';
import { wallTimeMs } from './types/workout';

export function validSwimCatalog(value: unknown): value is SwimRecord[] {
  if (!Array.isArray(value) || value.length > 10000) return false;
  const numberOrNull = (n: unknown) => n === null || (typeof n === 'number' && Number.isFinite(n) && n >= 0);
  return value.every(r => r && typeof r === 'object' && Object.keys(r).length === 4
    && Number.isSafeInteger(r.activity_id) && r.activity_id > 0 && typeof r.start_local === 'string'
    && Number.isFinite(wallTimeMs(r.start_local)) && validDate(r.start_local.slice(0, 10))
    && numberOrNull(r.distance) && numberOrNull(r.moving_time))
    && new Set(value.map(r => r.activity_id)).size === value.length;
}

type Render = (body: string) => string;
type Escape = (value: unknown) => string;

export function swimNoteRoutes(store: CoachStore, page: Render, escape: Escape, csrf: string, swimReviews = false): express.Router {
  const router = express.Router();
  const back = '<nav><a href="/dashboard">운동 분석</a> · <a href="/swim-notes">수영 후기</a></nav>';
  const token = `<input type="hidden" name="csrf" value="${csrf}">`;
  const label = (r: SwimRecord) => `${escape(r.start_local.slice(0, 10))} ${escape(r.start_local.slice(11, 16))}`
    + ` · ${r.distance === null ? '거리 미기록' : `${Math.round(r.distance).toLocaleString('ko-KR')}m`}`
    + (r.moving_time === null ? '' : ` · 이동 ${Math.round(r.moving_time / 60)}분`);
  const linked = (r: SwimRecord, status: string) =>
    `<p><strong>${status}</strong> · <a href="/swim-notes/${r.activity_id}">${label(r)}</a></p>`;
  const reviewNotice = swimReviews ? '연결하면 후기 본문과 운동 기록을 Claude로 분석해 운동 상세에 표시해요.' : '';
  const form = `<form method="post" action="/swim-notes/import">${token}
    <h2>후기 링크 붙이기</h2><label for="links">수영일기 게시글 링크 (한 줄에 하나, 최대 8개)</label>
    <textarea id="links" name="links" rows="5" maxlength="13000" required placeholder="https://…"></textarea>
    <details><summary>운동 날짜 직접 지정</summary><label>운동 날짜 <input type="date" name="date"></label>
    <small>날짜를 지정할 때는 링크를 하나만 넣어주세요.</small></details>
    <p>운동 기록이 명확하면 바로 연결하고, 선택이 필요한 경우 후보를 보여드려요. ${reviewNotice}</p><button>후기 연결</button></form>`;
  router.use((req, res, next) => {
    if (req.method === 'POST' && req.body?.csrf !== csrf) { res.sendStatus(403); return; }
    next();
  });
  router.get('/', (req, res) => {
    const date = typeof req.query.date === 'string' ? req.query.date : '';
    const records = validDate(date) ? store.swimsOn(date) : [];
    const day = validDate(date) ? `<article><h2>${date} 수영</h2>${records.map(r =>
      `<p><a href="/swim-notes/${r.activity_id}">${label(r)}</a> · ${store.swimNote(r.activity_id) ? '후기 연결됨' : '후기 연결'}</p>`).join('') || '<p>해당 날짜의 수영 기록이 없습니다.</p>'}</article>` : '';
    const recent = store.recentLinkedSwims().map(r => linked(r, '연결됨')).join('');
    res.send(page(`${back}<h1>수영 후기</h1>${form}${recent ? `<article><h2>최근 연결한 후기</h2>${recent}</article>` : ''}
      <form method="get" action="/swim-notes">
      <label>운동 날짜로 찾기 <input type="date" name="date" value="${escape(date)}" required></label> <button>수영 기록 보기</button></form>${day}`));
  });
  router.post(['/import', '/preview'], async (req, res, next) => {
    try {
      const input: string = typeof req.body.links === 'string' ? req.body.links : '';
      const date: string = typeof req.body.date === 'string' ? req.body.date : '';
      const urls = [...new Set(input.split(/\s+/).filter(Boolean).map(noteUrl))];
      if (!urls.length || urls.length > 8 || (date && (!validDate(date) || urls.length !== 1))) {
        res.status(400).send(page(`${back}<p>링크는 최대 8개까지, 날짜를 지정하면 한 개만 입력해 주세요.</p>${form}`)); return;
      }
      const imports = await Promise.all(urls.map(async url => {
        const existing = store.swimIdForNoteUrl(url);
        const post = date || existing !== undefined ? undefined : await fetchDiaryPost(url).catch(() => undefined);
        const day = date || post?.date;
        const candidates = day && existing === undefined ? rankSwims(store.swimsOn(day), post) : [];
        return {url, post, day, candidates};
      }));
      const results = imports.map(({url, post, day, candidates}) => {
        const title = escape(post?.title ?? '수영 후기');
        const source = `<p><a href="${escape(url)}" rel="noreferrer" target="_blank">후기 원문</a></p>`;
        const existing = store.swimIdForNoteUrl(url);
        if (existing !== undefined) {
          const record = store.swim(existing);
          const status = record ? linked(record, '이미 연결됨') : '<p>이미 연결됨</p>';
          return `<article><h2>${title}</h2>${status}${source}</article>`;
        }
        if (!day) return `<article><h2>${title}</h2><p>날짜를 읽을 수 없습니다. 링크 하나와 운동 날짜를 입력해 주세요.</p>
          ${source}</article>`;
        const match = req.path === '/import' ? automaticSwim(candidates, !!date, post) : undefined;
        const collision = match && imports.some(other => other.url !== url
          && other.candidates.some(r => r.activity_id === match.activity_id));
        if (match && !collision && store.saveSwimNote(match.activity_id, {url, body:null}, false, swimReviews)) {
          const status = swimReviews ? '<p>후기 반영 분석을 요청했어요.</p>' : '';
          return `<article><h2>${title}</h2>${linked(match, '자동 연결됨')}${status}${source}</article>`;
        }
        const basis = date ? '입력한 운동일' : post?.dateSource === 'text' ? '후기에서 읽은 날짜'
          : post?.dateSource === 'ambiguous' ? '본문에 날짜가 여러 개 있어 게시일을 사용' : '게시일 기준 후보';
        const items = candidates.map(r => store.swimNote(r.activity_id)
          ? `<p>${label(r)} · <a href="/swim-notes/${r.activity_id}">다른 후기 연결됨</a></p>`
          : `<form method="post" action="/swim-notes/${r.activity_id}/link">${token}
              <input type="hidden" name="url" value="${escape(url)}"><span>${label(r)}</span> <button>연결</button></form>`).join('');
        return `<article><h2>${title}</h2><small>${day} · ${basis}</small>${source}
          ${items || '<p>일치하는 수영 기록이 없습니다. 날짜를 직접 지정해 다시 찾아보세요.</p>'}</article>`;
      });
      res.send(page(`${back}<h1>후기 연결 결과</h1>${results.join('')}${form}`));
    } catch (err) { next(err); }
  });
  router.param('id', (req, res, next, value) => {
    const id = /^\d+$/.test(value) ? Number(value) : NaN;
    const record = Number.isSafeInteger(id) && id > 0 ? store.swim(id) : undefined;
    if (!record) { res.status(404).send(page(`${back}<p>수영 기록을 찾을 수 없습니다.</p>`)); return; }
    res.locals.record = record;
    next();
  });
  router.get('/:id', (_req, res) => {
    const r: SwimRecord = res.locals.record;
    const note = store.swimNote(r.activity_id);
    const url = escape(note?.url);
    const review = store.swimReview(r.activity_id);
    const analyzeButton = (label: string) => `<form method="post" action="/swim-notes/${r.activity_id}/analyze">${token}
      <p>${reviewNotice}</p><button>${label}</button></form>`;
    let reviewContent = '';
    if (review?.state === 'complete') reviewContent = `<p>${escape(review.analysis)}</p>`;
    else if (swimReviews && note) {
      if (review?.state === 'needs_body') reviewContent = '<p>링크에서 후기 본문을 읽을 수 없어요. 아래에 본문을 붙여넣고 저장해 주세요.</p>';
      else if (review?.state === 'failed') reviewContent = `<p>분석을 완료하지 못했어요. 링크만 연결했다면 본문을 직접 저장할 수도 있어요.</p>${analyzeButton('분석 다시 시도')}`;
      else if (review) reviewContent = `<p>후기와 운동 기록을 분석 중이에요. <a href="/swim-notes/${r.activity_id}">결과 확인</a></p>`;
      else reviewContent = analyzeButton('후기를 반영해 분석');
    }
    res.send(page(`${back}<h1>${note ? '수영 후기 연결됨' : '수영 후기 연결'}</h1><p>${label(r)}</p>
      ${url ? `<p><a href="${url}" rel="noreferrer" target="_blank">연결된 후기 열기</a></p>` : ''}
      ${reviewContent ? `<article><h2>후기를 반영한 분석</h2>${reviewContent}</article>` : ''}
      <form method="post" action="/swim-notes/${r.activity_id}">${token}
        <label for="url">후기 링크</label><input type="url" id="url" name="url" maxlength="1500" value="${url}" placeholder="https://…">
        <label for="body">후기 본문</label><textarea id="body" name="body" rows="6" maxlength="4000">${escape(note?.body)}</textarea>
        <small>링크나 본문 중 하나만 입력해도 됩니다. ${reviewNotice}</small><br><button>${note ? '수정 저장' : '후기 저장'}</button>
      </form>${note ? `<form method="post" action="/swim-notes/${r.activity_id}/remove">${token}<button>연결 해제</button></form>` : ''}`));
  });
  const save = (replace: boolean): express.RequestHandler => (req, res) => {
    const { activity_id: id } = res.locals.record as SwimRecord;
    const url = typeof req.body.url === 'string' ? req.body.url.trim() : '';
    const body = replace && typeof req.body.body === 'string' ? req.body.body.trim() : '';
    let normalized: string | null = null;
    try {
      if (url) normalized = noteUrl(url);
      if ((!normalized && !body) || (!replace && !normalized) || body.length > 4000) throw new Error();
    } catch {
      res.status(400).send(page(`${back}<p>유효한 HTTPS 링크나 후기 본문을 입력해 주세요.</p>`)); return;
    }
    if (!replace && normalized && store.swimIdForNoteUrl(normalized) === id) {
      res.redirect(303, `/swim-notes/${id}`);
      return;
    }
    if (!store.saveSwimNote(id, { url: normalized, body: body || null }, replace, swimReviews)) {
      res.status(409).send(page(`${back}<p>이미 연결된 후기 또는 수영입니다.</p><a href="/swim-notes/${id}">연결 확인</a>`)); return;
    }
    res.redirect(303, `/swim-notes/${id}`);
  };
  router.post('/:id/link', save(false));
  router.post('/:id', save(true));
  router.post('/:id/analyze', (_req, res) => {
    if (!swimReviews) { res.sendStatus(404); return; }
    const { activity_id: id } = res.locals.record as SwimRecord;
    store.queueSwimReview(id);
    res.redirect(303, `/swim-notes/${id}`);
  });
  router.post('/:id/remove', (_req, res) => {
    const { activity_id: id } = res.locals.record as SwimRecord;
    store.removeSwimNote(id);
    res.redirect(303, `/swim-notes/${id}`);
  });
  return router;
}
