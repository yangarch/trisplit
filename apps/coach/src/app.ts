import express from 'express';
import cookieParser from 'cookie-parser';
import { createHmac, timingSafeEqual } from 'crypto';
import { CoachStore } from './store';
import { validateBundle } from './types/workout';
import { swimNoteRoutes, validSwimCatalog } from './swimNoteRoutes';

export interface AppConfig {
  ingestToken: string; adminToken: string; sessionSecret: string;
  secureCookie: boolean; since: string; reportUrl: string;
  swimReviews?: boolean;
}
const equal = (a: string, b: string) => !!b && Buffer.byteLength(a) === Buffer.byteLength(b)
  && timingSafeEqual(Buffer.from(a), Buffer.from(b));
const escape = (s: unknown) => String(s ?? '').replace(/[&<>"']/g, c => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
}[c]!));

export function createApp(store: CoachStore, config: AppConfig): express.Express {
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', 1);
  app.use((_req, res, next) => {
    res.set('Cache-Control', 'no-store');
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('Referrer-Policy', 'no-referrer');
    res.set('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'");
    next();
  });
  app.use(express.json({ limit: '8mb' }));
  app.use('/swim-notes', express.urlencoded({ extended: false, limit: '64kb' }));
  app.use(express.urlencoded({ extended: false, limit: '4kb' }));
  app.use(cookieParser(config.sessionSecret));
  app.get('/health', (_req, res) => { res.json({ status: 'ok', service: 'trisplit-coach', schema_version: 1 }); });
  const apiAuth: express.RequestHandler = (req, res, next) => {
    if (!equal(req.headers.authorization ?? '', `Bearer ${config.ingestToken}`)) { res.sendStatus(403); return; }
    next();
  };
  app.post('/api/workouts', apiAuth, (req, res) => {
    try { validateBundle(req.body); } catch { res.status(400).json({ error: 'Invalid workout contract' }); return; }
    const bundle = req.body;
    if (bundle.activity.start_date_local!.slice(0, 10) < config.since) {
      res.status(200).json({ status: 'before_cutover' }); return;
    }
    res.status(200).json({ status: store.enqueue(bundle) });
  });
  app.get('/api/status', apiAuth, (_req, res) => { res.json(store.status()); });
  app.post('/api/swim-catalog', apiAuth, (req, res) => {
    if (!validSwimCatalog(req.body)) { res.sendStatus(400); return; }
    store.saveSwimCatalog(req.body);
    res.json({ status: 'stored', count: req.body.length });
  });
  app.post('/api/retry/:kind/:id', apiAuth, (req, res) => {
    const id = Number(req.params.id);
    const kind = req.params.kind;
    if (!Number.isSafeInteger(id) || id <= 0 || !['analysis', 'delivery'].includes(kind)
        || (kind === 'delivery' && req.body.confirm_checked !== true)) { res.sendStatus(400); return; }
    res.json({ retried: store.retry(id, kind as 'analysis' | 'delivery') });
  });
  const page = (body: string) => `<!doctype html><html lang="ko"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1"><title>Training Companion</title>
    <style>body{font:16px/1.7 system-ui,sans-serif;background:#f4f5f7;color:#202936;margin:0}
    main{max-width:760px;margin:40px auto;padding:0 20px}h1{font-size:26px}article,form{background:white;padding:24px;border-radius:12px;margin:20px 0}
    h2{font-size:18px;margin:0}small{color:#637080}p{white-space:pre-wrap}a{color:#236387}input,button,textarea{font:inherit;padding:10px;max-width:100%;box-sizing:border-box}
    textarea,input[type=url]{width:100%;display:block;margin:4px 0 12px}article form{margin:6px 0;padding:6px 0}</style><main>${body}</main></html>`;
  const returnTo = (v: unknown) => typeof v === 'string' && /^\/swim-notes(?:\/\d{1,16})?$/.test(v) ? v : '/dashboard';
  const login = (next?: unknown) => page(`<h1>Training Companion</h1><p>사이클 · 러닝 · 수영 분석 기록</p><form method="post" action="/login"><label>접속 토큰 <input name="token" type="password" required autocomplete="current-password"></label> <input type="hidden" name="next" value="${returnTo(next)}"><button>로그인</button></form>`);
  app.get('/', (req, res) => {
    if (req.signedCookies.coach_owner === String(store.ownerId)) { res.redirect('/dashboard'); return; }
    res.send(login());
  });
  app.get('/login', (req, res) => { res.send(login(req.query.next)); });
  // No token in URLs, and legacy unsigned athlete_id cookies grant no access.
  app.post('/login', (req, res) => {
    if (!equal(typeof req.body.token === 'string' ? req.body.token : '', config.adminToken)) { res.sendStatus(403); return; }
    res.cookie('coach_owner', String(store.ownerId), {
      signed: true, httpOnly: true, secure: config.secureCookie, sameSite: 'strict', maxAge: 7 * 86400000,
    });
    res.redirect(returnTo(req.body.next));
  });
  app.post('/logout', (_req, res) => { res.clearCookie('coach_owner'); res.redirect('/'); });
  app.get('/dashboard', (req, res) => {
    if (req.signedCookies.coach_owner !== String(store.ownerId)) { res.redirect('/'); return; }
    const names: Record<string, string> = { swimming: '🏊 수영', running: '🏃 러닝', cycling: '🚴 사이클' };
    const cards = store.analyses().map(a => `<article><h2>${escape(names[a.sport])} · ${escape(a.activity_name)}</h2>
      <small>${escape(a.activity_date?.slice(0, 10))} · ${a.distance === null ? '거리 미기록' : a.sport === 'swimming' ? `${Math.round(a.distance)}m` : `${(a.distance / 1000).toFixed(1)}km`}</small>
      <p>${escape(a.analysis)}</p>${a.sport === 'swimming' ? `<a href="/swim-notes/${a.activity_id}">수영 후기</a>` : ''}</article>`).join('');
    const report = config.reportUrl ? `<a href="${escape(config.reportUrl)}">운동 현황·기록 보기</a>` : '';
    res.send(page(`<h1>Training Companion</h1><nav><a href="/swim-notes">수영 후기</a>${report ? ` · ${report}` : ''}</nav><form method="post" action="/logout"><button>로그아웃</button></form>${cards || '<p>아직 분석 기록이 없습니다.</p>'}`));
  });
  const csrf = createHmac('sha256', config.sessionSecret).update(`swim-notes:${store.ownerId}`).digest('hex');
  app.use('/swim-notes', (req, res, next) => {
    if (req.signedCookies.coach_owner === String(store.ownerId)) { next(); return; }
    if (req.method !== 'GET') { res.sendStatus(403); return; }
    res.redirect(`/login?next=${encodeURIComponent(returnTo(req.originalUrl.split('?')[0]))}`);
  }, swimNoteRoutes(store, page, escape, csrf, config.swimReviews));
  app.use((_err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    // Do not echo submitted health data, model requests, or credentials.
    res.status(400).json({ error: 'Request could not be processed' });
  });
  return app;
}
