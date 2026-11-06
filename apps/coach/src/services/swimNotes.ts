import { wallTimeMs } from '../types/workout';
import type { SwimRecord } from '../store';

export function diaryUrl(value: string): string | undefined {
  const url = new URL(value);
  let id: string | null = null;
  if (url.origin === 'https://gall.dcinside.com' && url.pathname === '/mini/board/view/'
      && url.searchParams.get('id') === 'swimmingdiary') id = url.searchParams.get('no');
  if (url.origin === 'https://m.dcinside.com') id = /^\/mini\/swimmingdiary\/(\d+)$/.exec(url.pathname)?.[1] ?? null;
  return id && /^\d{1,12}$/.test(id) ? `https://gall.dcinside.com/mini/board/view/?id=swimmingdiary&no=${id}` : undefined;
}

export function noteUrl(value: string): string {
  const url = new URL(value);
  if (value.length > 1500 || url.protocol !== 'https:' || url.username || url.password) throw new Error('Invalid note URL');
  return diaryUrl(value) ?? url.href;
}

export function validDate(date: string): boolean {
  const ms = wallTimeMs(`${date}T00:00:00`);
  return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 10) === date;
}

function textOnly(html: string): string {
  return html.replace(/<\/(?:p|div)>|<br\s*\/?>/gi, '\n').replace(/<[^>]*>/g, '')
    .replace(/&(amp|lt|gt|quot|apos|nbsp|#39);/g, (_, entity: string) =>
      ({amp:'&', lt:'<', gt:'>', quot:'"', apos:"'", nbsp:' ', '#39':"'"})[entity]!).trim();
}

export interface DiaryPost {
  title: string; date: string; dateSource: 'text' | 'published'; published: string;
  distance?: number; duration?: number;
}

export function readDiaryHtml(html: string): DiaryPost {
  const published = /class="gall_date"\s+title="(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})"/.exec(html);
  if (!published || !validDate(published[1])) throw new Error('Post date unavailable');
  const title = textOnly(/<meta property="og:title" content="([^"]*)"/.exec(html)?.[1] ?? '수영 후기')
    .replace(/ - 수영일기 미니 갤러리$/, '').slice(0, 120);
  const body = textOnly(/<div\b[^>]*class="write_div"[^>]*>([\s\S]*?)<\/div>/.exec(html)?.[1] ?? '');
  const dates = new Set<string>();
  for (const m of `${title}\n${body}`.matchAll(/\b(20\d{2})[-./년]\s*(\d{1,2})[-./월]\s*(\d{1,2})(?:일|\b)/g)) {
    const date = `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;
    if (validDate(date) && date <= published[1]) dates.add(date);
  }
  const distance = /(?:총\s*거리|기록\s*거리)\s*[:：]?\s*(\d[\d,.]*)\s*(km|m|미터)(?![a-z])/i.exec(body);
  const duration = /(?:이동\s*시간|운동\s*시간)\s*[:：]?\s*(\d+)\s*분(?:\s*(\d+)\s*초)?/.exec(body);
  return {
    title, date: dates.size === 1 ? [...dates][0] : published[1], dateSource: dates.size === 1 ? 'text' : 'published',
    published: `${published[1]}T${published[2]}`,
    distance: distance ? Number(distance[1].replace(/,/g, '')) * (distance[2].toLowerCase() === 'km' ? 1000 : 1) : undefined,
    duration: duration ? Number(duration[1]) * 60 + Number(duration[2] ?? 0) : undefined,
  };
}

export async function fetchDiaryPost(url: string): Promise<DiaryPost> {
  const source = diaryUrl(url);
  if (!source) throw new Error('Unsupported diary source');
  const response = await fetch(source, { redirect: 'error', signal: AbortSignal.timeout(6500) });
  if (!response.ok || !response.body || !response.headers.get('content-type')?.includes('text/html')) {
    await response.body?.cancel();
    throw new Error('Diary post unavailable');
  }
  let bytes = 0;
  const chunks: Uint8Array[] = [];
  const reader = response.body.getReader();
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > 1_500_000) throw new Error('Diary post too large');
      chunks.push(chunk.value);
    }
  } finally { await reader.cancel(); }
  return readDiaryHtml(Buffer.concat(chunks).toString('utf8'));
}

export function rankSwims(swims: SwimRecord[], post?: DiaryPost): SwimRecord[] {
  const score = (r: SwimRecord) => {
    const close = (a: number | undefined, b: number | null, tolerance: number) =>
      a !== undefined && b !== null && Math.abs(a - b) <= tolerance;
    return Number(close(post?.distance, r.distance, 25)) + Number(close(post?.duration, r.moving_time, 120));
  };
  return swims.filter(r => !post || r.start_local.slice(0, 19) <= post.published)
    .sort((a, b) => score(b) - score(a) || b.start_local.localeCompare(a.start_local));
}
