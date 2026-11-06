import 'dotenv/config';
import { createApp } from './app';
import { CoachStore } from './store';
import { analyze } from './services/analysis';
import { CoachWorker, sendNotification, sendText } from './services/worker';

const required = (key: string) => {
  const value = process.env[key];
  if (!value) throw new Error(`Missing ${key}`);
  return value;
};
const secret = (key: string) => {
  const value = required(key);
  if (value.length < 32) throw new Error(`${key} must be at least 32 characters`);
  return value;
};
const enabled = process.env.COACH_ENABLED === 'true';
const notifications = process.env.COACH_SEND_NOTIFICATIONS === 'true';
if (notifications && !enabled) throw new Error('Notifications require COACH_ENABLED=true');
const ownerId = Number(required('COACH_OWNER_ATHLETE_ID'));
if (!Number.isSafeInteger(ownerId) || ownerId <= 0) throw new Error('Invalid COACH_OWNER_ATHLETE_ID');
const since = required('COACH_SINCE');
if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || Number.isNaN(Date.parse(since))) throw new Error('Invalid COACH_SINCE');
const apiKey = enabled ? required('ANTHROPIC_API_KEY') : '';
const webhook = notifications ? required('NOTIFICATION_WEBHOOK_URL') : '';
const port = Number(process.env.PORT || '3003');
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid PORT');
const reportUrl = process.env.COACH_REPORT_URL || '';
if (reportUrl && !/^https?:\/\//.test(reportUrl)) throw new Error('Invalid COACH_REPORT_URL');
const publicUrl = process.env.COACH_PUBLIC_URL ? new URL(process.env.COACH_PUBLIC_URL) : undefined;
if (publicUrl && (publicUrl.protocol !== 'https:' || publicUrl.username || publicUrl.password
    || publicUrl.pathname !== '/' || publicUrl.search || publicUrl.hash)) throw new Error('Invalid COACH_PUBLIC_URL');
const store = new CoachStore(process.env.DATABASE_URL || './data/coach.db', ownerId);
const app = createApp(store, {
  ingestToken: secret('COACH_INGEST_TOKEN'), adminToken: secret('ADMIN_TOKEN'), sessionSecret: secret('SESSION_SECRET'),
  secureCookie: process.env.COOKIE_SECURE !== 'false', since, reportUrl,
});
const worker = new CoachWorker(store,
  b => analyze(b, apiKey, process.env.COACH_MODEL || 'claude-sonnet-4-6'),
  notifications ? a => sendNotification(webhook, a, publicUrl?.origin) : undefined,
  notifications ? id => sendText(webhook, `🚨 Training Companion 분석 실패 · 활동 ${id}\n자동 재시도는 최대 5회입니다. 관리 상태에서 확인하세요.`) : undefined);
if (enabled) {
  void worker.tick().catch(() => console.error('Coach worker failed'));
  setInterval(() => void worker.tick().catch(() => console.error('Coach worker failed')), 15000);
}
app.listen(port, () => console.log(JSON.stringify({ service: 'trisplit-coach', port, enabled, notifications })));
