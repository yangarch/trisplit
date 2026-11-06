import { CoachStore, Analysis, SwimRecord } from '../store';
import { WorkoutBundle } from '../types/workout';
import { diaryUrl, fetchDiaryPost } from './swimNotes';

type SwimReviewAnalyzer = (record: SwimRecord, note: string, bundle?: WorkoutBundle) => Promise<string>;

export type DeliveryResult = 'sent' | 'failed' | 'unknown';
export async function sendText(url: string, text: string): Promise<DeliveryResult> {
  try {
    const response = await fetch(url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: text.slice(0, 1900), content: text.slice(0, 1900) }), signal: AbortSignal.timeout(10000), redirect: 'error',
    });
    if (response.ok) return 'sent';
    // 5xx may follow a successful remote write; automatic retry could duplicate.
    return response.status >= 500 ? 'unknown' : 'failed';
  } catch { return 'unknown'; }
}

export async function sendNotification(url: string, a: Analysis, publicOrigin = ''): Promise<DeliveryResult> {
  const icon = a.sport === 'swimming' ? '🏊' : a.sport === 'running' ? '🏃' : '🚴';
  const distance = a.distance === null ? '거리 미기록' : a.sport === 'swimming' ? `${Math.round(a.distance)}m` : `${(a.distance / 1000).toFixed(1)}km`;
  const note = a.sport === 'swimming' && publicOrigin ? `\n<${publicOrigin}/swim-notes/${a.activity_id}|후기 연결>` : '';
  const text = `${icon} ${a.activity_name} · ${a.activity_date.slice(0, 10)} · ${distance}\n${a.analysis}`;
  return sendText(url, text.slice(0, 1900 - note.length) + note);
}

export class CoachWorker {
  private running = false;
  constructor(private readonly store: CoachStore,
    private readonly analyze: (bundle: WorkoutBundle) => Promise<string>,
    private readonly notify?: (analysis: Analysis) => Promise<DeliveryResult>,
    private readonly failureAlert?: (id: number) => Promise<DeliveryResult>,
    private readonly analyzeReview?: SwimReviewAnalyzer) {}

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const job = this.store.nextJob();
      if (job) {
        try {
          const text = await this.analyze(JSON.parse(job.payload));
          if (!text.trim()) throw new Error('Empty analysis');
          this.store.save(job, text, !!this.notify);
          console.log(JSON.stringify({ event: 'coach_analyzed', activity_id: job.activity_id }));
        } catch {
          // SDK/HTTP exceptions can carry keys and request bodies. Never log them.
          this.store.fail(job);
          console.error(JSON.stringify({ event: 'coach_analysis_failed', activity_id: job.activity_id }));
        }
      }
      if (this.notify) {
        const next = this.store.nextDelivery();
        if (next) {
          let result: DeliveryResult;
          try { result = await this.notify(next); } catch { result = 'unknown'; }
          this.store.delivery(next.activity_id, result);
          console.log(JSON.stringify({ event: 'coach_delivery', activity_id: next.activity_id, state: result }));
        }
      }
      if (this.failureAlert) {
        const id = this.store.nextAlert();
        if (id !== undefined) {
          let result: DeliveryResult;
          try { result = await this.failureAlert(id); } catch { result = 'unknown'; }
          this.store.alert(id, result);
        }
      }
      if (this.analyzeReview) await this.reviewSwim(this.analyzeReview);
    } finally { this.running = false; }
  }

  private async reviewSwim(analyze: SwimReviewAnalyzer): Promise<void> {
    const job = this.store.nextSwimReview();
    if (!job) return;
    const id = job.activity_id;
    try {
      const note = this.store.swimNote(id);
      const swim = this.store.swim(id);
      if (!note || !swim) { this.store.finishSwimReview(job, 'failed'); return; }
      const body = note.body || (note.url && diaryUrl(note.url) ? (await fetchDiaryPost(note.url)).body : '');
      if (this.store.swimReview(id)?.revision !== job.revision) return;
      if (!body.trim()) { this.store.finishSwimReview(job, 'needs_body'); return; }
      const text = await analyze(swim, body, this.store.swimReviewBundle(id));
      if (!text.trim()) throw new Error('Empty swim review');
      this.store.finishSwimReview(job, 'complete', text);
    } catch {
      this.store.finishSwimReview(job, job.attempts >= 3 ? 'failed' : 'pending');
    }
  }
}
