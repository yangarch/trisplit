import { CoachStore, Analysis } from '../store';
import { WorkoutBundle } from '../types/workout';

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

export async function sendNotification(url: string, a: Analysis): Promise<DeliveryResult> {
  const icon = a.sport === 'swimming' ? '🏊' : a.sport === 'running' ? '🏃' : '🚴';
  const distance = a.distance === null ? '거리 미기록' : a.sport === 'swimming' ? `${Math.round(a.distance)}m` : `${(a.distance / 1000).toFixed(1)}km`;
  return sendText(url, `${icon} ${a.activity_name} · ${a.activity_date.slice(0, 10)} · ${distance}\n${a.analysis}`);
}

export class CoachWorker {
  private running = false;
  constructor(private readonly store: CoachStore,
    private readonly analyze: (bundle: WorkoutBundle) => Promise<string>,
    private readonly notify?: (analysis: Analysis) => Promise<DeliveryResult>,
    private readonly failureAlert?: (id: number) => Promise<DeliveryResult>) {}

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
    } finally { this.running = false; }
  }
}
