/** Offline contract + queue preview. Never imports the worker or model client. */
import fs from 'fs';
import path from 'path';
import { CoachStore } from './store';
import { validateBundle, WorkoutBundle } from './types/workout';

function main(): void {
  const dir = process.argv[2];
  if (!dir) throw new Error('Usage: preview PRIVATE_OUTBOX');
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'));
  if (manifest.schema_version !== 1 || !Array.isArray(manifest.files)) throw new Error('Invalid manifest');
  const store = new CoachStore(':memory:', 1);
  const sports: Record<string, number> = {};
  try {
    for (const name of manifest.files) {
      if (!/^\d+\.json$/.test(name)) throw new Error('Invalid filename');
      const b: WorkoutBundle = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'));
      validateBundle(b);
      if (String(b.activity.id) + '.json' !== name || b.provenance.pipeline_run !== manifest.pipeline_run) throw new Error('Mismatched manifest');
      if (store.enqueue(b) !== 'queued' || store.enqueue(b) !== 'already_queued') throw new Error('Queue deduplication failed');
      sports[b.activity.sport] = (sports[b.activity.sport] || 0) + 1;
    }
    console.log(JSON.stringify({ valid: manifest.files.length, sports, status: store.status(), model_calls: 0, notifications: 0 }));
  } finally { store.close(); }
}
try { main(); } catch { console.error('Preview failed: invalid private input; no model calls or notifications'); process.exitCode = 1; }
