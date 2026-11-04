import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { CoachStore } from './store';

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const value = (key: string) => args[args.indexOf(key) + 1];
  if (!args.includes('--source')) throw new Error('Usage: migrate --source legacy.db [--target coach.db --owner ID --apply]');
  const source = path.resolve(value('--source'));
  const db = new Database(source, { readonly: true, fileMustExist: true });
  try {
    const integrity = db.pragma('integrity_check', { simple: true });
    if (integrity !== 'ok') throw new Error('Source integrity check failed');
    const counts = db.prepare('SELECT athlete_id,count(*) AS count FROM swim_analyses GROUP BY athlete_id').all();
    const processed = db.prepare('SELECT count(*) AS count FROM processed_activities').get();
    console.log(JSON.stringify({ source_integrity: integrity, analyses_by_owner: counts, processed, apply: args.includes('--apply') }));
    if (!args.includes('--apply')) return;
    if (!args.includes('--target') || !args.includes('--owner')) throw new Error('--target and --owner required');
    const target = path.resolve(value('--target'));
    const owner = Number(value('--owner'));
    if (!Number.isSafeInteger(owner) || owner <= 0) throw new Error('Invalid owner');
    if (fs.existsSync(target) || source === target) throw new Error('Target must not exist');
    fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
    // Online SQLite backup includes committed WAL pages. Never copy a live .db alone.
    await db.backup(target);
    fs.chmodSync(target, 0o600);
    const store = new CoachStore(target, owner);
    try {
      const missing = store.db.prepare(`SELECT count(*) AS n FROM swim_analyses l LEFT JOIN activity_analyses a USING(activity_id)
        WHERE a.activity_id IS NULL OR a.analysis!=l.analysis OR a.athlete_id!=l.athlete_id`).get() as { n: number };
      if (missing.n) throw new Error('Migration verification failed');
      console.log(JSON.stringify({ migrated: true, verification: 'all legacy analyses and owners preserved', status: store.status() }));
    } finally { store.close(); }
  } finally { db.close(); }
}
void main().catch(() => { console.error('Migration failed; source unchanged. Inspect arguments and private target copy.'); process.exitCode = 1; });
