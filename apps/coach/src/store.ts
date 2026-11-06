import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { WorkoutBundle } from './types/workout';

export interface Job {
  activity_id: number; payload: string; state: string; attempts: number;
}
export interface Analysis {
  activity_id: number; athlete_id: number; sport: string; activity_name: string;
  activity_date: string; distance: number | null; analysis: string; delivery_state: string | null;
}
export interface SwimRecord {
  activity_id: number; start_local: string; distance: number | null; moving_time: number | null;
}
export interface SwimNote { url: string | null; body: string | null }

/** Open a COPY of the legacy DB. Existing tables are retained for rollback. */
export class CoachStore {
  readonly db: Database.Database;
  constructor(filename: string, readonly ownerId: number) {
    if (filename !== ':memory:') fs.mkdirSync(path.dirname(path.resolve(filename)), { recursive: true });
    this.db = new Database(filename);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('busy_timeout = 5000');
    this.db.transaction(() => {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS activity_analyses (
          activity_id INTEGER PRIMARY KEY, athlete_id INTEGER NOT NULL, sport TEXT NOT NULL,
          activity_name TEXT, activity_date TEXT, distance REAL, analysis TEXT NOT NULL,
          processed_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS coach_jobs (
          activity_id INTEGER PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0,
          error TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS coach_deliveries (
          activity_id INTEGER PRIMARY KEY, state TEXT NOT NULL, error TEXT,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS coach_alerts (
          activity_id INTEGER PRIMARY KEY, state TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS processed_activities (
          id INTEGER PRIMARY KEY AUTOINCREMENT, activity_id INTEGER UNIQUE NOT NULL,
          athlete_id INTEGER NOT NULL, processed_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS swim_catalog (
          athlete_id INTEGER NOT NULL, activity_id INTEGER NOT NULL, start_local TEXT NOT NULL,
          distance REAL, moving_time REAL, PRIMARY KEY (athlete_id, activity_id)
        );
        CREATE INDEX IF NOT EXISTS swim_catalog_date ON swim_catalog(athlete_id, start_local);
        CREATE TABLE IF NOT EXISTS swim_notes (
          athlete_id INTEGER NOT NULL, activity_id INTEGER NOT NULL, url TEXT, body TEXT,
          PRIMARY KEY (athlete_id, activity_id), UNIQUE (athlete_id, url)
        );
      `);
      if (this.hasTable('swim_analyses')) {
        this.db.exec(`INSERT OR IGNORE INTO activity_analyses
          (activity_id, athlete_id, sport, activity_name, activity_date, distance, analysis, processed_at)
          SELECT activity_id, athlete_id, 'swimming', activity_name, activity_date, distance, analysis, processed_at
          FROM swim_analyses`);
        // Legacy messages are never sent again, including analyses lacking a processed row.
        this.db.exec(`INSERT OR IGNORE INTO processed_activities (activity_id, athlete_id)
          SELECT activity_id, athlete_id FROM swim_analyses`);
      }
      this.db.exec(`UPDATE coach_jobs SET state='pending' WHERE state='analyzing';
        UPDATE coach_alerts SET state='unknown' WHERE state='sending';
        UPDATE coach_deliveries SET state='unknown', error='Interrupted during delivery; inspect Slack before retrying'
        WHERE state='sending'`);
    })();
  }

  private hasTable(name: string): boolean {
    return !!this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
  }
  enqueue(bundle: WorkoutBundle): 'queued' | 'already_processed' | 'already_queued' {
    return this.db.transaction(() => {
      const id = bundle.activity.id;
      if (this.db.prepare('SELECT 1 FROM processed_activities WHERE activity_id=?').get(id)) return 'already_processed' as const;
      const inserted = this.db.prepare('INSERT OR IGNORE INTO coach_jobs (activity_id,payload) VALUES (?,?)')
        .run(id, JSON.stringify(bundle));
      return inserted.changes ? 'queued' as const : 'already_queued' as const;
    })();
  }
  nextJob(now = Date.now()): Job | undefined {
    return this.db.transaction(() => {
      const job = this.db.prepare(`SELECT * FROM coach_jobs WHERE state='pending' AND next_attempt<=?
        ORDER BY json_extract(payload,'$.activity.start_date_local'), activity_id LIMIT 1`).get(now) as Job | undefined;
      if (job) this.db.prepare("UPDATE coach_jobs SET state='analyzing', attempts=attempts+1 WHERE activity_id=?").run(job.activity_id);
      return job;
    })();
  }
  save(job: Job, analysis: string, notify: boolean): void {
    const bundle: WorkoutBundle = JSON.parse(job.payload);
    const a = bundle.activity;
    this.db.transaction(() => {
      this.db.prepare(`INSERT INTO activity_analyses
        (activity_id,athlete_id,sport,activity_name,activity_date,distance,analysis) VALUES (?,?,?,?,?,?,?)`)
        .run(a.id, this.ownerId, a.sport, a.name, a.start_date_local, a.distance, analysis);
      this.db.prepare('INSERT OR IGNORE INTO processed_activities (activity_id,athlete_id) VALUES (?,?)').run(a.id, this.ownerId);
      this.db.prepare("UPDATE coach_jobs SET state='complete',error=NULL WHERE activity_id=?").run(a.id);
      this.db.prepare("DELETE FROM coach_alerts WHERE activity_id=? AND state='pending'").run(a.id);
      this.db.prepare('INSERT INTO coach_deliveries (activity_id,state) VALUES (?,?)').run(a.id, notify ? 'pending' : 'suppressed');
    })();
  }
  fail(job: Job): void {
    const attempts = job.attempts + 1;
    this.db.prepare('UPDATE coach_jobs SET state=?, error=?, next_attempt=? WHERE activity_id=?')
      .run(attempts >= 5 ? 'failed' : 'pending', 'Model request failed; see service status',
        Date.now() + Math.min(60, 2 ** attempts) * 60000, job.activity_id);
    this.db.prepare('INSERT OR IGNORE INTO coach_alerts (activity_id) VALUES (?)').run(job.activity_id);
  }
  nextDelivery(): Analysis | undefined {
    return this.db.transaction(() => {
      const a = this.db.prepare(`SELECT a.* FROM activity_analyses a JOIN coach_deliveries d USING(activity_id)
        WHERE d.state='pending' ORDER BY a.activity_date LIMIT 1`).get() as Analysis | undefined;
      if (a) this.delivery(a.activity_id, 'sending');
      return a;
    })();
  }
  delivery(id: number, state: string, error: string | null = null): void {
    this.db.prepare('UPDATE coach_deliveries SET state=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE activity_id=?').run(state, error, id);
  }
  nextAlert(): number | undefined {
    return this.db.transaction(() => {
      const row = this.db.prepare("SELECT activity_id FROM coach_alerts WHERE state='pending' LIMIT 1").get() as { activity_id: number } | undefined;
      if (row) this.alert(row.activity_id, 'sending');
      return row?.activity_id;
    })();
  }
  alert(id: number, state: string): void {
    this.db.prepare('UPDATE coach_alerts SET state=? WHERE activity_id=?').run(state, id);
  }
  retry(id: number, kind: 'analysis' | 'delivery'): boolean {
    const result = kind === 'analysis'
      ? this.db.prepare("UPDATE coach_jobs SET state='pending',attempts=0,next_attempt=0 WHERE activity_id=? AND state='failed'").run(id)
      : this.db.prepare("UPDATE coach_deliveries SET state='pending',error=NULL WHERE activity_id=? AND state IN ('unknown','failed')").run(id);
    return result.changes > 0;
  }
  analyses(): Analysis[] {
    return this.db.prepare(`SELECT a.*,d.state AS delivery_state FROM activity_analyses a
      LEFT JOIN coach_deliveries d USING(activity_id) WHERE athlete_id=? ORDER BY activity_date DESC, activity_id DESC LIMIT 200`)
      .all(this.ownerId) as Analysis[];
  }
  saveSwimCatalog(records: SwimRecord[]): void {
    const upsert = this.db.prepare(`INSERT INTO swim_catalog VALUES (?,?,?,?,?)
      ON CONFLICT(athlete_id,activity_id) DO UPDATE SET
      start_local=excluded.start_local,distance=excluded.distance,moving_time=excluded.moving_time`);
    this.db.transaction(() => {
      for (const r of records) upsert.run(this.ownerId, r.activity_id, r.start_local, r.distance, r.moving_time);
    })();
  }
  swim(id: number): SwimRecord | undefined {
    return this.db.prepare(`SELECT activity_id,start_local,distance,moving_time FROM swim_catalog
      WHERE athlete_id=@owner AND activity_id=@id
      UNION ALL SELECT activity_id,activity_date,distance,NULL FROM activity_analyses
      WHERE athlete_id=@owner AND activity_id=@id AND sport='swimming' LIMIT 1`)
      .get({owner:this.ownerId,id}) as SwimRecord | undefined;
  }
  swimsOn(date: string): SwimRecord[] {
    return this.db.prepare(`SELECT activity_id,start_local,distance,moving_time FROM swim_catalog
      WHERE athlete_id=@owner AND start_local>=@date AND start_local<@end
      UNION ALL SELECT a.activity_id,a.activity_date,a.distance,NULL FROM activity_analyses a
      WHERE a.athlete_id=@owner AND a.sport='swimming' AND substr(a.activity_date,1,10)=@date
        AND NOT EXISTS (SELECT 1 FROM swim_catalog c WHERE c.athlete_id=@owner AND c.activity_id=a.activity_id)
      ORDER BY start_local,activity_id`).all({owner:this.ownerId,date,end:`${date}U`}) as SwimRecord[];
  }
  swimNote(id: number): SwimNote | undefined {
    return this.db.prepare('SELECT url,body FROM swim_notes WHERE athlete_id=? AND activity_id=?')
      .get(this.ownerId,id) as SwimNote | undefined;
  }
  swimIdForNoteUrl(url: string): number | undefined {
    const row = this.db.prepare('SELECT activity_id FROM swim_notes WHERE athlete_id=? AND url=?')
      .get(this.ownerId,url) as { activity_id: number } | undefined;
    return row?.activity_id;
  }
  recentLinkedSwims(): SwimRecord[] {
    const rows = this.db.prepare('SELECT activity_id FROM swim_notes WHERE athlete_id=? ORDER BY rowid DESC LIMIT 20')
      .all(this.ownerId) as { activity_id: number }[];
    return rows.map(r => this.swim(r.activity_id)).filter((r): r is SwimRecord => r !== undefined);
  }
  saveSwimNote(id: number, note: SwimNote, replace: boolean): boolean {
    return this.db.transaction(() => {
      if (!this.swim(id)) return false;
      const duplicate = note.url && this.db.prepare('SELECT 1 FROM swim_notes WHERE athlete_id=? AND url=? AND activity_id<>?')
        .get(this.ownerId,note.url,id);
      if (duplicate || (!replace && this.swimNote(id))) return false;
      this.db.prepare(`INSERT INTO swim_notes VALUES (?,?,?,?) ON CONFLICT(athlete_id,activity_id)
        DO UPDATE SET url=excluded.url,body=excluded.body`).run(this.ownerId,id,note.url,note.body);
      return true;
    })();
  }
  removeSwimNote(id: number): void {
    this.db.prepare('DELETE FROM swim_notes WHERE athlete_id=? AND activity_id=?').run(this.ownerId,id);
  }
  status(): unknown {
    return {
      jobs: this.db.prepare('SELECT state, count(*) AS count FROM coach_jobs GROUP BY state').all(),
      deliveries: this.db.prepare('SELECT state,count(*) AS count FROM coach_deliveries GROUP BY state').all(),
      alerts: this.db.prepare('SELECT state,count(*) AS count FROM coach_alerts GROUP BY state').all(),
      analyses: this.db.prepare('SELECT sport,count(*) AS count FROM activity_analyses GROUP BY sport').all(),
    };
  }
  close(): void { this.db.close(); }
}
