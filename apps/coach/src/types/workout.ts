import { StravaActivity, StravaLap, StravaStreams } from './strava';

export type Sport = 'cycling' | 'running' | 'swimming';
export const HISTORY_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

// The producer supplies local wall time without an offset. Parse on a UTC
// clock so the deployment host's timezone cannot shift a weekly boundary.
export function wallTimeMs(value: string): number {
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(value) ? Date.parse(`${value}Z`) : NaN;
}

export interface WorkoutActivity extends Omit<StravaActivity, 'average_heartrate' | 'max_heartrate' | 'average_speed' | 'max_speed'> {
  sport: Sport;
  is_indoor: boolean;
  device_name: string | null;
  total_elevation_gain: number | null;
  average_heartrate: number | null;
  max_heartrate: number | null;
  average_speed: number | null;
  max_speed: number | null;
}
export interface WorkoutBundle {
  schema_version: 1;
  activity: WorkoutActivity;
  metrics: {
    np_computed: number | null;
    intensity_factor: number | null;
    tss: number | null;
    ftp_watts: number | null;
    ftp_date: string | null;
    swim_rest_included: boolean;
    pool_length_m?: number | null;
  };
  laps: StravaLap[];
  streams: StravaStreams;
  recent: Array<{
    id: number; sport: Sport; date: string; distance: number; moving_time: number;
    average_speed: number | null; average_heartrate: number | null;
    is_indoor: boolean; swim_rest_included: boolean;
    type?: string; np_computed?: number | null; pool_length_m?: number | null;
  }>;
  stream_summary: Record<string, number | null>;
  provenance: {
    pipeline_run: string;
    exported_at: string;
    quality_checked: true;
    source: 'trisplit-gold';
    limitations: string[];
  };
}

const SPORTS = new Set(['cycling', 'running', 'swimming']);
const optionalPositive = (v: unknown) => v == null || (typeof v === 'number' && Number.isFinite(v) && v > 0);
const TYPES: Record<Sport, Set<string>> = {
  cycling: new Set(['Ride', 'VirtualRide', 'EBikeRide', 'GravelRide', 'MountainBikeRide']),
  running: new Set(['Run', 'TrailRun', 'VirtualRun']),
  swimming: new Set(['Swim', 'OpenWaterSwim']),
};

/** Only the allowlisted producer contract enters the queue or model prompt. */
export function validateBundle(value: unknown): asserts value is WorkoutBundle {
  if (!value || typeof value !== 'object') throw new Error('Invalid bundle');
  const b = value as WorkoutBundle;
  const a = b.activity;
  if (b.schema_version !== 1 || !a || !SPORTS.has(a.sport) || !TYPES[a.sport]?.has(a.type)
      || !Number.isSafeInteger(a.id) || a.id <= 0 || typeof a.name !== 'string' || a.name.length > 500
      || !a.start_date_local || !Number.isFinite(wallTimeMs(a.start_date_local)) || typeof a.is_indoor !== 'boolean'
      || ![a.distance, a.moving_time, a.elapsed_time].every(n => Number.isFinite(n) && n >= 0)
      || [a.average_speed,a.max_speed,a.average_heartrate,a.max_heartrate,a.total_elevation_gain]
        .some(n => n !== null && (!Number.isFinite(n) || n < 0))
      || (a.device_name !== null && (typeof a.device_name !== 'string' || a.device_name.length > 500))
      || !Array.isArray(b.laps) || !Array.isArray(b.recent) || b.recent.length > 200
      || !b.streams || !b.metrics || !b.stream_summary
      || b.provenance?.quality_checked !== true || b.provenance.source !== 'trisplit-gold'
      || typeof b.provenance.pipeline_run !== 'string' || !b.provenance.pipeline_run
      || !Array.isArray(b.provenance.limitations)) throw new Error('Invalid workout contract');
  if (b.laps.length > 2000 || b.laps.some(l => !l || ![l.distance, l.moving_time, l.elapsed_time, l.average_speed]
    .every(n => Number.isFinite(n) && n >= 0))) throw new Error('Invalid laps');
  for (const [key, values] of Object.entries(b.streams)) {
    if (!['time', 'distance', 'velocity_smooth', 'heartrate'].includes(key) || !Array.isArray(values)
        || values.length > 100000 || values.some(v => v !== null && !Number.isFinite(v))) throw new Error('Invalid streams');
  }
  if (a.sport !== 'swimming' && Object.keys(b.streams).length) throw new Error('Raw streams are swimming-only');
  const cutoff = wallTimeMs(a.start_date_local);
  if (new Set(b.recent.map(r => r?.id)).size !== b.recent.length
    || b.recent.some(r => !r || !SPORTS.has(r.sport) || typeof r.date !== 'string'
      || !Number.isSafeInteger(r.id) || r.id <= 0 || !Number.isFinite(wallTimeMs(r.date))
      || wallTimeMs(r.date) < cutoff - 4 * HISTORY_WEEK_MS
      || ![r.distance, r.moving_time].every(n => Number.isFinite(n) && n >= 0)
      || [r.average_speed,r.average_heartrate].some(n => n !== null && (!Number.isFinite(n) || n < 0))
      || typeof r.is_indoor !== 'boolean' || typeof r.swim_rest_included !== 'boolean'
      || (r.type !== undefined && !TYPES[r.sport].has(r.type))
      || !optionalPositive(r.pool_length_m) || (r.pool_length_m != null && r.sport !== 'swimming')
      || (r.np_computed != null && (r.sport !== 'cycling' || !Number.isFinite(r.np_computed) || r.np_computed < 0))
      || r.date >= a.start_date_local! || r.id === a.id)) throw new Error('Invalid historical context');
  // Unknown fields are forbidden, including coordinates, tokens and descriptions.
  const allowed: Record<string, string[]> = {
    root: ['schema_version', 'activity', 'metrics', 'laps', 'streams', 'recent', 'stream_summary', 'provenance'],
    activity: ['id', 'name', 'sport', 'type', 'sport_type', 'distance', 'moving_time', 'elapsed_time',
      'average_speed', 'max_speed', 'average_heartrate', 'max_heartrate', 'start_date_local',
      'is_indoor', 'device_name', 'total_elevation_gain'],
    metrics: ['np_computed', 'intensity_factor', 'tss', 'ftp_watts', 'ftp_date', 'swim_rest_included', 'pool_length_m'],
    provenance: ['pipeline_run', 'exported_at', 'quality_checked', 'source', 'limitations'],
  };
  for (const [part, fields] of Object.entries(allowed)) {
    const obj = part === 'root' ? b : (b as unknown as Record<string, unknown>)[part];
    if (!obj || Object.keys(obj).some(k => !fields.includes(k))) throw new Error(`Unknown ${part} field`);
  }
  for (const key of ['np_computed', 'intensity_factor', 'tss', 'ftp_watts'] as const)
    if (b.metrics[key] !== null && (!Number.isFinite(b.metrics[key]) || b.metrics[key]! < 0)) throw new Error('Invalid metrics');
  if (typeof b.metrics.swim_rest_included !== 'boolean'
      || !optionalPositive(b.metrics.pool_length_m) || (b.metrics.pool_length_m != null && a.sport !== 'swimming')
      || (b.metrics.ftp_date !== null && (typeof b.metrics.ftp_date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(b.metrics.ftp_date)))
      || b.provenance.limitations.length > 30 || b.provenance.limitations.some(s => typeof s !== 'string' || s.length > 1000)) throw new Error('Invalid metadata');
  const lapKeys = ['id', 'name', 'distance', 'moving_time', 'elapsed_time', 'average_speed', 'max_speed', 'average_heartrate', 'lap_index'];
  const recentKeys = ['id', 'sport', 'date', 'distance', 'moving_time', 'average_speed', 'average_heartrate', 'is_indoor', 'swim_rest_included', 'type', 'np_computed', 'pool_length_m'];
  if (b.laps.some(l => Object.keys(l).some(k => !lapKeys.includes(k)))
      || b.recent.some(r => Object.keys(r).some(k => !recentKeys.includes(k)))
      || Object.entries(b.stream_summary).some(([k, v]) => !/^(recorded_seconds|elapsed_seconds|time_coverage|(first|second)_(watts|heartrate|speed_ms)(_seconds)?)$/.test(k)
        || (v !== null && !Number.isFinite(v)))) throw new Error('Invalid evidence field');
}
