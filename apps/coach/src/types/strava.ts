export interface StravaTokenData {
  access_token: string;
  refresh_token: string;
  expires_at: number;
  athlete_id: number;
  athlete_name?: string;
}

export interface StravaWebhookPayload {
  aspect_type: 'create' | 'update' | 'delete';
  event_time: number;
  object_id: number;
  object_type: 'activity' | 'athlete';
  owner_id: number;
  subscription_id: number;
  updates?: Record<string, unknown>;
}

export interface StravaActivity {
  id: number;
  name: string;
  distance: number;
  moving_time: number;
  elapsed_time: number;
  sport_type: string;
  type: string;
  average_speed: number;
  max_speed: number;
  average_heartrate?: number;
  max_heartrate?: number;
  description?: string;
  start_date_local?: string;
  laps?: StravaLap[];
  splits_metric?: StravaSplit[];
}

export interface StravaLap {
  id: number;
  name: string;
  distance: number;
  elapsed_time: number;
  moving_time: number;
  average_speed: number;
  max_speed: number;
  average_heartrate?: number;
  lap_index: number;
}

export interface StravaSplit {
  distance: number;
  elapsed_time: number;
  moving_time: number;
  average_speed: number;
  average_heartrate?: number;
  split: number;
}

export interface StravaStreams {
  time?: number[];
  distance?: number[];
  velocity_smooth?: number[];
  heartrate?: number[];
}

export interface StravaTokenResponse {
  access_token: string;
  refresh_token: string;
  expires_at: number;
  token_type: string;
  athlete?: {
    id: number;
    firstname: string;
    lastname: string;
  };
}
