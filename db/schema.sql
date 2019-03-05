create table if not exists player (
  timestamp          int,
  api_timestamp      real,
  track_id           text,
  progress_ms        int,
  duration_ms        int,
  is_playing         int,
  repeat             int,
  shuffle_state      int,
  device_id          text,
  device_active      int,
  volume_percent     int,
  is_private_session int,
  device_type        text
);

