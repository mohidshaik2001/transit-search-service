CREATE OR REPLACE TABLE `cityprogressmobilityl2c.real_time.integrated` AS

WITH
  pings AS (
    SELECT
      vehicle_id,
      TIMESTAMP(timestamp) AS ping_ts,
      lat, lon,
      SPLIT(vehicle_id, "_")[OFFSET(0)] AS route_id
    FROM `cityprogressmobilityl2c.real_time.vehicle_locations`
    WHERE DATE(timestamp) = CURRENT_DATE()
  ),
  schedule AS (
    SELECT
      route_id, stop_id, stop_name,
      TIMESTAMP(scheduled_time) AS sched_ts
    FROM `cityprogressmobilityl2c.legacy_gtfs.gtfs_summary_norm`
    WHERE DATE(scheduled_time) = CURRENT_DATE()
  ),
  next_stop AS (
    SELECT
      p.*,
      s.stop_id,
      s.stop_name,
      s.sched_ts,
      -- find the next scheduled stop time after each ping
      ROW_NUMBER() OVER (PARTITION BY p.vehicle_id, p.ping_ts ORDER BY s.sched_ts ASC) AS rn
    FROM pings AS p
    JOIN schedule AS s
      ON p.route_id = s.route_id
     AND s.sched_ts >= p.ping_ts
  ),
  joined AS (
    SELECT
      vehicle_id,
      ping_ts,
      sched_ts,
      stop_id,
      stop_name,
      lat, lon,
      TIMESTAMP_DIFF(ping_ts, sched_ts, SECOND) AS delay_sec
    FROM next_stop
    WHERE rn = 1  -- only the next upcoming stop
  ),
  incidents_count AS (
    SELECT
      *,
      (SELECT COUNT(1) 
       FROM `cityprogressmobilityl2c.real_time.incidents` i
       WHERE
         i.event_time BETWEEN TIMESTAMP_SUB(p.ping_ts, INTERVAL 10 MINUTE)
                         AND p.ping_ts
        ) AS incident_count
    FROM joined AS p
  )

SELECT
  vehicle_id,
  ping_ts,
  stop_id,
  stop_name,
  sched_ts,
  delay_sec,
  lat, lon,
  incident_count
FROM incidents_count;
