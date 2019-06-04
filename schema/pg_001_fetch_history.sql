-- --------------------------------------------------- --
-- SLA function for Icinga/IDO                         --
--                                                     --
-- Author    : Icinga Developer Team <infoicinga.org> --
-- Copyright : 2012 Icinga Developer Team              --
-- License   : GPL 2.0                                 --
-- --------------------------------------------------- --

--
-- History
-- 
-- 2012-08-31: Added to Icinga
-- 2013-08-20: Simplified and improved
-- 2013-08-23: Refactored, added SLA time period support
--

-- DROP FUNCTION IF EXISTS fetch_history();
CREATE OR REPLACE FUNCTION fetch_history (
	id BIGINT,
	start_ts TIMESTAMP, 
	end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
RETURNS TABLE (
	r_state_time TIMESTAMP,
	r_type TEXT, 
	r_state INTEGER,
	r_last_state INTEGER
)
AS $_$
DECLARE 
--v_sla_timeperiod_object_id BIGINT			= sla_timeperiod_object_id;
--v_id BIGINT						 		= id;
--v_start_ts TIMESTAMP       		 		= start_ts;
--v_end_ts TIMESTAMP        		 		= end_ts;

BEGIN

RAISE NOTICE 'start_ts: %, end_ts: %', start_ts, end_ts;

RETURN QUERY SELECT
     state_time::TIMESTAMP,
     CASE state_type WHEN 1 THEN 'hard_state'::TEXT ELSE 'soft_state'::TEXT END AS type,
     state,
     -- Workaround for a nasty Icinga issue. In case a hard state is reached
     -- before max_check_attempts, the last_hard_state value is wrong. As of
     -- this we are stepping through all single events, even soft ones. Of
     -- course soft states do not have an influence on the availability:
     CASE state_type WHEN 1 THEN last_state ELSE last_hard_state END AS last_state
  FROM icinga_statehistory
  WHERE object_id = id
    AND state_time >= start_ts
    AND state_time <= end_ts
  -- STOP fetching statehistory events

  -- START fetching last state BEFORE the given interval as an event
  UNION SELECT * FROM (
    SELECT
      start_ts AS state_time,
      'former_state'::TEXT AS type,
      CASE state_type WHEN 1 THEN state ELSE last_hard_state END AS state,
      CASE state_type WHEN 1 THEN last_state ELSE last_hard_state END AS last_state
    FROM icinga_statehistory h
    WHERE object_id = id
      AND state_time < start_ts
    ORDER BY h.state_time DESC 
    LIMIT 1
  ) formerstate
  -- END fetching last state BEFORE the given interval as an event

  -- START fetching first state AFTER the given interval as an event
  UNION SELECT * FROM (
    SELECT
      end_ts AS state_time,
      'future_state'::TEXT AS type,
      CASE state_type WHEN 1 THEN last_state ELSE last_hard_state END AS state,
      CASE state_type WHEN 1 THEN state ELSE last_hard_state END AS last_state
    FROM icinga_statehistory h
    WHERE object_id = id
      AND state_time > end_ts
    ORDER BY h.state_time ASC 
	LIMIT 1
  ) futurestate
  -- END fetching first state AFTER the given interval as an event

  -- START ADDING a fake end
  UNION SELECT
    end_ts AS state_time,
    'fake_end'::TEXT AS type,
    NULL AS state,
    NULL AS last_state
  -- FROM DUAL
  -- END ADDING a fake end

  -- START fetching current host state as an event
  -- TODO: This is not 100% correct. state should be fine, last_state sometimes isn't.
  UNION SELECT 
    GREATEST(
      start_ts,
      CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END
    ) AS state_time,
    'current_state'::TEXT AS type,
    CASE state_type WHEN 1 THEN current_state ELSE last_hard_state END AS state,
    last_hard_state AS last_state
  FROM icinga_hoststatus
  WHERE CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END < start_ts
    AND host_object_id = id
    AND CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END <= end_ts
    AND status_update_time > start_ts
  -- END fetching current host state as an event

  -- START fetching current service state as an event
  UNION SELECT 
    GREATEST(
      start_ts,
      CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END
    ) AS state_time,
    'current_state'::TEXT AS type,
    CASE state_type WHEN 1 THEN current_state ELSE last_hard_state END AS state,
    last_hard_state AS last_state
  FROM icinga_servicestatus
  WHERE CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END < start_ts
    AND service_object_id = id
    AND CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END <= end_ts
    AND status_update_time > start_ts
  -- END fetching current service state as an event

  -- START adding add all related downtime start times
  -- TODO: Handling downtimes still being active would be nice.
  --       But pay attention: they could be completely outdated
  UNION SELECT
    GREATEST(actual_start_time, start_ts) AS state_time,
    'dt_start'::TEXT AS type,
    NULL AS state,
    NULL AS last_state
  FROM icinga_downtimehistory
  WHERE object_id = id
    AND actual_start_time < end_ts
    AND actual_end_time > start_ts
  -- STOP adding add all related downtime start times

  -- START adding add all related downtime end times
  UNION SELECT
    LEAST(actual_end_time, end_ts) AS state_time,
    'dt_end'::TEXT AS type,
    NULL AS state,
    NULL AS last_state
  FROM icinga_downtimehistory
  WHERE object_id = id
    AND actual_start_time < end_ts
    AND actual_end_time > start_ts
  -- STOP adding add all related downtime end times

  -- START fetching SLA time period start times ---
  UNION ALL
    SELECT
      start_time AS state_time,
      'sla_start'::TEXT AS type,
      NULL AS state,
      NULL AS last_state
    FROM icinga_outofsla_periods
    WHERE timeperiod_object_id = sla_timeperiod_object_id
      AND start_time >= start_ts AND start_time <= end_ts
  -- STOP fetching SLA time period start times ---

  -- START fetching SLA time period end times ---
  UNION ALL SELECT
      end_time AS state_time,
      'sla_end'::TEXT AS type,
      NULL AS state,
      NULL AS last_state
    FROM icinga_outofsla_periods
    WHERE timeperiod_object_id = sla_timeperiod_object_id
      AND end_time >= start_ts AND end_time <= end_ts
  -- STOP fetching SLA time period end times ---

  ORDER BY state_time ASC;

END;
$_$ LANGUAGE plpgsql SECURITY DEFINER;
