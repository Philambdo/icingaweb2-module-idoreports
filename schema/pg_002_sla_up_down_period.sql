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

DROP FUNCTION IF EXISTS icinga_sla_updown_period();
CREATE OR REPLACE FUNCTION icinga_sla_updown_period (
	id BIGINT,
	start_ts TIMESTAMP, 
	end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
--RETURNS DECIMAL(7, 4)
--RETURNS TABLE ( duration INTEGER , current_state INTEGER, next_state INTEGER, addd INTEGER, dt_depth INTEGER, type TEXT, start_time TIMESTAMP, end_time TIMESTAMP)
RETURNS TABLE( 	state_time TIMESTAMP,
			   	unix_state_time INTEGER,
				unsigned_last_time INTEGER,
                duration INTEGER,
				add_duration INTEGER,
            	current_type TEXT, 
            	next_type TEXT,
				current_state INTEGER,
            	is_problem INTEGER,
				in_downtime INTEGER,
            	out_of_slatime INTEGER,
				next_dt_depth INTEGER,
				next_tp_depth INTEGER,
            	next_state INTEGER,
            	start_time INTEGER,
            	end_time INTEGER)
--RETURNS TABLE ( AVAILABILITY DECIMAL(7,4))
AS $_$
DECLARE 
v_availability DECIMAL(7, 4);
dummy_id BIGINT;
former_id BIGINT						= id;
--tp_lastday 					= "-1 day";
--tp_lastend TIMESTAMP					= 0;
former_sla_timeperiod_object_id BIGINT 	= sla_timeperiod_object_id;
former_start TIMESTAMP 					= start_ts;
former_end TIMESTAMP 				 	= end_ts;
sla_timeperiod_object_id BIGINT			= sla_timeperiod_object_id;
id BIGINT						 		= id;
v_start_ts TIMESTAMP       		 		= start_ts;
v_end_ts TIMESTAMP        		 		= end_ts;
v_last_state INTEGER        		 		:= NULL;
v_write_last_state INTEGER := NULL;
--v_last_uts TIMESTAMP         		 		:= NULL;
v_last_uts INTEGER         		 		:= NULL;
v_cnt_dt INTEGER            		 		:= NULL;
v_cnt_tp INTEGER           		 		:= NULL;
v_add_duration INTEGER     		 		:= NULL;
v_current_state INTEGER  := NULL;
v_addd INTEGER :=NULL;
v_dt_depth INTEGER := NULL;
v_start_uts INTEGER;
v_end_uts INTEGER;
v_type_id INTEGER							:= NULL;
evdu RECORD;

v_is_problem INTEGER := NULL;
v_tp_depth INTEGER := NULL;
v_in_downtime INTEGER := NULL;
v_out_of_slatime INTEGER := NULL;
v_next_dt_depth INTEGER := NULL;
v_next_tp_depth INTEGER := NULL;
v_uts INTEGER := NULL;
v_unsigned_last_time_uts INTEGER := NULL;
v_duration INTEGER := NULL;
v_current_type TEXT := NULL;
v_next_type TEXT := NULL;
v_next_state INTEGER := NULL;

BEGIN
    SELECT objecttype_id INTO v_type_id FROM icinga_objects WHERE object_id = id;
	IF v_type_id NOT IN (1, 2) THEN
	    RETURN NEXT;
	END IF;

CREATE TEMP TABLE events_duration AS (
	SELECT * FROM (
		SELECT r_state_time AS state_time,
				r_type as type,
				r_state as state,
				r_last_state as last_state 
		FROM 
			icinga_normalized_history ( id, v_start_ts, v_end_ts, sla_timeperiod_object_id )
	) events
	  ORDER BY events.state_time ASC,
    	CASE events.type 
	      WHEN 'former_state' THEN 0
	      WHEN 'soft_state' THEN 1
	      WHEN 'hard_state' THEN 2
	      WHEN 'current_state' THEN 3
	      WHEN 'future_state' THEN 4
	      WHEN 'sla_end' THEN 5
	      WHEN 'sla_start' THEN 6
	      WHEN 'dt_start' THEN 7
	      WHEN 'dt_end' THEN 8
	      ELSE 9
	    END ASC
);


--CREATE TEMP TABLE t_duration ( duration INTEGER , current_state INTEGER, next_state INTEGER, addd INTEGER, dt_depth INTEGER, type TEXT, start_time TIMESTAMP, end_time TIMESTAMP);
CREATE TEMP TABLE t_duration (
		state_time TIMESTAMP,
                unix_state_time INTEGER,
                unsigned_last_time INTEGER,
                duration INTEGER,
                add_duration INTEGER,
                current_type TEXT,
                next_type TEXT,
                current_state INTEGER,
                is_problem INTEGER,
                in_downtime INTEGER,
                out_of_slatime INTEGER,
                next_dt_depth INTEGER,
                next_tp_depth INTEGER,
                next_state INTEGER,
                start_time INTEGER,
                end_time INTEGER);


FOR evdu IN SELECT * FROM events_duration LOOP

	v_uts:= UNIX_TIMESTAMP(evdu.state_time);

    v_unsigned_last_time_uts := CAST(COALESCE(v_last_uts, UNIX_TIMESTAMP(start_ts)) AS DECIMAL);

	v_duration := CAST(v_uts - CAST(COALESCE(v_last_uts, UNIX_TIMESTAMP(start_ts)) AS DECIMAL) 
                                         + CAST(COALESCE(v_add_duration,0) AS DECIMAL) 
						AS DECIMAL);

	v_current_type := v_next_type;
	v_next_type := evdu.type;
 
  IF v_last_state is NULL THEN
    v_is_problem := NULL;
  ELSE 
    IF v_type_id = 1 THEN
      IF v_last_state > 0 THEN
        v_is_problem := 1;
      ELSE
        v_is_problem := 0;
      END IF;
    ELSE 
      IF v_last_state > 1 THEN
		v_is_problem := 1;
	  ELSE
		v_is_problem := 0;
      END IF;
    END IF;
  END IF;

  IF evdu.type = 'dt_start'  THEN 
	v_cnt_dt := COALESCE(v_cnt_dt, 0) + 1;
  ELSIF evdu.type = 'dt_end'  THEN 
	v_cnt_dt := COALESCE(v_cnt_dt -1, 0);
  ELSIF evdu.type = 'sla_end'  THEN 
	v_cnt_tp := COALESCE(v_cnt_tp, 0) + 1;
  ELSIF evdu.type = 'sla_start'  THEN 
	v_cnt_tp := COALESCE(v_cnt_tp-1, 0);
  END IF;

  v_dt_depth := COALESCE(v_cnt_dt, 0);
  v_tp_depth := COALESCE(v_cnt_tp, 0);

  IF COALESCE(v_cnt_dt, 0) > 0 THEN
	v_in_downtime := 1;
  ELSE
    v_in_downtime := 0;
  END IF;

  IF COALESCE(v_cnt_tp, 0) > 0 THEN
	v_out_of_slatime := 1;
  ELSE
    v_out_of_slatime := 0;
  END IF;

  IF evdu.type IN ('dt_start') THEN
    v_next_dt_depth := COALESCE(v_cnt_dt, 0) + 1;
  ELSIF evdu.type in ( 'dt_end') THEN
    v_next_dt_depth := GREATEST(v_cnt_dt -1 , 0);
  ELSE
	v_next_dt_depth := COALESCE(v_cnt_dt, 0);
  END IF;

  IF evdu.type IN ('sla_end') THEN
    v_next_tp_depth := COALESCE(v_cnt_tp, 0) + 1;
  ELSIF evdu.type in ( 'sla_start') THEN
    v_next_tp_depth := GREATEST(v_cnt_tp -1 , 0);
  ELSE
	v_next_tp_depth := COALESCE(v_cnt_tp, 0);
  END IF;

  IF v_last_uts IS NULL THEN
    -- ...remember the duration and return 0...
    v_add_duration = ((COALESCE(v_add_duration, 0)
      + UNIX_TIMESTAMP(evdu.state_time)
      - COALESCE(v_last_uts, UNIX_TIMESTAMP(v_start_ts)) + 1
    )::INTEGER - 1);
  ELSE
    -- ...otherwise return a correct duration...
    v_add_duration=(UNIX_TIMESTAMP(evdu.state_time)
      - COALESCE(v_last_uts, UNIX_TIMESTAMP(v_start_ts))
      -- ...and don't forget to add what we remembered 'til now:
      + COALESCE(CASE v_cnt_dt + v_cnt_tp WHEN 0 THEN v_add_duration ELSE NULL END, 0));
  END IF;
  RAISE NOTICE 'v_add_duration: %', v_add_duration;

  IF v_cnt_dt + v_cnt_tp >= 1 THEN
	v_current_state=0;
  ELSE
	v_current_state=COALESCE(v_last_state, evdu.last_state);
  END IF;

  -- We need v_write_last_state and v_last_state as separate variables to remember
  -- the correct last_state also during downtimes - downtimes don't have to update
  -- the correct last hard state
  -- To make things complicated: v_last_state will be current state, v_write_last_state will be next_state
  IF evdu.type in ('hard_state', 'former_state', 'future_state', 'current_state') THEN
	v_last_state := evdu.state;
	v_write_last_state := evdu.state;
	v_next_state := evdu.state;
  ELSIF evdu.type = 'soft_state' THEN
    IF v_last_state is NULL THEN
      	v_last_state := evdu.last_state;
     	v_next_state := evdu.last_state;
	  	v_write_last_state := evdu.last_state;
	END IF;
  ELSIF evdu.type IN ('dt_start', 'sla_end') THEN
	v_write_last_state := v_last_state;
	v_next_state := v_last_state;
  ELSIF evdu.type IN ('dt_end', 'sla_start') THEN
    v_write_last_state := v_last_state;
	v_next_state := v_last_state;
  END IF;

  IF v_add_duration IS NOT NULL AND v_cnt_dt = 0 and v_cnt_tp = 0
  THEN
    v_addd := v_add_duration;
  ELSE
    v_addd := 0;
  END IF;

  RAISE NOTICE 'type: %, v_cnt_dt: %, v_dt_depth: %', evdu.type, v_cnt_dt, v_dt_depth;

  v_start_uts := COALESCE(v_last_uts, UNIX_TIMESTAMP(v_start_ts));

  v_last_uts := UNIX_TIMESTAMP(evdu.state_time);

  IF evdu.type = 'fake_end' THEN
	 v_end_uts := UNIX_TIMESTAMP(evdu.state_time);
  ELSE
     v_end_uts := UNIX_TIMESTAMP(evdu.state_time);
     v_last_uts := UNIX_TIMESTAMP(evdu.state_time);
  END IF;

  INSERT INTO t_duration ( 	state_time,
						   	unix_state_time,
						   	unsigned_last_time,
                           	duration,
						   	add_duration,
                           	current_type, 
                           	next_type,
						   	current_state,
                           	is_problem,
						   	in_downtime,
                           	out_of_slatime,
						   	next_dt_depth,
						   	next_tp_depth,
                           	next_state,
                           	start_time,
                           	end_time) VALUES
  					    ( 	evdu.state_time,
                          	v_uts,
                          	v_unsigned_last_time_uts,
							v_duration,
							v_addd,
							v_current_type,
							v_next_type,
							v_last_state,
							v_is_problem,
							v_in_downtime,
							v_out_of_slatime,
							v_next_dt_depth,
							v_next_tp_depth,
							v_next_state,
							v_start_uts,
							v_end_uts);
			
							
  v_add_duration := NULL;

END LOOP;

RETURN QUERY 
	SELECT * FROM t_duration;
DROP TABLE t_duration;       
DROP TABLE events_duration;
RETURN;                     
--RETURN v_availability;

END;
$_$ LANGUAGE plpgsql SECURITY DEFINER;
