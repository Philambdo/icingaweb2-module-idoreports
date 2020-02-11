CREATE OR REPLACE FUNCTION icinga_sla_updown_period(
  id BIGINT,
	start_ts TIMESTAMP, 
	end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
  RETURNS TABLE
  (
    state_time TIMESTAMP,
    duration INTEGER,
    add_ok_duration INTEGER,
    old_type TEXT,
    current_type TEXT,
    old_state INTEGER,
    current_state INTEGER,
    was_problem INTEGER,
    was_in_downtime INTEGER,
    was_in_slatime INTEGER
  )
  AS $_$
  DECLARE 
  event_row RECORD;
  v_old_type TEXT := NULL;
  v_old_state INTEGER := NULL;
  v_old_state_time TIMESTAMP := NULL;
  v_current_state INTEGER := NULL;
  v_duration INTEGER := NULL;
  v_was_in_slatime INTEGER := NULL;
  v_was_problem INTEGER := NULL;
  v_was_in_downtime INTEGER := NULL;
  v_add_ok_duration INTEGER := NULL;
  v_was_hard_state INTEGER := NULL;
  
  BEGIN
    FOR event_row IN 
        SELECT  r_state_time AS state_time,
                r_type as type,
                r_state as state,
                r_last_state as last_state 
        FROM 
            fetch_history_in_sla ( id, 
                                              start_ts, 
                                              end_ts, 
                                              sla_timeperiod_object_id )
    LOOP

      -- Calculate Duration until last state event so we know what to sum up
      IF v_old_state_time IS NULL THEN
        v_duration := 0;
      ELSE
        IF v_old_state_time < start_ts THEN
          v_duration := EXTRACT(EPOCH FROM ((event_row.state_time - start_ts)::INTERVAL));
        ELSE
          v_duration := EXTRACT(EPOCH FROM ((event_row.state_time - v_old_state_time)::INTERVAL));
        END IF;
      END IF;

      -- CHECK what current type we are to see what to change for future calculations
      IF event_row.type IN ('hard_state', 'former_state', 
                            'future_state', 'current_state') THEN
        -- Hard States means we have to change the state as long as we have a valid state
          v_current_state := COALESCE (event_row.state, v_current_state);
          v_was_hard_state := COALESCE (v_was_hard_state, 1);
      END IF;

      -- Check old Type for Downtime / SLA - damit man weiss wie man rechnen muss 
      -- (wenn downtime/sla start war, dann add to ok, ansonsten bei problem NICHT
      IF v_old_type = 'dt_start' THEN
        v_was_in_downtime := 1;
      ELSIF v_old_type = 'dt_end' THEN
        v_was_in_downtime := 0;
      END IF;
      
      IF v_old_type = 'sla_start' THEN
        v_was_in_slatime := 1;
      ELSIF v_old_type = 'sla_end' THEN
        v_was_in_slatime := 0;
      END IF;

      -- CHECK OLD Values for Problem / Downtime / SLA Status
      IF v_old_state IS NULL THEN
       -- old state unknown => no problem
        v_was_problem := NULL;
      ELSE
        IF v_was_hard_state = 1 THEN
          IF v_old_state > 1 THEN
          -- Hard state and critical/unknown => problem
          -- v_was_problem wird nur hier getoucht - bei hard state changes
          -- AB gesetzt wird es immer weitervererbt
            v_was_problem := 1;
          ELSE
            v_was_problem := 0;
          END IF;
        END IF;
      END IF;
      -- END OF CHECK OLD Values
      IF v_was_problem IS NULL OR v_was_problem = 0 THEN
      -- Wenn Kein Problem dann wird die Zeit als für die 
        v_add_ok_duration := v_duration;
      ELSE
      -- Wir haben ein Problem mal schauen ob eine Downtime aktiv 
      -- war oder wir ausserhalb SLA Zeit sind
        IF v_was_in_downtime = 1 OR v_was_in_slatime = 0 THEN
        -- Downtime Aktiv/Ausserhalb SLA Zeit - also ist das Problem irrElefant
          v_add_ok_duration := v_duration;
        ELSE
        -- Weder Downtime noch SLA, und mitten im Problem 
        -- Das war eine Outage - die Zeit wird nicht aufaddiert
          v_add_ok_duration := 0;
        END IF;
      END IF;
 
      -- Return Current state of calculation - replaces the temp table 
      -- because of out of shared memory issues
      RETURN QUERY SELECT event_row.state_time AS state_time,
        v_duration AS duration,
        v_add_ok_duration AS add_ok_duration,
        v_old_type AS old_type,
        event_row.type AS current_type,
        v_old_state AS old_state,
        v_current_state AS current_state,
        v_was_problem AS was_problem,
        v_was_in_downtime AS was_in_downtime,
        v_was_in_slatime AS was_in_slatime;

      v_old_state := v_current_state;
      v_old_type := event_row.type;
      v_old_state_time := event_row.state_time;
      
    END LOOP;
    -- On Return the function really is over
    RETURN;                     
  END;
  $_$ LANGUAGE plpgsql SECURITY DEFINER;