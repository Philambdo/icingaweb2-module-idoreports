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
-- 2019-05-15: Rewritten for postgres 
--

-- DROP FUNCTION IF EXISTS idoreports_get_sla_ok_percent();
CREATE OR REPLACE FUNCTION idoreports_get_sla_ok_percent (
	id BIGINT,
	start_ts TIMESTAMP, 
	end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
RETURNS DECIMAL(7, 4)
AS $_$
DECLARE 
v_availability DECIMAL(7, 4);
t_du RECORD;
v_type_id INTEGER;
v_multiplicator INTEGER := 0;


v_wanted_timeframe_uts FLOAT := UNIX_TIMESTAMP(end_ts) - UNIX_TIMESTAMP(start_ts);
v_sum_sla_ok_seconds INTEGER := 0;
v_sum_sla_ok_percent DECIMAL(7,4);
v_sla_ok_seconds INTEGER := 0;
v_sla_ok_percent DECIMAL(7,4);
v_if_downtime_sla_ok INTEGER :=  NULL;

BEGIN
  SELECT objecttype_id INTO v_type_id FROM icinga_objects WHERE object_id = id;
  IF v_type_id NOT IN (1, 2) THEN
    RETURN NULL;
  END IF;

  FOR t_du in SELECT * from icinga_sla_updown_period(id, start_ts, end_ts, sla_timeperiod_object_id)
LOOP
	IF t_du.in_downtime + t_du.out_of_slatime > 0 THEN
		v_if_downtime_sla_ok := 1;
    ELSIF t_du.is_problem THEN 
		v_if_downtime_sla_ok := 0;
	ELSE 
		v_if_downtime_sla_ok := 1;
    END IF;

  v_sla_ok_seconds := v_if_downtime_sla_ok * t_du.duration;
  v_sum_sla_ok_seconds := v_sum_sla_ok_seconds + v_sla_ok_seconds;
  RAISE NOTICE 'SUM_SLA_OK_SECONDS: % , ALL: %', v_sum_sla_ok_seconds, v_wanted_timeframe_uts;

--  v_sla_ok_percent := v_sla_ok_seconds * 100 / v_wanted_timeframe;
--  RAISE NOTICE 'SLA_OK_PERCENT: %', v_sla_ok_percent;
END LOOP;

--  FOR t_du in SELECT * from icinga_sla_updown_period(id, start_ts, end_ts, sla_timeperiod_object_id)
-- LOOP
--    IF v_type_id=1 THEN
--        IF t_du.current_state = 0 THEN
--            v_multiplicator := 1;
--        ELSE
--            v_multiplicator := 0;
--        END IF;
--    ELSE
--        IF t_du.current_state < 2 THEN
--            v_multiplicator := 1;
--        ELSE
--            v_multiplicator := 0;
--        END IF;
--    END IF;
--    v_sum_up = v_sum_up + t_du.duration*v_multiplicator;
--END LOOP;

v_sum_sla_ok_percent := (v_sum_sla_ok_seconds::FLOAT / v_wanted_timeframe_uts)*100;
RAISE NOTICE 'v_sum_sla_ok_percent: %' , v_sum_sla_ok_percent;
--v_availability = v_sum_up / (UNIX_TIMESTAMP(end_ts)-UNIX_TIMESTAMP(start_ts))::FLOAT * 100::FLOAT;

RETURN v_sum_sla_ok_percent;

END;
$_$ LANGUAGE plpgsql SECURITY DEFINER;
