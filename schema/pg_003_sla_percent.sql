CREATE OR REPLACE FUNCTION idoreports_get_sla_ok_percent(
  IN id BIGINT,
  IN start_ts TIMESTAMP,
  IN end_ts TIMESTAMP,
  IN sla_timeperiod_object_id BIGINT,
  OUT availability_sla DECIMAL(4,4)
  )
  AS $_$
  DECLARE
  v_availability_sla DECIMAL(5,4);
  v_availability_24x7 DECIMAL(5,4);
  v_availability_sla_to_fc DECIMAL(5,4);
  BEGIN
    RAISE WARNING 'id: %, start_ts: %, end_ts: %, TP: %', id, start_ts, end_ts, sla_timeperiod_object_id;
    SELECT INTO v_availability_sla, v_availability_24x7, v_availability_sla_to_fc 
                ii.availability_sla, ii.availability_24x7, ii.availability_sla_to_fc  
    FROM icinga_idoreports_get_sla_ok_percent(id, start_ts, end_ts, sla_timeperiod_object_id) ii;
    RAISE NOTICE 'v_availability_sla: %', v_availability_sla;
    RAISE NOTICE 'v_availability_24x7: %', v_availability_24x7;
    RAISE NOTICE 'v_availability_sla_to_fc: %', v_availability_sla_to_fc;

    availability_sla:=v_availability_sla*100;
  END;
  $_$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION icinga_idoreports_get_sla_ok_percent(
  IN id BIGINT,
  IN start_ts TIMESTAMP,
  IN end_ts TIMESTAMP,
  IN sla_timeperiod_object_id BIGINT,
  OUT availability_sla DECIMAL(4,4),
  OUT availability_24x7 DECIMAL(4,4),
  OUT availability_sla_to_fc DECIMAL(4,4)
  )
  AS $_$
  DECLARE
  -- availability_sla DECIMAL(7,4);
  -- availability_24x7 DECIMAL(7,4);
  -- Die gesamte fuer den SLA relevante Zeit (zb: 7-17 Uhr bei Economy)
  v_sla_ALL_seconds FLOAT := 0;
  -- Die Zeit die der Service innerhalb des SLA Zeitraums OK war
  v_sla_ok_seconds_sum FLOAT := 0;
  -- Die Zeit die der Service innerhalb SLA OK war + out of sla Zeiten sind automatisch ok
  v_sla_ok_to_fc_seconds_sum FLOAT := 0;
  -- Die Zeit die der Service den ganzen Tag ueber ok war (unabhaengig von SLA)
  v_sla_firstclass_ok_seconds_sum FLOAT := 0;
  -- Der komplette Berechnungszeitraum auf den der SLA bei FC aufgerechnet werden wuerde
  v_sla_firstclass_seconds FLOAT := EXTRACT(EPOCH FROM end_ts) - EXTRACT(EPOCH FROM start_ts);
  t_du RECORD;
  BEGIN

  FOR t_du in SELECT * from icinga_sla_updown_period(id, start_ts, end_ts, sla_timeperiod_object_id)
  LOOP
    -- Für allgemeines 24x7 ignorieren wir SLA-Zeiten komplett.
    -- Addiert werden OK Zustaende, wenn entweder kein Problem war oder eine Downtime gesetzt war
    IF t_du.was_problem = 0 or t_du.was_problem IS NULL THEN
      v_sla_firstclass_ok_seconds_sum = v_sla_firstclass_ok_seconds_sum + t_du.add_ok_duration;
    ELSIF t_du.was_problem = 1 AND t_du.was_in_downtime = 1 THEN
      v_sla_firstclass_ok_seconds_sum = v_sla_firstclass_ok_seconds_sum + t_du.add_ok_duration;
    END IF;
       
    -- Die restlichen Berechnungen beruecksichtigen die SLA Zeiten fuer den Service
    IF t_du.was_in_slatime = 1 THEN
      -- Bei Timeperiods die nicht 24x7 sind, ist der Betrachtungszeitraum und die Beruecksichtigung
      -- von SLA-Zeiten ein anderer - das wird hier abgefangen - deswegen auch 2 Prozent-Werte
      v_sla_ok_seconds_sum = v_sla_ok_seconds_sum + t_du.add_ok_duration;
      v_sla_ok_to_fc_seconds_sum = v_sla_ok_to_fc_seconds_sum + t_du.add_ok_duration;
      -- Berechnungsbasis um die SLA-Sekunden pro Tag zu haben
      v_sla_ALL_seconds := v_sla_ALL_seconds + t_du.duration::FLOAT;
    ELSE
      v_sla_ok_to_fc_seconds_sum = v_sla_ok_to_fc_seconds_sum + t_du.duration;
    END IF;
  END LOOP;
  IF v_sla_ALL_seconds = 0
  THEN
    -- Ohne SLA Zeiten in der SLA-Tabelle gibts einen Fallback auf 24x7
    v_sla_ALL_seconds := v_sla_firstclass_seconds;
    v_sla_ok_seconds_sum := v_sla_firstclass_ok_seconds_sum;
    RAISE WARNING 'Keine definierten SLA Zeiten für die Berechnung gefunden - Fallback auf 24x7';
  END IF;
    
    RAISE NOTICE 'v_availability_sla: %', v_sla_ok_seconds_sum / v_sla_ALL_seconds;
    RAISE NOTICE 'v_availability_24x7: %', v_sla_firstclass_ok_seconds_sum / v_sla_firstclass_seconds;
    RAISE NOTICE 'v_availability_sla_to_fc: %', v_sla_ok_to_fc_seconds_sum / v_sla_firstclass_seconds;
  
  availability_sla = v_sla_ok_seconds_sum / v_sla_ALL_seconds;
  availability_24x7 = v_sla_firstclass_ok_seconds_sum / v_sla_firstclass_seconds;
  availability_sla_to_fc = v_sla_ok_to_fc_seconds_sum / v_sla_firstclass_seconds;

  END;
  $_$ LANGUAGE plpgsql SECURITY DEFINER;