DROP TABLE IF EXISTS icinga_sla_periods;
CREATE TABLE icinga_sla_periods (
  timeperiod_object_id BIGINT NOT NULL,
  start_time timestamp NOT NULL,
  end_time timestamp NULL DEFAULT NULL
);
ALTER TABLE icinga_sla_periods ADD constraint tp_start PRIMARY KEY (timeperiod_object_id,start_time);
ALTER TABLE icinga_sla_periods ADD constraint tp_end UNIQUE(timeperiod_object_id,end_time);

DROP TABLE IF EXISTS icinga_outofsla_periods;
CREATE TABLE icinga_outofsla_periods (
  timeperiod_object_id BIGINT NOT NULL,
  start_time timestamp NOT NULL,
  end_time timestamp NULL DEFAULT NULL
) ;
ALTER TABLE icinga_outofsla_periods ADD constraint outofsla_tp_start PRIMARY KEY (timeperiod_object_id,start_time);
ALTER TABLE icinga_outofsla_periods ADD constraint outofsla_tp_end UNIQUE(timeperiod_object_id,end_time);
