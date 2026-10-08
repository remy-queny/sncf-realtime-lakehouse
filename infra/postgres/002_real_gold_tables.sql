BEGIN;

CREATE SCHEMA IF NOT EXISTS sncf_real;

CREATE TABLE IF NOT EXISTS sncf_real.gold_delay_by_line_15min (
    LIKE public.gold_delay_by_line_15min INCLUDING ALL
);

CREATE TABLE IF NOT EXISTS sncf_real.gold_station_delay_daily (
    LIKE public.gold_station_delay_daily INCLUDING ALL
);

COMMIT;