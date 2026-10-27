BEGIN;

CREATE SCHEMA sncf_active_test;

CREATE TABLE sncf_active_test.gold_delay_by_line_15min (
    LIKE public.gold_delay_by_line_15min INCLUDING ALL
);

CREATE TABLE sncf_active_test.gold_station_delay_daily (
    LIKE public.gold_station_delay_daily INCLUDING ALL
);

COMMIT;