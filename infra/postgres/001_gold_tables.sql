CREATE TABLE IF NOT EXISTS gold_delay_by_line_15min(
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    route_id TEXT NOT NULL,
    line_name TEXT,
    event_count BIGINT NOT NULL,
    delayed_trip_count BIGINT NOT NULL,
    average_delay_minutes DOUBLE PRECISION,
    max_delay_minutes DOUBLE PRECISION,
    PRIMARY KEY (window_start, route_id)
);

CREATE TABLE IF NOT EXISTS gold_station_delay_daily (
    event_date DATE NOT NULL,
    stop_id TEXT NOT NULL,
    stop_name TEXT,
    delayed_event_count BIGINT NOT NULL,
    average_delay_minutes DOUBLE PRECISION,
    max_delay_minutes DOUBLE PRECISION,
    event_count BIGINT NOT NULL,
    PRIMARY KEY (event_date, stop_id)
);