-- The source database holds exactly this one business table:
-- intersection id, UTC minute and vehicle count.
CREATE TABLE IF NOT EXISTS traffic_minute_counts (
    intersection_id text        NOT NULL,
    minute_utc      timestamptz NOT NULL,
    vehicle_count   integer     NOT NULL CHECK (vehicle_count >= 0),
    PRIMARY KEY (intersection_id, minute_utc)
);
