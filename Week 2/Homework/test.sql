-- - A cumulative query to generate `device_activity_datelist` from `events`
-- Recreate the table with NUMERIC for user_id
DROP TABLE IF EXISTS user_devices_cumulated_jsonb;
CREATE TABLE user_devices_cumulated_jsonb (
    user_id NUMERIC,  -- Changed to NUMERIC to handle very large numbers
    device_activity_datelist JSONB NOT NULL DEFAULT '{}'::jsonb,
    date DATE NOT NULL,
    CONSTRAINT valid_activity_dates CHECK (jsonb_typeof(device_activity_datelist) = 'object'),
    PRIMARY KEY (user_id, date)
);

-- Query to populate the data
WITH daily_activity AS (
    SELECT
        CAST(user_id as NUMERIC),  -- Explicit cast to NUMERIC
        DATE(event_time) as activity_date,
        jsonb_build_object(
            'active_dates',
            jsonb_agg(DISTINCT DATE(event_time))
        ) as device_activity_datelist
    FROM events
    WHERE
        DATE(event_time) BETWEEN '2023-01-01' AND '2023-01-31'
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
)
INSERT INTO user_devices_cumulated_jsonb (user_id, device_activity_datelist, date)
SELECT
    user_id,
    device_activity_datelist,
    activity_date
FROM daily_activity
ON CONFLICT (user_id, date)
DO UPDATE SET
    device_activity_datelist = jsonb_build_object(
        'active_dates',
        (user_devices_cumulated_jsonb.device_activity_datelist->'active_dates') ||
        (EXCLUDED.device_activity_datelist->'active_dates')
    );

-- Verify the results
SELECT * FROM user_devices_cumulated_jsonb ORDER BY date, user_id LIMIT 5;


-- Add an index to improve query performance
CREATE INDEX idx_user_devices_date ON user_devices_cumulated_jsonb(date);

-- Add function to get user activity for date range
CREATE OR REPLACE FUNCTION get_user_activity(start_date date, end_date date)
RETURNS TABLE (
    user_id NUMERIC,
    active_days INTEGER,
    first_seen DATE,
    last_seen DATE
) AS $$
SELECT
    user_id,
    jsonb_array_length(device_activity_datelist->'active_dates') as active_days,
    MIN(date) as first_seen,
    MAX(date) as last_seen
FROM user_devices_cumulated_jsonb
WHERE date BETWEEN start_date AND end_date
GROUP BY user_id, device_activity_datelist
ORDER BY active_days DESC;
$$ LANGUAGE SQL;

-- Add validation trigger
CREATE OR REPLACE FUNCTION validate_activity_dates()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT (NEW.device_activity_datelist ? 'active_dates') THEN
        RAISE EXCEPTION 'device_activity_datelist must contain active_dates key';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_activity_dates
BEFORE INSERT OR UPDATE ON user_devices_cumulated_jsonb
FOR EACH ROW EXECUTE FUNCTION validate_activity_dates();



--device_activity_datelistadd the datelist_int column to the table
ALTER TABLE user_devices_cumulated_jsonb
ADD COLUMN datelist_int INTEGER[] DEFAULT '{}'::integer[];

-- Then update it with converted values from device_activity_datelist
WITH date_conversion AS (
    SELECT
        user_id,
        date,
        (SELECT array_agg(CAST(TO_CHAR(date::date, 'YYYYMMDD') AS INTEGER))
         FROM jsonb_array_elements_text(device_activity_datelist->'active_dates') as date
        ) as datelist_int
    FROM user_devices_cumulated_jsonb
    GROUP BY user_id, date, device_activity_datelist
)
UPDATE user_devices_cumulated_jsonb u
SET datelist_int = dc.datelist_int
FROM date_conversion dc
WHERE u.user_id = dc.user_id
AND u.date = dc.date;

-- Verify the conversion
SELECT
    user_id,
    date,
    device_activity_datelist->'active_dates' as active_dates,
    datelist_int as integer_dates
FROM user_devices_cumulated_jsonb
LIMIT 5;


-- Create the hosts_cumulated table
CREATE TABLE hosts_cumulated (
    host VARCHAR NOT NULL,
    host_activity_datelist JSONB NOT NULL DEFAULT '{}'::jsonb,
    date DATE NOT NULL,
    CONSTRAINT hosts_cumulated_pkey PRIMARY KEY (host, date),
    CONSTRAINT valid_host_activity_dates CHECK (jsonb_typeof(host_activity_datelist) = 'object')
);

-- Populate the table with host activity
WITH daily_host_activity AS (
    SELECT
        host,
        DATE(event_time) as activity_date,
        jsonb_build_object(
            'active_dates',
            jsonb_agg(DISTINCT DATE(event_time))
        ) as host_activity_datelist
    FROM events
    WHERE
        DATE(event_time) BETWEEN '2023-01-01' AND '2023-01-31'
        AND host IS NOT NULL
    GROUP BY host, DATE(event_time)
)
INSERT INTO hosts_cumulated (host, host_activity_datelist, date)
SELECT
    host,
    host_activity_datelist,
    activity_date
FROM daily_host_activity
ON CONFLICT (host, date)
DO UPDATE SET
    host_activity_datelist = jsonb_build_object(
        'active_dates',
        (hosts_cumulated.host_activity_datelist->'active_dates') ||
        (EXCLUDED.host_activity_datelist->'active_dates')
    );