-- Create the monthly reduced fact table
CREATE TABLE host_activity_reduced (
    month DATE NOT NULL,  -- First day of month
    host VARCHAR NOT NULL,
    hit_array INTEGER[] NOT NULL DEFAULT '{}'::integer[],  -- Array of daily hit counts
    unique_visitors_array INTEGER[] NOT NULL DEFAULT '{}'::integer[],  -- Array of daily unique visitors
    PRIMARY KEY (month, host)
);

-- Populate the table with monthly data
WITH RECURSIVE dates AS (
    -- Generate series for all days in the month
    SELECT generate_series(
        '2023-01-01'::date,
        '2023-01-31'::date,
        '1 day'::interval
    )::date AS day
),
daily_stats AS (
    -- Calculate daily statistics
    SELECT
        DATE_TRUNC('month', DATE(event_time))::date as month,
        host,
        DATE(event_time) as day,
        COUNT(1) as hits,
        COUNT(DISTINCT user_id) as unique_visitors
    FROM events
    WHERE DATE(event_time) BETWEEN '2023-01-01' AND '2023-01-31'
    GROUP BY DATE_TRUNC('month', DATE(event_time))::date, host, DATE(event_time)
),
monthly_arrays AS (
    -- Create arrays of daily stats, filling in zeros for missing days
    SELECT
        m.month,
        m.host,
        array_agg(COALESCE(ds.hits, 0) ORDER BY d.day) as hit_array,
        array_agg(COALESCE(ds.unique_visitors, 0) ORDER BY d.day) as unique_visitors_array
    FROM
        (SELECT DISTINCT DATE_TRUNC('month', day)::date as month, host
         FROM dates CROSS JOIN (SELECT DISTINCT host FROM daily_stats) h
        ) m
    CROSS JOIN dates d
    LEFT JOIN daily_stats ds ON ds.day = d.day AND ds.host = m.host
    WHERE d.day BETWEEN '2023-01-01' AND '2023-01-31'
    GROUP BY m.month, m.host
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT
    month,
    host,
    hit_array,
    unique_visitors_array
FROM monthly_arrays
ON CONFLICT (month, host) DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;
-- Verify the results
SELECT
    month,
    host,
    array_length(hit_array, 1) as days_in_month,
    hit_array[1:5] as first_5_days_hits,
    unique_visitors_array[1:5] as first_5_days_visitors
FROM host_activity_reduced
ORDER BY month, host
LIMIT 5;