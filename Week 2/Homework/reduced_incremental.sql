CREATE OR REPLACE FUNCTION update_host_activity_reduced(target_date date)
RETURNS void AS $$
DECLARE
    target_month date := DATE_TRUNC('month', target_date)::date;
    days_in_month integer := extract(days from (target_month + interval '1 month - 1 day'))::integer;
    day_index integer := extract(day from target_date)::integer;
    event_count integer;
    host_count integer;
BEGIN
    -- Check if we have any events for this date
    SELECT
        COUNT(*), COUNT(DISTINCT host)
    INTO event_count, host_count
    FROM events
    WHERE DATE(event_time) = target_date;

    RAISE NOTICE 'Processing date: %. Found % events and % hosts', target_date, event_count, host_count;

    -- First ensure the monthly records exist
    INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
    SELECT DISTINCT
        target_month as month,
        host,
        array_fill(0, ARRAY[days_in_month]) as hit_array,
        array_fill(0, ARRAY[days_in_month]) as unique_visitors_array
    FROM events
    WHERE DATE(event_time) = target_date
    ON CONFLICT (month, host) DO NOTHING;

    -- Update the arrays for the specific day using array index assignment
    WITH daily_stats AS (
        SELECT
            host,
            COUNT(1) as hits,
            COUNT(DISTINCT user_id) as unique_visitors
        FROM events
        WHERE DATE(event_time) = target_date
        GROUP BY host
    )
    UPDATE host_activity_reduced har
    SET
        hit_array[day_index] = COALESCE(ds.hits, 0),
        unique_visitors_array[day_index] = COALESCE(ds.unique_visitors, 0)
    FROM daily_stats ds
    WHERE har.host = ds.host
    AND har.month = target_month;

    RAISE NOTICE 'Updated arrays for date % (day index %)', target_date, day_index;
END;
$$ LANGUAGE plpgsql;

-- Clear existing data
TRUNCATE TABLE host_activity_reduced;

-- Run the update for January 2023
DO $$
BEGIN
    FOR i IN 1..31 LOOP
        PERFORM update_host_activity_reduced(('2023-01-' || LPAD(i::text, 2, '0'))::date);
    END LOOP;
END;
$$;

-- Verify results
SELECT
    month,
    host,
    array_length(hit_array, 1) as days_in_month,
    hit_array as daily_hits,
    unique_visitors_array as daily_visitors
FROM host_activity_reduced
WHERE month = '2023-01-01'
ORDER BY host;