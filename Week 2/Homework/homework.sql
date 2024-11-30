-- # Week 2 Fact Data Modeling
-- The homework this week will be using the `devices` and `events` dataset
--
-- Construct the following eight queries:
--
-- - A query to deduplicate `game_details` from Day 1 so there's no duplicates
--

WITH RankedRows AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY
                game_id,
                team_id,
                player_id
            ORDER BY
                player_name,
                min DESC
        ) as row_num
    FROM game_details
)
select * FROM game_details
WHERE (game_id, team_id, player_id) IN (
    SELECT game_id, team_id, player_id
    FROM RankedRows
    WHERE row_num > 1
);


-- - A DDL for an `user_devices_cumulated` table that has:
--   - a `device_activity_datelist` which tracks a users active days by `browser_type`
--   - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--     - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE user_devices_cumulated_jsonb (
    user_id INTEGER PRIMARY KEY,
    device_activity_datelist JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT valid_activity_dates CHECK (jsonb_typeof(device_activity_datelist) = 'object')
);

DROP TABLE IF EXISTS user_devices_cumulated_jsonb;

CREATE TABLE user_devices_cumulated_jsonb (
    user_id INTEGER,
    device_activity_datelist JSONB NOT NULL DEFAULT '{}'::jsonb,
    date DATE NOT NULL,  -- Added date column
    CONSTRAINT valid_activity_dates CHECK (jsonb_typeof(device_activity_datelist) = 'object'),
    PRIMARY KEY (user_id, date)  -- Composite primary key
);

drop function aggregate_user_device_activity;

