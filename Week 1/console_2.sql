WITH RECURSIVE year_bounds AS (
    SELECT
        MIN(current_year) as min_year,
        MAX(current_year) as max_year
    FROM actors
),
all_years AS (
    SELECT generate_series(
        (SELECT min_year FROM year_bounds),
        (SELECT max_year FROM year_bounds)
    ) as year
),
actor_years AS (
    SELECT DISTINCT
        a.actorid,
        a.actor,
        y.year as current_year
    FROM actors a
    CROSS JOIN all_years y
    WHERE y.year BETWEEN (SELECT min_year FROM year_bounds)
                     AND (SELECT max_year FROM year_bounds)
),
base_data AS (
    SELECT
        ay.actorid,
        ay.actor,
        ay.current_year,
        COALESCE(a.films, '{}') as films,
        a.quality_class,
        COALESCE(a.is_active, false) as is_active
    FROM actor_years ay
    LEFT JOIN actors a ON a.actorid = ay.actorid
        AND a.current_year = ay.current_year
),
filled_data AS (
    SELECT
        actorid,
        actor,
        current_year,
        films,
        COALESCE(
            quality_class,
            FIRST_VALUE(quality_class) OVER (
                PARTITION BY actorid
                ORDER BY current_year
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) as quality_class,
        is_active
    FROM base_data
),
changes AS (
    SELECT
        actorid,
        actor,
        current_year,
        films,
        quality_class,
        is_active,
        CASE
            WHEN LAG(quality_class) OVER w IS DISTINCT FROM quality_class
            OR LAG(is_active) OVER w IS DISTINCT FROM is_active
            OR LAG(films::text) OVER w IS DISTINCT FROM films::text
            THEN true
            ELSE false
        END as is_change
    FROM filled_data
    WINDOW w AS (PARTITION BY actorid ORDER BY current_year)
),
change_periods AS (
    SELECT
        actorid,
        actor,
        current_year as start_year,
        LEAD(current_year - 1, 1, (SELECT max_year FROM year_bounds))
            OVER (PARTITION BY actorid ORDER BY current_year) as end_year,
        films,
        quality_class,
        is_active,
        is_change
    FROM changes
),
scd2 AS (
    SELECT
        actorid,
        actor,
        start_year,
        end_year,
        quality_class,
        is_active,
        films,
        end_year = (SELECT max_year FROM year_bounds) as is_current
    FROM change_periods
    WHERE is_change OR start_year = (SELECT min_year FROM year_bounds)
)
--INSERT INTO actors_history_scd
SELECT * FROM scd2;