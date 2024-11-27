WITH
last_processed_year AS (
    SELECT MAX(end_year) as max_year
    FROM actors_history_scd
),

relevant_actors AS (
    SELECT
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        films
    FROM actors
    WHERE current_year >= (SELECT max_year FROM last_processed_year)
),

changes AS (
    SELECT
        a.actorid,
        a.actor,
        a.current_year,
        a.quality_class,
        a.is_active,
        a.films,
        CASE
            WHEN h.actorid IS NULL THEN true
            WHEN h.quality_class != a.quality_class THEN true
            WHEN h.is_active != a.is_active THEN true
            WHEN h.films::text != a.films::text THEN true
            ELSE false
        END as requires_new_record
    FROM relevant_actors a
    LEFT JOIN actors_history_scd h ON
        h.actorid = a.actorid
        AND h.is_current = true
),


new_records AS (
    SELECT
        c.actorid,
        c.actor,
        c.current_year as start_year,
        LEAD(c.current_year - 1, 1, c.current_year) OVER (
            PARTITION BY c.actorid
            ORDER BY c.current_year
        ) as end_year,
        c.quality_class,
        c.is_active,
        c.films,
        CASE
            WHEN LEAD(c.current_year) OVER (
                PARTITION BY c.actorid
                ORDER BY c.current_year
            ) IS NULL THEN true
            ELSE false
        END as is_current
    FROM changes c
    WHERE c.requires_new_record = true
),


updates AS (
    UPDATE actors_history_scd h
    SET
        end_year = c.current_year - 1,
        is_current = false
    FROM changes c
    WHERE
        h.actorid = c.actorid
        AND h.is_current = true
        AND c.requires_new_record = true
    RETURNING h.*
),


inserts AS (
    INSERT INTO actors_history_scd (
        actorid,
        actor,
        start_year,
        end_year,
        quality_class,
        is_active,
        films,
        is_current
    )
    SELECT
        actorid,
        actor,
        start_year,
        end_year,
        quality_class,
        is_active,
        films,
        is_current
    FROM new_records
    RETURNING *
)


SELECT
    (SELECT COUNT(*) FROM updates) as records_updated,
    (SELECT COUNT(*) FROM inserts) as records_inserted;