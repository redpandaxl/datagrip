-- First ensure we have our custom types
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
CREATE TYPE films AS (
    title text,
    votes integer,
    rating numeric,
    filmid text
);

CREATE TYPE film_stats AS (
    year INTEGER,
    film TEXT,
    votes INTEGER,
    rating NUMERIC,
    filmid TEXT
);
drop table actors_history_scd
-- Create the actors history SCD table
-- Create the actors history SCD table
CREATE TABLE actors_history_scd AS
WITH years AS (
    -- Generate series from earliest to latest year in your data
    SELECT *
    FROM GENERATE_SERIES(
        (SELECT MIN(year) FROM actor_films),
        (SELECT MAX(year) FROM actor_films)
    ) AS current_year
), first_appearances AS (
    -- Get first appearance year for each actor
    SELECT
        actor,
        actorid,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actor, actorid
), actors_and_years AS (
    -- Create all combinations of actors and years since their first appearance
    SELECT *
    FROM first_appearances fa
    JOIN years y
        ON fa.first_year <= y.current_year
), windowed AS (
    -- Build cumulative array of film statistics with duplicate removal
    SELECT DISTINCT
        aay.actor,
        aay.actorid,
        aay.current_year,
        ARRAY(
            SELECT ROW(af.year, af.film, af.votes, af.rating, af.filmid)::film_stats
            FROM (
                SELECT DISTINCT year, film, votes, rating, filmid
                FROM actor_films af2
                WHERE af2.actor = aay.actor
                AND af2.year <= aay.current_year
            ) af
            ORDER BY af.year, af.film
        ) AS films
    FROM actors_and_years aay
), static AS (
    -- Get static/unchanging actor information
    SELECT
        actor,
        actorid,
        COUNT(DISTINCT film) as total_films,
        AVG(rating) as average_rating
    FROM actor_films
    GROUP BY actor, actorid
)
SELECT
    w.actor,
    w.actorid,
    w.current_year,
    s.total_films,
    s.average_rating,
    films,
    -- Quality classification based on latest film rating
    CASE
        WHEN (films[CARDINALITY(films)]::film_stats).rating > 8 THEN 'star'
        WHEN (films[CARDINALITY(films)]::film_stats).rating > 7 THEN 'good'
        WHEN (films[CARDINALITY(films)]::film_stats).rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    -- Check if actor had a film that year (fixed is_active calculation)
    EXISTS (
        SELECT 1
        FROM UNNEST(films) AS f(year, film, votes, rating, filmid)
        WHERE f.year = w.current_year
    ) AS is_active,
    -- Years since last film
    w.current_year - (films[CARDINALITY(films)]::film_stats).year as years_since_last_film
FROM windowed w
JOIN static s
    ON w.actor = s.actor;

-- Create indexes
CREATE INDEX idx_actors_history_actor_year ON actors_history_scd(actorid, current_year);
CREATE INDEX idx_actors_history_year ON actors_history_scd(current_year);

-- Example queries to verify the fixes
CREATE OR REPLACE FUNCTION verify_actor_history(p_actor_name TEXT, p_year INTEGER)
RETURNS TABLE (
    actor_name TEXT,
    current_year INTEGER,
    num_films INTEGER,
    is_active BOOLEAN,
    films_this_year TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.actor,
        a.current_year,
        ARRAY_LENGTH(a.films, 1),
        a.is_active,
        ARRAY_AGG(f.film)
    FROM actors_history_scd a,
    LATERAL UNNEST(a.films) AS f(year, film, votes, rating, filmid)
    WHERE a.actor = p_actor_name
    AND a.current_year = p_year
    AND f.year = p_year
    GROUP BY a.actor, a.current_year, a.films, a.is_active;
END;
$$ LANGUAGE plpgsql;