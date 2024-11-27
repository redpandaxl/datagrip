drop table players_scd

Create table players_scd (
    player_name TEXT,
    scoring_class scoring_class, --tracked column
    is_active boolean, --tracked column
    start_season integer,
    end_season integer,
    current_season integer, --date partition
    primary key (player_name, start_season)

)

--calculate streak of how long they were in a current dimension
-- Looking at what was the dimension before, using a window functuion.
-- using LAG with partition
-- unit economics used this pattern, spark crunched it well.
--windows functions arent expensive if partitioned well and regenerate each day
--why am i scanning all of history every day, how does this impact stuff?
--this query is prone to out of memory issues

-- create type scd_type as (
--     scoring_class scoring_class,
--     is_active boolean,
--     start_season INTEGER,
--     end_season INTEGER
-- )

insert into players_scd
with with_previous as (
select player_name,
       players.scoring_class,
       current_season,
       players.is_active,
       lag(players.scoring_class, 1) over (partition by player_name order by current_season) as prev_scoring_class,
        lag(players.is_active, 1) over (partition by player_name order by current_season) as prev_active

from players
where current_season <= 2021),
    with_indicators as (


select *,
       case
           when scoring_class <> prev_scoring_class then 1
           when is_active <> prev_active then 1
           else 0
           end as change_indicator
from with_previous),
    with_streaks as (


select *, sum(change_indicator) over (partition by  player_name order by current_season) as streak_identifier
from with_indicators)

select
    player_name,
        scoring_class,
            is_active,
    min(current_season) as start_season,
    MAX(current_season) as end_season,
    2021 as current_season
    from with_streaks
group by player_name, streak_identifier, is_active, scoring_class
order by player_name, streak_identifier

-- incremental filling

with last_season_scd as (
    SELECT * FROM players_scd
    where
    current_season = 2021
    and end_season = 2021
),
    historical_scd as (
    select
    player_name,
    scoring_class,
    is_active,
    start_season,
    end_season
    from players_scd
    where current_season = 2021
    and end_season < 2021
    ),
    this_season_data as (
    select * from players
    where current_season = 2022
),
    unchanged_records as (
    select ts.player_name,
           ts.scoring_class,
           ts.is_active,
           ls.start_season,
           ts.current_season as end_season
    from this_season_data ts
        join last_season_scd ls on ls.player_name = ts.player_name
    where ts.scoring_class = ls.scoring_class
    and ts.is_active = ls.is_active
),
    changed_records as (
        select ts.player_name,
        (unnest(ARRAY[
               (ls.scoring_class, ls.is_active, ls.start_season, ls.end_season)::scd_type,
               (ts.scoring_class, ts.is_active, ts.current_season, ts.current_season)::scd_type
           ])).*
    from this_season_data ts
    left join last_season_scd ls on ls.player_name = ts.player_name
        where (ts.scoring_class <> ls.scoring_class
            or ts.is_active <> ls.is_active)
        or ls.player_name is null
    ),
    new_records as (
        select
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ts.current_season as start_season,
            ts.current_season as end_season
        from this_season_data ts
    left join last_season_scd ls on ls.player_name = ts.player_name
        where ls.player_name is null


    )
select * from historical_scd

union all

select * from unchanged_records
union all

select * from changed_records

union all

select * from new_records