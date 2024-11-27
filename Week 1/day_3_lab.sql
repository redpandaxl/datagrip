--building graph data model
create type vertex_type
    as enum ('player', 'team', 'game')

create table vertices (
    identifier text,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
)

create type edge_type as
    enum ('plays_against',
        'shares_team',
        'plays_in',
        'plays_on'
        )

Create table edges (
    subject_identifier text,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    primary key (subject_identifier,subject_type, object_identifier, object_type, edge_type)
)

--create game as vertex type
--this is our verticy
insert into vertices
select
    game_id as identifier,
    'game'::vertex_type as type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end
    ) as properties

from games;



insert into vertices
with players_agg as (
select
    player_id as identifier,
    max(player_name) as player_name,
    'player'::vertex_type as type,
    count(1) as number_of_games,
    sum(pts) as total_points,
    array_agg(distinct team_id) as teams


from game_details
group by player_id)

select
    identifier as identifier,
     type,
    json_build_object(
    'player_name', player_name,
    'number_of_games', number_of_games,
    'total_points', total_points,
    'teams', teams
    ) as properties

from players_agg;

select * from teams;
insert into vertices
with teams_deduped as (
    select *, row_number() over (partition by team_id) as row_number
    from teams
)
select
    team_id as identifier,
    'team'::vertex_type as type,
    json_build_object(
    'abbreviation', abbreviation,
    'nickname', nickname,
    'city', city,
    'arena', arena,
    'year_founded', yearfounded
    ) as properties

from teams_deduped
where row_number = 1;

select type, count(1)  from vertices
group by 1

insert into edges
with deduped as (
    select *, row_number() over (partition by player_id, game_id) as row_number
    from game_details
),
    filtred as (
        select * from deduped
                 where row_number = 1
    ),
aggregated as (
select
    f1.player_id as subject_player_id,

    f2.player_id as object_player_id,

    case when f1.team_abbreviation = f2.team_abbreviation then
        'shares_team'::edge_type
        else 'plays_against'::edge_type
            end as edge_type,
    max(f1.player_name) as subject_player_name,
    max(f2.player_name) as object_player_name,
    count(1) as num_games,
    sum(f1.pts) as subject_points,
    sum(f2.pts) as object_points

from filtred f1
join filtred f2
on f1.game_id = f2.game_id and f1.player_name <> f2.player_name
where f1.player_id > f2.player_id
group by f1.player_id,
    f2.player_id,
    case when f1.team_abbreviation = f2.team_abbreviation then
        'shares_team'::edge_type
        else 'plays_against'::edge_type
            end)

select
    subject_player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id as object_identifier,
    'player'::vertex_type as object_type,
    edge_type as edge_type,
    json_build_object(
    'num_games', num_games,
    'subject_points', subject_points,
    'object_points', object_points
    )
     from aggregated



with deduped as (
    select *, row_number() over (partition by player_id, game_id) as row_number
    from game_details
)
select
    player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    game_id as object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
    'start_position', start_position,
    'pts', pts,
    'team_id', team_id,
    'team_abbreviation', team_abbreviation
    ) as properties
    from deduped
where row_number = 1

select * from game_details where player_id =1628370 and game_id = 22000069
select * from edges

select
v.properties->>'player_name' player_name,
cast(v.properties->>'number_of_games' as real) number_of_games,
cast(v.properties->>'total_points' as real) total_points,
cast(v.properties->>'total_points' as real) / case when cast(v.properties->>'number_of_games' as real) = 0 then 1 else  cast(v.properties->>'number_of_games' as real) end as ppg

from vertices v
    join edges e on e.subject_identifier = v.identifier and e.subject_type = v.type
where e.object_type = 'player'::vertex_type
group by 1,2,3
order by 2 desc;



SELECT typname, typtype, typlen
FROM pg_type
WHERE typname like '%season%';

SELECT a.attname as column_name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_type t ON t.typrelid = c.oid
WHERE typname like 'season_stats';

select * from edges