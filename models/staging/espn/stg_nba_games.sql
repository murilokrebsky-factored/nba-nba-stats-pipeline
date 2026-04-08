with source as (
    -- This points to raw.nba_raw.raw_nba_scoreboard via your sources.yml (name.table_name)
    select * except(venue) from {{ source('nba_raw', 'raw_nba_scoreboard') }}
),

deduplicated as (
    select 
        *,
        -- This creates a 'rank' for every row with the same game_id
        row_number() over (
            partition by game_id 
            order by ingested_at desc -- or any timestamp/ID that shows which came last
        ) as row_num
    from source
),

renamed as (
    select
        game_id,
        to_timestamp(game_date, "yyyy-MM-dd'T'HH:mm'Z'") as game_at,
        home_team,
        cast(home_score as int) as home_score,
        away_team,
        cast(away_score as int) as away_score,
        -- Calculate the winner here so you don't have to do it later
        case 
            when cast(home_score as int) > cast(away_score as int) then home_team
            when cast(home_score as int) < cast(away_score as int) then away_team
        end as winner_name
    from deduplicated
    where row_num = 1
)

select * from renamed