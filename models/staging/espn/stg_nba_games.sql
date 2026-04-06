with source as (
    -- This points to raw.nba_raw.raw_nba_scoreboard via your sources.yml (name.table_name)
    select * from {{ source('nba_raw', 'raw_nba_scoreboard') }}
),

renamed as (
    select
        game_id,
        to_timestamp(game_date, "yyyy-MM-dd'T'HH:mm'Z'") as game_at,
        home_team,
        cast(home_score as int) as home_score,
        away_team,
        cast(away_score as int) as away_score,
        venue,
        -- Calculate the winner here so you don't have to do it later
        case 
            when cast(home_score as int) > cast(away_score as int) then home_team
            when cast(home_score as int) < cast(away_score as int) then away_team
        end as winner_name
    from source
)

select * from renamed