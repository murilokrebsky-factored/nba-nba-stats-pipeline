with games as (
    select * from {{ ref('stg_nba_games') }}
),

teams_mapping as (
    select * from {{ ref('nba_teams_mapping') }}
),

unpivoted as (
    -- The HOME side
    select
        game_id,
        game_at,
        home_team as team_name,
        away_team as opponent_name, -- Added this for the Division lookup later!
        home_score as team_score,
        away_score as opponent_score,
        case when winner_name = home_team then 1 else 0 end as is_win,
        'home' as side
    from games

    union all

    -- The AWAY sideß
    select
        game_id,
        game_at,
        away_team as team_name,
        home_team as opponent_name, -- Added this for the Division lookup later!
        away_score as team_score,
        home_score as opponent_score,
        case when winner_name = away_team then 1 else 0 end as is_win,
        'away' as side
    from games
)

select 
    u.*,
    m.conference,
    m.division,
    -- Get the OPPONENT'S division so we can calculate "Division Record"
    opp.division as opponent_division
from unpivoted u
left join teams_mapping m on u.team_name = m.team_name
left join teams_mapping opp on u.opponent_name = opp.team_name