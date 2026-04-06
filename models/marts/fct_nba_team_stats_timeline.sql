with team_results as (
    select * from {{ ref('int_nba_team_results') }}
),

windowed_stats as (
    select
        *,
        -- 1) Season Record
        sum(is_win) over (partition by team_name order by game_at) as running_wins,
        sum(case when is_win = 0 then 1 else 0 end) over (partition by team_name order by game_at) as running_losses,
        
        -- 2) Home Record
        sum(case when side = 'home' and is_win = 1 then 1 else 0 end) over (partition by team_name order by game_at) as running_home_wins,
        sum(case when side = 'home' and is_win = 0 then 1 else 0 end) over (partition by team_name order by game_at) as running_home_losses,

        -- 3) Away Record
        sum(case when side = 'away' and is_win = 1 then 1 else 0 end) over (partition by team_name order by game_at) as running_away_wins,
        sum(case when side = 'away' and is_win = 0 then 1 else 0 end) over (partition by team_name order by game_at) as running_away_losses,

        -- 4) Division Record (Wins/Losses against teams in SAME division)
        sum(case when division = opponent_division and is_win = 1 then 1 else 0 end) over (partition by team_name order by game_at) as running_div_wins,
        sum(case when division = opponent_division and is_win = 0 then 1 else 0 end) over (partition by team_name order by game_at) as running_div_losses,

        -- 5) Streak Logic Helper
        row_number() over (partition by team_name order by game_at) - 
        row_number() over (partition by team_name, is_win order by game_at) as streak_group

    from team_results
),

final_streak as (
    select 
        *,
        row_number() over (partition by team_name, streak_group order by game_at) as streak_count
    from windowed_stats
)

select 
    team_name,
    conference,
    division,
    game_at,
    -- Display Strings
    concat(cast(running_wins as string), '-', cast(running_losses as string)) as season_record,
    concat(cast(running_home_wins as string), '-', cast(running_home_losses as string)) as home_record,
    concat(cast(running_away_wins as string), '-', cast(running_away_losses as string)) as away_record,
    concat(cast(running_div_wins as string), '-', cast(running_div_losses as string)) as division_record,
    
    -- Current Streak (e.g. W5, L2)
    concat(case when is_win = 1 then 'W' else 'L' end, cast(streak_count as string)) as streak,
    
    -- Sorting metric
    round(cast(running_wins as float) / nullif((running_wins + running_losses), 0), 3) as win_pct
from final_streak
order by game_at desc, win_pct desc