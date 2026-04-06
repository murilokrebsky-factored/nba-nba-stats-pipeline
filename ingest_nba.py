import requests
import pandas as pd
from datetime import datetime, timedelta

def get_espn_scoreboard(date_str=None):
    # If no date is provided, it defaults to today. 
    # For dbt practice, we want yesterday's finished games!
    if not date_str:
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%Y%m%d')
    
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    
    print(f"🏀 Fetching NBA Scoreboard for date: {date_str}...")
    response = requests.get(url)
    data = response.json()
    
    games_list = []
    events = data.get('events', [])
    
    for event in events:
        status_info = event.get('status', {})
        # Only grab games that are 'Final' so we have scores!
        status_desc = status_info.get('type', {}).get('description')
        
        competition = event.get('competitions', [{}])[0]
        competitors = competition.get('competitors', [])
        
        home_team = next((team for team in competitors if team.get('homeAway') == 'home'), {})
        away_team = next((team for team in competitors if team.get('homeAway') == 'away'), {})

        games_list.append({
            "game_id": event.get('id'),
            "game_date": event.get('date'),
            "status": status_desc,
            "venue": event.get('venue', {}).get('fullName'),
            "home_team": home_team.get('team', {}).get('displayName'),
            "home_score": home_team.get('score'),
            "away_team": away_team.get('team', {}).get('displayName'),
            "away_score": away_team.get('score')
        })

    return pd.DataFrame(games_list)

if __name__ == "__main__":
    # We fetch YESTERDAY (20260405) to get completed games
    df = get_espn_scoreboard() 
    
    if not df.empty:
        print(df[['home_team', 'home_score', 'away_team', 'away_score']])
        df.to_csv("nba_yesterday_results.csv", index=False)
        print(f"✅ Success! Captured {len(df)} completed games.")
    else:
        print("⚠️ No completed games found for yesterday.")