import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats
import os

def fetch_and_save_nba_data():
    print("🏀 Fetching 2024-25 Player Stats...")
    
    # Get stats for the current season
    stats = leaguedashplayerstats.LeagueDashPlayerStats(season='2024-25')
    df = stats.get_data_frames()[0]
    
    # Keep only the columns we actually want to play with for now
    cols_to_keep = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'AGE', 'GP', 'PTS', 'REB', 'AST', 'STL', 'BLK']
    df_clean = df[cols_to_keep]
    
    # Save to CSV locally first
    output_file = "raw_nba_player_stats.csv"
    df_clean.to_csv(output_file, index=False)
    
    print(f"✅ Success! Saved {len(df_clean)} players to {output_file}")

if __name__ == "__main__":
    fetch_and_save_nba_data()