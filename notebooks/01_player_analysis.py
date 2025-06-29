#!/usr/bin/env python3
"""
NBA Player Analysis Script
This script demonstrates the comprehensive NBA analytics system for analyzing player performance, value, and efficiency.

Features:
- Player performance metrics (basic and advanced)
- Salary and contract analysis
- Value per dollar calculations
- Position-based comparisons
- Efficiency ratings
"""

import sys
import os
sys.path.append(os.path.join(os.getcwd(), '..', 'src'))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from data.nba_data_collector import ComprehensiveNBADataCollector
from data.db_config import get_connection

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Configure pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

def main():
    print("NBA Player Analysis - 2024-25 Season")
    print("=" * 50)
    
    # 1. Initialize the System
    print("\n1. Initializing the system...")
    collector = ComprehensiveNBADataCollector()
    collector.check_database_state()
    
    # 2. Load Player Data
    print("\n2. Loading player data...")
    conn = get_connection()
    
    players_query = """
    SELECT p.player_id, p.full_name, p.position, p.age, p.experience_years,
           pss.points_per_game, pss.rebounds_per_game, pss.assists_per_game,
           pss.steals_per_game, pss.blocks_per_game, pss.turnovers_per_game,
           pss.minutes_per_game, pss.field_goal_percentage, pss.three_point_percentage,
           pss.free_throw_percentage, pss.player_efficiency_rating,
           pss.true_shooting_percentage, pss.usage_rate, pss.win_shares,
           pss.value_over_replacement_player
    FROM players p
    LEFT JOIN player_season_stats pss ON p.player_id = pss.player_id 
        AND pss.season_id = '2024-25'
    WHERE p.is_active = TRUE
        AND pss.games_played >= 10  -- Only players with significant playing time
    ORDER BY pss.points_per_game DESC
    """
    
    players_df = pd.read_sql_query(players_query, conn)
    print(f"Loaded {len(players_df)} active players with significant playing time")
    
    if players_df.empty:
        print("No player data found. Please run the data collection first.")
        return
    
    # 3. Basic Player Statistics Analysis
    print("\n3. Analyzing basic player statistics...")
    stats_summary = players_df[['points_per_game', 'rebounds_per_game', 'assists_per_game', 
                               'steals_per_game', 'blocks_per_game', 'minutes_per_game']].describe()
    
    print("League Statistics Summary (2024-25 Season)")
    print("=" * 50)
    print(stats_summary.round(2))
    
    # 4. Top Scorers Analysis
    print("\n4. Top 10 scorers:")
    top_scorers = players_df.nlargest(10, 'points_per_game')[['full_name', 'position', 'points_per_game', 'minutes_per_game']]
    print(top_scorers.round(2))
    
    # 5. Position Analysis
    print("\n5. Position analysis...")
    position_stats = players_df.groupby('position').agg({
        'points_per_game': ['mean', 'std'],
        'rebounds_per_game': ['mean', 'std'],
        'assists_per_game': ['mean', 'std'],
        'minutes_per_game': ['mean', 'std'],
        'player_efficiency_rating': ['mean', 'std']
    }).round(2)
    
    print("Performance by Position")
    print("=" * 40)
    print(position_stats)
    
    # 6. Advanced Metrics Analysis
    print("\n6. Loading value analysis data...")
    value_query = """
    SELECT p.full_name, p.position, pva.value_per_dollar, pva.efficiency_rating,
           pva.cost_efficiency_score, pva.total_team_improvement
    FROM player_value_analysis pva
    JOIN players p ON pva.player_id = p.player_id
    WHERE pva.season_id = '2024-25'
        AND pva.value_per_dollar > 0
    ORDER BY pva.value_per_dollar DESC
    """
    
    value_df = pd.read_sql_query(value_query, conn)
    
    if not value_df.empty:
        print(f"Loaded value analysis for {len(value_df)} players")
        print("\nTop 10 Most Valuable Players (Value per Dollar)")
        print("=" * 60)
        print(value_df.head(10).round(6))
    else:
        print("No value analysis data available. Run setup_value_analysis() first.")
    
    # 7. Shooting Efficiency Analysis
    print("\n7. Analyzing shooting efficiency...")
    shooting_data = players_df[['full_name', 'position', 'field_goal_percentage', 
                               'three_point_percentage', 'free_throw_percentage', 
                               'true_shooting_percentage']].dropna()
    
    if not shooting_data.empty:
        top_shooters = shooting_data.nlargest(10, 'true_shooting_percentage')
        print("Top 10 True Shooting Percentage:")
        print(top_shooters[['full_name', 'position', 'true_shooting_percentage']].round(3))
    
    # 8. Age and Experience Analysis
    print("\n8. Age and experience analysis...")
    age_data = players_df[['age', 'experience_years', 'points_per_game', 
                          'player_efficiency_rating', 'minutes_per_game']].dropna()
    
    if not age_data.empty:
        print(f"Average age: {age_data['age'].mean():.1f} years")
        print(f"Average experience: {age_data['experience_years'].mean():.1f} years")
        print(f"Age range: {age_data['age'].min():.0f} - {age_data['age'].max():.0f} years")
    
    # 9. Summary and Insights
    print("\n9. Summary and insights:")
    print("=" * 50)
    
    print(f"Total Players Analyzed: {len(players_df)}")
    print(f"Average Points Per Game: {players_df['points_per_game'].mean():.1f}")
    print(f"Average Player Efficiency Rating: {players_df['player_efficiency_rating'].mean():.1f}")
    print(f"Average Minutes Per Game: {players_df['minutes_per_game'].mean():.1f}")
    
    print(f"\nPosition Breakdown:")
    position_counts = players_df['position'].value_counts()
    for pos, count in position_counts.items():
        print(f"  {pos}: {count} players")
    
    print(f"\nAge Statistics:")
    print(f"  Average Age: {players_df['age'].mean():.1f} years")
    print(f"  Youngest Player: {players_df['age'].min():.0f} years")
    print(f"  Oldest Player: {players_df['age'].max():.0f} years")
    
    if not value_df.empty:
        print(f"\nValue Analysis:")
        print(f"  Average Value per Dollar: {value_df['value_per_dollar'].mean():.6f}")
        print(f"  Highest Value Player: {value_df.iloc[0]['full_name']} ({value_df.iloc[0]['value_per_dollar']:.6f})")
    
    print("\nKey Insights:")
    print("1. The system successfully collects and analyzes comprehensive NBA data")
    print("2. Player performance varies significantly by position")
    print("3. Age and experience show interesting patterns in player development")
    print("4. Advanced metrics provide deeper insights into player value")
    print("5. The value analysis system helps identify cost-effective players")
    
    # Close database connection
    conn.close()
    print("\nAnalysis complete! Database connection closed.")

if __name__ == "__main__":
    main() 