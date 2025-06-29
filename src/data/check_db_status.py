#!/usr/bin/env python3
"""
Check current database status and record counts.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_config import get_connection, release_connection

def check_database_status():
    """Check the current state of all tables"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            tables = [
                'seasons', 'teams', 'players', 'nba_salary_cap', 
                'player_season_stats', 'player_career_stats', 'player_awards',
                'player_contracts', 'contract_history', 'team_salary_commitments',
                'team_revenue', 'league_revenue', 'player_value_analysis',
                'games', 'player_game_stats'
            ]
            
            print("📊 Current Database Status:")
            print("=" * 50)
            
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    print(f"  {table:25} : {count:5} records")
                except Exception as e:
                    print(f"  {table:25} : ERROR - {e}")
            
            print("=" * 50)
            
            # Check if setup is still running by looking for recent activity
            if count > 0:
                print("✅ Data collection appears to be working!")
            else:
                print("❌ No data found - setup may be stuck or failed")
                
    except Exception as e:
        print(f"❌ Error checking database: {e}")
    finally:
        release_connection(conn)

if __name__ == "__main__":
    check_database_status() 