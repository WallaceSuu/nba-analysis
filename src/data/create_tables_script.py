#!/usr/bin/env python3
"""
Simple script to create all database tables and verify they exist.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_config import create_tables, get_connection, release_connection

def main():
    print("Creating database tables...")
    
    try:
        # Create all tables
        create_tables()
        print("✅ Tables created successfully!")
        
        # Verify tables exist
        conn = get_connection()
        cur = conn.cursor()
        
        # Get all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        
        tables = cur.fetchall()
        print(f"\n📋 Found {len(tables)} tables in database:")
        
        expected_tables = [
            'contract_history', 'games', 'league_revenue', 'nba_salary_cap',
            'player_awards', 'player_career_stats', 'player_contracts',
            'player_game_stats', 'player_season_stats', 'player_value_analysis',
            'players', 'seasons', 'team_revenue', 'team_salary_commitments', 'teams'
        ]
        
        found_tables = [table[0] for table in tables]
        
        for table in expected_tables:
            if table in found_tables:
                print(f"  ✅ {table}")
            else:
                print(f"  ❌ {table} - MISSING")
        
        # Check for any extra tables
        extra_tables = [table[0] for table in tables if table[0] not in expected_tables]
        if extra_tables:
            print(f"\n📝 Extra tables found: {extra_tables}")
        
        cur.close()
        release_connection(conn)
        
        print(f"\n🎉 Database setup complete! {len(found_tables)} tables created.")
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 