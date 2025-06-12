from db_config import create_tables
from nba_data_collector import NBADataCollector
from test_connection import test_connection
import time

def setup_database():
    print("Starting NBA Analytics Database Setup...")
     
    # Test db connection
    print("\nTesting database connection...")
    if not test_connection():
        print("Database connection failed. Please check your configuration.")
        return False
    
    # Create db tables
    print("\n Creating database tables...")
    try:
        create_tables()
        print("Database tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False
    
    # Initialize data collector
    print("\nInitializing data collector...")
    collector = NBADataCollector()
    
    # Fetch and save players
    print("\nFetching players data...")
    players = collector.get_all_players()
    if players is not None:
        collector.save_players_to_db(players)
        print(f"Saved {len(players)} players to database")
    else:
        print("Failed to fetch players")
        return False
    
    # Fetch and save teams
    print("\nFetching teams data")
    teams = collector.get_all_teams()
    if teams is not None:
        collector.save_teams_to_db(teams)
        print(f"Saved {len(teams)} teams to database")
    else:
        print("Failed to fetch teams")
        return False
    
    # Fetch and save games
    print("\nFetching games data...")
    for team_id in teams['id']:
        print(f"   Fetching games for team {team_id}...")
        games = collector.get_team_games(team_id)
        if games is not None:
            collector.save_games_to_db(games)
            print(f"Saved {len(games)} games for team {team_id}")
        time.sleep(1)  # Rate limiting
    
    # Fetch and save player stats
    print("\nFetching player statistics...")
    for player_id in players['PERSON_ID']:
        print(f"Fetching stats for player {player_id}...")
        stats = collector.get_player_games(player_id)
        if stats is not None:
            collector.save_player_stats_to_db(stats)
            print(f"Saved {len(stats)} games for player {player_id}")
        time.sleep(1)  # Rate limiting
    
    print("\nSetup completed successfully!")
    return True

if __name__ == "__main__":
    setup_database() 