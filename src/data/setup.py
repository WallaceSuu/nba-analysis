import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.nba_data_collector import NBADataCollector
from data.db_config import check_table_schema, recreate_tables
import logging
import time
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_database():
    """Set up the NBA analytics database with all necessary data"""
    
    logger.info("Starting NBA database setup...")
    
    # Check and recreate database schema if needed
    try:
        check_table_schema()
        logger.info("Database schema verified")
    except Exception as e:
        logger.warning(f"Schema issue detected: {e}")
        logger.info("Recreating database tables...")
        try:
            recreate_tables()
            logger.info("Database tables recreated")
        except Exception as e2:
            logger.error(f"Failed to recreate tables: {e2}")
            return
    
    collector = NBADataCollector()
    collector.check_database_state()
    
    # Fetch and save players
    logger.info("Fetching players...")
    players = collector.get_all_players()
    if players is not None:
        logger.info(f"Retrieved {len(players)} players")
        collector.save_players_to_db(players)
    
    # Fetch and save teams
    logger.info("Fetching teams...")
    teams = collector.get_all_teams()
    if teams is not None:
        logger.info(f"Retrieved {len(teams)} teams")
        collector.save_teams_to_db(teams)
    
    collector.check_database_state()
    
    # Fetch and save games for each team
    logger.info("Fetching games for all teams...")
    total_games = 0
    for i, team_id in enumerate(teams['id'], 1):
        logger.info(f"Processing team {i}/{len(teams)} (ID: {team_id})...")
        games = collector.get_team_games(team_id)
        if games is not None:
            collector.save_games_to_db(games)
            total_games += len(games)
            logger.info(f"Saved {len(games)} games (Total: {total_games})")
        else:
            logger.warning(f"No games for team {team_id}")
        time.sleep(random.uniform(5, 8))
    
    logger.info(f"Total games processed: {total_games}")
    
    # Fetch and save player stats in batches
    logger.info("Fetching player statistics...")
    if players is not None:
        player_ids = players['PERSON_ID'].tolist()
        logger.info(f"Processing stats for {len(player_ids)} players...")
        collector.fetch_all_player_stats(player_ids)

    # Final status
    collector.check_database_state()
    logger.info("NBA database setup complete!")

if __name__ == "__main__":
    setup_database() 