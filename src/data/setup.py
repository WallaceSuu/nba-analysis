import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.nba_data_collector import ComprehensiveNBADataCollector
from data.db_config import check_table_schema, recreate_tables
import logging
import time
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_database():
    """Set up the comprehensive NBA analytics database with all necessary data"""
    
    logger.info("Starting comprehensive NBA database setup...")
    
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
    
    # Initialize the comprehensive data collector
    collector = ComprehensiveNBADataCollector()
    collector.check_database_state()
    
    # Process all data collection
    logger.info("Starting comprehensive data collection...")
    collector.process_all_data('2024-25')
    
    # Final status check
    collector.check_database_state()
    logger.info("Comprehensive NBA database setup complete!")

def setup_contract_data():
    """Set up contract data collection (separate due to rate limiting)"""
    logger.info("Starting contract data collection...")
    
    collector = ComprehensiveNBADataCollector()
    
    # Get all players from database
    try:
        with collector.connection.cursor() as cur:
            cur.execute("SELECT player_id, full_name FROM players WHERE is_active = TRUE")
            players = cur.fetchall()
        
        logger.info(f"Found {len(players)} active players for contract data collection")
        
        # Process contract data for each player (with significant delays)
        for i, (player_id, full_name) in enumerate(players):
            logger.info(f"Processing contract data for {full_name} ({i+1}/{len(players)})")
            
            try:
                # Try multiple sources for contract data
                contract_data = collector.get_player_contract_data(full_name)
                if contract_data:
                    # Save contract data to database
                    collector._save_contract_data_to_db(player_id, contract_data, '2024-25')
                
                # Also try Basketball Reference
                salary_data = collector.get_basketball_reference_salary(full_name)
                if salary_data:
                    # Save salary data to database
                    collector._save_salary_data_to_db(player_id, salary_data)
                
                # Significant delay between players to avoid rate limiting
                delay = random.uniform(10, 20)
                logger.info(f"Waiting {delay:.2f}s before next player...")
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error processing contract data for {full_name}: {e}")
                continue
        
        logger.info("Contract data collection complete!")
        
    except Exception as e:
        logger.error(f"Error in contract data collection: {e}")

def setup_value_analysis():
    """Set up player value analysis calculations"""
    logger.info("Starting player value analysis...")
    
    collector = ComprehensiveNBADataCollector()
    
    try:
        with collector.connection.cursor() as cur:
            # Get all players with both stats and salary data
            cur.execute("""
                SELECT p.player_id, p.full_name, p.position, 
                       pss.points_per_game, pss.rebounds_per_game, pss.assists_per_game,
                       pss.steals_per_game, pss.blocks_per_game, pss.turnovers_per_game,
                       pss.minutes_per_game, pss.field_goal_percentage, pss.three_point_percentage,
                       pss.free_throw_percentage, pss.plus_minus, pss.player_efficiency_rating,
                       pss.true_shooting_percentage, pss.usage_rate, pss.win_shares,
                       pss.value_over_replacement_player, pc.annual_salary
                FROM players p
                LEFT JOIN player_season_stats pss ON p.player_id = pss.player_id AND pss.season_id = '2024-25'
                LEFT JOIN player_contracts pc ON p.player_id = pc.player_id AND pc.season_id = '2024-25'
                WHERE p.is_active = TRUE
            """)
            players = cur.fetchall()
        
        logger.info(f"Found {len(players)} players for value analysis")
        
        # Calculate value metrics for each player
        for player_data in players:
            try:
                value_metrics = collector._calculate_player_value(player_data)
                if value_metrics:
                    collector._save_value_analysis_to_db(value_metrics)
                
            except Exception as e:
                logger.error(f"Error calculating value for player {player_data[1]}: {e}")
                continue
        
        logger.info("Player value analysis complete!")
        
    except Exception as e:
        logger.error(f"Error in value analysis: {e}")

if __name__ == "__main__":
    # Run the main setup
    setup_database()
    
    # Uncomment the following lines to run additional data collection
    # (These are separated due to rate limiting and time constraints)
    
    # setup_contract_data()  # Uncomment to collect contract data
    # setup_value_analysis()  # Uncomment to run value analysis 