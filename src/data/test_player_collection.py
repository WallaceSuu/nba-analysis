#!/usr/bin/env python3
"""
Test script to debug player collection specifically.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nba_data_collector import ComprehensiveNBADataCollector
import logging

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('player_collection_debug.log')
    ]
)
logger = logging.getLogger(__name__)

def test_player_collection():
    """Test just the player collection part"""
    logger.info("🚀 Starting player collection test...")
    
    try:
        collector = ComprehensiveNBADataCollector()
        
        # Test 1: Get all players
        logger.info("📊 Step 1: Fetching all players from NBA API...")
        players = collector.get_all_players()
        
        if players is None:
            logger.error("❌ Failed to get players from NBA API")
            return False
        
        if players.empty:
            logger.error("❌ Players DataFrame is empty")
            return False
        
        logger.info(f"✅ Successfully fetched {len(players)} players")
        logger.info(f"📋 Players columns: {players.columns.tolist()}")
        
        # Test 2: Save players to database
        logger.info("💾 Step 2: Saving players to database...")
        collector.save_players_to_db(players)
        
        logger.info("✅ Player collection test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in player collection test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_player_collection()
    if success:
        print("🎉 Player collection test PASSED")
    else:
        print("❌ Player collection test FAILED") 