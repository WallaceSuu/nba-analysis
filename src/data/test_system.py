#!/usr/bin/env python3
"""
Test script for the NBA Analytics System
This script verifies that all components are working correctly.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.nba_data_collector import ComprehensiveNBADataCollector
from data.db_config import check_table_schema, get_connection
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test database connection"""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.info(f"Database connection successful. PostgreSQL version: {version[0]}")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def test_nba_api_connection():
    """Test NBA API connection"""
    try:
        collector = ComprehensiveNBADataCollector()
        
        # Test getting teams
        teams = collector.get_all_teams()
        if teams is not None and not teams.empty:
            logger.info(f"NBA API teams test successful. Found {len(teams)} teams.")
            return True
        else:
            logger.error("NBA API teams test failed - no teams returned")
            return False
    except Exception as e:
        logger.error(f"NBA API connection failed: {e}")
        return False

def test_player_data_collection():
    """Test player data collection"""
    try:
        collector = ComprehensiveNBADataCollector()
        
        # Test getting a few players
        players = collector.get_all_players()
        if players is not None and not players.empty:
            logger.info(f"Player data collection test successful. Found {len(players)} players.")
            
            # Test getting detailed info for one player
            if len(players) > 0:
                first_player = players.iloc[0]
                detailed_info = collector.get_player_detailed_info(first_player['PERSON_ID'])
                if detailed_info:
                    logger.info(f"Detailed player info test successful for {first_player['DISPLAY_FIRST_LAST']}")
                else:
                    logger.warning("Detailed player info test returned no data")
            
            return True
        else:
            logger.error("Player data collection test failed - no players returned")
            return False
    except Exception as e:
        logger.error(f"Player data collection failed: {e}")
        return False

def test_salary_cap_data():
    """Test salary cap data retrieval"""
    try:
        collector = ComprehensiveNBADataCollector()
        
        salary_cap_data = collector.get_nba_salary_cap_data('2024-25')
        if salary_cap_data:
            logger.info(f"Salary cap data test successful. Salary cap: ${salary_cap_data.get('salary_cap', 0):,}")
            return True
        else:
            logger.error("Salary cap data test failed - no data returned")
            return False
    except Exception as e:
        logger.error(f"Salary cap data test failed: {e}")
        return False

def test_database_schema():
    """Test database schema"""
    try:
        check_table_schema()
        logger.info("Database schema test successful")
        return True
    except Exception as e:
        logger.error(f"Database schema test failed: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    logger.info("Starting NBA Analytics System tests...")
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Database Schema", test_database_schema),
        ("NBA API Connection", test_nba_api_connection),
        ("Player Data Collection", test_player_data_collection),
        ("Salary Cap Data", test_salary_cap_data),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} Test ---")
        try:
            result = test_func()
            results.append((test_name, result))
            if result:
                logger.info(f"✅ {test_name} test PASSED")
            else:
                logger.error(f"❌ {test_name} test FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name} test FAILED with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! The system is ready to use.")
        return True
    else:
        logger.error("⚠️  Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1) 