import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'nba_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Connection pool
_connection_pool = None

def get_connection_pool():
    """Get or create the connection pool"""
    global _connection_pool
    if _connection_pool is None:
        try:
            _connection_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=20,  # Increased max connections
                **DB_CONFIG
            )
            logger.info("✅ Database connection pool created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create connection pool: {e}")
            raise
    return _connection_pool

def get_connection():
    """Get a connection from the pool"""
    try:
        pool = get_connection_pool()
        conn = pool.getconn()
        if conn is None:
            raise Exception("Failed to get connection from pool")
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to get database connection: {e}")
        raise

def release_connection(conn):
    """Release a connection back to the pool"""
    try:
        if conn and _connection_pool:
            _connection_pool.putconn(conn)
    except Exception as e:
        logger.error(f"❌ Error releasing connection: {e}")

def close_connection_pool():
    """Close the connection pool"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("✅ Database connection pool closed")

def create_tables():
    """Create comprehensive NBA analytics database tables"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Set the search path to public schema
            cur.execute("SET search_path TO public;")
            
            # Teams table (enhanced)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    team_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(100) NOT NULL,
                    abbreviation VARCHAR(10) UNIQUE NOT NULL,
                    nickname VARCHAR(50),
                    city VARCHAR(50),
                    state VARCHAR(50),
                    year_founded INTEGER,
                    conference VARCHAR(20),
                    division VARCHAR(20),
                    arena VARCHAR(100),
                    arena_capacity INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Players table (comprehensive)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    player_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(100) NOT NULL,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    is_active BOOLEAN DEFAULT TRUE,
                    birth_date DATE,
                    age INTEGER,
                    height_feet INTEGER,
                    height_inches INTEGER,
                    height_cm INTEGER,
                    weight_lbs INTEGER,
                    weight_kg DECIMAL(5,2),
                    position VARCHAR(10),
                    position_secondary VARCHAR(10),
                    draft_year INTEGER,
                    draft_round INTEGER,
                    draft_number INTEGER,
                    draft_team_id INTEGER REFERENCES teams(team_id),
                    college VARCHAR(100),
                    country VARCHAR(50),
                    experience_years INTEGER,
                    jersey_number INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Player Awards table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_awards (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players(player_id),
                    season VARCHAR(10),
                    award_type VARCHAR(50) NOT NULL,
                    award_name VARCHAR(100) NOT NULL,
                    team_id INTEGER REFERENCES teams(team_id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(player_id, season, award_name)
                )
            """)
            
            # Seasons table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seasons (
                    season_id VARCHAR(10) PRIMARY KEY,
                    season_name VARCHAR(20) NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    is_current BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Games table (enhanced)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id VARCHAR(20) PRIMARY KEY,
                    season_id VARCHAR(10) REFERENCES seasons(season_id),
                    game_date DATE NOT NULL,
                    home_team_id INTEGER REFERENCES teams(team_id),
                    away_team_id INTEGER REFERENCES teams(team_id),
                    home_score INTEGER,
                    away_score INTEGER,
                    home_win BOOLEAN,
                    game_type VARCHAR(20) DEFAULT 'Regular Season',
                    attendance INTEGER,
                    arena VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Player Game Stats table (comprehensive)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_game_stats (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(20) REFERENCES games(game_id),
                    player_id INTEGER REFERENCES players(player_id),
                    team_id INTEGER REFERENCES teams(team_id),
                    is_starter BOOLEAN DEFAULT FALSE,
                    minutes_played INTEGER,
                    minutes_played_decimal DECIMAL(4,2),
                    points INTEGER,
                    field_goals_made INTEGER,
                    field_goals_attempted INTEGER,
                    field_goal_percentage DECIMAL(5,3),
                    three_pointers_made INTEGER,
                    three_pointers_attempted INTEGER,
                    three_point_percentage DECIMAL(5,3),
                    free_throws_made INTEGER,
                    free_throws_attempted INTEGER,
                    free_throw_percentage DECIMAL(5,3),
                    offensive_rebounds INTEGER,
                    defensive_rebounds INTEGER,
                    total_rebounds INTEGER,
                    assists INTEGER,
                    steals INTEGER,
                    blocks INTEGER,
                    turnovers INTEGER,
                    personal_fouls INTEGER,
                    plus_minus DECIMAL(5,1),
                    true_shooting_percentage DECIMAL(5,3),
                    effective_field_goal_percentage DECIMAL(5,3),
                    offensive_rating DECIMAL(6,2),
                    defensive_rating DECIMAL(6,2),
                    net_rating DECIMAL(6,2),
                    usage_rate DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(game_id, player_id)
                )
            """)
            
            # Player Season Stats table (aggregated)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_season_stats (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players(player_id),
                    team_id INTEGER REFERENCES teams(team_id),
                    season_id VARCHAR(10) REFERENCES seasons(season_id),
                    games_played INTEGER,
                    games_started INTEGER,
                    minutes_per_game DECIMAL(4,2),
                    points_per_game DECIMAL(5,2),
                    rebounds_per_game DECIMAL(4,2),
                    offensive_rebounds_per_game DECIMAL(4,2),
                    defensive_rebounds_per_game DECIMAL(4,2),
                    assists_per_game DECIMAL(4,2),
                    steals_per_game DECIMAL(4,2),
                    blocks_per_game DECIMAL(4,2),
                    turnovers_per_game DECIMAL(4,2),
                    personal_fouls_per_game DECIMAL(4,2),
                    field_goal_percentage DECIMAL(5,3),
                    three_point_percentage DECIMAL(5,3),
                    free_throw_percentage DECIMAL(5,3),
                    true_shooting_percentage DECIMAL(5,3),
                    effective_field_goal_percentage DECIMAL(5,3),
                    offensive_rating DECIMAL(6,2),
                    defensive_rating DECIMAL(6,2),
                    net_rating DECIMAL(6,2),
                    usage_rate DECIMAL(5,2),
                    player_efficiency_rating DECIMAL(5,2),
                    win_shares DECIMAL(5,2),
                    win_shares_per_48 DECIMAL(5,3),
                    box_plus_minus DECIMAL(5,2),
                    value_over_replacement_player DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(player_id, season_id)
                )
            """)
            
            # Player Career Stats table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_career_stats (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players(player_id),
                    total_games INTEGER,
                    total_games_started INTEGER,
                    total_minutes INTEGER,
                    total_points INTEGER,
                    total_rebounds INTEGER,
                    total_assists INTEGER,
                    total_steals INTEGER,
                    total_blocks INTEGER,
                    total_turnovers INTEGER,
                    total_personal_fouls INTEGER,
                    career_points_per_game DECIMAL(5,2),
                    career_rebounds_per_game DECIMAL(4,2),
                    career_assists_per_game DECIMAL(4,2),
                    career_steals_per_game DECIMAL(4,2),
                    career_blocks_per_game DECIMAL(4,2),
                    career_field_goal_percentage DECIMAL(5,3),
                    career_three_point_percentage DECIMAL(5,3),
                    career_free_throw_percentage DECIMAL(5,3),
                    career_true_shooting_percentage DECIMAL(5,3),
                    career_effective_field_goal_percentage DECIMAL(5,3),
                    career_player_efficiency_rating DECIMAL(5,2),
                    career_win_shares DECIMAL(5,2),
                    career_win_shares_per_48 DECIMAL(5,3),
                    career_box_plus_minus DECIMAL(5,2),
                    career_value_over_replacement_player DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(player_id)
                )
            """)
            
            # Player Contracts table (comprehensive)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_contracts (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players(player_id),
                    team_id INTEGER REFERENCES teams(team_id),
                    contract_type VARCHAR(50),
                    contract_status VARCHAR(20),
                    start_season VARCHAR(10),
                    end_season VARCHAR(10),
                    total_value DECIMAL(12,2),
                    annual_average DECIMAL(10,2),
                    guaranteed_amount DECIMAL(12,2),
                    non_guaranteed_amount DECIMAL(12,2),
                    team_option BOOLEAN DEFAULT FALSE,
                    player_option BOOLEAN DEFAULT FALSE,
                    early_termination_option BOOLEAN DEFAULT FALSE,
                    trade_kicker BOOLEAN DEFAULT FALSE,
                    no_trade_clause BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Contract History table (annual breakdown)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS contract_history (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players(player_id),
                    team_id INTEGER REFERENCES teams(team_id),
                    season VARCHAR(10),
                    salary DECIMAL(10,2),
                    cap_hit DECIMAL(10,2),
                    dead_cap DECIMAL(10,2),
                    base_salary DECIMAL(10,2),
                    signing_bonus DECIMAL(10,2),
                    roster_bonus DECIMAL(10,2),
                    performance_bonus DECIMAL(10,2),
                    other_bonus DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(player_id, season)
                )
            """)
            
            # NBA Salary Cap table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nba_salary_cap (
                    id SERIAL PRIMARY KEY,
                    season VARCHAR(10) UNIQUE NOT NULL,
                    salary_cap DECIMAL(12,2),
                    luxury_tax_threshold DECIMAL(12,2),
                    apron_threshold DECIMAL(12,2),
                    minimum_team_salary DECIMAL(12,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Team Salary Commitments table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS team_salary_commitments (
                    id SERIAL PRIMARY KEY,
                    team_id INTEGER REFERENCES teams(team_id),
                    season VARCHAR(10),
                    total_salary DECIMAL(12,2),
                    active_roster_salary DECIMAL(12,2),
                    dead_cap DECIMAL(12,2),
                    luxury_tax_payment DECIMAL(12,2),
                    salary_cap_space DECIMAL(12,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(team_id, season)
                )
            """)
            
            # Team Revenue table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS team_revenue (
                    id SERIAL PRIMARY KEY,
                    team_id INTEGER REFERENCES teams(team_id),
                    season VARCHAR(10),
                    total_revenue DECIMAL(15,2),
                    gate_revenue DECIMAL(12,2),
                    media_revenue DECIMAL(12,2),
                    sponsorship_revenue DECIMAL(12,2),
                    merchandise_revenue DECIMAL(12,2),
                    other_revenue DECIMAL(12,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(team_id, season)
                )
            """)
            
            # League Revenue table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS league_revenue (
                    id SERIAL PRIMARY KEY,
                    season VARCHAR(10) UNIQUE NOT NULL,
                    total_revenue DECIMAL(15,2),
                    basketball_related_income DECIMAL(15,2),
                    national_tv_revenue DECIMAL(15,2),
                    international_revenue DECIMAL(15,2),
                    merchandise_revenue DECIMAL(15,2),
                    other_revenue DECIMAL(15,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Player Value Analysis table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_value_analysis (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players(player_id),
                    season VARCHAR(10),
                    team_id INTEGER REFERENCES teams(team_id),
                    salary DECIMAL(10,2),
                    points_per_dollar DECIMAL(8,4),
                    win_shares_per_dollar DECIMAL(8,4),
                    vorp_per_dollar DECIMAL(8,4),
                    efficiency_rating DECIMAL(5,2),
                    value_score DECIMAL(5,2),
                    contract_efficiency DECIMAL(5,2),
                    market_value DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(player_id, season)
                )
            """)
            
            conn.commit()
            print("All tables created successfully!")
            
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def drop_all_tables():
    """Drop all tables to recreate them with the correct schema"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Drop tables in reverse order of dependencies
            tables_to_drop = [
                'player_value_analysis',
                'league_revenue',
                'team_revenue',
                'team_salary_commitments',
                'nba_salary_cap',
                'contract_history',
                'player_contracts',
                'player_career_stats',
                'player_season_stats',
                'player_game_stats',
                'games',
                'player_awards',
                'seasons',
                'players',
                'teams'
            ]
            
            for table in tables_to_drop:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            
            conn.commit()
            print("All tables dropped successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error dropping tables: {e}")
        raise e
    finally:
        release_connection(conn)

def check_table_schema():
    """Check if tables exist and have the correct schema"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check if tables exist
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """)
            existing_tables = [row[0] for row in cur.fetchall()]
            print(f"Existing tables: {existing_tables}")
            
            # Check table counts
            for table in existing_tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"{table}: {count} records")
                
    except Exception as e:
        print(f"Error checking schema: {e}")
        raise e
    finally:
        release_connection(conn)

def recreate_tables():
    """Drop and recreate all tables with the correct schema"""
    print("Dropping all existing tables...")
    drop_all_tables()
    print("Creating tables with correct schema...")
    create_tables()
    print("Tables recreated successfully!")

if __name__ == "__main__":
    create_tables() 