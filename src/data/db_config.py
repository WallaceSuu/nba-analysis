import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'nba_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1,  # minconn
    10, # maxconn
    **DB_CONFIG
)

def get_connection():
    """Get a connection from the pool"""
    return connection_pool.getconn()

def release_connection(conn):
    """Release a connection back to the pool"""
    connection_pool.putconn(conn)

def create_tables():
    """Create necessary database tables"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Players table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    player_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(100),
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    is_active BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Teams table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    team_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(100),
                    abbreviation VARCHAR(10),
                    nickname VARCHAR(50),
                    city VARCHAR(50),
                    state VARCHAR(50),
                    year_founded INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Games table - Updated to match NBA API structure
            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(20) UNIQUE NOT NULL,
                    season_id VARCHAR(10),
                    team_id INTEGER REFERENCES teams(team_id),
                    team_abbreviation VARCHAR(10),
                    team_name VARCHAR(100),
                    game_date DATE,
                    matchup VARCHAR(50),
                    win_loss VARCHAR(5),
                    minutes_played INTEGER,
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )   
            """)
            
            # Player stats table - Updated to match NBA API structure
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_stats (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(20),
                    player_id INTEGER REFERENCES players(player_id),
                    team_id INTEGER REFERENCES teams(team_id),
                    team_abbreviation VARCHAR(10),
                    team_name VARCHAR(100),
                    game_date DATE,
                    matchup VARCHAR(50),
                    win_loss VARCHAR(5),
                    minutes_played INTEGER,
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(game_id, player_id)
                )
            """)
            
            conn.commit()
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
            cur.execute("DROP TABLE IF EXISTS player_stats CASCADE")
            cur.execute("DROP TABLE IF EXISTS games CASCADE")
            cur.execute("DROP TABLE IF EXISTS teams CASCADE")
            cur.execute("DROP TABLE IF EXISTS players CASCADE")
            conn.commit()
            print("All tables dropped successfully")
    except Exception as e:
        conn.rollback()
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
                AND table_name IN ('players', 'teams', 'games', 'player_stats')
            """)
            existing_tables = [row[0] for row in cur.fetchall()]
            print(f"Existing tables: {existing_tables}")
            
            # Check games table schema
            if 'games' in existing_tables:
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'games' 
                    ORDER BY ordinal_position
                """)
                games_columns = cur.fetchall()
                print(f"Games table columns: {games_columns}")
            
            # Check player_stats table schema
            if 'player_stats' in existing_tables:
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'player_stats' 
                    ORDER BY ordinal_position
                """)
                player_stats_columns = cur.fetchall()
                print(f"Player_stats table columns: {player_stats_columns}")
                
    except Exception as e:
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