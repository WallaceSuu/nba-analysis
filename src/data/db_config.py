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
            
            # Games table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id VARCHAR(20) PRIMARY KEY,
                    season_id INTEGER,
                    game_date DATE,
                    home_team_id INTEGER REFERENCES teams(team_id),
                    away_team_id INTEGER REFERENCES teams(team_id),
                    home_team_score INTEGER,
                    away_team_score INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Player stats table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_stats (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(20) REFERENCES games(game_id),
                    player_id INTEGER REFERENCES players(player_id),
                    team_id INTEGER REFERENCES teams(team_id),
                    points INTEGER,
                    rebounds INTEGER,
                    assists INTEGER,
                    steals INTEGER,
                    blocks INTEGER,
                    turnovers INTEGER,
                    minutes_played INTEGER,
                    field_goals_made INTEGER,
                    field_goals_attempted INTEGER,
                    three_pointers_made INTEGER,
                    three_pointers_attempted INTEGER,
                    free_throws_made INTEGER,
                    free_throws_attempted INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

if __name__ == "__main__":
    create_tables() 