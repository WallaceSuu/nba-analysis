from nba_api.stats.endpoints import commonallplayers, leaguegamefinder, playergamelog, playercareerstats
from nba_api.stats.static import teams
import pandas as pd
from datetime import datetime
import time
from db_config import get_connection, release_connection
import random
from requests.exceptions import RequestException
import logging
from typing import List, Optional
import concurrent.futures

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NBADataCollector:
    def __init__(self):
        self.connection = get_connection()
        self.max_retries = 5  # Retries
        self.base_delay = 5   # Base delay
        self.batch_size = 5   # Process players in smaller batches
    
    def __del__(self):
        release_connection(self.connection)
    
    def _make_api_request(self, api_call, *args, **kwargs):
        """Helper method to make API requests with retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Add exponential backoff with jitter
                if attempt > 0:
                    delay = self.base_delay * (2 ** attempt) + random.uniform(2, 5)
                    logger.info(f"Retry attempt {attempt + 1}/{self.max_retries} after {delay:.2f}s delay")
                    time.sleep(delay)
                
                result = api_call(*args, **kwargs)
                return result
            except RequestException as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                continue
    
    def get_player_games(self, player_id: int, season: str = '2024-25') -> Optional[pd.DataFrame]:
        """Fetch all games for a specific player in a season"""
        try:
            logger.info(f"Fetching games for player {player_id}")
            games = self._make_api_request(
                playergamelog.PlayerGameLog,
                player_id=player_id,
                season=season
            ).get_data_frames()[0]
            
            # Longer delay between requests
            time.sleep(random.uniform(5, 8))
            return games
        except Exception as e:
            logger.error(f"Error fetching player games: {e}")
            return None

    def process_player_batch(self, player_ids: List[int], season: str = '2024-25'):
        """Process a batch of players with delays between each"""
        for player_id in player_ids:
            try:
                stats = self.get_player_games(player_id, season)
                if stats is not None:
                    self.save_player_stats_to_db(stats)
                    logger.info(f"Successfully processed player {player_id}")
                else:
                    logger.warning(f"No stats available for player {player_id}")
            except Exception as e:
                logger.error(f"Error processing player {player_id}: {e}")
            
            # Add delay between players in the same batch
            time.sleep(random.uniform(3, 5))

    def fetch_all_player_stats(self, player_ids: List[int], season: str = '2024-25'):
        """Fetch stats for all players in batches"""
        # Split player IDs into batches
        for i in range(0, len(player_ids), self.batch_size):
            batch = player_ids[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1} of {(len(player_ids) + self.batch_size - 1)//self.batch_size}")
            
            self.process_player_batch(batch, season)
            
            # Add longer delay between batches
            if i + self.batch_size < len(player_ids):
                delay = random.uniform(10, 15)
                logger.info(f"Waiting {delay:.2f}s before next batch...")
                time.sleep(delay)

    def get_team_games(self, team_id: int, season: str = '2024-25') -> Optional[pd.DataFrame]:
        """Fetch all games for a specific team in a season"""
        try:
            logger.info(f"Fetching games for team {team_id}")
            games = self._make_api_request(
                leaguegamefinder.LeagueGameFinder,
                team_id_nullable=team_id,
                season_nullable=season
            ).get_data_frames()[0]
            
            # Add debugging to see the actual data structure
            logger.info(f"Raw games data shape: {games.shape}")
            logger.info(f"Raw games columns: {games.columns.tolist()}")
            if not games.empty:
                logger.info(f"Sample raw game data: {games.head(1).to_dict('records')[0]}")
            
            # Longer delay between requests
            time.sleep(random.uniform(5, 8))
            return games
        except Exception as e:
            logger.error(f"Error fetching team games: {e}")
            return None

    def get_all_players(self) -> Optional[pd.DataFrame]:
        """Fetch all NBA players"""
        try:
            logger.info("Fetching all players")
            players = self._make_api_request(
                commonallplayers.CommonAllPlayers
            ).get_data_frames()[0]
            return players
        except Exception as e:
            logger.error(f"Error fetching players: {e}")
            return None

    def get_all_teams(self) -> Optional[pd.DataFrame]:
        """Fetch all NBA teams"""
        try:
            logger.info("Fetching all teams")
            teams_list = teams.get_teams()
            return pd.DataFrame(teams_list)
        except Exception as e:
            logger.error(f"Error fetching teams: {e}")
            return None

    def save_players_to_db(self, players_df):
        """Save players data to database"""
        try:
            # Log the actual columns we received
            logger.info(f"Columns in players data: {players_df.columns.tolist()}")
            
            with self.connection.cursor() as cur:
                for _, row in players_df.iterrows():
                    # Extract first and last name from DISPLAY_FIRST_LAST
                    full_name = row['DISPLAY_FIRST_LAST']
                    name_parts = full_name.split(' ', 1)
                    first_name = name_parts[0] if len(name_parts) > 0 else ''
                    last_name = name_parts[1] if len(name_parts) > 1 else ''
                    
                    # Convert ROSTERSTATUS to boolean for is_active
                    is_active = bool(row['ROSTERSTATUS'])
                    
                    cur.execute("""
                        INSERT INTO players (player_id, full_name, first_name, last_name, is_active)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (player_id) DO UPDATE
                        SET full_name = EXCLUDED.full_name,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            is_active = EXCLUDED.is_active
                    """, (
                        row['PERSON_ID'],
                        full_name,
                        first_name,
                        last_name,
                        is_active
                    ))
            self.connection.commit()
            logger.info(f"Successfully committed {len(players_df)} players to database")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving players to database: {e}")
            logger.error(f"Sample row data: {row.to_dict() if 'row' in locals() else 'No row data available'}")
            raise  # Re-raise the exception to see the full traceback
    
    def save_teams_to_db(self, teams_df):
        """Save teams data to database"""
        try:
            with self.connection.cursor() as cur:
                for _, row in teams_df.iterrows():
                    cur.execute("""
                        INSERT INTO teams (team_id, full_name, abbreviation, nickname, city, state, year_founded)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (team_id) DO UPDATE
                        SET full_name = EXCLUDED.full_name,
                            abbreviation = EXCLUDED.abbreviation,
                            nickname = EXCLUDED.nickname,
                            city = EXCLUDED.city,
                            state = EXCLUDED.state,
                            year_founded = EXCLUDED.year_founded
                    """, (
                        row['id'],
                        row['full_name'],
                        row['abbreviation'],
                        row['nickname'],
                        row['city'],
                        row['state'],
                        row['year_founded']
                    ))
            self.connection.commit()
            logger.info(f"Successfully committed {len(teams_df)} teams to database")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving teams to database: {e}")
            raise  # Re-raise the exception to see the full traceback
    
    def ensure_teams_exist(self, games_df):
        """Ensure all teams referenced in games data exist in the teams table"""
        if games_df.empty:
            return
        
        # Get unique team IDs from games data
        unique_team_ids = set(games_df['TEAM_ID'].unique())
        logger.info(f"Found {len(unique_team_ids)} unique team IDs in games data")
        
        # Get existing team IDs from database
        with self.connection.cursor() as cur:
            cur.execute("SELECT team_id FROM teams")
            existing_team_ids = {row[0] for row in cur.fetchall()}
            logger.info(f"Found {len(existing_team_ids)} existing team IDs in database")
        
        # Find missing team IDs
        missing_team_ids = unique_team_ids - existing_team_ids
        
        if missing_team_ids:
            logger.warning(f"Found {len(missing_team_ids)} missing team IDs: {missing_team_ids}")
            logger.warning("This suggests teams data may be incomplete. Please ensure teams are loaded first.")
            return False
        else:
            logger.info("All teams referenced in games data exist in database")
            return True

    def save_games_to_db(self, games_df):
        """Save games data to database, matching the NBA API structure exactly."""
        if games_df.empty:
            logger.warning("No games data to save - DataFrame is empty")
            return
        
        # Ensure all teams exist before saving games
        if not self.ensure_teams_exist(games_df):
            logger.error("Cannot save games - some teams are missing from database")
            return
        
        log_games_dataframe_info(games_df, "processing")
        
        try:
            with self.connection.cursor() as cur:
                # Get all valid team IDs from the teams table
                cur.execute("SELECT team_id FROM teams")
                valid_team_ids = {row[0] for row in cur.fetchall()}
                
                successful_inserts = 0
                failed_inserts = 0
                
                for idx, row in games_df.iterrows():
                    try:
                        # Check if team_id exists in teams table
                        team_id = row['TEAM_ID']
                        if team_id not in valid_team_ids:
                            logger.warning(f"Team ID {team_id} not found for game {row['GAME_ID']}")
                            failed_inserts += 1
                            continue
                        
                        cur.execute("""
                            INSERT INTO games (
                                game_id, season_id, team_id, team_abbreviation, team_name,
                                game_date, matchup, win_loss, minutes_played, points,
                                field_goals_made, field_goals_attempted, field_goal_percentage,
                                three_pointers_made, three_pointers_attempted, three_point_percentage,
                                free_throws_made, free_throws_attempted, free_throw_percentage,
                                offensive_rebounds, defensive_rebounds, total_rebounds,
                                assists, steals, blocks, turnovers, personal_fouls, plus_minus
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (game_id) DO UPDATE
                            SET season_id = EXCLUDED.season_id,
                                team_id = EXCLUDED.team_id,
                                team_abbreviation = EXCLUDED.team_abbreviation,
                                team_name = EXCLUDED.team_name,
                                game_date = EXCLUDED.game_date,
                                matchup = EXCLUDED.matchup,
                                win_loss = EXCLUDED.win_loss,
                                minutes_played = EXCLUDED.minutes_played,
                                points = EXCLUDED.points,
                                field_goals_made = EXCLUDED.field_goals_made,
                                field_goals_attempted = EXCLUDED.field_goals_attempted,
                                field_goal_percentage = EXCLUDED.field_goal_percentage,
                                three_pointers_made = EXCLUDED.three_pointers_made,
                                three_pointers_attempted = EXCLUDED.three_pointers_attempted,
                                three_point_percentage = EXCLUDED.three_point_percentage,
                                free_throws_made = EXCLUDED.free_throws_made,
                                free_throws_attempted = EXCLUDED.free_throws_attempted,
                                free_throw_percentage = EXCLUDED.free_throw_percentage,
                                offensive_rebounds = EXCLUDED.offensive_rebounds,
                                defensive_rebounds = EXCLUDED.defensive_rebounds,
                                total_rebounds = EXCLUDED.total_rebounds,
                                assists = EXCLUDED.assists,
                                steals = EXCLUDED.steals,
                                blocks = EXCLUDED.blocks,
                                turnovers = EXCLUDED.turnovers,
                                personal_fouls = EXCLUDED.personal_fouls,
                                plus_minus = EXCLUDED.plus_minus
                        """,
                        (
                            row['GAME_ID'],
                            row['SEASON_ID'],
                            row['TEAM_ID'],
                            row['TEAM_ABBREVIATION'],
                            row['TEAM_NAME'],
                            datetime.strptime(row['GAME_DATE'], '%Y-%m-%d').date(),
                            row['MATCHUP'],
                            row['WL'],
                            row['MIN'],
                            row['PTS'],
                            row['FGM'],
                            row['FGA'],
                            row['FG_PCT'],
                            row['FG3M'],
                            row['FG3A'],
                            row['FG3_PCT'],
                            row['FTM'],
                            row['FTA'],
                            row['FT_PCT'],
                            row['OREB'],
                            row['DREB'],
                            row['REB'],
                            row['AST'],
                            row['STL'],
                            row['BLK'],
                            row['TOV'],
                            row['PF'],
                            row['PLUS_MINUS']
                        ))
                        successful_inserts += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing game row {idx}: {str(e)}")
                        failed_inserts += 1
                        continue
                
                self.connection.commit()
                logger.info(f"Saved {successful_inserts} games to database")
                if failed_inserts > 0:
                    logger.warning(f"Failed to insert {failed_inserts} games")
                    
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving games to database: {str(e)}")
            raise
    
    def save_player_stats_to_db(self, stats_df):
        """Save player statistics to database, matching the NBA API structure exactly."""
        if stats_df.empty:
            logger.warning("No stats available for this player - DataFrame is empty")
            return
            
        log_player_stats_dataframe_info(stats_df, "processing")
            
        try:
            with self.connection.cursor() as cur:
                successful_inserts = 0
                failed_inserts = 0
                
                for idx, row in stats_df.iterrows():
                    try:
                        # Handle different possible column names
                        game_id = row.get('GAME_ID') or row.get('Game_ID') or row.get('game_id')
                        player_id = row.get('PLAYER_ID') or row.get('Player_ID') or row.get('player_id')
                        game_date = row.get('GAME_DATE') or row.get('Game_Date') or row.get('game_date')
                        matchup = row.get('MATCHUP') or row.get('matchup')
                        
                        if not all([game_id, player_id, game_date, matchup]):
                            logger.warning(f"Missing required fields in row {idx}")
                            failed_inserts += 1
                            continue
                        
                        # Extract team ID from the MATCHUP column
                        team_abbr = matchup.split(' ')[0]  # Get team abbreviation
                        
                        # Query to get team_id from abbreviation
                        cur.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (team_abbr,))
                        team_result = cur.fetchone()
                        
                        if team_result is None:
                            logger.warning(f"Could not find team_id for abbreviation {team_abbr} in game {game_id}")
                            failed_inserts += 1
                            continue
                            
                        team_id = team_result[0]
                        
                        cur.execute("""
                            INSERT INTO player_stats (
                                game_id, player_id, team_id, team_abbreviation, team_name,
                                game_date, matchup, win_loss, minutes_played, points,
                                field_goals_made, field_goals_attempted, field_goal_percentage,
                                three_pointers_made, three_pointers_attempted, three_point_percentage,
                                free_throws_made, free_throws_attempted, free_throw_percentage,
                                offensive_rebounds, defensive_rebounds, total_rebounds,
                                assists, steals, blocks, turnovers, personal_fouls, plus_minus
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (game_id, player_id) DO UPDATE
                            SET team_id = EXCLUDED.team_id,
                                team_abbreviation = EXCLUDED.team_abbreviation,
                                team_name = EXCLUDED.team_name,
                                game_date = EXCLUDED.game_date,
                                matchup = EXCLUDED.matchup,
                                win_loss = EXCLUDED.win_loss,
                                minutes_played = EXCLUDED.minutes_played,
                                points = EXCLUDED.points,
                                field_goals_made = EXCLUDED.field_goals_made,
                                field_goals_attempted = EXCLUDED.field_goals_attempted,
                                field_goal_percentage = EXCLUDED.field_goal_percentage,
                                three_pointers_made = EXCLUDED.three_pointers_made,
                                three_pointers_attempted = EXCLUDED.three_pointers_attempted,
                                three_point_percentage = EXCLUDED.three_point_percentage,
                                free_throws_made = EXCLUDED.free_throws_made,
                                free_throws_attempted = EXCLUDED.free_throws_attempted,
                                free_throw_percentage = EXCLUDED.free_throw_percentage,
                                offensive_rebounds = EXCLUDED.offensive_rebounds,
                                defensive_rebounds = EXCLUDED.defensive_rebounds,
                                total_rebounds = EXCLUDED.total_rebounds,
                                assists = EXCLUDED.assists,
                                steals = EXCLUDED.steals,
                                blocks = EXCLUDED.blocks,
                                turnovers = EXCLUDED.turnovers,
                                personal_fouls = EXCLUDED.personal_fouls,
                                plus_minus = EXCLUDED.plus_minus
                        """, (
                            game_id,
                            player_id,
                            team_id,
                            team_abbr,
                            row.get('TEAM_NAME', '') or row.get('Team_Name', ''),
                            datetime.strptime(game_date, '%Y-%m-%d').date() if isinstance(game_date, str) else game_date,
                            matchup,
                            row.get('WL', '') or row.get('wl', ''),
                            row.get('MIN', 0) or row.get('Min', 0),
                            row.get('PTS', 0) or row.get('Pts', 0),
                            row.get('FGM', 0) or row.get('Fgm', 0),
                            row.get('FGA', 0) or row.get('Fga', 0),
                            row.get('FG_PCT', 0.0) or row.get('FG_Pct', 0.0),
                            row.get('FG3M', 0) or row.get('FG3M', 0),
                            row.get('FG3A', 0) or row.get('FG3A', 0),
                            row.get('FG3_PCT', 0.0) or row.get('FG3_Pct', 0.0),
                            row.get('FTM', 0) or row.get('Ftm', 0),
                            row.get('FTA', 0) or row.get('Fta', 0),
                            row.get('FT_PCT', 0.0) or row.get('FT_Pct', 0.0),
                            row.get('OREB', 0) or row.get('Oreb', 0),
                            row.get('DREB', 0) or row.get('Dreb', 0),
                            row.get('REB', 0) or row.get('Reb', 0),
                            row.get('AST', 0) or row.get('Ast', 0),
                            row.get('STL', 0) or row.get('Stl', 0),
                            row.get('BLK', 0) or row.get('Blk', 0),
                            row.get('TOV', 0) or row.get('Tov', 0),
                            row.get('PF', 0) or row.get('Pf', 0),
                            row.get('PLUS_MINUS', 0.0) or row.get('Plus_Minus', 0.0)
                        ))
                        successful_inserts += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing stats row {idx}: {str(e)}")
                        failed_inserts += 1
                        continue
                        
            self.connection.commit()
            logger.info(f"Saved {successful_inserts} player stats to database")
            if failed_inserts > 0:
                logger.warning(f"Failed to insert {failed_inserts} player stats")
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving player stats to database: {str(e)}")
            raise

    def check_database_state(self):
        """Check the current state of the database"""
        try:
            with self.connection.cursor() as cur:
                # Get counts for all tables
                cur.execute("SELECT COUNT(*) FROM teams")
                teams_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM games")
                games_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM players")
                players_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM player_stats")
                player_stats_count = cur.fetchone()[0]
                
                logger.info(f"Database Status: {teams_count} teams, {games_count} games, {players_count} players, {player_stats_count} player stats")
                
                # Check for orphaned games (only if there are games)
                if games_count > 0:
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM games g 
                        LEFT JOIN teams t ON g.team_id = t.team_id 
                        WHERE t.team_id IS NULL
                    """)
                    orphaned_count = cur.fetchone()[0]
                    if orphaned_count > 0:
                        logger.warning(f"Found {orphaned_count} games with invalid team references")
                    else:
                        logger.info("All games have valid team references")
                
        except Exception as e:
            logger.error(f"Error checking database state: {e}")

def log_games_dataframe_info(games_df, stage="before_db_insert"):
    """Log essential information about the games DataFrame"""
    if games_df.empty:
        logger.warning("Games DataFrame is EMPTY!")
        return
    
    logger.info(f"Games DataFrame - {stage}: {len(games_df)} games, {games_df['TEAM_ID'].nunique()} teams")
    logger.info(f"Date range: {games_df['GAME_DATE'].min()} to {games_df['GAME_DATE'].max()}")
    logger.info(f"Sample game: {games_df.iloc[0]['TEAM_ABBREVIATION']} vs {games_df.iloc[0]['MATCHUP']}")

def log_player_stats_dataframe_info(stats_df, stage="before_db_insert"):
    """Log essential information about the player stats DataFrame"""
    if stats_df.empty:
        logger.warning("Player stats DataFrame is EMPTY!")
        return
    
    # Handle different possible column names
    game_id_col = next((col for col in ['GAME_ID', 'Game_ID', 'game_id'] if col in stats_df.columns), None)
    player_id_col = next((col for col in ['PLAYER_ID', 'Player_ID', 'player_id'] if col in stats_df.columns), None)
    team_id_col = next((col for col in ['TEAM_ID', 'Team_ID', 'team_id'] if col in stats_df.columns), None)
    
    unique_games = stats_df[game_id_col].nunique() if game_id_col else "N/A"
    unique_players = stats_df[player_id_col].nunique() if player_id_col else "N/A"
    unique_teams = stats_df[team_id_col].nunique() if team_id_col else "N/A"
    
    logger.info(f"Player Stats - {stage}: {len(stats_df)} records, {unique_games} games, {unique_players} players, {unique_teams} teams")

def main():
    collector = NBADataCollector()
    
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
    
    # Fetch and save games for each team
    logger.info("Fetching games...")
    for team_id in teams['id']:
        logger.info(f"Fetching games for team {team_id}...")
        games = collector.get_team_games(team_id)
        if games is not None:
            logger.info(f"Retrieved {len(games)} games for team {team_id}")
            collector.save_games_to_db(games)
        else:
            logger.warning(f"No games retrieved for team {team_id}")
        time.sleep(random.uniform(5, 8))  # Increased rate limiting
    
    # Fetch and save player stats in batches
    logger.info("Fetching player stats...")
    if players is not None:
        player_ids = players['PERSON_ID'].tolist()
        logger.info(f"Processing stats for {len(player_ids)} players")
        collector.fetch_all_player_stats(player_ids)

    # Check database state
    collector.check_database_state()

if __name__ == "__main__":
    main() 