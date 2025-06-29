import requests
from nba_api.stats.endpoints import (
    commonallplayers, leaguegamefinder, playergamelog, playercareerstats,
    leaguedashplayerstats, commonplayerinfo, playerawards
)
from nba_api.stats.static import teams
import pandas as pd
from datetime import datetime, date
import time
import random
import logging
from typing import List, Optional, Dict, Any
import concurrent.futures
from bs4 import BeautifulSoup
import re
from db_config import get_connection, release_connection
from requests.exceptions import RequestException

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComprehensiveNBADataCollector:
    def __init__(self):
        self.connection = get_connection()
        self.max_retries = 5  # Retries
        self.base_delay = 3   # Base delay
        self.batch_size = 10   # Process players in smaller batches
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # API endpoints and configurations
        self.nba_api_base = "https://stats.nba.com/stats/"
        self.basketball_reference_base = "https://www.basketball-reference.com"
        self.spotrac_base = "https://www.spotrac.com"
        self.hoopshype_base = "https://hoopshype.com"
    
    def __del__(self):
        release_connection(self.connection)
    
    def _make_api_request(self, api_call, *args, **kwargs):
        """Helper method to make API requests with retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Add exponential backoff with jitter
                if attempt > 0:
                    delay = self.base_delay * (2 ** attempt) + random.uniform(1, 3)
                    logger.info(f"Retry attempt {attempt + 1}/{self.max_retries} after {delay:.2f}s delay")
                    time.sleep(delay)
                
                result = api_call(*args, **kwargs)
                return result
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                continue
    
    def _make_web_request(self, url: str, params: Dict = None) -> Optional[requests.Response]:
        """Make web requests with retry logic"""
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    delay = self.base_delay * (2 ** attempt) + random.uniform(1, 3)
                    time.sleep(delay)
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to fetch {url}: {str(e)}")
                    return None
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
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
        """Fetch all NBA players with enhanced data"""
        try:
            logger.info("🔄 Fetching all players from NBA API...")
            players = self._make_api_request(
                commonallplayers.CommonAllPlayers
            ).get_data_frames()[0]
            
            logger.info(f"📊 Raw players data shape: {players.shape}")
            logger.info(f"📋 Raw players columns: {players.columns.tolist()}")
            
            if players.empty:
                logger.error("❌ No players data returned from NBA API")
                return None
            
            # Get detailed player info for active players
            active_players = players[players['ROSTERSTATUS'] == 1]
            logger.info(f"🏀 Found {len(active_players)} active players out of {len(players)} total players")
            
            enhanced_players = []
            
            for idx, player in active_players.iterrows():
                try:
                    logger.info(f"🔍 Getting detailed info for player {idx + 1}/{len(active_players)}: {player['DISPLAY_FIRST_LAST']}")
                    player_info = self.get_player_detailed_info(player['PERSON_ID'])
                    if player_info:
                        enhanced_players.append({**player.to_dict(), **player_info})
                        logger.info(f"  ✅ Enhanced data added for {player['DISPLAY_FIRST_LAST']}")
                    else:
                        enhanced_players.append(player.to_dict())
                        logger.info(f"  ⚠️ No enhanced data for {player['DISPLAY_FIRST_LAST']}")
                    
                    time.sleep(random.uniform(0.5, 1.5))
                except Exception as e:
                    logger.warning(f"⚠️ Error getting detailed info for player {player['PERSON_ID']}: {e}")
                    enhanced_players.append(player.to_dict())
            
            result_df = pd.DataFrame(enhanced_players)
            logger.info(f"🎉 Successfully created enhanced players DataFrame with {len(result_df)} players")
            logger.info(f"📋 Final columns: {result_df.columns.tolist()}")
            
            return result_df
        except Exception as e:
            logger.error(f"❌ Error fetching players: {e}")
            return None

    def get_all_teams(self) -> Optional[pd.DataFrame]:
        """Fetch all NBA teams with enhanced data"""
        try:
            logger.info("Fetching all teams")
            teams_list = teams.get_teams()
            teams_df = pd.DataFrame(teams_list)
            
            # Add conference and division information
            conferences = {
                'Eastern': ['ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DET', 'IND', 'MIA', 'MIL', 'NYK', 'ORL', 'PHI', 'TOR', 'WAS'],
                'Western': ['DAL', 'DEN', 'GSW', 'HOU', 'LAC', 'LAL', 'MEM', 'MIN', 'NOP', 'OKC', 'PHX', 'POR', 'SAC', 'SAS', 'UTA']
            }
            
            divisions = {
                'Atlantic': ['BOS', 'BKN', 'NYK', 'PHI', 'TOR'],
                'Central': ['CHI', 'CLE', 'DET', 'IND', 'MIL'],
                'Southeast': ['ATL', 'CHA', 'MIA', 'ORL', 'WAS'],
                'Northwest': ['DEN', 'MIN', 'OKC', 'POR', 'UTA'],
                'Pacific': ['GSW', 'LAC', 'LAL', 'PHX', 'SAC'],
                'Southwest': ['DAL', 'HOU', 'MEM', 'NOP', 'SAS']
            }
            
            teams_df['conference'] = teams_df['abbreviation'].apply(
                lambda x: next((conf for conf, teams in conferences.items() if x in teams), None)
            )
            teams_df['division'] = teams_df['abbreviation'].apply(
                lambda x: next((div for div, teams in divisions.items() if x in teams), None)
            )
            
            return teams_df
        except Exception as e:
            logger.error(f"Error fetching teams: {e}")
            return None

    def get_player_detailed_info(self, player_id: int) -> Optional[Dict]:
        """Get detailed player information"""
        try:
            player_info = self._make_api_request(
                commonplayerinfo.CommonPlayerInfo,
                player_id=player_id
            ).get_data_frames()[0]
            
            if player_info.empty:
                return None
            
            info = player_info.iloc[0]
            
            # Parse height
            height_str = info.get('HEIGHT', '')
            height_feet, height_inches = 0, 0
            if height_str and '-' in height_str:
                try:
                    feet, inches = height_str.split('-')
                    height_feet = int(feet)
                    height_inches = int(inches)
                except:
                    pass
            
            # Calculate age
            birth_date = info.get('BIRTHDATE')
            age = None
            if birth_date:
                try:
                    birth_date = datetime.strptime(birth_date, '%Y-%m-%d').date()
                    age = (date.today() - birth_date).days // 365
                except:
                    pass
            
            return {
                'birth_date': birth_date,
                'age': age,
                'height_feet': height_feet,
                'height_inches': height_inches,
                'height_cm': info.get('HEIGHT_CM'),
                'weight_lbs': info.get('WEIGHT'),
                'weight_kg': info.get('WEIGHT_KG'),
                'position': info.get('POSITION'),
                'draft_year': info.get('DRAFT_YEAR'),
                'draft_round': info.get('DRAFT_ROUND'),
                'draft_number': info.get('DRAFT_NUMBER'),
                'college': info.get('SCHOOL'),
                'country': info.get('COUNTRY'),
                'experience_years': info.get('SEASON_EXP'),
                'jersey_number': info.get('JERSEY')
            }
        except Exception as e:
            logger.warning(f"Error getting detailed info for player {player_id}: {e}")
            return None

    def save_players_to_db(self, players_df):
        """Save players data to database"""
        try:
            logger.info(f"Columns in players data: {players_df.columns.tolist()}")
            logger.info(f"Total players to process: {len(players_df)}")
            
            successful_inserts = 0
            failed_inserts = 0
            first_error_logged = False
            
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    for idx, row in players_df.iterrows():
                        try:
                            logger.info(f"Processing player {idx + 1}/{len(players_df)}: {row.get('DISPLAY_FIRST_LAST', 'Unknown')}")
                            
                            # Parse name
                            full_name = row['DISPLAY_FIRST_LAST']
                            name_parts = full_name.split(' ', 1)
                            first_name = name_parts[0] if len(name_parts) > 0 else ''
                            last_name = name_parts[1] if len(name_parts) > 1 else ''
                            
                            # Extract data with proper handling of None values
                            player_id = row['PERSON_ID']
                            team_id = row.get('TEAM_ID')
                            position = row.get('position', '')
                            height_feet = row.get('height_feet')
                            height_inches = row.get('height_inches')
                            weight_lbs = row.get('weight_lbs')
                            birth_date = row.get('birth_date')
                            draft_year = row.get('draft_year', 'Undrafted')
                            draft_round = row.get('draft_round', 'Undrafted')
                            draft_number = row.get('draft_number', 'Undrafted')
                            college = row.get('college', '')
                            country = row.get('country', '')
                            experience_years = row.get('experience_years', 0)
                            jersey_number = row.get('jersey_number', '')
                            
                            if height_feet is None:
                                height_feet = 0
                            if height_inches is None:
                                height_inches = 0
                            if weight_lbs is None or weight_lbs == '':
                                weight_lbs = 0
                            else:
                                try:
                                    weight_lbs = int(weight_lbs)
                                except (ValueError, TypeError):
                                    weight_lbs = 0
                            
                            if birth_date and isinstance(birth_date, str):
                                try:
                                    if 'T' in birth_date:
                                        birth_date = birth_date.split('T')[0]
                                except:
                                    birth_date = None
                            
                            cur.execute("""
                                INSERT INTO players (
                                    player_id, first_name, last_name, full_name, team_id,
                                    position, height_feet, height_inches, weight_lbs,
                                    birth_date, draft_year, draft_round, draft_number,
                                    college, country, experience_years, jersey_number
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (player_id) DO UPDATE
                                SET first_name = EXCLUDED.first_name,
                                    last_name = EXCLUDED.last_name,
                                    full_name = EXCLUDED.full_name,
                                    team_id = EXCLUDED.team_id,
                                    position = EXCLUDED.position,
                                    height_feet = EXCLUDED.height_feet,
                                    height_inches = EXCLUDED.height_inches,
                                    weight_lbs = EXCLUDED.weight_lbs,
                                    birth_date = EXCLUDED.birth_date,
                                    draft_year = EXCLUDED.draft_year,
                                    draft_round = EXCLUDED.draft_round,
                                    draft_number = EXCLUDED.draft_number,
                                    college = EXCLUDED.college,
                                    country = EXCLUDED.country,
                                    experience_years = EXCLUDED.experience_years,
                                    jersey_number = EXCLUDED.jersey_number
                            """, (
                                player_id, first_name, last_name, full_name, team_id,
                                position, height_feet, height_inches, weight_lbs,
                                birth_date, draft_year, draft_round, draft_number,
                                college, country, experience_years, jersey_number
                            ))
                            
                            successful_inserts += 1
                            logger.info(f"✅ Successfully inserted player {idx + 1}: {full_name}")
                            
                        except Exception as e:
                            failed_inserts += 1
                            logger.error(f"❌ Error processing player {idx + 1}: {str(e)}")
                            logger.error(f"  Row data: {row.to_dict()}")
                            conn.rollback()
                            if not first_error_logged:
                                logger.error(f"🚨 FIRST ERROR OCCURRED for player {idx + 1}: {full_name}")
                                first_error_logged = True
                            continue
                    
                    conn.commit()
                    logger.info(f"🎉 Successfully committed {successful_inserts} players to database")
                    
                    if failed_inserts > 0:
                        logger.warning(f"⚠️ Failed to insert {failed_inserts} players")
                        
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"❌ Error in save_players_to_db: {str(e)}")
            raise
    
    def save_teams_to_db(self, teams_df):
        """Save teams data to database"""
        try:
            with self.connection.cursor() as cur:
                for _, row in teams_df.iterrows():
                    cur.execute("""
                        INSERT INTO teams (
                            team_id, full_name, abbreviation, nickname, city, state,
                            year_founded, conference, division, arena, arena_capacity
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (team_id) DO UPDATE
                        SET full_name = EXCLUDED.full_name,
                            abbreviation = EXCLUDED.abbreviation,
                            nickname = EXCLUDED.nickname,
                            city = EXCLUDED.city,
                            state = EXCLUDED.state,
                            year_founded = EXCLUDED.year_founded,
                            conference = EXCLUDED.conference,
                            division = EXCLUDED.division,
                            arena = EXCLUDED.arena,
                            arena_capacity = EXCLUDED.arena_capacity
                    """, (
                        row['id'], row['full_name'], row['abbreviation'],
                        row['nickname'], row['city'], row['state'],
                        row['year_founded'], row.get('conference'),
                        row.get('division'), row.get('arena'), row.get('arena_capacity')
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
                        
                        # Parse the game date
                        parsed_date = self.parse_game_date(game_date)
                        if parsed_date is None:
                            logger.warning(f"Could not parse date '{game_date}' for game {game_id}")
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
                            parsed_date,
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

    def parse_game_date(self, date_str):
        """Parse game date from various formats returned by NBA API"""
        if not date_str or not isinstance(date_str, str):
            return None
        
        # Try different date formats
        date_formats = [
            '%Y-%m-%d',           # 2025-04-11
            '%b %d, %Y',          # Apr 11, 2025
            '%B %d, %Y',          # April 11, 2025
            '%m/%d/%Y',           # 04/11/2025
            '%m/%d/%y',           # 04/11/25
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        # If none of the formats work, log the problematic date and return None
        logger.warning(f"Could not parse date: {date_str}")
        return None

    # ==================== PLAYER PERFORMANCE DATA ====================
    
    def get_player_season_stats(self, player_id: int, season: str) -> Optional[pd.DataFrame]:
        """Fetch player season statistics"""
        try:
            logger.info(f"Fetching season stats for player {player_id}")
            
            # Use the correct API endpoint
            stats = self._make_api_request(
                leaguedashplayerstats.LeagueDashPlayerStats(
                    season=season,
                    per_mode_detailed='PerGame'
                )
            ).get_data_frames()[0]
            
            # Filter for the specific player
            player_stats = stats[stats['PLAYER_ID'] == player_id]
            
            if player_stats.empty:
                logger.warning(f"No stats found for player {player_id}")
                return None
            
            return player_stats
            
        except Exception as e:
            logger.error(f"Error fetching stats for player {player_id}: {str(e)}")
            return None
    
    def get_player_career_stats(self, player_id: int) -> Optional[pd.DataFrame]:
        """Get player career statistics"""
        try:
            logger.info(f"Fetching career stats for player {player_id}")
            career_stats = self._make_api_request(
                playercareerstats.PlayerCareerStats,
                player_id=player_id
            ).get_data_frames()[0]
            
            time.sleep(random.uniform(1, 2))
            return career_stats
        except Exception as e:
            logger.error(f"Error fetching career stats for player {player_id}: {e}")
            return None
    
    def get_player_awards(self, player_id: int) -> Optional[pd.DataFrame]:
        """Get player awards and honors"""
        try:
            logger.info(f"Fetching awards for player {player_id}")
            awards = self._make_api_request(
                playerawards.PlayerAwards,
                player_id=player_id
            ).get_data_frames()[0]
            
            time.sleep(random.uniform(1, 2))
            return awards
        except Exception as e:
            logger.error(f"Error fetching awards for player {player_id}: {e}")
            return None
    
    # ==================== SALARY AND CONTRACT DATA ====================
    
    def get_player_contract_data(self, player_name: str) -> Optional[Dict]:
        """Get player contract data from Spotrac"""
        try:
            # Search for player on Spotrac
            search_url = f"{self.spotrac_base}/search"
            search_params = {'q': player_name}
            
            response = self._make_web_request(search_url, search_params)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find player link
            player_link = soup.find('a', href=re.compile(r'/nba/.*/contracts'))
            if not player_link:
                return None
            
            # Get player contract page
            contract_url = f"{self.spotrac_base}{player_link['href']}"
            contract_response = self._make_web_request(contract_url)
            if not contract_response:
                return None
            
            contract_soup = BeautifulSoup(contract_response.content, 'html.parser')
            
            # Parse contract data
            contract_data = self._parse_spotrac_contract(contract_soup)
            
            time.sleep(random.uniform(2, 4))
            return contract_data
        except Exception as e:
            logger.error(f"Error fetching contract data for {player_name}: {e}")
            return None
    
    def _parse_spotrac_contract(self, soup: BeautifulSoup) -> Dict:
        """Parse contract data from Spotrac page"""
        contract_data = {}
        
        try:
            # Find contract table
            contract_table = soup.find('table', class_='contract-table')
            if contract_table:
                rows = contract_table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        contract_data[key] = value
            
            # Extract salary information
            salary_info = soup.find('div', class_='salary-info')
            if salary_info:
                contract_data['salary_details'] = salary_info.get_text(strip=True)
            
        except Exception as e:
            logger.warning(f"Error parsing Spotrac contract: {e}")
        
        return contract_data
    
    def get_basketball_reference_salary(self, player_name: str) -> Optional[Dict]:
        """Get salary data from Basketball Reference"""
        try:
            # Search for player on Basketball Reference
            search_url = f"{self.basketball_reference_base}/search/search.fcgi"
            search_params = {'search': player_name}
            
            response = self._make_web_request(search_url, search_params)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find player link
            player_link = soup.find('a', href=re.compile(r'/players/'))
            if not player_link:
                return None
            
            # Get player salary page
            salary_url = f"{self.basketball_reference_base}{player_link['href']}"
            salary_response = self._make_web_request(salary_url)
            if not salary_response:
                return None
            
            salary_soup = BeautifulSoup(salary_response.content, 'html.parser')
            
            # Parse salary data
            salary_data = self._parse_basketball_reference_salary(salary_soup)
            
            time.sleep(random.uniform(2, 4))
            return salary_data
        except Exception as e:
            logger.error(f"Error fetching Basketball Reference salary for {player_name}: {e}")
            return None
    
    def _parse_basketball_reference_salary(self, soup: BeautifulSoup) -> Dict:
        """Parse salary data from Basketball Reference page"""
        salary_data = {}
        
        try:
            # Find salary table
            salary_table = soup.find('table', id='all_salaries')
            if salary_table:
                rows = salary_table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        season = cells[0].get_text(strip=True)
                        team = cells[1].get_text(strip=True)
                        salary = cells[2].get_text(strip=True)
                        salary_data[season] = {'team': team, 'salary': salary}
            
        except Exception as e:
            logger.warning(f"Error parsing Basketball Reference salary: {e}")
        
        return salary_data
    
    # ==================== MARKET DATA ====================
    
    def get_nba_salary_cap_data(self, season: str = '2024-25') -> Optional[Dict]:
        """Get NBA salary cap data"""
        try:
            # This would typically come from official NBA sources or reliable sports finance sites
            # For now, we'll use a simplified approach with known data
            
            salary_cap_data = {
                '2024-25': {
                    'salary_cap': 141000000,
                    'luxury_tax_threshold': 172000000,
                    'minimum_team_salary': 127000000,
                    'maximum_individual_salary': 49200000,
                    'mid_level_exception': 12900000,
                    'bi_annual_exception': 4500000
                },
                '2023-24': {
                    'salary_cap': 136021000,
                    'luxury_tax_threshold': 165000000,
                    'minimum_team_salary': 122655000,
                    'maximum_individual_salary': 47600000,
                    'mid_level_exception': 12400000,
                    'bi_annual_exception': 4500000
                }
            }
            
            return salary_cap_data.get(season, {})
        except Exception as e:
            logger.error(f"Error fetching salary cap data: {e}")
            return None
    
    def get_team_salary_commitments(self, team_id: int, season: str = '2024-25') -> Optional[Dict]:
        """Get team salary commitments"""
        try:
            # This would require access to team salary data
            # For now, return a placeholder structure
            return {
                'total_salary_commitment': 0,
                'guaranteed_salary': 0,
                'luxury_tax_payment': 0,
                'available_cap_space': 0,
                'dead_cap_space': 0
            }
        except Exception as e:
            logger.error(f"Error fetching team salary commitments: {e}")
            return None
    
    # ==================== DATA SAVING METHODS ====================
    
    def save_player_season_stats_to_db(self, stats_df: pd.DataFrame, season: str):
        """Save player season statistics to database"""
        if stats_df.empty:
            return
        
        try:
            with self.connection.cursor() as cur:
                for _, row in stats_df.iterrows():
                    cur.execute("""
                        INSERT INTO player_season_stats (
                            player_id, team_id, season_id, games_played, games_started,
                            minutes_per_game, points_per_game, rebounds_per_game,
                            offensive_rebounds_per_game, defensive_rebounds_per_game,
                            assists_per_game, steals_per_game, blocks_per_game,
                            turnovers_per_game, personal_fouls_per_game,
                            field_goal_percentage, three_point_percentage, free_throw_percentage,
                            true_shooting_percentage, effective_field_goal_percentage,
                            offensive_rating, defensive_rating, net_rating, usage_rate,
                            player_efficiency_rating, win_shares, win_shares_per_48,
                            box_plus_minus, value_over_replacement_player
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_id, season_id) DO UPDATE
                        SET team_id = EXCLUDED.team_id,
                            games_played = EXCLUDED.games_played,
                            games_started = EXCLUDED.games_started,
                            minutes_per_game = EXCLUDED.minutes_per_game,
                            points_per_game = EXCLUDED.points_per_game,
                            rebounds_per_game = EXCLUDED.rebounds_per_game,
                            offensive_rebounds_per_game = EXCLUDED.offensive_rebounds_per_game,
                            defensive_rebounds_per_game = EXCLUDED.defensive_rebounds_per_game,
                            assists_per_game = EXCLUDED.assists_per_game,
                            steals_per_game = EXCLUDED.steals_per_game,
                            blocks_per_game = EXCLUDED.blocks_per_game,
                            turnovers_per_game = EXCLUDED.turnovers_per_game,
                            personal_fouls_per_game = EXCLUDED.personal_fouls_per_game,
                            field_goal_percentage = EXCLUDED.field_goal_percentage,
                            three_point_percentage = EXCLUDED.three_point_percentage,
                            free_throw_percentage = EXCLUDED.free_throw_percentage,
                            true_shooting_percentage = EXCLUDED.true_shooting_percentage,
                            effective_field_goal_percentage = EXCLUDED.effective_field_goal_percentage,
                            offensive_rating = EXCLUDED.offensive_rating,
                            defensive_rating = EXCLUDED.defensive_rating,
                            net_rating = EXCLUDED.net_rating,
                            usage_rate = EXCLUDED.usage_rate,
                            player_efficiency_rating = EXCLUDED.player_efficiency_rating,
                            win_shares = EXCLUDED.win_shares,
                            win_shares_per_48 = EXCLUDED.win_shares_per_48,
                            box_plus_minus = EXCLUDED.box_plus_minus,
                            value_over_replacement_player = EXCLUDED.value_over_replacement_player
                    """, (
                        row['PLAYER_ID'], row.get('TEAM_ID'), season,
                        row.get('GP', 0), row.get('GS', 0), row.get('MIN', 0),
                        row.get('PTS', 0), row.get('REB', 0), row.get('OREB', 0),
                        row.get('DREB', 0), row.get('AST', 0), row.get('STL', 0),
                        row.get('BLK', 0), row.get('TOV', 0), row.get('PF', 0),
                        row.get('FG_PCT', 0), row.get('FG3_PCT', 0), row.get('FT_PCT', 0),
                        row.get('TS_PCT', 0), row.get('EFG_PCT', 0), row.get('OFFRTG', 0),
                        row.get('DEFRTG', 0), row.get('NETRTG', 0), row.get('USG_PCT', 0),
                        row.get('PER', 0), row.get('WS', 0), row.get('WS_PER_48', 0),
                        row.get('BPM', 0), row.get('VORP', 0)
                    ))
            self.connection.commit()
            logger.info(f"Successfully saved season stats for {len(stats_df)} players")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving season stats to database: {e}")
            raise
    
    def save_salary_cap_to_db(self, salary_cap_data: Dict, season: str):
        """Save salary cap data to database"""
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO nba_salary_cap (
                        season, salary_cap, luxury_tax_threshold, apron_threshold,
                        minimum_team_salary
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (season) DO UPDATE
                    SET salary_cap = EXCLUDED.salary_cap,
                        luxury_tax_threshold = EXCLUDED.luxury_tax_threshold,
                        apron_threshold = EXCLUDED.apron_threshold,
                        minimum_team_salary = EXCLUDED.minimum_team_salary
                """, (
                    season, salary_cap_data.get('salary_cap'),
                    salary_cap_data.get('luxury_tax_threshold'),
                    salary_cap_data.get('apron_threshold'),
                    salary_cap_data.get('minimum_team_salary')
                ))
            self.connection.commit()
            logger.info(f"Successfully saved salary cap data for {season}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving salary cap data: {e}")
            raise
    
    # ==================== BATCH PROCESSING ====================
    
    def process_all_data(self, season: str = '2024-25'):
        """Process all data collection in the correct order"""
        logger.info("Starting comprehensive NBA data collection...")
        
        # 1. Create seasons record
        self._create_season_record(season)
        
        # 2. Get and save teams
        logger.info("Collecting teams data...")
        teams = self.get_all_teams()
        if teams is not None:
            self.save_teams_to_db(teams)
        
        # 3. Get and save players
        logger.info("Collecting players data...")
        players = self.get_all_players()
        if players is not None:
            self.save_players_to_db(players)
        
        # 4. Get salary cap data
        logger.info("Collecting salary cap data...")
        salary_cap_data = self.get_nba_salary_cap_data(season)
        if salary_cap_data:
            self.save_salary_cap_to_db(salary_cap_data, season)
        
        # 5. Get player statistics
        logger.info("Collecting player statistics...")
        if players is not None:
            self._process_player_stats_batch(players['PERSON_ID'].tolist(), season)
        
        # 6. Get contract data (this would be a separate process due to rate limiting)
        logger.info("Note: Contract data collection would require separate processing due to rate limiting")
        
        logger.info("Data collection complete!")
    
    def _create_season_record(self, season: str):
        """Create season record in database"""
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO seasons (season_id, season_name, is_current)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (season_id) DO UPDATE
                    SET season_name = EXCLUDED.season_name,
                        is_current = EXCLUDED.is_current
                """, (season, season, True))
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error creating season record: {e}")
    
    def _process_player_stats_batch(self, player_ids: List[int], season: str):
        """Process player statistics in batches"""
        for i in range(0, len(player_ids), self.batch_size):
            batch = player_ids[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1} of {(len(player_ids) + self.batch_size - 1)//self.batch_size}")
            
            for player_id in batch:
                try:
                    stats = self.get_player_season_stats(player_id, season)
                    if stats is not None and not stats.empty:
                        self.save_player_season_stats_to_db(stats, season)
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    logger.error(f"Error processing player {player_id}: {e}")
                    continue
            
            # Delay between batches
            if i + self.batch_size < len(player_ids):
                delay = random.uniform(5, 10)
                logger.info(f"Waiting {delay:.2f}s before next batch...")
                time.sleep(delay)
    
    def check_database_state(self):
        """Check the current state of the database"""
        try:
            with self.connection.cursor() as cur:
                tables = [
                    'teams', 'players', 'seasons', 'player_season_stats',
                    'player_career_stats', 'player_awards', 'player_contracts',
                    'nba_salary_cap', 'team_salary_commitments'
                ]
                
                for table in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    logger.info(f"{table}: {count} records")
                
        except Exception as e:
            logger.error(f"Error checking database state: {e}")

    def _save_contract_data_to_db(self, player_id: int, contract_data: Dict, season: str):
        """Save contract data to database"""
        try:
            with self.connection.cursor() as cur:
                # Extract contract information from the parsed data
                annual_salary = self._extract_salary_from_contract(contract_data)
                contract_type = contract_data.get('Contract Type', 'Unknown')
                contract_length = self._extract_contract_length(contract_data)
                guaranteed_money = self._extract_guaranteed_money(contract_data)
                
                cur.execute("""
                    INSERT INTO player_contracts (
                        player_id, team_id, season_id, contract_type, contract_length_years,
                        annual_salary, guaranteed_money, total_contract_value,
                        performance_bonuses, incentive_clauses, is_active
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player_id, season_id) DO UPDATE
                    SET contract_type = EXCLUDED.contract_type,
                        contract_length_years = EXCLUDED.contract_length_years,
                        annual_salary = EXCLUDED.annual_salary,
                        guaranteed_money = EXCLUDED.guaranteed_money,
                        total_contract_value = EXCLUDED.total_contract_value,
                        performance_bonuses = EXCLUDED.performance_bonuses,
                        incentive_clauses = EXCLUDED.incentive_clauses
                """, (
                    player_id, None, season, contract_type, contract_length,
                    annual_salary, guaranteed_money, contract_data.get('Total Value'),
                    contract_data.get('Performance Bonuses'), contract_data.get('Incentives'),
                    True
                ))
            self.connection.commit()
            logger.info(f"Saved contract data for player {player_id}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving contract data for player {player_id}: {e}")
    
    def _save_salary_data_to_db(self, player_id: int, salary_data: Dict):
        """Save salary data from Basketball Reference to database"""
        try:
            with self.connection.cursor() as cur:
                for season, data in salary_data.items():
                    salary_amount = self._parse_salary_string(data.get('salary', '0'))
                    
                    cur.execute("""
                        INSERT INTO contract_history (
                            player_id, team_id, season_id, previous_contract_amount,
                            salary_progression, free_agency_type, contract_date
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        player_id, None, season, salary_amount,
                        0, 'Unknown', None
                    ))
            self.connection.commit()
            logger.info(f"Saved salary history for player {player_id}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving salary data for player {player_id}: {e}")
    
    def _calculate_player_value(self, player_data: tuple) -> Optional[Dict]:
        """Calculate player value metrics"""
        try:
            (player_id, full_name, position, ppg, rpg, apg, spg, bpg, tov, mpg,
             fg_pct, three_pct, ft_pct, plus_minus, per, ts_pct, usage_rate,
             win_shares, vorp, annual_salary) = player_data
            
            if not all([ppg, rpg, apg, spg, bpg, tov, mpg, annual_salary]):
                return None
            
            # Calculate total team improvement (simplified metric)
            # This is a basic calculation - you can make this more sophisticated
            total_improvement = (
                ppg * 1.0 +  # Points are most valuable
                rpg * 0.8 +  # Rebounds are valuable
                apg * 0.9 +  # Assists are very valuable
                spg * 1.2 +  # Steals are highly valuable
                bpg * 1.1 -  # Blocks are valuable
                tov * -0.8   # Turnovers are negative
            )
            
            # Calculate value per dollar
            value_per_dollar = total_improvement / annual_salary if annual_salary > 0 else 0
            
            # Get league average for position (simplified)
            league_average = self._get_position_average(position)
            
            # Calculate value vs average
            value_vs_average = value_per_dollar - league_average
            
            # Calculate efficiency rating
            efficiency_rating = (
                (fg_pct or 0) * 0.3 +
                (three_pct or 0) * 0.2 +
                (ft_pct or 0) * 0.1 +
                (per or 0) / 100 * 0.4
            )
            
            # Calculate cost efficiency score
            cost_efficiency_score = efficiency_rating * value_per_dollar * 1000
            
            return {
                'player_id': player_id,
                'team_id': None,  # Would need to be determined
                'season_id': '2024-25',
                'position': position,
                'total_team_improvement': total_improvement,
                'salary_cost': annual_salary,
                'value_per_dollar': value_per_dollar,
                'league_average_value_per_dollar': league_average,
                'value_vs_average': value_vs_average,
                'efficiency_rating': efficiency_rating,
                'cost_efficiency_score': cost_efficiency_score
            }
            
        except Exception as e:
            logger.error(f"Error calculating value for player {player_id}: {e}")
            return None
    
    def _save_value_analysis_to_db(self, value_metrics: Dict):
        """Save player value analysis to database"""
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO player_value_analysis (
                        player_id, team_id, season_id, position, total_team_improvement,
                        salary_cost, value_per_dollar, league_average_value_per_dollar,
                        value_vs_average, efficiency_rating, cost_efficiency_score
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player_id, season_id) DO UPDATE
                    SET team_id = EXCLUDED.team_id,
                        position = EXCLUDED.position,
                        total_team_improvement = EXCLUDED.total_team_improvement,
                        salary_cost = EXCLUDED.salary_cost,
                        value_per_dollar = EXCLUDED.value_per_dollar,
                        league_average_value_per_dollar = EXCLUDED.league_average_value_per_dollar,
                        value_vs_average = EXCLUDED.value_vs_average,
                        efficiency_rating = EXCLUDED.efficiency_rating,
                        cost_efficiency_score = EXCLUDED.cost_efficiency_score
                """, (
                    value_metrics['player_id'], value_metrics['team_id'],
                    value_metrics['season_id'], value_metrics['position'],
                    value_metrics['total_team_improvement'], value_metrics['salary_cost'],
                    value_metrics['value_per_dollar'], value_metrics['league_average_value_per_dollar'],
                    value_metrics['value_vs_average'], value_metrics['efficiency_rating'],
                    value_metrics['cost_efficiency_score']
                ))
            self.connection.commit()
            logger.info(f"Saved value analysis for player {value_metrics['player_id']}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error saving value analysis: {e}")
    
    def _extract_salary_from_contract(self, contract_data: Dict) -> float:
        """Extract annual salary from contract data"""
        try:
            # Look for various salary fields
            salary_fields = ['Annual Salary', 'Base Salary', 'Salary', '2024-25 Salary']
            for field in salary_fields:
                if field in contract_data:
                    return self._parse_salary_string(contract_data[field])
            return 0.0
        except:
            return 0.0
    
    def _extract_contract_length(self, contract_data: Dict) -> int:
        """Extract contract length from contract data"""
        try:
            length_str = contract_data.get('Contract Length', '1')
            # Extract number from string like "3 years" or "3"
            import re
            match = re.search(r'(\d+)', str(length_str))
            return int(match.group(1)) if match else 1
        except:
            return 1
    
    def _extract_guaranteed_money(self, contract_data: Dict) -> float:
        """Extract guaranteed money from contract data"""
        try:
            guaranteed_str = contract_data.get('Guaranteed Money', '0')
            return self._parse_salary_string(guaranteed_str)
        except:
            return 0.0
    
    def _parse_salary_string(self, salary_str: str) -> float:
        """Parse salary string to float"""
        try:
            if not salary_str:
                return 0.0
            
            # Remove common currency symbols and commas
            cleaned = str(salary_str).replace('$', '').replace(',', '').replace(' ', '')
            
            # Handle different formats
            if 'M' in cleaned or 'million' in cleaned.lower():
                # Convert millions to dollars
                number = float(cleaned.replace('M', '').replace('million', ''))
                return number * 1000000
            elif 'K' in cleaned or 'thousand' in cleaned.lower():
                # Convert thousands to dollars
                number = float(cleaned.replace('K', '').replace('thousand', ''))
                return number * 1000
            else:
                # Assume it's already in dollars
                return float(cleaned)
        except:
            return 0.0
    
    def _get_position_average(self, position: str) -> float:
        """Get league average value per dollar for position"""
        # This would ideally come from actual league data
        # For now, using simplified averages
        position_averages = {
            'PG': 0.00015,  # Point guards
            'SG': 0.00012,  # Shooting guards
            'SF': 0.00013,  # Small forwards
            'PF': 0.00011,  # Power forwards
            'C': 0.00010,   # Centers
        }
        return position_averages.get(position, 0.00012)

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
    collector = ComprehensiveNBADataCollector()
    collector.process_all_data('2024-25')
    collector.check_database_state()

if __name__ == "__main__":
    main() 