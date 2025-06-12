from nba_api.stats.endpoints import commonallplayers, leaguegamefinder, playergamelog, playercareerstats
from nba_api.stats.static import teams
import pandas as pd
from datetime import datetime
import time
from db_config import get_connection, release_connection

class NBADataCollector:
    def __init__(self):
        self.connection = get_connection()
    
    def __del__(self):
        release_connection(self.connection)
    
    def get_all_players(self):
        """Fetch all NBA players"""
        try:
            players = commonallplayers.CommonAllPlayers().get_data_frames()[0]
            return players
        except Exception as e:
            print(f"Error fetching players: {e}")
            return None
    
    def get_all_teams(self):
        """Fetch all NBA teams"""
        try:
            teams_list = teams.get_teams()
            return pd.DataFrame(teams_list)
        except Exception as e:
            print(f"Error fetching teams: {e}")
            return None
    
    def get_team_games(self, team_id, season='2024-25'):
        """Fetch all games for a specific team in a season"""
        try:
            games = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=team_id,
                season_nullable=season
            ).get_data_frames()[0]
            return games
        except Exception as e:
            print(f"Error fetching team games: {e}")
            return None
    
    def get_player_games(self, player_id, season='2024-25'):
        """Fetch all games for a specific player in a season"""
        try:
            games = playergamelog.PlayerGameLog(
                player_id=player_id,
                season=season
            ).get_data_frames()[0]
            return games
        except Exception as e:
            print(f"Error fetching player games: {e}")
            return None
    
    def save_players_to_db(self, players_df):
        """Save players data to database"""
        try:
            with self.connection.cursor() as cur:
                for _, row in players_df.iterrows():
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
                        row['DISPLAY_FIRST_LAST'],
                        row['FIRST_NAME'],
                        row['LAST_NAME'],
                        row['IS_ACTIVE']
                    ))
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            print(f"Error saving players to database: {e}")
    
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
        except Exception as e:
            self.connection.rollback()
            print(f"Error saving teams to database: {e}")
    
    def save_games_to_db(self, games_df):
        """Save games data to database"""
        try:
            with self.connection.cursor() as cur:
                for _, row in games_df.iterrows():
                    # Determine home and away teams based on the MATCHUP column
                    matchup = row['MATCHUP']
                    # vs. is a home game, @ is an away game
                    is_home = 'vs.' in matchup
                    
                    # Extract team IDs
                    team_id = row['TEAM_ID']
                    opponent_id = row['OPPONENT_TEAM_ID'] if 'OPPONENT_TEAM_ID' in row else None
                    
                    if opponent_id is None:
                        # If OPPONENT_TEAM_ID is not available, try to extract from MATCHUP
                        opponent_abbr = matchup.split(' ')[-1]
                        # Might need to add a mapping from team abbreviations to IDs
                        continue
                    
                    home_team_id = team_id if is_home else opponent_id
                    away_team_id = opponent_id if is_home else team_id
                    
                    cur.execute("""
                        INSERT INTO games (game_id, season_id, game_date, home_team_id, away_team_id,
                                         home_team_score, away_team_score)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id) DO UPDATE
                        SET season_id = EXCLUDED.season_id,
                            game_date = EXCLUDED.game_date,
                            home_team_id = EXCLUDED.home_team_id,
                            away_team_id = EXCLUDED.away_team_id,
                            home_team_score = EXCLUDED.home_team_score,
                            away_team_score = EXCLUDED.away_team_score
                    """, (
                        row['GAME_ID'],
                        row['SEASON_ID'],
                        datetime.strptime(row['GAME_DATE'], '%Y-%m-%d').date(),
                        home_team_id,
                        away_team_id,
                        row['PTS'] if is_home else row['OPPONENT_PTS'],
                        row['OPPONENT_PTS'] if is_home else row['PTS']
                    ))
                    
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Error saving games to database: {e}")
            # Print the row that caused the error for debugging
            print(f"Problematic row: {row.to_dict()}")
    
    def save_player_stats_to_db(self, stats_df):
        """Save player statistics to database"""
        if stats_df.empty:
            print("No stats available for this player")
            return
            
        try:
            with self.connection.cursor() as cur:
                for _, row in stats_df.iterrows():
                    try:
                        cur.execute("""
                            INSERT INTO player_stats (
                                game_id, player_id, team_id, points, rebounds, assists,
                                steals, blocks, turnovers, minutes_played,
                                field_goals_made, field_goals_attempted,
                                three_pointers_made, three_pointers_attempted,
                                free_throws_made, free_throws_attempted
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (game_id, player_id) DO UPDATE
                            SET points = EXCLUDED.points,
                                rebounds = EXCLUDED.rebounds,
                                assists = EXCLUDED.assists,
                                steals = EXCLUDED.steals,
                                blocks = EXCLUDED.blocks,
                                turnovers = EXCLUDED.turnovers,
                                minutes_played = EXCLUDED.minutes_played,
                                field_goals_made = EXCLUDED.field_goals_made,
                                field_goals_attempted = EXCLUDED.field_goals_attempted,
                                three_pointers_made = EXCLUDED.three_pointers_made,
                                three_pointers_attempted = EXCLUDED.three_pointers_attempted,
                                free_throws_made = EXCLUDED.free_throws_made,
                                free_throws_attempted = EXCLUDED.free_throws_attempted
                        """, (
                            row['GAME_ID'],
                            row['PLAYER_ID'],
                            row['TEAM_ID'],
                            row.get('PTS', 0),
                            row.get('REB', 0),
                            row.get('AST', 0),
                            row.get('STL', 0),
                            row.get('BLK', 0),
                            row.get('TOV', 0),
                            row.get('MIN', 0),
                            row.get('FGM', 0),
                            row.get('FGA', 0),
                            row.get('FG3M', 0),
                            row.get('FG3A', 0),
                            row.get('FTM', 0),
                            row.get('FTA', 0)
                        ))
                    except Exception as e:
                        print(f"Error processing row: {e}")
                        print(f"Problematic row: {row.to_dict()}")
                        continue
                        
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Error saving player stats to database: {e}")

def main():
    collector = NBADataCollector()
    
    # Fetch and save players
    print("Fetching players...")
    players = collector.get_all_players()
    if players is not None:
        collector.save_players_to_db(players)
    
    # Fetch and save teams
    print("Fetching teams...")
    teams = collector.get_all_teams()
    if teams is not None:
        collector.save_teams_to_db(teams)
    
    # Fetch and save games for each team
    print("Fetching games...")
    for team_id in teams['id']:
        print(f"Fetching games for team {team_id}...")
        games = collector.get_team_games(team_id)
        if games is not None:
            collector.save_games_to_db(games)
        time.sleep(1)  # Rate limiting
    
    # Fetch and save player stats
    print("Fetching player stats...")
    for player_id in players['PERSON_ID']:
        print(f"Fetching stats for player {player_id}...")
        stats = collector.get_player_games(player_id)
        if stats is not None:
            collector.save_player_stats_to_db(stats)
        time.sleep(1)  # Rate limiting

if __name__ == "__main__":
    main() 