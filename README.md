# NBA Analytics System

A comprehensive NBA analytics platform that collects, analyzes, and evaluates player performance, salary data, and market information to determine player value and team improvement potential.

## Features

### Player Performance Data

- **Basic Stats**: PPG, RPG, APG, SPG, BPG, TOV, MPG, FG%, 3P%, FT%, +/-
- **Advanced Metrics**: PER (Player Efficiency Rating), TS% (True Shooting %), Usage Rate, Win Shares, VORP (Value Over Replacement Player)
- **Game-by-Game Stats**: Detailed individual game performance data
- **Season Aggregates**: Per-game averages and totals for each season
- **Career Statistics**: Lifetime performance metrics

### Salary and Contract Data

- **Annual Salary**: Current season compensation
- **Contract Details**: Length, type, guaranteed money, performance bonuses
- **Incentive Clauses**: Performance-based contract incentives
- **Team/Player Options**: Contract option details
- **Historical Salary Data**: Previous contracts, salary progression, free agency history

### Market Data

- **NBA Salary Cap**: Annual salary cap and luxury tax thresholds
- **Team Salary Commitments**: Current team salary obligations
- **Available Cap Space**: Team financial flexibility
- **League Revenue Trends**: NBA financial data

### Player Information

- **Demographics**: Age, experience, position, physical attributes
- **Draft Information**: Draft position, year, team
- **Awards and Honors**: All-Star selections, All-NBA teams, MVP, DPOY, etc.
- **Career Milestones**: Games played, career totals, playoff experience

### Value Analysis

- **Value per Dollar**: Player contribution relative to salary cost
- **Position Comparisons**: League average performance by position
- **Team Improvement Impact**: Total team enhancement per dollar spent
- **Efficiency Ratings**: Cost-effectiveness scores

## Data Sources

The system integrates data from multiple sources:

- **NBA API**: Official NBA statistics and player information
- **Basketball Reference**: Historical data and salary information
- **Spotrac**: Contract details and salary cap data
- **Hoopshype**: Additional contract and market information

## Database Schema

The system uses a comprehensive PostgreSQL database with the following main tables:

### Core Tables

- `teams`: Team information and metadata
- `players`: Player demographics and basic information
- `seasons`: Season tracking and metadata
- `games`: Game results and team performance

### Performance Tables

- `player_game_stats`: Individual game statistics
- `player_season_stats`: Season aggregate statistics
- `player_career_stats`: Career totals and averages
- `player_awards`: Awards and honors

### Financial Tables

- `player_contracts`: Current contract information
- `contract_history`: Historical contract data
- `nba_salary_cap`: League salary cap data
- `team_salary_commitments`: Team financial obligations
- `team_revenue`: Team revenue data
- `league_revenue`: League financial trends

### Analysis Tables

- `player_value_analysis`: Calculated value metrics

## Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Git

### Setup Instructions

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd nba-analysis
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up PostgreSQL database**

   ```bash
   # Create database
   createdb nba_analytics

   # Set up environment variables
   cp .env.example .env
   # Edit .env with your database credentials
   ```

5. **Initialize the database**
   ```bash
   cd src/data
   python setup.py
   ```

## Configuration

Create a `.env` file in the root directory with the following variables:

```env
DB_NAME=nba_analytics
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## Usage

### Basic Data Collection

Run the main setup script to collect all available data:

```bash
cd src/data
python setup.py
```

This will:

1. Create the database schema
2. Collect team and player information
3. Gather current season statistics
4. Load salary cap data
5. Calculate initial value metrics

### Advanced Data Collection

For contract and salary data (requires additional time due to rate limiting):

```python
from src.data.setup import setup_contract_data, setup_value_analysis

# Collect contract data (separate due to rate limiting)
setup_contract_data()

# Run value analysis calculations
setup_value_analysis()
```

### Data Analysis

Use the Jupyter notebooks for analysis:

```bash
cd notebooks
jupyter notebook
```

Available notebooks:

- `01_player_analysis.ipynb`: Basic player performance analysis
- `02_value_analysis.ipynb`: Player value and efficiency analysis
- `03_contract_analysis.ipynb`: Salary and contract analysis
- `04_market_analysis.ipynb`: Market trends and cap analysis

## API Usage

### Basic Data Collection

```python
from src.data.nba_data_collector import ComprehensiveNBADataCollector

# Initialize collector
collector = ComprehensiveNBADataCollector()

# Collect all data for current season
collector.process_all_data('2024-25')

# Get specific player stats
player_stats = collector.get_player_season_stats(player_id=2544, season='2024-25')

# Get contract data
contract_data = collector.get_player_contract_data("LeBron James")
```

### Database Queries

```python
from src.data.db_config import get_connection

conn = get_connection()
cursor = conn.cursor()

# Get top value players
cursor.execute("""
    SELECT p.full_name, pva.value_per_dollar, pva.efficiency_rating
    FROM player_value_analysis pva
    JOIN players p ON pva.player_id = p.player_id
    WHERE pva.season_id = '2024-25'
    ORDER BY pva.value_per_dollar DESC
    LIMIT 10
""")

top_players = cursor.fetchall()
```

## Value Calculation Methodology

The system calculates player value using the following approach:

1. **Total Team Improvement**: Weighted combination of key statistics

   - Points: 1.0x weight
   - Rebounds: 0.8x weight
   - Assists: 0.9x weight
   - Steals: 1.2x weight
   - Blocks: 1.1x weight
   - Turnovers: -0.8x weight

2. **Value per Dollar**: Total improvement divided by annual salary

3. **Position Comparison**: League average value per dollar for each position

4. **Efficiency Rating**: Weighted combination of shooting percentages and PER

5. **Cost Efficiency Score**: Overall value metric combining efficiency and cost

## Rate Limiting and Best Practices

The system implements several rate limiting strategies:

- **API Delays**: Random delays between NBA API calls (1-3 seconds)
- **Web Scraping Delays**: Longer delays for web scraping (2-4 seconds)
- **Batch Processing**: Process players in small batches with delays
- **Retry Logic**: Exponential backoff with jitter for failed requests

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This system is for educational and analytical purposes only. All data is collected from publicly available sources. Please respect rate limits and terms of service for all data sources.

## Support

For questions or issues:

1. Check the documentation
2. Review existing issues
3. Create a new issue with detailed information

## Roadmap

- [ ] Real-time data updates
- [ ] Machine learning models for player prediction
- [ ] Advanced visualization dashboard
- [ ] API endpoints for external access
- [ ] Mobile application
- [ ] Historical data backfill
- [ ] Advanced statistical models
