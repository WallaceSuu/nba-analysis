# NBA Analytics Project

This project is a comprehensive NBA analytics application that leverages various libraries and tools to analyze NBA data using the NBA API.

## Features

- Data collection from NBA API
- Statistical analysis and visualization
- Machine learning models for player/team performance prediction
- Interactive dashboards and visualizations
- PostgreSQL database integration

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/nba-analysis.git
cd nba-analysis
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up PostgreSQL database:
- Install PostgreSQL if not already installed
- Create a new database named 'nba_analytics'
- Create a .env file with your database credentials

5. Run the Jupyter notebook:
```bash
jupyter notebook
```

## Project Structure

```
nba-analysis/
├── data/                  # Data storage
├── notebooks/            # Jupyter notebooks
├── src/                  # Source code
│   ├── data/            # Data collection and processing
│   ├── analysis/        # Analysis modules
│   ├── models/          # Machine learning models
│   └── visualization/   # Visualization modules
├── requirements.txt      # Project dependencies
└── README.md            # Project documentation
```

## Database Setup

Create a `.env` file in the root directory with the following variables:
```
DB_NAME=nba_analytics
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## Usage

1. Start with the data collection notebook in `notebooks/01_data_collection.ipynb`
2. Follow the analysis notebooks in order
3. Use the visualization notebooks to create interactive dashboards

## Features

1. Best dynamic duos/trios
2. Player value based on salary/performance/plus-minus
3. Impact per minute
4. Assist quality score
5. Usage-adjusted efficiency
6. Floor spacing score
7. Scoring efficiency 
8. Player clusters by role/playstyle
9. Most improved players -> breakout stars/declining vets
10. Building most cost efficient team under salary cap



