import pandas as pd

# I want to build the RECONCILED DATA LAYER:
# Traditional database, but integrated and cleaned,
# built from all the data I have and hystorical data.

# First of all I load all my sources in pandas dataframes.
df_matches = pd.read_csv('european_football_games.csv')
df_leagues = pd.read_csv('football_data_competitions_clubs_players.csv')
df_stats = pd.read_csv('big_5_european_football_leagues_teams_stats.csv')
df_stadiums = pd.read_csv('football_stadiums.csv')
df_trophies = pd.read_csv('european_football_soccer_clubs_on_google_SERPs.csv')

# PHASE 1: PREPROCESSING