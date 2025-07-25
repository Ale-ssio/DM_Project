import pandas as pd
import re
from rapidfuzz import process, fuzz

# I want to build the RECONCILED DATA LAYER:
# Traditional database, but integrated and cleaned,
# built from all the data I have and hystorical data.

# First of all I load all my sources in pandas dataframes.
df_matches = pd.read_csv('european_football_games.csv', low_memory=False)
df_leagues = pd.read_csv('football_data_competitions_clubs_players.csv')
df_stats = pd.read_csv('big_5_european_football_leagues_teams_stats.csv')
df_stadiums = pd.read_csv('football_stadiums.csv')
df_trophies = pd.read_csv('european_football_soccer_clubs_on_google_SERPs.csv')

# PREPROCESSING

# Keep only the attributes I'm interested in, for each dataframe.
df_matches = df_matches[['away coach', 'away goals', 'away name',
                         'date', 'home coach', 'home goals', 'home name',
                         'league', 'referee', 'season', 'stadium', 'visitor count']]
df_leagues = df_leagues[['name', 'type', 'country_name']]
df_stats = df_stats[['competition', 'season', 'rank', 'squad']]
df_stadiums = df_stadiums[['Stadium', 'City', 'Capacity', 'Country', 'Population']]
df_trophies = df_trophies[['Club', 'UCL', 'UEL', 'CWC', 'USC']]

# I noticed there are a couple of rows in df_matches with all null values.
# Such rows are useless for the project since they offer no information,
# so I want to drop them. I refer to league since each row has one.
df_matches = df_matches.dropna(subset=['league'])

##########################################################################################################################
##########################################################################################################################
##                                           PHASE 1: MERGE MATCHES & LEAGUES                                           ##
##########################################################################################################################
##########################################################################################################################

# Problem: matches database uses 'Primera División' as name to identify 'La Liga', which
# is part of the official name, but not the known and common used one.
# I want to substitute that value with 'La Liga' to have a more known name and easier joins.
df_matches.loc[df_matches['league']=='Primera División', 'league'] = 'La Liga'

# Problem: leagues has a particular format with hyphens (-) in the names that gives problems
# when one tries to match the leagues to perform join. The solution is to modify those names
# by using regular expressions to keep only letters (lowercase) and numbers.
def normalize_name(league):
    return re.sub(r'[^a-z0-9]', '', league.lower())
df_leagues['name'] = df_leagues['name'].apply(normalize_name)

# - Join the matches database and the leagues database on the competition attribute.
#   The idea is to have the type and the country of each competition in which the game was played.
# However, leagues are not written in the same exact way, so I need to set some similarity score
# after which the join is performed anyway. The solution is Fuzzy Matching.

# Store the names of the leagues of the competitions database.
league_names = df_leagues['name'].unique()
# Define a function that, given a league (from matches), check for the most similar
# league in the stored ones and returns the most accurate match only if it has a score > 85.
def getBestMatch(league):
    match, score, _ = process.extractOne(league, league_names)
    return match if score > 80 else None
# Create a new column in the matches database with the matched league.
df_matches['matched_league'] = df_matches['league'].str.lower().apply(getBestMatch)

# Merge using the normalized matched league name, then remove the useless columns.
df1 = df_matches.merge(df_leagues, left_on='matched_league', right_on='name', how='left')
df1.drop(columns=['matched_league', 'name'], inplace=True)

##########################################################################################################################
##########################################################################################################################
##                                            PHASE 2: MERGE DF1 & STADIUMS                                             ##
##########################################################################################################################
##########################################################################################################################