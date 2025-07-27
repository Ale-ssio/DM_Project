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
df_matches = df_matches[['location', 'away coach', 'away goals', 'away name',
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
# Define a function that, given a league (from matches), checks for the most similar
# league in the stored ones and returns the most accurate match only if it has a score > 80.
def getLeague(league):
    match, score, _ = process.extractOne(league, league_names)
    return match if score > 80 else None
# Create a new column in the matches database with the matched league.
df_matches['matched_league'] = df_matches['league'].str.lower().apply(getLeague)

# Merge using the normalized matched league name, then remove the useless columns.
df1 = df_matches.merge(df_leagues, left_on='matched_league', right_on='name', how='left')
df1.drop(columns=['matched_league', 'name'], inplace=True)

##########################################################################################################################
##########################################################################################################################
##                                            PHASE 2: MERGE DF1 & STADIUMS                                             ##
##########################################################################################################################
##########################################################################################################################

# Problem: when there are multiple matchings with the same exact name, it matches nothing.
# To solve this problem I want to sort the stadiums dataset on the name of the stadium and
# on the population. The idea is that two stadiums with the same name will be one next to the other
# and the first one will be the one in the biggest country, which will likely be the needed one
# since the database involves the top 5 leagues, I can also filter the dataset to keep only those
# stadiums such that the country is one of the top 5, then keep only the first duplicate.
top_countries = ['Italy', 'Spain', 'Germany', 'France', 'England']
df_stadiums_sorted = df_stadiums.sort_values(by=['Stadium', 'Population'], ascending=[True, False])
df_stadiums_sorted = df_stadiums_sorted[df_stadiums_sorted['Country'].isin(top_countries)]
df_stadiums_unique = df_stadiums_sorted.drop_duplicates(subset='Stadium', keep='first')
df_stadiums_unique = df_stadiums_unique[['Stadium', 'City', 'Capacity']]
# Store the sorted names of the stadiums of the dedicated database.
stadium_names = df_stadiums_unique['Stadium'].unique()

# Define a function that, given a row (from df1), checks for the most similar
# stadium in the stored ones and returns the most accurate match only if it has a score > 80.
def getStadium(row):
    # I want to get also the location because there are some comlex cases where I want to compare
    # also the city to be sure of the matching.
    stadium = row['stadium']
    location = row['location']
    if pd.isnull(stadium) or pd.isnull(location): return None
    match, score, _ = process.extractOne(stadium, stadium_names, scorer = fuzz.partial_ratio)
    if score < 65: return None
    # If the score is greater compare also the city.
    matched_row = df_stadiums_unique[df_stadiums_unique['Stadium'] == match]
    matched_city = matched_row.iloc[0]['City']
    city_score = fuzz.partial_ratio(str(location).lower(), str(matched_city).lower())
    return match if city_score > 80 else None
# Create a new column in the df1 merged database with the matched stadium.
df1['matched_stadium'] = df1.apply(getStadium, axis=1)

# Merge using the matched stadium, then remove the useless columns.
df2 = df1.merge(df_stadiums_unique, left_on='matched_stadium', right_on='Stadium', how='left')
df2.drop(columns=['location', 'stadium', 'Stadium'], inplace=True)
# I want also to merge on the country to assign to each one the respective population.
df_population = df_stadiums[['Country', 'Population']]
df_population = df_population.drop_duplicates()
df2 = df2.merge(df_population, left_on='country_name', right_on='Country', how='left')
df2.drop(columns=['Country'], inplace=True)
# I kept the matched stadium column and renamed it because stadium is an optional dimension,
# but it makes no sense to me to keep a stadium without the additional information, so
# the rows who had no match will also have no stadium at all.
df2 = df2.rename(columns={'matched_stadium': 'stadium',
                          'country_name': 'country',
                          'City': 'city',
                          'Capacity': 'capacity',
                          'Population': 'population'})