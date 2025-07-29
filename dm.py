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

# Since I have datasets with data from multiple seasons, I decided to analyze
# only the period of time corresponding to the intersection between the
# available seasons. So I want to keep only rows referring to seasons between
# 2010/2011 and 2018/2019 in the various datasets.

# Take the starting year of the season for each row and store it to use it as a filter.
df_matches['season_start'] = df_matches['season'].str.extract(r'(\d{4})').astype(int)
df_matches = df_matches[(df_matches['season_start'] >= 2010) & (df_matches['season_start'] <= 2018)]
df_matches.drop(columns='season_start', inplace=True)
# Do the same thing for the stats dataframe.
df_stats['season_start'] = df_stats['season'].str.extract(r'(\d{4})').astype(int)
df_stats = df_stats[(df_stats['season_start'] >= 2010) & (df_stats['season_start'] <= 2018)]
df_stats.drop(columns='season_start', inplace=True)
# Problem: df_matches has the year written as '2010/2011', while df_stats uses a format like '2010-2011'.
# Since I will need to join on the season, I need to uniform the format of the two dataframes,
# and I will do that by transforming the hyphen (-) into a slash (/).
df_stats['season'] = df_stats['season'].str.replace('-', '/')

# Problem: matches database uses 'Primera División' as name to identify 'La Liga', which
# is part of the official name, but not the known and common used one.
# I want to substitute that value with 'La Liga' to have a more known name and easier joins.
df_matches.loc[df_matches['league']=='Primera División', 'league'] = 'La Liga'
# Same thing for stats database and Bundesliga.
df_stats.loc[df_stats['competition']=='Fußball-Bundesliga', 'competition'] = 'Bundesliga'

##########################################################################################################################
##########################################################################################################################
##                                           PHASE 1: MERGE MATCHES & LEAGUES                                           ##
##########################################################################################################################
##########################################################################################################################

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

##########################################################################################################################
##########################################################################################################################
##                                             PHASE 3: MERGE DF2 & STATS                                               ##
##########################################################################################################################
##########################################################################################################################

# For each match tuple I have an away team, an home team, a league and a season. 
# The idea is to add to each row 3 columns: final table position of the away team in that season,
#                                           final table position of the home team in that season,
#                                           league winner of such competition in that season.

# There are a few clubs that are written so strangely in the matches dataset that they will
# not find any match in df_stats. Since they are very few, I decided to adjust them manually.
df2.replace({'Espanyol Barcelona': 'Espanyol'}, regex=True, inplace=True)
df2.replace({'OGC Nizza': 'Nice'}, regex=True, inplace=True)
df2.replace({'AS St. Etienne': 'Saint-Étienne'}, regex=True, inplace=True)
df2.replace({'AC Florenz': 'Fiorentina'}, regex=True, inplace=True)
df2.replace({'SSC Neapel': 'Napoli'}, regex=True, inplace=True)
df2.replace({'Queens Park Rangers': 'QPR'}, regex=True, inplace=True)
df2.replace({'FC Turin': 'Torino'}, regex=True, inplace=True)
df2.replace({'Racing Straßburg': 'Strasbourg'}, regex=True, inplace=True)

df3 = df2.copy()
# Problem: again, different formats between the teams names. I need to use fuzzy matching.
# This time I will substitute the original column with the matched values because those names
# are much better for a database since they are more standard. Also, in the matches database
# there are some matches with teams that played only play-out in Bundesliga, which I don't
# really consider part of the season and in fact they are not even in the stats. In this
# way I will drop the rows corresponding to such teams cause of null values (no match).
team_names = df_stats['squad'].unique()
def getHomeTeam(row):
    home_team = row['home name']
    league = row['league']
    if pd.isnull(home_team) or pd.isnull(league): return None
    match, score, _ = process.extractOne(home_team, team_names, scorer = fuzz.partial_ratio)
    if score < 75: return None
    # If the score is greater compare also the league.
    matched_row = df_stats[df_stats['squad'] == match]
    matched_league = matched_row.iloc[0]['competition']
    return match if league == matched_league else None
# Create a new column in the df2 database with the matched home team.
df3['home team'] = df2.apply(getHomeTeam, axis=1)
def getAwayTeam(row):
    away_team = row['away name']
    league = row['league']
    if pd.isnull(away_team) or pd.isnull(league): return None
    match, score, _ = process.extractOne(away_team, team_names, scorer = fuzz.partial_ratio)
    if score < 75: return None
    # If the score is greater compare also the league.
    matched_row = df_stats[df_stats['squad'] == match]
    matched_league = matched_row.iloc[0]['competition']
    return match if league == matched_league else None
# Create a new column in the df2 database with the matched home team.
df3['away team'] = df2.apply(getAwayTeam, axis=1)
# Drop the old columns to keep the new team names, then delete rows with no team.
df3.drop(columns=['home name', 'away name'], inplace=True)
df3 = df3.dropna(subset=['home team', 'away team'])

# Now I can perform the actual joins to add to the dataframe the position
# in the table of the respective season both for the home and the away team.
df3 = df3.merge(df_stats, 
                left_on=['league', 'season', 'home team'],
                right_on=['competition', 'season', 'squad'],
                how='left').rename(columns={'rank': 'home rank'}).drop(columns=['competition', 'squad'])
df3 = df3.merge(df_stats, 
                left_on=['league', 'season', 'away team'],
                right_on=['competition', 'season', 'squad'],
                how='left').rename(columns={'rank': 'away rank'}).drop(columns=['competition', 'squad'])

# Add a column specifying the winner of that league in that season.
# The winner of the league is the team occupying the rank n. 1 in that season.
winners = df_stats[df_stats['rank'] == 1][['competition', 'season', 'squad']]
winners = winners.rename(columns={
    'competition': 'league',
    'squad': 'league_winner'
})
# Merge into df3 on league and season to add the column league winner.
df3 = df3.merge(winners, on=['league', 'season'], how='left')
df3.to_csv('df3.csv', index=False)

##########################################################################################################################
##########################################################################################################################
##                                            PHASE 4: MERGE DF3 & TROPHIES                                             ##
##########################################################################################################################
##########################################################################################################################

