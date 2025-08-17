import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, String, Date, MetaData, PrimaryKeyConstraint, ForeignKey

##########################################################################################################################
##########################################################################################################################
##                          POPULATE THE DATA WAREHOUSE FROM THE RECONCILED DATA LAYER                                  ##
##########################################################################################################################
##########################################################################################################################

# Database connection parameters.
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'rdlname': 'Reconciled_Data_Layer',
    'dwname' : 'Data_Warehouse',
    'user': 'postgres',
    'password': 'biar'
}

def populate_dw():
    try:
        print("\n" + "="*60)
        print("POPULATING DATA WAREHOUSE")
        print("="*60)

        # Connect to the Reconciled Data Layer to retrieve data.
        rdl_engine = create_engine(f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['rdlname']}")
        # Store the Reconciled Data Layer.
        rdl = pd.read_sql("SELECT * FROM football_matches", rdl_engine)

        # Connect to the Data Warehouse database to populate it.
        dw_engine = create_engine(f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dwname']}")

        metadata = MetaData()

        #---------------------------------------
        # Cast rdl columns to the correct types.
        #---------------------------------------
        numeric_columns = ['home goals', 'away goals', 'visitor count', 'capacity', 'population', 'home rank', 'away rank',
                           'UCL home', 'UCL away', 'UEL home', 'UEL away', 'CWC home', 'CWC away', 'USC home', 'USC away', 'year']
        for col in numeric_columns:
            rdl[col] = rdl[col].astype('Int64')
        
        rdl['date'] = pd.to_datetime(rdl['date']).dt.date
        rdl['month'] = rdl['month'].astype(str)

        #-----------------
        # SEASON dimension
        #-----------------
        # Take the unique values of season and compute a new index on them.
        dim_season = rdl[['season']].drop_duplicates().reset_index(drop=True).rename(
            columns = {'season': 'season_year'}
        )
        dim_season['key_season'] = dim_season.index + 1
        dim_season = dim_season[['key_season', 'season_year']]

        # Debug.
        dim_season.to_csv('./debug/dim_season.csv', index=False)
        
        season = Table('season', metadata,
            Column('key_season', Integer, primary_key = True),
            Column('season_year', String(20))
        )

        #---------------
        # TEAM dimension
        #---------------
        # Get the list of unique teams from both home and away ones and recompute the index.
        home_teams = rdl[['home team', 'UCL home', 'UEL home', 'CWC home', 'USC home']].rename(
            columns = {
                'home team': 'team_name',
                'UCL home': 'ucl_won',
                'UEL home': 'uel_won',
                'CWC home': 'cwc_won',
                'USC home': 'usc_won'
            }
        )
        away_teams = rdl[['away team', 'UCL away', 'UEL away', 'CWC away', 'USC away']].rename(
            columns = {
                'away team': 'team_name',
                'UCL away': 'ucl_won',
                'UEL away': 'uel_won',
                'CWC away': 'cwc_won',
                'USC away': 'usc_won'
            }
        )
        dim_team = pd.concat([home_teams, away_teams]).drop_duplicates().reset_index(drop=True)
        dim_team['key_team'] = dim_team.index + 1
        dim_team = dim_team[['key_team', 'team_name', 'ucl_won', 'uel_won', 'cwc_won', 'usc_won']]

        # Debug.
        dim_team.to_csv('./debug/dim_team.csv', index=False)

        team = Table('team', metadata,
            Column('key_team', Integer, primary_key = True),
            Column('team_name', String(100)),
            Column('ucl_won', Integer),
            Column('uel_won', Integer),
            Column('cwc_won', Integer),
            Column('usc_won', Integer),
            Column('manager', String(100))
        )

        #---------------
        # DATE dimension
        #---------------
        # Take the unique dates in the rdl, then associate to each row the key of the season
        # instead of the season year itself.
        dim_date = rdl[['date', 'month', 'year', 'season']].drop_duplicates().reset_index(drop=True).rename(
            columns = {'date': 'day'}
        )
        dim_date['key_date'] = dim_date.index + 1
        dim_date = dim_date.merge(dim_season, left_on='season', right_on='season_year', how='left')
        dim_date = dim_date[['key_date', 'day', 'month', 'year', 'key_season']]

        # Debug.
        dim_date.to_csv('./debug/dim_date.csv', index=False)

        date = Table('date', metadata,
            Column('key_date', Integer, primary_key = True),
            Column('day', Date),
            Column('month', String(20)),
            Column('year', Integer),
            Column('key_season', Integer)
        )

        #----------------------
        # COMPETITION dimension
        #----------------------
        dim_competition = rdl[['league', 'type', 'country', 'population']].drop_duplicates().reset_index(drop=True).rename(
            columns = {
                'league': 'competition_name',
                'population': 'country_population'
            }
        )
        dim_competition['key_competition'] = dim_competition.index + 1
        dim_competition = dim_competition[['key_competition', 'competition_name', 'type', 'country', 'country_population']]

        # Debug.
        dim_competition.to_csv('./debug/dim_competition.csv', index=False)

        competition = Table('competition', metadata,
            Column('key_competition', Integer, primary_key = True),
            Column('competition_name', String(100)),
            Column('type', String(50)),
            Column('country', String(50)),
            Column('country_population', BigInteger)
        )

        #------------------
        # REFEREE dimension
        #------------------
        # Take only the unique non-null referee values.
        dim_referee = rdl[['referee']].dropna().drop_duplicates().reset_index(drop=True).rename(
            columns = {'referee': 'referee_name'}
        )
        dim_referee['key_referee'] = dim_referee.index + 1
        dim_referee = dim_referee[['key_referee', 'referee_name']]

        # Debug.
        dim_referee.to_csv('./debug/dim_referee.csv', index=False)

        referee = Table('referee', metadata,
            Column('key_referee', Integer, primary_key = True),
            Column('referee_name', String(100))
        )

        #------------------
        # STADIUM dimension
        #------------------
        dim_stadium = rdl[['stadium', 'city', 'capacity']].dropna().drop_duplicates().reset_index(drop=True).rename(
            columns = {'stadium': 'stadium_name'}
        )
        dim_stadium['key_stadium'] = dim_stadium.index + 1
        dim_stadium = dim_stadium[['key_stadium', 'stadium_name', 'city', 'capacity']]

        # Debug.
        dim_stadium.to_csv('./debug/dim_stadium.csv', index=False)

        stadium = Table('stadium', metadata,
            Column('key_stadium', Integer, primary_key = True),
            Column('stadium_name', String(100)),
            Column('city', String(100)),
            Column('capacity', Integer)
        )

        #---------------
        # POSITION table
        #---------------
        # Unify the lists of positions of the various teams keeping only unique values.
        home_rank = rdl[['home rank', 'home team', 'season']].rename(
            columns = {'home rank': 'position',
                       'home team': 'team_name'}
        )
        away_rank = rdl[['away rank', 'away team', 'season']].rename(
            columns = {'away rank': 'position',
                       'away team': 'team_name'}
        )
        dim_position = pd.concat([home_rank, away_rank]).drop_duplicates().reset_index(drop=True)
        # For the star schema I need the keys of the team and the season instead of the actual values,
        # so I need to merge to retrieve them.
        dim_position = dim_position.merge(dim_team[['key_team', 'team_name']], on='team_name', how='left')
        dim_position = dim_position.merge(dim_season, left_on='season', right_on='season_year', how='left')
        dim_position['key_position'] = dim_position.index + 1
        dim_position = dim_position[['key_position', 'key_team', 'key_season', 'position']]

        # Debug.
        dim_position.to_csv('./debug/dim_position.csv', index=False)

        position = Table('position', metadata,
            Column('key_position', Integer, primary_key = True),
            Column('key_team', Integer),
            Column('key_season', Integer),
            Column('position', Integer)
        )

        #--------------
        # MANAGER table
        #--------------
        home_coach = rdl[['home coach', 'home team', 'date']].rename(
            columns = {'home coach': 'manager_name',
                       'home team': 'team_name'}
        )
        away_coach = rdl[['away coach', 'away team', 'date']].rename(
            columns = {'away coach': 'manager_name',
                       'away team': 'team_name'}
        )
        dim_manager = pd.concat([home_coach, away_coach]).drop_duplicates().reset_index(drop=True)

        dim_manager = dim_manager.merge(dim_team[['key_team', 'team_name']], on='team_name', how='left')
        dim_manager = dim_manager.merge(dim_date[['key_date', 'day']], left_on='date', right_on='day', how='left')
        dim_manager['key_manager'] = dim_manager.index + 1
        dim_manager = dim_manager[['key_manager', 'key_team', 'key_date', 'manager_name']]

        # Debug.
        dim_manager.to_csv('./debug/dim_manager.csv', index=False)

        manager = Table('manager', metadata,
            Column('key_manager', Integer, primary_key = True),
            Column('key_team', Integer),
            Column('key_date', Integer),
            Column('manager_name', String(100))
        )

        #-------------
        # WINNER table
        #-------------
        # Take the winner team of each league in each season and merge to store the keys instead of the values.
        dim_winner = rdl[['league', 'season', 'league_winner']].drop_duplicates().reset_index(drop=True)
        dim_winner = dim_winner.merge(dim_competition[['key_competition', 'competition_name']], left_on='league', right_on='competition_name', how='left')
        dim_winner = dim_winner.merge(dim_season, left_on='season', right_on='season_year', how='left')
        dim_winner = dim_winner.merge(dim_team[['key_team', 'team_name']], left_on='league_winner', right_on='team_name', how='left')
        dim_winner['key_winner'] = dim_winner.index + 1
        dim_winner.rename(columns = {'key_team': 'key_winner_team'}, inplace = True)
        dim_winner = dim_winner[['key_winner', 'key_competition', 'key_season', 'key_winner_team']]

        # Debug.
        dim_winner.to_csv('./debug/dim_winner.csv', index=False)

        winner = Table('winner', metadata,
            Column('key_winner', Integer, primary_key = True),
            Column('key_competition', Integer),
            Column('key_season', Integer),
            Column('key_winner_team', Integer)
        )

        #-----------------
        # MATCH fact table
        #-----------------
        # The fact table 'Match' contains the measures stored in rdl and the surrogate keys towards the dimensions.
        # To store such surrogate keys I need to merge with every involved dimension table.
        fact_match = rdl.merge(dim_team[['key_team', 'team_name']], left_on='home team', right_on='team_name', how='left').rename(
            columns = {'key_team': 'key_home',
                       'home goals': 'home_goals',
                       'away goals': 'away_goals',
                       'visitor count': 'attendance'}
        )
        fact_match = fact_match.merge(dim_team[['key_team', 'team_name']], left_on='away team', right_on='team_name', how='left').rename(
            columns = {'key_team': 'key_away'}
        )
        fact_match = fact_match.merge(dim_date[['key_date', 'day']], left_on='date', right_on='day', how='left')
        fact_match = fact_match.merge(dim_competition[['key_competition', 'competition_name']], left_on='league', right_on='competition_name', how='left')
        fact_match = fact_match.merge(dim_referee[['key_referee', 'referee_name']], left_on='referee', right_on='referee_name', how='left')
        fact_match = fact_match.merge(dim_stadium[['key_stadium', 'stadium_name']], left_on='stadium', right_on='stadium_name', how='left')
        fact_match = fact_match[['key_home', 'key_away', 'key_date', 'key_competition', 'key_referee', 'key_stadium', 'home_goals', 'away_goals', 'attendance']]

        # Convert float keys to int and handle NaN.
        key_columns = ['key_home', 'key_away', 'key_date', 'key_competition', 'key_referee', 'key_stadium']
        for col in key_columns:
            if col in ['key_referee', 'key_stadium']:  # Nullable.
                fact_match[col] = fact_match[col].replace({np.nan: None})
                fact_match[col] = pd.to_numeric(fact_match[col], errors='coerce').astype('Int64')
            else:  # Not null.
                fact_match[col] = pd.to_numeric(fact_match[col], errors='coerce').astype('Int64')
        
        # Debug.
        fact_match.to_csv('./debug/fact_match.csv', index=False)

        match_fact = Table('match', metadata,
            Column('key_home', Integer, ForeignKey('team.key_team')),
            Column('key_away', Integer, ForeignKey('team.key_team')),
            Column('key_date', Integer, ForeignKey('date.key_date')),
            Column('key_competition', Integer, ForeignKey('competition.key_competition')),
            Column('key_referee', Integer, ForeignKey('referee.key_referee'), nullable=True),
            Column('key_stadium', Integer, ForeignKey('stadium.key_stadium'), nullable=True),
            Column('home_goals', Integer),
            Column('away_goals', Integer),
            Column('attendance', Integer, nullable=True),
            PrimaryKeyConstraint('key_home', 'key_away', 'key_date', 'key_competition')
        )

        #----------------------------------------------------------------------------
        # DELETE the tables from the database if they exists, then CREATE them again.
        #----------------------------------------------------------------------------
        metadata.drop_all(dw_engine)
        metadata.create_all(dw_engine)

        #-------------------------------------------------------
        # POPULATE the schema by INSERTING data into the tables.
        #-------------------------------------------------------
        with dw_engine.connect() as conn:

            print("\n" + "="*60)
            print("Inserting dimensions...")
            conn.execute(season.insert(), dim_season.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_season)} seasons.")
            conn.execute(team.insert(), dim_team.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_team)} teams.")
            conn.execute(date.insert(), dim_date.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_date)} dates.")
            conn.execute(competition.insert(), dim_competition.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_competition)} competitions.")
            conn.execute(referee.insert(), dim_referee.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_referee)} referees.")
            conn.execute(stadium.insert(), dim_stadium.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_stadium)} stadiums.")
            conn.execute(position.insert(), dim_position.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_position)} positions.")
            conn.execute(manager.insert(), dim_manager.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_manager)} managers.")
            conn.execute(winner.insert(), dim_winner.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(dim_winner)} winners.")

            print("\n" + "="*60)
            print("Inserting the fact table...")
            conn.execute(match_fact.insert(), fact_match.to_dict(orient='records'))
            print(f"  ✅ Inserted {len(fact_match)} matches.")
            print("="*60)

            # Commit the transaction.
            conn.commit()
            print("✅ Transaction committed.")
            print("="*60)

        print("\n" + "="*60)
        print("✅ Data warehouse successfully populated!")
        print("="*60)

    except Exception as e:
        print(f"❌ Failed to populate Data Warehouse: {e}")

populate_dw()