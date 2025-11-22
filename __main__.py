import pandas as pd
from src.fortnite_api import *
from src.azure_db import *
from sqlalchemy import text 

def connect_to_db():
    """
    Connect to the Azure database and return the engine.
    """
    engine = azure_db_connect()
    return engine

def retrieve_existing_data(engine):
    """
    Retrieve existing players from the database.
    """
    players_df = pd.read_sql("SELECT * FROM fortnite_player", con=engine)
    players_stats_hist = pd.read_sql("SELECT * FROM fortnite_player_stats", con=engine)
    return players_df, players_stats_hist

def upload_data(engine, players_stats_hist, players_stats_current):
    """
    Upload the cleaned data to the database.
    """
    if pd.to_datetime(players_stats_hist['update_date'].max()).date() == date.today():
        print("Stats for today already exist in the historical table.")
    else:
        with engine.begin() as conn:
            #Reliable fallback that respects FK constraints:
            conn.execute(text("DELETE FROM fortnite_player_stats"))

            #Update Current Stats to players_stats
            players_stats_current.to_sql('fortnite_player_stats', con=engine, if_exists='append', index=False)
            #Update same Stats to players_stats_hist --> Place a security to check if stats were already inserted for the day
            players_stats_current.to_sql('fortnite_player_stats_hist', con=engine, if_exists='append', index=False)
        print('Data upload complete.')

def main():
    #Connect to Azure DB
    print('Connecting to the database...')
    engine = connect_to_db()

    #Retrieve existing players from DB
    print('Retrieving existing players and history stats from the database...')
    players_df, players_stats_hist = retrieve_existing_data(engine)

    #Get today's stats from Fortnite API
    print('Getting today\'s stats from Fortnite API...')
    players_stats_current = get_stats(players_df)

    #Clean the data retrieved
    print('Cleaning the data retrieved...')
    players_stats_current = clean_current_stats(players_stats_current)

    #compare current stats with historical stats and prepare for upload
    players_stats_current_update = clean_upload_stats(players_stats_hist, players_stats_current)

    #Upload data to Azure DB
    print('Uploading the data to the database...')
    upload_data(engine, players_stats_hist, players_stats_current_update)

    #Dispose engine after all DB operations are complete
    engine.dispose()

if __name__ == "__main__":
    main()





