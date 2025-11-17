from io import BytesIO
import dagster as dg
import requests
from dagster_essentials_football.defs.assets import constants
from dagster_essentials_football.defs.partitions import monthly_partition
import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd
from dagster_duckdb import DuckDBResource
from bs4 import BeautifulSoup
import os

@dg.asset(group_name="raw_files")
def football_player_valuations_file():
    """"""
    df: pd.DataFrame = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "davidcariboo/player-scores",
    "player_valuations.csv")

    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    df["player_club_domestic_competition_id"] = df["player_club_domestic_competition_id"].astype(str)
    df["market_value_in_eur"] = pd.to_numeric(df["market_value_in_eur"], errors='coerce').astype('Int64')

    with open(constants.RAW_PLAYER_VALUATIONS_FILE_PATH, "wb") as f:
        df.to_parquet(f, index=False)

@dg.asset(
    partitions_def=monthly_partition,
    deps=["football_player_valuations_file"],
    group_name="partitioned_files"
)
def monthly_player_valuations(context: dg.AssetExecutionContext) -> None:
    """
    Loads the LOCAL raw parquet, filters it for the given month, 
    and saves that partition as a Parquet file.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    start_date = pd.to_datetime(partition_date_str)
    end_date = start_date + pd.offsets.MonthEnd(1)

    df = pd.read_parquet(constants.RAW_PLAYER_VALUATIONS_FILE_PATH)

    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    df_partition = df[mask].copy()
    
    with open(constants.PLAYER_VALUATIONS_FILE_PATH.format(month_to_fetch), "wb") as f:
        df_partition.to_parquet(f, index=False)


@dg.asset(
    partitions_def=monthly_partition,
    deps=["monthly_player_valuations"],
    group_name="persisted"
)
def player_valuations_db(
    context: dg.AssetExecutionContext, 
    database: DuckDBResource,
) -> None:
    """
    Scans all processed parquet files and loads them into a single
    table in the DuckDB database.
    """
    monthly_partition = context.partition_key
    month_to_fetch = monthly_partition[:-3]
    sql_query = f"""
        create table if not exists player_valuations (
            player_id integer,
            date date,
            market_value float,
            current_club_id integer,
            competition_id varchar,
            partition_date varchar
        );

        delete from player_valuations where partition_date = '{month_to_fetch}';

        insert into player_valuations 
        select
            player_id,
            date,
            market_value_in_eur,
            current_club_id,
            player_club_domestic_competition_id,
            '{month_to_fetch}' as partition_date
          from '{constants.PLAYER_VALUATIONS_FILE_PATH.format(month_to_fetch)}';
    """
    
    # Use the resource to run the query
    with database.get_connection() as conn:
        conn.execute(sql_query)


@dg.asset(group_name="raw_files")
def football_competitions_file():
    """"""
    df: pd.DataFrame = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "davidcariboo/player-scores",
    "competitions.csv",
    )
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    
    object_cols = df.select_dtypes(['object']).columns
    if len(object_cols) > 0:
        for col in object_cols:
            df[col] = df[col].astype(str)

    with open(constants.COMPETITIONS_FILE_PATH, "wb") as f:
        df.to_parquet(f, index=False)


@dg.asset(deps=["football_competitions_file"],
          group_name="persisted")
def football_competitions_db(database: DuckDBResource) -> None:
    """
    Loads the competitions parquet file into a DuckDB table.
    """
    sql_query = f"""
        create table if not exists competitions as 
        select * from '{constants.COMPETITIONS_FILE_PATH}';
    """
    
    # Use the resource to run the query
    with database.get_connection() as conn:
        conn.execute(sql_query)


@dg.asset(group_name="raw_files")
def football_players_file():
    """"""
    df: pd.DataFrame = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "davidcariboo/player-scores",
    "players.csv",
    )

    with open(constants.PLAYERS_FILE_PATH, "wb") as f:
        df.to_parquet(f, index=False)


@dg.asset(
        deps=["football_players_file"],
    group_name="persisted"
)
def football_players_db(
    database: DuckDBResource,
) -> None:
    """
    Loads the players parquet file into a DuckDB table.
    """
    sql_query = f"""
        create table if not exists players as 
        select * from '{constants.PLAYERS_FILE_PATH}';
    """
    
    # Use the resource to run the query
    with database.get_connection() as conn:
        conn.execute(sql_query)


@dg.asset(group_name="raw_files")
def football_player_appearances_file():
    """
    Downloads the player appearances dataset from Kaggle and saves it as a Parquet file.
    """
    df: pd.DataFrame = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "davidcariboo/player-scores",
    "appearances.csv"
    )

    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    
    object_cols = df.select_dtypes(['object']).columns
    if len(object_cols) > 0:
        for col in object_cols:
            df[col] = df[col].astype(str)

    with open(constants.RAW_PLAYER_APPEARANCES_FILE_PATH, "wb") as f:
        df.to_parquet(f, index=False)


@dg.asset(
    partitions_def=monthly_partition,
    deps=["football_player_appearances_file"],
    group_name="partitioned_files"
)
def monthly_player_appearances(context: dg.AssetExecutionContext) -> None:
    """
    Loads the LOCAL raw parquet, filters it for the given month, 
    and saves that partition as a Parquet file.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    start_date = pd.to_datetime(partition_date_str)
    end_date = start_date + pd.offsets.MonthEnd(1)

    df = pd.read_parquet(constants.RAW_PLAYER_APPEARANCES_FILE_PATH)
    
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    df_partition = df[mask].copy()
    
    with open(constants.PLAYER_APPEARANCES_FILE_PATH.format(month_to_fetch), "wb") as f:
        df_partition.to_parquet(f, index=False)


@dg.asset(
    partitions_def=monthly_partition,
    deps=["monthly_player_appearances"],
    group_name="persisted"
)
def player_appearances_db(
    context: dg.AssetExecutionContext, 
    database: DuckDBResource,
) -> None:
    
    monthly_partition = context.partition_key
    month_to_fetch = monthly_partition[:-3]
    sql_query = f"""
        create table if not exists player_appearances (
            appearance_id varchar,
            game_id integer,
            player_id integer,
            player_club_id integer,
            player_current_club_id integer,
            date date,
            player_name varchar,
            competition_id varchar,
            yellow_cards integer,
            red_cards integer,
            goals integer,
            assists integer,
            minutes_played integer,
            partition_date varchar
        );

        delete from player_appearances where partition_date = '{month_to_fetch}';

        insert into player_appearances 
        select
            appearance_id,
            game_id,
            player_id,
            player_club_id,
            player_current_club_id,
            date,
            player_name,
            competition_id,
            yellow_cards,
            red_cards,
            goals,
            assists,
            minutes_played,
            '{month_to_fetch}' as partition_date
          from '{constants.PLAYER_APPEARANCES_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)


@dg.asset(group_name="raw_files")
def football_clubs_file():
    """"""
    df: pd.DataFrame = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "davidcariboo/player-scores",
    "clubs.csv",
    )

    df["total_market_value"] = pd.to_numeric(df["total_market_value"], errors='coerce').astype('Int64')
    df["average_age"] = pd.to_numeric(df["average_age"], errors='coerce').astype('Float64')
    df["foreigners_percentage"] = pd.to_numeric(df["foreigners_percentage"], errors='coerce').astype('Float64')
    

    object_cols = df.select_dtypes(['object']).columns
    if len(object_cols) > 0:
        for col in object_cols:
            df[col] = df[col].astype(str)

    with open(constants.CLUBS_FILE_PATH, "wb") as f:
        df.to_parquet(f, index=False)


@dg.asset(deps=["football_clubs_file"],
    group_name="persisted")
def football_clubs_db(database: DuckDBResource) -> None:
    """
    Loads the clubs parquet file into a DuckDB table.
    """
    sql_query = f"""
        create table if not exists clubs as 
        select * from '{constants.CLUBS_FILE_PATH}';
    """
    
    # Use the resource to run the query
    with database.get_connection() as conn:
        conn.execute(sql_query)


@dg.asset(
        deps=["football_competitions_db"],
        group_name="static_files"
)
def league_logos(
    database: DuckDBResource,
):

    query = """
    select distinct competition_id, url
    from competitions
    """
    
    with database.get_connection() as conn:
        df = conn.execute(query).fetchdf()

    for _, row in df.iterrows():
        competition_id = row['competition_id']
        url = row['url']
        image_path = constants.LEAGUE_LOGOS_PATH.format(competition_id)
        if os.path.exists(image_path):
            continue  # Logo already exists, skip downloading

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        try:
            page_response = requests.get(url, headers=headers)
            page_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Fehler beim Abrufen der Seite für Wettbewerb {competition_id} von {url}: {e}")
            raise

        soup = BeautifulSoup(page_response.text, "html.parser")
        logo_container = soup.find("div", class_="data-header__profile-container")

        if not logo_container:
            raise Exception("Konnte den Logo-Container (div.data-header__profile-container) nicht finden. Die Seitenstruktur hat sich möglicherweise geändert.")

        # 4. Finde das <img>-Tag *innerhalb* dieses Containers
        logo_img_tag = logo_container.find("img")

        if not logo_img_tag:
            raise Exception("Logo-Container gefunden, aber kein <img>-Tag darin.")

        # 5. Die 'src'-URL aus dem Tag extrahieren
        image_url = logo_img_tag.get("src")
        
        if not image_url:
            raise Exception("<img>-Tag gefunden, aber es hat kein 'src'-Attribut.")

        # 6. Das eigentliche Bild herunterladen
        try:
            image_response = requests.get(str(image_url), headers=headers)
            
            image_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise

        # 7. Das Bild lokal speichern
        with open(image_path, "wb") as img_file:
            img_file.write(image_response.content)


@dg.asset(group_name="raw_files")
def football_games_file():
    """dd"""
    df: pd.DataFrame = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "davidcariboo/player-scores",
    "games.csv",
    )

    with open(constants.GAMES_FILE_PATH, "wb") as f:
        df.to_parquet(f, index=False)


@dg.asset(
    deps=["football_games_file"],
    group_name="persisted"
)
def football_games_db(
    database: DuckDBResource
) -> None:
    """
    Loads the games parquet file into a DuckDB table.
    """
    sql_query = f"""
        create table if not exists games as 
        select * from '{constants.GAMES_FILE_PATH}';
    """
    
    # Use the resource to run the query
    with database.get_connection() as conn:
        conn.execute(sql_query)