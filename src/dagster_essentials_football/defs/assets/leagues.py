import base64
import dagster as dg
from dagster_duckdb import DuckDBResource
import matplotlib

from utils.plot import plot_leagues
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from dagster_essentials_football.defs.partitions import monthly_partition
import pandas as pd
from dagster_essentials_football.defs.assets import constants
import seaborn as sns


    

@dg.asset(
        partitions_def=monthly_partition,
        deps=["football_competitions_db",
              "football_clubs_db",
              "player_valuations_db"],
        group_name="persisted"
)
def league_valuation_evolution_db(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
):
    """
    calculates and stores league valuation evolution of leagues in the database.
    """
    monthly_partition = context.partition_key
    query = f"""
        select
            c.domestic_competition_id,
            v.market_value
        from
            player_valuations as v
        join clubs as c
            on v.current_club_id = c.club_id
        where v.date >= '{monthly_partition}'
            and v.date < '{monthly_partition}'::date + interval '1 month'
        """
    
    with database.get_connection() as conn:
        result = conn.execute(query).fetch_df()

    if result.empty:
        return
    
    object_cols = result.select_dtypes(['object']).columns
    if len(object_cols) > 0:
        for col in object_cols:
            result[col] = result[col].astype(str)
    

    monthly_league_valuations = result.groupby('domestic_competition_id').agg(
        total_valuation=('market_value', 'sum'),
        min_valuation=('market_value', 'min'),
        max_valuation=('market_value', 'max'),
        player_count=('market_value', 'count')
    ).reset_index()
    
    monthly_league_valuations['partition_date'] = monthly_partition

    ddl_query = f"""
        create table if not exists league_valuation_evolution (
            domestic_competition_id varchar,
            total_valuation float,
            min_valuation float,
            max_valuation float,
            player_count integer,
            partition_date varchar
        );

        delete from league_valuation_evolution where partition_date = '{monthly_partition}';
    """

    with database.get_connection() as conn:
        conn.execute(ddl_query)
        conn.register('temp_data_view', monthly_league_valuations)

        insert_query = """
            insert into league_valuation_evolution (
                domestic_competition_id,
                total_valuation,
                min_valuation,
                max_valuation,
                player_count,
                partition_date
            )
            select 
                domestic_competition_id,
                total_valuation,
                min_valuation,
                max_valuation,
                player_count,
                partition_date
            from temp_data_view;
        """

        conn.execute(insert_query)
        conn.unregister('temp_data_view')


@dg.asset(
        deps=["league_valuation_evolution_db",
              "league_logos"],
        group_name="reports"
)
def first_league_valuation(
    database: DuckDBResource,
):
    """
    creates a graph showing the evolution of first leagues valuations evolution over time.
    """
    query = """
        select
            lve.domestic_competition_id,
            lve.total_valuation,
            lve.max_valuation,
            lve.partition_date,
            fc.name as competition_name,
            fc.country_name
        from
            league_valuation_evolution as lve
        join competitions as fc
            on lve.domestic_competition_id = fc.competition_id
        where fc.sub_type = 'first_tier'
        order by
            lve.partition_date asc;
    """

    with database.get_connection() as conn:
        result = conn.execute(query).fetch_df()

    if result.empty:
        return
    
    result['partition_date'] = pd.to_datetime(result['partition_date'])

    result = result[result['partition_date'] < pd.to_datetime("2025-01-01")]

    all_league_ids = result['domestic_competition_id'].unique()
    total_leagues_count = len(all_league_ids)

    N = 5
    latest_year = result['partition_date'].dt.year.max()
    data_last_year = result[result['partition_date'].dt.year == latest_year]
    top_n_vals = data_last_year.groupby('domestic_competition_id')['total_valuation'].max()
    top_n_ids = top_n_vals.sort_values(ascending=False).head(N).index

    N = 7
    top_n_vals = data_last_year.groupby('domestic_competition_id')['max_valuation'].max()
    top_n_ids_max = top_n_vals.sort_values(ascending=False).head(N).index
    
    result['year'] = result['partition_date'].dt.year
    yearly_data = result.groupby(['domestic_competition_id', 'year']).agg(
        total_valuation=('total_valuation', 'max'),
        competition_name=('competition_name', 'first'),
        country_name=('country_name', 'first'),
        max_valuation=('max_valuation', 'max'),
    ).reset_index()


    yearly_data["competition_name"] = yearly_data["competition_name"].map(
        lambda x: " ".join(x.capitalize() for x in x.split("-"))
    )

    
    yearly_data_total = yearly_data[yearly_data['domestic_competition_id'].isin(top_n_ids)]
    other_total = yearly_data[~yearly_data['domestic_competition_id'].isin(top_n_ids)].groupby('year').agg(
        total_valuation=('total_valuation', 'max'),
    ).reset_index()

    other_leagues_count = total_leagues_count - len(top_n_ids)
    other_total['domestic_competition_id'] = 'Other'
    other_total['competition_name'] = 'Other'
    other_total['country_name'] = str(other_leagues_count)
    yearly_data_total = pd.concat([yearly_data_total, other_total], ignore_index=True)

    yearly_data_total['league_label'] = (
    yearly_data_total['competition_name'] + " (" + yearly_data_total['country_name'] + ")"
    )

    output_path_all = plot_leagues(
        yearly_data_total,
        groupby_columns=['league_label', 'domestic_competition_id'],
        statistic='total_valuation',
        output_path="first_league_valuation_evolution.png",
        set_ylabel='Total Valuation (EUR)',
        set_xlabel='Year',
        set_title='Market Value of All Players In The League',
    )


    yearly_data_max = yearly_data[yearly_data['domestic_competition_id'].isin(top_n_ids_max)]
    other_total = yearly_data[~yearly_data['domestic_competition_id'].isin(top_n_ids_max)].groupby('year').agg(
        max_valuation=('max_valuation', 'max'),
    ).reset_index()

    other_leagues_count = total_leagues_count - len(top_n_ids_max)
    other_total['domestic_competition_id'] = 'Other'
    other_total['competition_name'] = 'Other'
    other_total['country_name'] = str(other_leagues_count)
    yearly_data_max = pd.concat([yearly_data_max, other_total], ignore_index=True)

    yearly_data_max['league_label'] = (
    yearly_data_max['competition_name'] + " (" + yearly_data_max['country_name'] + ")"
    )

    output_path_max = plot_leagues(
        yearly_data_max,
        groupby_columns=['league_label', 'domestic_competition_id'],
        statistic='max_valuation',
        output_path="first_league_max_valuation_evolution.png",
        set_ylabel='Player Valuation (EUR)',
        set_xlabel='Year',
        set_title='Maximum Market Value of The Players In The League',
        step=1_000_000
    )

    with open(output_path_all, 'rb') as file:
        image_data = file.read()

    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content = f"![Image](data:image/jpeg;base64,{base64_data})"

    with open(output_path_max, 'rb') as file:
        image_data = file.read()

    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content += f"\n![Image](data:image/jpeg;base64,{base64_data})"

    return dg.MaterializeResult(
    metadata={
        "preview": dg.MetadataValue.md(md_content)
    }
)
