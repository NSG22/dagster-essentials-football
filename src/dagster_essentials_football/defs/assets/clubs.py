import dagster as dg
from dagster_duckdb import DuckDBResource
import matplotlib
matplotlib.use("Agg")

from dagster_essentials_football.defs.partitions import monthly_partition


@dg.asset(
        partitions_def=monthly_partition,
        group_name="persisted",
)
def club_valuation_evolution_db(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
):
    """
    calculates and stores club valuation evolution of clubs in the database.
    """
    monthly_partition = context.partition_key
    query = f"""
        select
            c.name as club_name,
            v.market_value,
            v.domestic_competition_id,
        from
            player_valuations v
        join clubs c
            on v.current_club_id = c.club_id
        where v.date >= '{monthly_partition}'
            and v.date < '{monthly_partition}'::date + interval '1 month'
        """
    
    with database.get_connection() as conn:
        result = conn.execute(query).fetch_df()

    if result.empty:
        return
    

    monthly_club_valuations = result.groupby('club_name').agg(
        club_name=('club_name', 'first'),
        total_valuation=('market_value', 'sum'),
        min_valuation=('market_value', 'min'),
        max_valuation=('market_value', 'max'),
        squad_size=('market_value', 'count'),
        domestic_competition_id=('domestic_competition_id', 'first')
    )
    
    monthly_club_valuations['partition_date'] = monthly_partition

    query_insert = f"""
        create table if not exists club_valuation_evolution (
            club_name varchar,
            total_valuation float,
            min_valuation float,
            max_valuation float,
            squad_size integer,
            domestic_competition_id varchar,
            partition_date varchar
        );

        delete from club_valuation_evolution where partition_date = '{monthly_partition}';

        insert into club_valuation_evolution 
        values (
            '{monthly_club_valuations['club_name']}',
            {monthly_club_valuations['total_valuation']},
            {monthly_club_valuations['min_valuation']},
            {monthly_club_valuations['max_valuation']},
            {monthly_club_valuations['squad_size']},
            '{monthly_club_valuations['domestic_competition_id']}',
            '{monthly_partition}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query_insert)