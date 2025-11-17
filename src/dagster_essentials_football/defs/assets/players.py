import dagster as dg
from dagster_duckdb import DuckDBResource
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter

@dg.asset(
    deps=["player_valuations_db",
          "football_players_db"]
)
def player_valuation_stats_to_json(
    database: DuckDBResource,
) -> None:
    """
    Calculates and stores player valuation metrics in the database.
    """
    query = """
        select
            p.player_id,
            p.name,
            avg(v.market_value) as avg_valuation,
            max(v.market_value) as max_valuation,
            min(v.market_value) as min_valuation,
            c.name as club_name
            from
            player_valuations v
            join players p
            on v.player_id = p.player_id
            join clubs c
            on v.current_club_id = c.club_id
            group by
            p.player_id,
            p.name,
            c.name
            order by
            avg_valuation desc
            limit 100;
        """
    with database.get_connection() as conn:
        result = conn.execute(query).fetch_df()
    
    unique_clubs = result['club_name'].unique()
    
    cmap = plt.cm.get_cmap('tab20', len(unique_clubs))
    color_map = {club: cmap(i) for i, club in enumerate(unique_clubs)}
    colors = result['club_name'][::-1].map(color_map)
    fig, ax = plt.subplots(figsize=(12, 30)) 
    
    ax.barh(result['name'][::-1], result['avg_valuation'][::-1], color=colors)
    
    ax.set_xlabel('Average Valuation (EUR)')
    ax.set_title('Top 100 Players by Average Valuation')
    
    formatter = FuncFormatter(lambda x, pos: f'{x / 1_000_000:.0f}M')
    ax.xaxis.set_major_formatter(formatter)

    ax.tick_params(axis='y', labelsize=8)
    patches = [mpatches.Patch(color=color_map[club], label=club) for club in unique_clubs]
    ax.legend(handles=patches, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    
    output_path = "data/outputs/top_100_player_valuations.png"
    plt.savefig(output_path, bbox_inches='tight')