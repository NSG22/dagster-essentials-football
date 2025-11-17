import os
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from dagster_essentials_football.defs.assets import constants
import seaborn as sns
import pandas as pd

def plot_leagues(yearly_data_total: pd.DataFrame, 
                 groupby_columns= ['league_label', 'domestic_competition_id'], 
                 statistic='total_valuation', 
                 output_path="first_league_valuation_evolution.png",
                 set_ylabel='Total Valuation (EUR)',
                 set_xlabel='Year',
                 set_title='First League Valuation Evolution Over Time',
                 step=1_000_000_000
                 ):

    league_max_vals = yearly_data_total.groupby(groupby_columns)[statistic].max()
    league_max_vals = league_max_vals.reset_index().sort_values(statistic, ascending=False)
    sorted_labels = league_max_vals['league_label'].tolist()
    sorted_ids = league_max_vals['domestic_competition_id'].tolist()

    fig, ax = plt.subplots(figsize=(12, 8))
    colors = sns.color_palette("tab10", n_colors=len(sorted_labels))
    color_map = dict(zip(sorted_labels, colors))
    
    sns.lineplot(
        data=yearly_data_total,
        x='year',
        y=statistic,
        hue='league_label',
        hue_order=sorted_labels,
        palette=color_map,
        marker='o',
        ax=ax,
        legend=False                    
    )

    Y_START = 0.95  # Obere Position der Legende (95% von oben)
    Y_STEP = 0.12   # Vertikaler Abstand zwischen den Einträgen
    IMG_ZOOM = 0.2  # Bildgröße (muss angepasst werden)
    TEXT_X_OFFSET = 1.08 # X-Position des Texts (rechts neben dem Bild)

    fig.subplots_adjust(right=0.75)

    for i, (label, competition_id) in enumerate(zip(sorted_labels, sorted_ids)):

        y_pos = Y_START - (i * Y_STEP)
        
        img = None
        try:
            img_path = constants.LEAGUE_LOGOS_PATH.format(competition_id)
            img = mpimg.imread(img_path)
        except FileNotFoundError:
            try:
                img_path = constants.LEAGUE_LOGOS_PATH.format("default")
                img = mpimg.imread(img_path)
            except FileNotFoundError:
                pass
        
        if img is not None:
            imagebox = OffsetImage(img, zoom=IMG_ZOOM)
            ab = AnnotationBbox(
                imagebox,
                (1.02, y_pos),
                xycoords='axes fraction',
                frameon=False,
                box_alignment=(0.0, 0.5) 
            )
            ax.add_artist(ab)

        ax.text(
            TEXT_X_OFFSET,
            y_pos,
            label,
            color=color_map[label],
            transform=ax.transAxes, 
            fontsize=10,
            verticalalignment='center'
        )

    ax.set_xlabel(set_xlabel) 
    ax.set_ylabel(set_ylabel)
    ax.set_title(set_title)

    
    unit = "B" if step >= 1_000_000_000 else "M" if step >= 1_000_000 else "K"
    formatter = FuncFormatter(lambda x, pos: f'{x / step:.0f}{unit}')
    ax.yaxis.set_major_formatter(formatter)


    output_path = os.path.join("data/outputs/", output_path)
    plt.savefig(output_path, bbox_inches='tight')

    return output_path