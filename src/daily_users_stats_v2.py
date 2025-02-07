"""
Author: Bertrand, 2025-01-16
Description:
This script creates statistics for daily users database
PARAM:  -y or --year : a year or a list of year
        -l or --labo : the name of the laboratory
python3 -y 2023,2024 -l 'IAE Lille"
"""

import os
import pandas as pd
import sys
import argparse
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt

# rajouter dans la variable d'environnement PATH contenant la liste des répertoires systèmes (programme python, librairies, ...)
# c'est très important quand on crée un package, de rajouter ce répertoire dans PATH
sys.path.append(str(Path(os.getcwd())))
from utils.LogWriter import log_location, log_config, log_args
from utils.Toolbox_lib import create_year_calendar
from utils.dbclient.DatabaseClient import DbConnector
from module.env import *
os.environ[ 'MPLCONFIGDIR' ] = '/tmp/'

# Path Definitions
HOME = Path(__file__).parent.parent
CHEMIN_RESULTAT = Path(HOME, "resultat")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)


parser = argparse.ArgumentParser(description="This script creates statistics for daily users database. \n \
                                 Param: -y or --year(or list of years)")

parser.add_argument('--year', '-y',
                    help="a year or a list of year", required=False)

parser.add_argument('--labo', '-l',
                    help="the name of the laboratory", required=False)

args = parser.parse_args()


def get_multiple_locator(number: int) -> int:
    magnitude = 0
    n = number
    multiple = {}
    while n >= 10:
        n //= 10
        magnitude += 1        
    
    multiple = {0:10, 1:10, 2:100, 3:1000, 4:10000, 5:100000,6:1000000,7:10000000,8:100000000,9:1000000000}
    return multiple[magnitude]


def create_statistique_requete(condition_year: str, type_data: str , condition_labo: str = "") -> pd.DataFrame:
    """
    This function creates dataframes for daily users database depending on the type of data requested (laboratory, user, database)
    """
    # Connect to the PostgreSQL database server   
    req_extraction_stats = f"SELECT distinct id_utilisateur_drupal as id_user, nom_utilisateur as user_name ,id_groupe_labo as id_labo , node.name as institution_name, \
    date_part('year',date_heure_extraction) as year, date_part('month',date_heure_extraction) as month, nom_base_interrogee as database_name, \
    type_interrogation, nb_codes_en_entree as nb_codes, date_heure_extraction,CASE date_part('month',date_heure_extraction) \
    WHEN 1 THEN 'January' \
    WHEN 2 THEN 'February' \
    WHEN 3 THEN 'March' \
    WHEN 4 THEN 'April' \
    WHEN 5 THEN 'May' \
    WHEN 6 THEN 'June' \
    WHEN 7 THEN 'July' \
    WHEN 8 THEN 'August' \
    WHEN 9 THEN 'September' \
    WHEN 10 THEN 'October' \
    WHEN 11 THEN 'November' \
    WHEN 12 THEN 'December' \
    END as month2, \
    CASE type_interrogation \
    WHEN 1 THEN 'prévisualisation' \
    WHEN 2 THEN 'téléchargement' \
    WHEN 3 THEN 'téléchargement' \
    END as type_interrogation2, \
    CASE \
    WHEN nom_base_interrogee LIKE '%histo_actions%' THEN 'Stocks' \
    WHEN nom_base_interrogee LIKE '%actions_%' THEN 'Stocks' \
    WHEN nom_base_interrogee LIKE '%indices_telekurs%' THEN 'Global\Market Indices' \
    WHEN nom_base_interrogee LIKE '%histo_indices_telekurs%' THEN 'Global\Market Indices' \
    WHEN nom_base_interrogee LIKE '%indices_eurofidai%' THEN 'Eurofidai Indices' \
    WHEN nom_base_interrogee LIKE '%histo_indices_eurofidai%' THEN 'IEurofidai Indices' \
    WHEN nom_base_interrogee LIKE '%corres_code%' THEN 'Code Mapping table' \
    WHEN nom_base_interrogee LIKE '%fonds_mutuel_cote%' THEN 'Mutual Funds' \
    WHEN nom_base_interrogee LIKE '%fonds_mutuel_valeur%' THEN 'Mutual Funds' \
    WHEN nom_base_interrogee LIKE '%fonds_mutuel_code%' THEN 'Mutual Funds' \
    WHEN nom_base_interrogee LIKE '%fonds_mutuel_infos_comp' THEN 'Mutual Funds ' \
    WHEN nom_base_interrogee LIKE '%fonds_mutuel_cote_infos_comp' THEN 'Mutual Funds' \
    WHEN nom_base_interrogee LIKE '%change%' THEN 'Spot Exchange Rate' \
    WHEN nom_base_interrogee LIKE '%histo_ost%' THEN 'Corporate Events' \
    WHEN nom_base_interrogee LIKE 'ost%' THEN 'Corporate Events' \
    WHEN nom_base_interrogee LIKE '%esg%' THEN 'ESG' \
    END as database_name2, \
    CASE \
    WHEN nom_base_interrogee LIKE '%histo%' THEN 'Search_Code' \
    ELSE 'Extract_Data' \
    END AS code_ou_data \
    FROM statistique_requete as sr LEFT JOIN institution_entity as node \
    ON sr.id_groupe_labo=node.id \
    WHERE {condition_year} AND  nom_groupe_labo NOT IN ('EUROFIDAI','administrateur Drupal') \
    AND id_utilisateur_drupal NOT IN (1178,1922,367) {condition_labo} \
    ORDER BY year,month,date_heure_extraction,id_utilisateur_drupal,node.name \
    ;"

    df_stats_daily_users = DbConnector('yakari', echo=True).execute_query(req_extraction_stats)
    df_stats_daily_users['year'] = df_stats_daily_users['year'].astype(int)
    df_stats_daily_users['month'] = df_stats_daily_users['month'].astype(int)
    df_stats_daily_users['nb_codes'] = df_stats_daily_users['nb_codes'].astype(int)
    df_stats_daily_users['yearmonth'] = df_stats_daily_users['year'].astype(str) + df_stats_daily_users['month'].astype(str).str.zfill(2)
    
    #df_stats_daily_users = df_stats_daily_users[['institution_name','yearmonth','year','month','month2','nb_codes']].groupby(['institution_name','year','yearmonth','month2','month']).sum('nb_codes').reset_index()
    df_stats_daily_users.sort_values(by=['institution_name','year','month'],inplace=True)
    
    # Convert 'year_month' to datetime objects
    df_stats_daily_users['date'] = pd.to_datetime(df_stats_daily_users['yearmonth'], format='%Y%m')

    # Extract formatted month names
    df_stats_daily_users['month_name'] = df_stats_daily_users['date'].dt.strftime('%b%y')  # %b for abbreviated month names (Jan, Feb...)
   
    df_stats_daily_users.to_csv(str(CHEMIN_RESULTAT / "dataframe_stats_daily_users.csv"), index=False, encoding='utf-8')

    #stats for all laboratories
    df_all_laboratories = df_stats_daily_users[['year','month','month2','date','month_name','nb_codes','institution_name','user_name']].drop_duplicates(subset=['institution_name','user_name','year','month','month2']).groupby(['year','month','month2']).size().reset_index(name='nb_users')
    
    # stats per laboratory
    df_per_laboratory = df_stats_daily_users[['institution_name','year','month','month2','date','month_name','nb_codes']].groupby(['institution_name','year','month2','month','date','month_name']).sum('nb_codes').reset_index()
    df_per_laboratory.sort_values(by=['institution_name','year','month'],inplace=True)
    
    # stats per user
    df_all_users = df_stats_daily_users[['institution_name','year','month','month2','date','user_name','month_name','nb_codes']].drop_duplicates(subset=['institution_name','user_name', 'year','month','month2']).groupby(['institution_name','year','month','month2']).size().reset_index(name='nb_users')
    df_all_users.sort_values(by=['institution_name','year','month'],inplace=True)     

    # stats per database
    df_all_db = df_stats_daily_users[['institution_name','database_name2','year','month','month2','date','month_name','nb_codes']].groupby(['institution_name','database_name2','year']).sum('nb_codes').reset_index()
    df_all_db.sort_values(by=['institution_name','database_name2','year'],inplace=True) 
  
    
    if type_data == 'laboratory':
        return df_per_laboratory
    elif type_data == 'user':
        return df_all_users
    elif type_data == 'database':
        return df_all_db
    elif type_data == 'all':
        return df_all_laboratories
    else:
        return None


def create_graph(df: pd.DataFrame, labo_name: str, year: int, x_var: str, y_var: str, color: str , legend_title: str=None, \
                  xlabel: str = None, ylabel: str = None, title: str = None,  save: bool = False, **kwargs):  
      
    def get_yaxis_width(ax):  # Changed to y-axis width
        width = 0
        axes = [ax] + ax.child_axes
        for ax in axes:
            width += ax.yaxis.get_tightbbox().width  # y-axis width
            width += ax.yaxis.get_tick_params()['pad']
        return width * 72 / fig.dpi
    
    
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.style.use('seaborn-v0_8-dark-palette')
    plt.rc('font', size=12, family='Arial', weight='normal')  # Set font properties
    ax.spines[['bottom', 'top', 'right']].set_visible(True)  # Hide spines    
    bc = ax.bar(df[x_var], df[y_var], color=color ) # barh for horizontal   
    ax.tick_params(axis='x', rotation=60, labelsize=10)  # Rotate x-axis labels 

    ax.bar_label(bc, labels=df[y_var], padding=30, fmt='%s', fontsize=10)  # fmt='%s' for string labels
    
    # Add custom legend if needed
    ax.legend(title=legend_title, loc='upper right', labels=[f"{year}"])
    ax.set_xlabel(xlabel, fontsize=14, loc='center', fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=14, loc='center', fontweight='bold')
    ax.set_title(f"{labo_name}({year})", fontsize=20, fontweight='bold', \
                backgroundcolor='lightgrey', loc='center', pad=15)
    
    # ax.yaxis.set_tick_params(   # y-axis ticks for horizontal chart
    #     rotation=0,        
    #     left=True,
    #     length=0,
    #     pad=5,
    # )
    # ax_left = get_yaxis_width(ax)  # Get y-axis width for tick lines

    from matplotlib.ticker import MultipleLocator
    max_value = df[y_var].max()
    if max_value <= 100 :   # Set x-axis limits
        ax.set_ylim(0, max_value + 10) 
    elif max_value <= 50000:
        ax.set_ylim(0, max_value + max_value*0.5) 
    elif max_value > 50000:
        ax.set_ylim(0, max_value + max_value*0.25)
       
    multiple = get_multiple_locator(max_value)
    ax.yaxis.set_major_locator(MultipleLocator(multiple))  # Keep x-axis ticks as multiples of 5 for horizontal
    
    plt.tight_layout()  # Adjust subplot parameters to give specified padding

    if save:
        plt.savefig(str(CHEMIN_RESULTAT / f"{title}.png"), bbox_inches='tight')  # Ensure all elements are included
    plt.show()


def create_seaborn_relplot(df: pd.DataFrame, x_var: str, y_var: str, kind: str = "scatter", palette: str = None, height: int = 5, aspect: float = 1.5, title: str = None, hue: str = None, legend_labels: list = None, legend_title: str=None, save: bool = False, filename: str=None, **kwargs):
    """
    Creates a flexible relplot with nested x-axis variables and customizable aesthetics.

    Args:
        df: The Pandas DataFrame containing the data.
        x_var: The variable to plot on the x-axis.
        y_var: The variable to plot on the y-axis.
        kind: The kind of plot (e.g., "bar", "violin", "box"). Defaults to "bar".
        palette: The color palette. Can be a string (e.g., "viridis", "pastel") or a list of colors.
        height: The height of each facet.
        aspect: The aspect ratio of each facet.
        title: The plot title.
        legend_title: The title for the legend
        **kwargs: Additional keyword arguments to pass to `sns.catplot`.
    """
    g = sns.relplot(
        x=x_var,
        y=y_var,
        data=df,
        kind=kind,
        hue=hue,
        palette=palette,
        style=hue,
        markers=True,        
        height=height,
        aspect=aspect,
        ci=None,  # Disable error bars
        legend=False,  # Disable the default legend        
        **kwargs
    )

    g.tick_params(axis='x', rotation=60, labelsize=10)  # Rotate x-axis labels 
          
    # Add custom legend if needed
    plt.style.use('seaborn-v0_8-dark-palette')
    ax = g.ax
    ax.grid(True, axis='y', linestyle='--', linewidth=0.5, color='grey', alpha=0.5)  # Add gridlines
    #ax.bar_label(ax, labels=y_var, padding=30, fmt='%s', fontsize=10)  # fmt='%s' for string labels
    plt.legend(title="year", loc='upper right',labels=legend_labels)
    plt.xlabel("Month", fontsize=14, loc='center', fontweight='bold')
    plt.ylabel("Number of users", fontsize=14, loc='center', fontweight='bold')
    plt.title(title, fontsize=20, fontweight='bold', \
                backgroundcolor='lightgrey', loc='center', pad=15)

    # Set the title if provided
    # if title:
    #     g.fig.suptitle(title)

    plt.tight_layout()  # Adjust subplot parameters to give specified padding

    if save:
        plt.savefig(str(CHEMIN_RESULTAT / f"{filename}.png"))
    plt.show()


def create_and_save_graph(df_labo: pd.DataFrame, df_user: pd.DataFrame, df_db: pd.DataFrame, years: int, labo: str):

    for labo in df_labo['institution_name'].unique():
            
            if isinstance(years, int): 
                # graphics Laboratories 
                df_labo_year =  df_labo[(df_labo['institution_name'] == labo) & (df_labo['year'] == int(years))]
                if not df_labo_year.empty:               
                    create_graph(df_labo_year, labo_name=labo, year=years, x_var='month2', y_var='nb_codes', \
                    color='purple', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{labo}_{years}', save=True)          
                # graphics Users
                df_user_year = df_user[(df_user['year'] == years) & (df_user["institution_name"] == labo)]
                if not df_user_year.empty:           
                    create_graph(df_user_year, labo_name=labo, year=years, x_var='month2', y_var='nb_users', \
                    color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{labo}_users_{years}', save=True)          
                
                # graphics Databases
                df_db_year = df_db[(df_db['year'] == years) & (df_db["institution_name"] == labo)]
                if not df_db_year.empty:
                        create_graph(df_db_year, labo_name=labo, year=years, x_var='database_name2', y_var='nb_codes', \
                    color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{labo}_database_{years}', save=True)   
               
            elif isinstance(years, list):
                # graphics Laboratories 
                for year in years:
                    df_labo_year =  df_labo[(df_labo['institution_name'] == labo) & (df_labo['year'] == int(year))]                    
                    if not df_labo_year.empty:              
                        create_graph( df_labo_year, labo_name=labo, year=year, x_var='month2', y_var='nb_codes', \
                    color='purple', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{labo}_{year}', save=True)                   
                    
                    # graphics Users
                    df_user_year = df_user[(df_user['year'] == int(year)) & (df_user["institution_name"] == labo)]
                    if not df_user_year.empty:           
                        create_graph(df_user_year, labo_name=labo, year=year, x_var='month2', y_var='nb_users', \
                        color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{labo}_users_{year}', save=True) 

                    # graphics Databases
                    df_db_year = df_db[(df_db['year'] == int(year)) & (df_db["institution_name"] == labo)]
                    #print(df_db_year.head(20))
                    if not df_db_year.empty:
                        create_graph( df_db_year, labo_name=labo, year=year, x_var='database_name2', y_var='nb_codes', \
                    color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{labo}_database_{year}', save=True)                   
                    

def main():
    if args.year:
        years = args.year.split(',')
        if len(years) == 1:
            years = int(years[0])        
            condition_year= f"date_part('year',date_heure_extraction) IN ({years})"       
        else:
            condition_year= f"date_part('year',date_heure_extraction) IN {tuple(years)}"
    else:
        condition_year= f"date_part('year',date_heure_extraction) >= 2020"
    

    df_count_all_labo_users = create_statistique_requete(condition_year, type_data='all', condition_labo="")     
    df_count_all_labo_users.to_csv(str(CHEMIN_RESULTAT / "dataframe_all_labo_users.csv"), index=False, encoding='utf-8', ) 
    #print(df_count_all_labo_users.head(20))
   
    if not df_count_all_labo_users.empty:
        create_seaborn_relplot(df_count_all_labo_users, x_var='month2', y_var='nb_users', kind='line', hue ="year", title="Number of Eurofidai's Database Users", legend_labels=years, filename="Number of Eurofidai's Database Users", save=True, height=5, aspect=1.5)

    if args.labo:
        condition_labo = f"AND node.name = '{args.labo}'"
        labo = args.labo  
        df_labo = create_statistique_requete(condition_year, type_data='laboratory', condition_labo=condition_labo)
        df_user = create_statistique_requete(condition_year, type_data='user', condition_labo=condition_labo)
        df_db = create_statistique_requete(condition_year, type_data='database', condition_labo=condition_labo)                         
                    
        # graphics Laboratories 
        df_labo_year =  df_labo[(df_labo['institution_name'] == labo) & (df_labo['year'] == int(years))]
        if not df_labo_year.empty:               
            create_graph(df_labo_year, labo_name=labo, year=years, x_var='month2', y_var='nb_codes', \
            color='purple', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{labo}_{years}', save=True)          
    
        # graphics Users
        df_user_year = df_user[(df_user['year'] == years) & (df_user["institution_name"] == labo)]
        if not df_user_year.empty:           
            create_graph(df_user_year, labo_name=labo, year=years, x_var='month2', y_var='nb_users', \
            color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{labo}_users_{years}', save=True)          
                    
        # graphics Databases
        df_db_year = df_db[(df_db['year'] == years) & (df_db["institution_name"] == labo)]
        if not df_db_year.empty:
            create_graph(df_db_year, labo_name=labo, year=years, x_var='database_name2', y_var='nb_codes', \
            color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{labo}_database_{years}', save=True)   
        
    else:
        condition_labo = ""
        df_labo = create_statistique_requete(condition_year, type_data='laboratory', condition_labo=condition_labo)
        df_user = create_statistique_requete(condition_year, type_data='user', condition_labo=condition_labo)
        df_db = create_statistique_requete(condition_year, type_data='database', condition_labo=condition_labo)
        
        for labo in df_labo['institution_name'].unique():
            
            if isinstance(years, int): 
                # graphics Laboratories 
                df_labo_year =  df_labo[(df_labo['institution_name'] == labo) & (df_labo['year'] == int(years))]
                if not df_labo_year.empty:               
                    create_graph(df_labo_year, labo_name=labo, year=years, x_var='month2', y_var='nb_codes', \
                    color='purple', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{labo}_{years}', save=True)          
                # graphics Users
                df_user_year = df_user[(df_user['year'] == years) & (df_user["institution_name"] == labo)]
                if not df_user_year.empty:           
                    create_graph(df_user_year, labo_name=labo, year=years, x_var='month2', y_var='nb_users', \
                    color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{labo}_users_{years}', save=True)          
                
                # graphics Databases
                df_db_year = df_db[(df_db['year'] == years) & (df_db["institution_name"] == labo)]
                if not df_db_year.empty:
                        create_graph(df_db_year, labo_name=labo, year=years, x_var='database_name2', y_var='nb_codes', \
                    color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{labo}_database_{years}', save=True)   
               
            elif isinstance(years, list):
                # graphics Laboratories 
                for year in years:
                    df_labo_year =  df_labo[(df_labo['institution_name'] == labo) & (df_labo['year'] == int(year))]                    
                    if not df_labo_year.empty:              
                        create_graph( df_labo_year, labo_name=labo, year=year, x_var='month2', y_var='nb_codes', \
                    color='purple', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{labo}_{year}', save=True)                   
                    
                    # graphics Users
                    df_user_year = df_user[(df_user['year'] == int(year)) & (df_user["institution_name"] == labo)]
                    if not df_user_year.empty:           
                        create_graph(df_user_year, labo_name=labo, year=year, x_var='month2', y_var='nb_users', \
                        color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{labo}_users_{year}', save=True) 

                    # graphics Databases
                    df_db_year = df_db[(df_db['year'] == int(year)) & (df_db["institution_name"] == labo)]
                    #print(df_db_year.head(20))
                    if not df_db_year.empty:
                        create_graph( df_db_year, labo_name=labo, year=year, x_var='database_name2', y_var='nb_codes', \
                    color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{labo}_database_{year}', save=True)                   
                    

if __name__ == "__main__":
    main()