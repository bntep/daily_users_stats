"""
Author: Bertrand, 2025-01-16
This script creates statistics for daily users database
Arguments:  -y or --year : a year or a list of year (optional, default is all years to current year)
            -l or --labo : the name of the laboratory or a list of laboratories separated by ";" (optional, default is all laboratories)
Example of usage:
python3 src/oop_daily_users_stats.py -y 2023,2024 -l "IAE Lille"
python3  oop_daily_users_stats.py
python3 oop_daily_users_stats.py -y 2023,2024 -l "IAE Lille;ESSEC"

"""


import os
import pandas as pd
import sys
import argparse
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
from pptx import Presentation
from pptx.util import Pt


# rajouter dans la variable d'environnement PATH contenant la liste des répertoires systèmes (programme python, librairies, ...)
# c'est très important quand on crée un package, de rajouter ce répertoire dans PATH avant d'importer les modules
# sinon python ne trouvera pas les modules à importer
sys.path.append(str(Path(os.getcwd())))
from utils.LogWriter import log_location, log_config, log_args
from utils.Toolbox_lib import create_year_calendar
from utils.dbclient.DatabaseClient import DbConnector
from module.env import *
os.environ[ 'MPLCONFIGDIR' ] = '/tmp/'

# Path Definitions
#HOME = Path(__file__).parent.parent
HOME = Path("/home/groups/daily/travail/Laura/")
WORKDIR = Path("/home/groups/daily/travail/Bertrand/Developpement/daily_users_stats")
CHEMIN_RESULTAT = Path(HOME, "drupal_stats_daily_users")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
CHEMIN_INPUT_CSV = Path(WORKDIR, "input_csv")

def normalize(name: str) -> str:
    return str(name).strip().lower()

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

    plt.tight_layout()  # Adjust subplot parameters to give specified padding

    if save:
        print(f"Saving filename ...: {filename}")
        plt.savefig(str(CHEMIN_RESULTAT / f"{filename}.png"))
    #plt.show()

def create_and_clean_all_dataframe(save: bool = 1 , user: str = "all") -> list:
    # Extraction Statistics Dataframe  
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
    WHEN nom_base_interrogee LIKE '%corres_code%' THEN 'Code Mapping Table' \
    WHEN nom_base_interrogee LIKE '%fonds_mutuel_%' THEN 'Mutual Funds' \
    WHEN nom_base_interrogee LIKE '%change%' THEN 'Spot Exchange Rate' \
    WHEN nom_base_interrogee LIKE '%histo_ost%' THEN 'Corporate Events' \
    WHEN nom_base_interrogee LIKE 'ost%' THEN 'Corporate Events' \
    WHEN nom_base_interrogee LIKE '%esg%' THEN 'ESG' \
    WHEN nom_base_interrogee LIKE '%greenbonds%' THEN 'Green Bonds' \
    END as database_name2, \
    CASE \
    WHEN nom_base_interrogee LIKE '%histo%' THEN 'Search_Code' \
    ELSE 'Extract_Data' \
    END AS code_ou_data \
    FROM statistique_requete as sr LEFT JOIN institution_entity as node \
    ON sr.id_groupe_labo=node.id \
    WHERE nom_groupe_labo NOT IN ('EUROFIDAI','administrateur Drupal') AND  nom_groupe_labo IS NOT NULL AND node.name IS NOT NULL\
    AND id_utilisateur_drupal NOT IN (1178,1922,367,274,594,896,904) \
    ORDER BY year,month,date_heure_extraction,id_utilisateur_drupal,node.name \
    ;"

    req_extraction_stats_users="select distinct ufd.uid as id_user, ie.name as labo_name, to_timestamp(ufd.created)::date AS date_created, \
        to_timestamp(access)::date AS date_last_access, ufs.field_statut_value AS statut \
        FROM users_field_data AS ufd \
			LEFT OUTER JOIN user__roles AS ur ON ur.entity_id=ufd.uid \
            LEFT OUTER JOIN user__field_institution AS ufi ON ufi.entity_id=ufd.uid \
            LEFT OUTER JOIN institution_entity AS ie ON ie.id=field_institution_target_id \
            FULL OUTER JOIN user__field_statut AS ufs  ON ufd.uid = ufs.entity_id \
            WHERE ufd.uid NOT IN (1178,1922,367,274) AND ie.name NOT IN ('EUROFIDAI','administrateur Drupal', 'probesys2 probesys') AND ie.name IS NOT NULL \
             ORDER BY  ie.name, ufd.uid ;"

    df_stats_daily_users = DbConnector('yakari', echo=True).execute_query(req_extraction_stats)
    df_stats_daily_subscription = DbConnector('yakari', echo=True).execute_query(req_extraction_stats_users)
    df_stats_users_with_subscription = df_stats_daily_users.merge(df_stats_daily_subscription, how='left', left_on='id_user', right_on='id_user')
    
    df_stats_users_with_subscription = df_stats_users_with_subscription[df_stats_users_with_subscription['labo_name'].isnull() == False].drop_duplicates()
    df_stats_users_with_subscription = df_stats_users_with_subscription.sort_values(by=['labo_name', 'id_user'])

    df_stats_daily_subscription['date_created'] = pd.to_datetime(df_stats_daily_subscription['date_created'], format='%Y-%m-%d')
    df_stats_daily_subscription['date_create_year'] = df_stats_daily_subscription['date_created'].dt.year
    df_stats_daily_subscription['date_last_access'] = pd.to_datetime(df_stats_daily_subscription['date_last_access'], format='%Y-%m-%d')
    df_stats_daily_subscription['date_last_access_year'] = df_stats_daily_subscription['date_last_access'].dt.year

    df_stats_users_with_subscription['date_created'] = pd.to_datetime(df_stats_users_with_subscription['date_created'], format='%Y-%m-%d')
    df_stats_users_with_subscription['date_create_year'] = df_stats_users_with_subscription['date_created'].dt.year
    df_stats_users_with_subscription['date_last_access'] = pd.to_datetime(df_stats_users_with_subscription['date_last_access'], format='%Y-%m-%d')
    df_stats_users_with_subscription['date_last_access_year'] = df_stats_users_with_subscription['date_last_access'].dt.year
    
    # Total number of subscribers per labo
    df_stats_number_of_all_subscribers_per_labo = df_stats_daily_subscription.groupby(['labo_name','statut']).size().reset_index(name='nb_subscribers')
    df_stats_number_of_all_subscribers_per_labo.sort_values(by=['labo_name','statut'],inplace=True)
    
    # Total number of subscribers per year_created
    df_stats_number_of_subscribers_created = df_stats_daily_subscription.groupby(['labo_name','statut', 'date_create_year']).size().reset_index(name='nb_subscribers')
    df_stats_number_of_subscribers_created.sort_values(by=['labo_name','date_create_year','statut'],inplace=True)
    
    # Total number of subscribers per year_last_access
    df_stats_number_of_subscribers_last_access = df_stats_daily_subscription.groupby(['labo_name','statut', 'date_last_access_year']).size().reset_index(name='nb_subscribers')
    df_stats_number_of_subscribers_last_access.sort_values(by=['labo_name','date_last_access_year','statut'],inplace=True)
    

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
   
    #stats for all laboratories
    df_all_laboratories = df_stats_daily_users[['year','month','month2','date','month_name','nb_codes','institution_name','user_name']].drop_duplicates(subset=['institution_name','user_name','year','month','month2']).groupby(['year','month','month2']).size().reset_index(name='nb_users')
    df_all_laboratories.sort_values(by=['month'],inplace=True)
    
    # stats per laboratory
    df_per_laboratory = df_stats_daily_users[['institution_name','year','month','month2','date','month_name','nb_codes']].groupby(['institution_name','year','month2','month','date','month_name']).sum('nb_codes').reset_index()
    df_per_laboratory.sort_values(by=['institution_name','year','month'],inplace=True)
    
    # stats per user
    df_all_users = df_stats_daily_users[['institution_name','year','month','month2','date','user_name','month_name','nb_codes']].drop_duplicates(subset=['institution_name','user_name', 'year','month','month2']).groupby(['institution_name','year','month','month2']).size().reset_index(name='nb_users')
    df_all_users.sort_values(by=['institution_name','year','month'],inplace=True)     

    # stats per database
    df_all_db = df_stats_daily_users[['institution_name','database_name2','year','month','month2','date','month_name','nb_codes']].groupby(['institution_name','database_name2','year']).sum('nb_codes').reset_index()
    df_all_db.sort_values(by=['institution_name','database_name2','year'],inplace=True) 

    if save:
        df_stats_daily_subscription.to_csv(str(CHEMIN_RESULTAT / "raw_data_stats_daily_subscription.csv"), index=False, encoding='utf-8',  sep='|')
        df_stats_users_with_subscription.to_csv(str(CHEMIN_RESULTAT / "stats_all_users_with_subscription_informations.csv"), index=False, encoding='utf-8',  sep='|')
        df_stats_users_with_subscription.to_csv(str(CHEMIN_INPUT_CSV / "stats_all_users_with_subscription_informations.csv"), index=False, encoding='utf-8',  sep='|')
        df_stats_number_of_all_subscribers_per_labo.to_csv(str(CHEMIN_RESULTAT / "stats_number_of_subscribers_per_labo_and_status.csv"), index=False, encoding='utf-8', sep='|')
        df_stats_number_of_all_subscribers_per_labo.to_csv(str(CHEMIN_INPUT_CSV / "stats_number_of_subscribers_per_labo_and_status.csv"), index=False, encoding='utf-8', sep='|')
        df_stats_number_of_subscribers_created.to_csv(str(CHEMIN_RESULTAT / "stats_number_of_subscribers_per_status_and_year_creation.csv"), index=False, encoding='utf-8', sep='|')
        df_stats_number_of_subscribers_created.to_csv(str(CHEMIN_INPUT_CSV / "stats_number_of_subscribers_per_status_and_year_creation.csv"), index=False, encoding='utf-8', sep='|')
        df_stats_number_of_subscribers_last_access.to_csv(str(CHEMIN_RESULTAT / "stats_number_of_subscribers_per_status_and_year_last_access.csv"), index=False, encoding='utf-8', sep='|') 
        df_stats_number_of_subscribers_last_access.to_csv(str(CHEMIN_INPUT_CSV / "stats_number_of_subscribers_per_status_and_year_last_access.csv"), index=False, encoding='utf-8', sep='|') 
        df_stats_daily_users.to_csv(str(CHEMIN_RESULTAT / "raw_data_stats_daily_users.csv"), index=False, encoding='utf-8', sep='|')


    return [df_stats_users_with_subscription,df_all_laboratories, df_per_laboratory, df_all_users, df_all_db] 

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument('-y', '--year', type=str, help='Year or list of years separated by comma (default is all years to current year)', default=None)
    parser.add_argument('-l', '--labo', type=str, help='Name of the laboratory (default is all laboratories)', default=None)
    args = parser.parse_args()
    return args


[DF_STATS_INFOS,DF_STATS_INSTITUTIONS_ALL_USERS,DF_STATS_NUMBER_ALL_INSTITUTIONS,
 DF_STATS_NUMBER_ALL_USERS,DF_STATS_NUMBER_ALL_DATABASES] = create_and_clean_all_dataframe() 


class User:
    """" This class represents a user of the database."""

    def __init__(self, name: str, id: int):
        self.name = name
        self.id = id 
        self.data = DF_STATS_INFOS 

    def set_name(self, name: str):
        self.name = name

    def set_id(self, id: int):
        self.id = id

    def get_name(self) -> str:
        return self.name
    
    def get_id(self) -> int:
        return self.id
     
    def __str__(self) -> str:
        date_created =list(self.data[self.data['id_user'] == self.id]['date_created'].unique())
        date_last_access = list(self.data[self.data['id_user'] == self.id]['date_last_access'].unique())
        return f"User(name={self.name}, id={self.id}, date_created={date_created}, date_last_access={date_last_access})"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def list_user_databases(self) -> list:
        """ This function returns the list of databases used by the user."""
        user_databases = self.data[self.data['id_user'] == self.id]['database_name2'].unique().tolist()
        return user_databases
    

class Database:
    """ This class represents a database."""

    #DF_STATS_NUMBER_ALL_DATABASES = create_and_clean_all_dataframe()['df_stats_number_of_all_databases']

    def __init__(self, name: str):
        self.name = name
        self.data = DF_STATS_INFOS

    def set_name(self, name: str):
        self.name = name

    def get_name(self) -> str:
        return self.name
    
    def __str__(self) -> str:
        return f"Database(name={self.name})"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def list_database_users(self) -> list:
        """ This function returns the list of users who have used the database."""
        database_users = self.data[normalize(self.data['database_name2']) == normalize(self.name)]['user_name'].unique().tolist()
        return database_users
    

class Institution:
    """ This class represents an institution which has subscribed to the database and/or has used the database."""

    #DF_STATS_NUMBER_ALL_INSTITUTIONS = create_and_clean_all_dataframe()['df_all_laboratories']
    
    def __init__(self, name: str):
        self.name : str = name
        self.data : pd.DataFrame = DF_STATS_INFOS
        self.institution_folder : str = self.name.strip().replace(",","").replace(" ", "_")       
        if self.name not in set(self.data['labo_name'].tolist()):
            raise ValueError(f"The institution '{self.name}' does not exist in the database. Please check the spelling and the capitalization and try again.")
    
    def set_name(self, name: str):
        self.name = name

    def get_name(self) -> str:
        return self.name
    
    def __str__(self) -> str:
        return f"institution(name={self.name})"
    
    def __repr__(self) -> str:
        return self.__str__()    
    
    def list_institution_users(self) -> list:
        """ This function returns the list of users who belong to the institution."""
        institution_users = self.data[normalize(self.data['labo_name']) == normalize(self.name)]['user_name'].unique().tolist()
        return institution_users
    
    def list_institution_databases(self) -> list:
        """ This function returns the list of databases used by the institution."""
        institution_databases = self.data[normalize(self.data['labo_name']) == normalize(self.name)]['database_name2'].unique().tolist()
        return institution_databases
    
    def create_institution_folder(self):
        """ Create a folder for each laboratory in the result directory        
        """        
        #institution_name = self.name.strip().replace(",","").replace(" ", "_")
        CHEMIN_LABO = Path(CHEMIN_RESULTAT, self.institution_folder)
        print(f"\nCreating folder for institution: {CHEMIN_LABO} \n")
        CHEMIN_LABO.mkdir(parents=True, exist_ok=True)
    
    def create_graph(self, df : pd.DataFrame, year: int, x_var: str, y_var: str, color: str , legend_title: str=None, \
                  xlabel: str = None, ylabel: str = None, title: str = None,  save: bool = False, **kwargs):            
        """ This function creates a bar graph for a specific laboratory and year."""

        def get_multiple_locator(n: int) -> int:
            """ This function returns the multiple locator for the y-axis based on the number of digits in the number.
            For example, if the number is 1250, it returns 1000, if the number is 158, it returns 100, and so on."""
            magnitude = 0   
            multiple = {}
            while n >= 10:
                n //= 10
                magnitude += 1    

            multiple = {0:10, 1:10, 2:100, 3:1000, 4:10000, 5:100000,6:1000000,7:10000000,8:100000000,9:1000000000}
            return multiple[magnitude]
                
        fig, ax = plt.subplots(figsize=(12, 8))
        plt.style.use('seaborn-v0_8-dark-palette')
        plt.rc('font', size=12, family='Arial', weight='normal')  # Set font properties
        ax.spines[['bottom', 'top', 'right']].set_visible(True)  # Hide spines    
        bc = ax.bar(df[x_var], df[y_var], color=color ) # barh for horizontal   
        ax.tick_params(axis='x', rotation=70, labelsize=10)  # Rotate x-axis labels 

        ax.bar_label(bc, labels=df[y_var], padding=30, fmt='%s', fontsize=10)  # fmt='%s' for string labels
        
        # Add custom legend if needed
        ax.legend(title=legend_title, loc='upper right', labels=[f"{year}"])
        ax.set_xlabel(xlabel, fontsize=14, loc='center', fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=14, loc='center', fontweight='bold')
        ax.set_title(f"{self.name}({year})", fontsize=20, fontweight='bold', \
                    backgroundcolor='lightgrey', loc='center', pad=15)

        from matplotlib.ticker import MultipleLocator
        max_value = df[y_var].max()
        if max_value <= 100 :   # Set x-axis limits
            ax.set_ylim(0, max_value + 10) 
        elif max_value <= 50000:
            ax.set_ylim(0, max_value + max_value*0.5) 
        elif max_value > 50000:
            ax.set_ylim(0, max_value + max_value*0.35)
        
        multiple = get_multiple_locator(max_value)
        ax.yaxis.set_major_locator(MultipleLocator(multiple))  # Keep x-axis ticks as multiples of 5 for horizontal
        
        plt.tight_layout()  # Adjust subplot parameters to give specified padding
        CHEMIN_LABO = Path(CHEMIN_RESULTAT, self.institution_folder)
        if save:
            print(f"Saving filename ...: {title}")
            plt.savefig(str(CHEMIN_LABO/f"{title}.png"), bbox_inches='tight')  # Ensure all elements are included
        #plt.show()

    def create_and_save_graph(self,  years: int):  


        """This function creates and saves graphs for a specific laboratory and year(s).
        It generates three types of graphs: 
        # 1. Number of extracted Eurofidai codes per month for the laboratory
        # 2. Number of users per month for the laboratory
        # 3. Number of extracted Eurofidai codes per database for the laboratory
        #  The graphs are saved in the laboratory's folder in the result directory.
        # Args:    
        #   years (int or list): Year or list of years for which to create graphs.        #  
        # Returns:
        #   None
        """

        if isinstance(years, int): 
            # graphics Laboratories 
            df_stats_number_all_labo_per_year =  DF_STATS_NUMBER_ALL_INSTITUTIONS[(DF_STATS_NUMBER_ALL_INSTITUTIONS['institution_name'] == self.name) & (DF_STATS_NUMBER_ALL_INSTITUTIONS['year'] == int(years))]
            if not df_stats_number_all_labo_per_year.empty:               
                self.create_graph(df_stats_number_all_labo_per_year, year=years, x_var='month2', y_var='nb_codes', \
                color='lightsteelblue', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{self.institution_folder}_{years}', save=True)          
            # graphics Users
            df_stats_number_all_users_per_year = DF_STATS_NUMBER_ALL_USERS[(DF_STATS_NUMBER_ALL_USERS['year'] == years) & (DF_STATS_NUMBER_ALL_USERS["institution_name"] == self.name)]
            if not df_stats_number_all_users_per_year.empty:           
                self.create_graph(df_stats_number_all_users_per_year, year=years, x_var='month2', y_var='nb_users', \
                color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{ self.institution_folder}_users_{years}', save=True)          
            
            # graphics Databases
            df_stats_number_all_databases_year = DF_STATS_NUMBER_ALL_DATABASES[(DF_STATS_NUMBER_ALL_DATABASES['year'] == years) & (DF_STATS_NUMBER_ALL_DATABASES["institution_name"] == self.name)]
            if not df_stats_number_all_databases_year.empty:
                    self.create_graph(df_stats_number_all_databases_year, year=years, x_var='database_name2', y_var='nb_codes', \
                color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{self.institution_folder}_database_{years}', save=True)   
            
        elif isinstance(years, list):
            # graphics Laboratories 
            for year in years:
                df_stats_number_all_labo =  DF_STATS_NUMBER_ALL_INSTITUTIONS[(DF_STATS_NUMBER_ALL_INSTITUTIONS['institution_name'] == self.name) & (DF_STATS_NUMBER_ALL_INSTITUTIONS['year'] == int(year))]                    
                if not df_stats_number_all_labo.empty:              
                    self.create_graph( df_stats_number_all_labo, year=year, x_var='month2', y_var='nb_codes', \
                color='lightsteelblue', legend_title ="Number of extracted Eurofidai codes", xlabel="Month", ylabel ="Number of codes" , title=f'{labo}_{year}', save=True)                   
                
                # graphics Users
                df_stats_number_all_users = DF_STATS_NUMBER_ALL_USERS[(DF_STATS_NUMBER_ALL_USERS['year'] == int(year)) & (DF_STATS_NUMBER_ALL_USERS["institution_name"] == self.name)]
                if not df_stats_number_all_users.empty:           
                    self.create_graph(df_stats_number_all_users, year=year, x_var='month2', y_var='nb_users', \
                    color='darkred', legend_title ="Number of users", xlabel="Month", ylabel ="Number of users" , title=f'{labo}_users_{year}', save=True) 

                # graphics Databases
                df_stats_number_all_databases = DF_STATS_NUMBER_ALL_DATABASES[(DF_STATS_NUMBER_ALL_DATABASES['year'] == int(year)) & (DF_STATS_NUMBER_ALL_DATABASES["institution_name"] == self.name)]
                if not df_stats_number_all_databases.empty:
                    self.create_graph( df_stats_number_all_databases, year=year, x_var='database_name2', y_var='nb_codes', \
                color='deeppink', legend_title ="Number of extracted Eurofidai codes", xlabel="Database", ylabel ="Number of codes" , title=f'{labo}_database_{year}', save=True)                   
  
    def treat_argument_year(self, argyears: str, labo: str):

        if argyears is None:
                years = self.data[self.data['institution_name'] == labo]['year'].sort_values().unique()
                if years is None:
                    print(f"The institution '{labo}' does not have any activities for the year {years}.")
                    sys.exit(1)
                
                years_int = list(map(int, years))           
                self.create_institution_folder()           
                for year in years_int:
                    print(f"\nCreating graphs for year: {year} \n")
                    self.create_and_save_graph(years=year) 

        elif ',' in argyears:            
            try:
                year_int = map(int,  argyears.split(','))
                print(year_int)
            except ValueError:
                raise ValueError("The year parameter must be an integer or a list of integers.")
            
            years = list(year_int)            
            self.create_institution_folder()
            l = [year for year in years if year not in set(self.data['year'].tolist())]
            if l:
                print(f"The institution '{labo}' does not have any activities for the years {l}.")
                sys.exit(1)

            for year in years:
                print(f"\nCreating graphs for year: {year} \n")
                self.create_and_save_graph(years=year)

        elif isinstance(int(argyears), int):
            year = int(argyears)
            years=[year]
            self.create_institution_folder()

            l = [year for year in years if year not in set(self.data['year'].tolist())]
            if l:
                print(f"The institution '{labo}' does not have any activities for the years {l}.")
            
            for year in years:
                print(f"\nCreating graphs for year: {year} \n")
                self.create_and_save_graph(years=year)        
            
        else: 
            raise ValueError("The year parameter must be an integer or a list of integers.")        

   
if __name__ == "__main__": 

    start_time=datetime.datetime.now()  
        
    args = parse_arguments()

    if not DF_STATS_INSTITUTIONS_ALL_USERS.empty:
        all_years = DF_STATS_INSTITUTIONS_ALL_USERS[['year']].drop_duplicates().sort_values(by='year')['year'].tolist()
        create_seaborn_relplot(DF_STATS_INSTITUTIONS_ALL_USERS, x_var='month2', y_var='nb_users', kind='line', hue ="year", title="Number of Eurofidai's Database Users", legend_labels=all_years, filename="Number of Eurofidai's Database Users", save=True, height=5, aspect=1.5)

    if args.labo is None:
        labos = set(DF_STATS_INFOS['labo_name'].tolist())
        for labo in labos:
            c_institution=Institution(name=labo)            
            c_institution.treat_argument_year(args.year, labo)
            del c_institution
    elif ";" in args.labo:
        labos = args.labo.split(";")   
        for labo in labos:
            c_institution=Institution(name=labo)
            c_institution.treat_argument_year(args.year, labo)
            del c_institution     
    elif ";" not in args.labo:
        labo = args.labo
        c_institution=Institution(name=labo)
        c_institution.treat_argument_year(args.year, labo)
        del c_institution
    else:
        raise ValueError("The labo parameter must be a string or a list of strings separated by ';'.")
               

    end_time=datetime.datetime.now()

    print(f"\nStart time: {start_time}\nEnd time: {end_time}\nDuration: {end_time - start_time}")