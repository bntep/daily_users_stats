
import pandas as pd
import datetime
import os
from pathlib import Path

WEEKMASK = "Mon Tue Wed Thu Fri"
DATE_YEAR = datetime.datetime.today().strftime('%Y')
DATE_DEBUT = datetime.datetime.today().date() - datetime.timedelta(days=15)
DATE_FIN = datetime.datetime.today().date()
DATE_DEBUT_MONTH = datetime.datetime.today().date() - datetime.timedelta(days=365)
DATE_FIN_MONTH = datetime.datetime.today().date()
DATE_YEAR_N1 = DATE_DEBUT_MONTH.strftime('%Y')
date = datetime.datetime.now().strftime("%Y-%m-%d")

PAYS_EU = "('Germany','United Kingdom','Austria','Belgium','Bosnia and Herzegovina','Bulgaria','Cyprus','Croatia','Denmark','Spain','Estonia','Finland','France','Greece','Hungary','Ireland','Iceland','Italy','Latvia','Lithuania','Luxembourg','Macedonia','Malta','Norway','Netherlands','Poland','Portugal','Romania','Russian Federation','Serbia','Slovakia','Slovenia','Sweden','Switzerland','Turkey','Ukraine','Czech Republic')"

PAYS_AS = "('Australia','Bahrain','Bangladesh','China','Hong Kong','India','Indonesia','Israel','Japan','Jordan','Kazakhstan','Kuwait','Laos','Lebanon','Malaysia','Nepal','New Zealand','Oman','Pakistan','Philippines','Qatar','Saudi Arabia','Singapore','South Korea','Sri Lanka','Taiwan Province of China', 'Thailand','United Arab Emirates','Viet Nam')"

PAYS_AM = "('Argentina','Barbados','Brazil','Cayman Islands','Chile','Colombia','Ecuador','El Salvador','Jamaica','Mexico','Panama','Peru','Trinidad and Tobago','Uruguay','Venezuela')"

PAYS_AF = "('Botswana','Ivory Coast','Egypt','Ghana','Kenya','Malawi','Mauritius','Morocco','Namibia','Nigeria','South Africa','Tanzania, United Republic of','Tunisia','Uganda','Zambia','Zimbabwe')"

LIST_VAR = f"distinct date_cotation,identifiant,place"

df_actions = pd.DataFrame()

df_indices = pd.DataFrame()

df_fonds = pd.DataFrame()

dict_pays_continent ={'eu': PAYS_EU, 'as': PAYS_AS , 'am': PAYS_AM, 'af' : PAYS_AF}

dict_instrument = {'actions': 2 , 'indices': 7 , 'fonds': 3}

dict_df = {'actions': df_actions , 'indices': df_indices, 'fonds': df_fonds}

dict_req = {}                       

dict_table_cours = {"actions" :"src_cours_actions", "indices":"src_cours_indices", "fonds":"src_cours_fonds"}



if __name__ == "__main__":  
    #results = DbConnector('durango').execute_query("SELECT * FROM information_schema.tables WHERE table_name LIKE 'esg%';")  # This might return multiple columns
    # print(results)
    # base = BaseInstrument("actions","europe")
    # print(f"{base.basename} \n")
    # print(f"{base.table_cours} \n")
    current_file_path = Path(sys.argv[0])
    parent_folder_1 = current_file_path.parent.name
    
   
    
       
   
  
      
