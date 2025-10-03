import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


# Set your directories
BASE_DIR = "/home/groups/daily/travail/Bertrand/Developpement/daily_users_stats/stat_graphs_files"
EXCEL_DIR = Path("/home/groups/daily/travail/Bertrand/Developpement/daily_users_stats/stat_graphs_files")
GRAPH_DIR = os.path.join(BASE_DIR, "graphs")

os.makedirs(GRAPH_DIR, exist_ok=True)

# Load Excel files
df_total_codes = pd.read_excel(os.path.join(EXCEL_DIR, "stats_sum_of_codes.xlsx"))
df_unique_users = pd.read_excel(os.path.join(EXCEL_DIR, "stats_number_of_unique_users.xlsx"))
df_user_codes = pd.read_excel(os.path.join(EXCEL_DIR, "user_monthly_sum_of_codes.xlsx"))
df_institution_codes = pd.read_excel(os.path.join(EXCEL_DIR, "institution_monthly_sum_of_codes.xlsx"))


      
# --- Chart 1: Total Codes Per Month by Year (grouped bars) ---
def plot_total_codes_by_month():
    month_order = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    df_total_codes['month2'] = pd.Categorical(df_total_codes['month2'], categories=month_order, ordered=True)
    pivot_df = df_total_codes.pivot_table(index='month2', columns='year', values='sum_of_codes', aggfunc='sum')
    pivot_df = pivot_df.loc[month_order]

    pivot_df.plot(kind='bar', figsize=(14, 6))
    plt.title('Monthly Code Extractions by Year')
    plt.xlabel('Month')
    plt.ylabel('Sum of Codes')
    plt.xticks(rotation=45)
    plt.legend(title='Year')
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPH_DIR, "total_codes_by_month_grouped.png"), dpi=300)
    plt.close()

# --- Chart 2: Unique Users by Month ---
def plot_unique_users():
    df_unique_users['month2'] = pd.Categorical(df_unique_users['month2'], categories=month_order, ordered=True)
    df_unique_users.sort_values(by=['year', 'month2'], inplace=True)
    
    # 🔧 Fix: Cast month2 to str before combining
    df_unique_users['period'] = df_unique_users['year'].astype(str) + '-' + df_unique_users['month2'].astype(str)

    plt.figure(figsize=(14, 5))
    plt.bar(df_unique_users['period'], df_unique_users['nb_unique_users'], color='slateblue')
    plt.title("Number of Unique Users Per Month")
    plt.xticks(rotation=45)
    plt.ylabel("Unique Users")
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPH_DIR, "unique_users_by_month.png"), dpi=300)
    plt.close()

# --- Chart 3: Institution Activity (1 chart per year) ---
def plot_institution_activity_by_year():
    df = df_institution_codes.copy()
    df['label'] = df['month2'] + " - " + df['institution_name']
    df.sort_values(by=['year', 'month2', 'institution_name'], inplace=True)

    for year, df_year in df.groupby('year'):
        plt.figure(figsize=(12, len(df_year) * 0.25))
        plt.barh(df_year['label'], df_year['sum_of_codes'], color='skyblue')
        plt.title(f'Institution Code Extractions – {year}')
        plt.xlabel('Sum of Codes')
        plt.tight_layout()
        filename = os.path.join(GRAPH_DIR, f"institution_codes_{year}.png")
        plt.savefig(filename, dpi=300)
        plt.close()

# --- Run all charts ---
if __name__ == "__main__":
    print("Generating charts...")
    month_order = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
   
    plot_total_codes_by_month()
    plot_unique_users()
    plot_institution_activity_by_year()

    print(f"✅ All charts saved in: {GRAPH_DIR}")
