# Autorzy: Wojciech Blyskal 247632, Magdalena Ozarek 247752
# 2.5
import os
import pandas as pd
from google.cloud import bigquery
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/wojte/Documents/studia/semestr 6/BigData/BigData24-25/cool-bay-452611-b5-3811a64fd49a.json" # lokalizacja pobranego klucza z punktu 1.4.
client = bigquery.Client() 

# 2.6
query = ('select * from bigquery-public-data.covid19_open_data.covid19_open_data limit 10')
query_job = client.query(query)    
query_result = query_job.result()  
df = query_result.to_dataframe()

#print(df)

# 3.1. Sprawdź, ile jest zapisanych wierszy z danymi. 
query = f"SELECT COUNT(*) AS total_rows FROM bigquery-public-data.covid19_open_data.covid19_open_data"
result = client.query(query).result()
row_count = list(result)[0]["total_rows"]

print(f"Total rows: {row_count}")
# Wniosek: Jest ponad 22 miliony wierszy, co jest wartością wykraczającą poza percepcję człowieka 
# i nie da się tych danych przejrzeć wszystkich ręcznie w żadnym sensownym czasie

# 3.2. Sprawdź, ile krajów jest uwzględnionych w danych. 
query = f"SELECT COUNT(DISTINCT country_code) AS total_rows FROM bigquery-public-data.covid19_open_data.covid19_open_data"
result = client.query(query).result()
country_count = list(result)[0]["total_rows"]

print(f"Total countries: {country_count}")

query = f"SELECT COUNT(DISTINCT country_name) AS total_rows FROM bigquery-public-data.covid19_open_data.covid19_open_data "
result = client.query(query).result()
country_count = list(result)[0]["total_rows"]

#country_list = list(result)[0]
#df = client.query_and_wait(query).to_dataframe()

query = f"SELECT DISTINCT country_name FROM bigquery-public-data.covid19_open_data.covid19_open_data "
result = client.query(query).result()
country_list = list(result)[0]
print(country_list[0])
print(f"Total countries: {country_count}")
with open("countries.txt", "w") as f:
    for country in country_list:
      f.write(country)

# Wniosek:Są różne definicje krajów, ale w uproszczeniu można przyjąć, że jest ich około 200. Zwracanych jest 246 kodów państw, co znaczy, 
# że w jakiś sposób te państwa zostały podzielone na więcej(np. przez liczenie terytoriów zależnych oddzielnie, na jedno państwo przypada kilka kodów etc.)
# Liczba nazw państw to pół miliona co znaczy, że ich nazewnictwo jest mocno nieujednolicone(w wielu językach, literówki itp.)

# 3.3. Sprawdź, w jaki sposób zapisywane są dzienne informacje dla krajów.

#query = f"SELECT column_name, COUNT(DISTINCT date_column) as distinct_days FROM bigquery-public-data.covid19_open_data.covid19_open_data GROUP BY column_name ORDER BY distinct_days DESC"
#result = client.query(query).result()
#country_count = list(result)[0]["total_rows"]

query = """
SELECT *
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE country_code = 'US'
ORDER BY date
LIMIT 24
"""
df = client.query_and_wait(query).to_dataframe()
print(df)
# nie wiem



"""with open("columns.txt", "w") as f:
    for col in df.columns:
        f.write(col + "\n")

print("Column names saved to columns.txt")"""

# 3.4. Sprawdź, w jaki sposób zapisywane są wartości liczbowe.  
#numeric_cols = df.select_dtypes(include=['number']).columns
#print("Numeric columns:", numeric_cols.tolist())
#print(df[['aggregation_level', 'new_confirmed', 'new_deceased', 'cumulative_confirmed', 'cumulative_deceased', 'cumulative_tested']])

