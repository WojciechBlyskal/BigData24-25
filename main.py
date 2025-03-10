# Autorzy: Wojciech Blyskal 247632, Magdalena Ozarek 247752
# 2.5
import os
import pandas as pd
from google.cloud import bigquery
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/ozare/Desktop/BigData/bigdata-zadania-e945fab257f4.json" # lokalizacja pobranego klucza z punktu 1.4.
client = bigquery.Client()

# 2.6
query = ('select * from bigquery-public-data.covid19_open_data.covid19_open_data limit 10')
query_job = client.query(query)
query_result = query_job.result()
df = query_result.to_dataframe()

# print(df)

# 3.1. Sprawdź, ile jest zapisanych wierszy z danymi.
query = f"SELECT COUNT(*) AS total_rows FROM bigquery-public-data.covid19_open_data.covid19_open_data"
result = client.query(query).result()
row_count = list(result)[0]["total_rows"]

print(f"Total rows: {row_count}")
# Wniosek: Są ponad 22 miliony wierszy, co jest wartością wykraczającą poza percepcję człowieka
# i nie da się tych danych przejrzeć wszystkich ręcznie w żadnym sensownym czasie

# 3.2. Sprawdź, ile krajów jest uwzględnionych w danych.
query = f"SELECT distinct COUNT(*) AS total_rows FROM bigquery-public-data.covid19_open_data.covid19_open_data GROUP BY country_code"
result = client.query(query).result()
country_count = list(result)[0]["total_rows"]

print(f"Total countries: {country_count}")

query = f"SELECT COUNT(*) AS total_rows FROM bigquery-public-data.covid19_open_data.covid19_open_data GROUP BY country_name"
result = client.query(query).result()
country_count = list(result)[0]["total_rows"]

print(f"Total countries: {country_count}")

#Wniosek:Są różne definicje krajów, ale w uproszczeniu można przyjąć, że jest ich około 200. Zwracanych jest prawie 1000 kodów państw, co znaczy,
#że w jakiś sposób te państwa zostały podzielone na więcej(np. przez liczenie terytoriów zależnych oddzielnie, na jedno państwo przypada kilka kodów etc.)
#Liczba nazw państw to pół miliona co znaczy, że ich nazewnictwo jest mocno nieujednolicone(w wielu językach, literówki itp.)

#3.3. Sprawdź, w jaki sposób zapisywane są dzienne informacje dla krajów.

#nie wiem

"""with open("columns.txt", "w") as f:
    for col in df.columns:
        f.write(col + "\n")

print("Column names saved to columns.txt")"""

#3.4. Sprawdź, w jaki sposób zapisywane są wartości liczbowe.
#numeric_cols = df.select_dtypes(include=['number']).columns
#print("Numeric columns:", numeric_cols.tolist())
print(df[['aggregation_level', 'new_confirmed', 'new_deceased', 'cumulative_confirmed', 'cumulative_deceased', 'cumulative_tested']])