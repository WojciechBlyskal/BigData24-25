# Autorzy: Wojciech Blyskal 247632, Magdalena Ozarek 247752
# 2.5
import os
import pandas as pd
from google.cloud import bigquery
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
#       "C:/Users/ozare/Desktop/BigData/bigdata-zadania-e945fab257f4.json"
# ) # lokalizacja pobranego klucza z punktu 1.4.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        "C:/Users/wojte/Documents/studia/semestr 6/BigData/" 
        "cool-bay-452611-b5-3811a64fd49a.json"
) # lokalizacja pobranego klucza z punktu 1.4.
client = bigquery.Client() 

# 2.6
query = ('select * from '
        'bigquery-public-data.covid19_open_data.covid19_open_data limit 10')
query_job = client.query(query)    
query_result = query_job.result()  
df = query_result.to_dataframe()

# with open("columns.txt", "w") as f:
#     for col in df.columns:
#         f.write(col + "\n")
#
# print("Column names saved to columns.txt")

#print(df)

# 3.1. Sprawdź, ile jest zapisanych wierszy z danymi. 
query = ('SELECT COUNT(*) AS total_rows '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
result = client.query(query).result()
row_count = list(result)[0]["total_rows"]

print(f"Total rows: {row_count}")
# Wnioski: Jest ponad 22 miliony wierszy, co jest wartością 
# wykraczającą poza percepcję człowieka i nie da się tych danych 
# przejrzeć wszystkich ręcznie w żadnym sensownym czasie

# 3.2. Sprawdź, ile krajów jest uwzględnionych w danych. 
query = ('SELECT COUNT(DISTINCT country_code) AS total_rows '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
result = client.query(query).result()
country_count = list(result)[0]["total_rows"]

print(f"Total countries: {country_count}")

# Wnioski:Są różne definicje krajów, ale w uproszczeniu można przyjąć,
# że jest ich około 200. Zwracanych jest 246 kodów państw, co znaczy, 
# że w jakiś sposób te państwa zostały podzielone na więcej (przez
# liczenie terytoriów zależnych oddzielnie)

# 3.3. Sprawdź, w jaki sposób zapisywane są dzienne informacje dla krajów.
query = """
        SELECT *
        FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
        WHERE country_name LIKE 'Morocco' AND date = '2021-10-06'
        ORDER BY location_key
"""

df = client.query_and_wait(query).to_dataframe()
print(df[['location_key', 'date', 'country_name', 'new_confirmed',
        'new_deceased']])

query = """
        SELECT *
        FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
        WHERE country_name LIKE 'France' AND date = '2021-10-06'
        ORDER BY location_key
"""

df = client.query_and_wait(query).to_dataframe()
print(df[['location_key', 'date', 'country_name', 'new_confirmed', 
        'new_deceased']])

query = """
        SELECT *
        FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
        WHERE country_name LIKE 'Guatemala' AND date = '2021-10-06'
        ORDER BY location_key
"""

df = client.query_and_wait(query).to_dataframe()
print(df[['location_key', 'date', 'country_name', 'new_confirmed', 
        'new_deceased', 'average_temperature_celsius']])

# Wnioski: Są państwa, np. Maroko, Tunezja, Samoa, gdzie z danego dnia
# otrzymujemy tylko 1 rekord jako wynik, a są też państwa, np. Polska,
# Francja, USA, gdzie zwracane jest wiele rekordów. Po przejrzeniu
# danych okazuje się, iż wynika to z podziału danych na poszczególne
# regiony - kolumna location_key jest zbudowana następująco: pierwsze
# dwie litery to kod kraju, kolejne kilka liter to region, po czym 
# następuje ciąg cyfr, który najprawdopodobniej oznacza rozróżnienie na
# jeszcze mniejsze regiony (np. na departamenty we Francji). Są też 
# kraje, np. Gwatemala, która pomimo posiadania oficjalnego podziału na
# regiony, dla niektórych danych rozróżnia je między poszczególne 
# regiony, a dla innych są jedynie wartości zbiorcze, np. dane 
# o pogodzie są rozróżnione między regionami, a o zachorowaniach podano
# jedynie liczby zbiorczo dla całego kraju.


# 3.4. Sprawdź, w jaki sposób zapisywane są wartości liczbowe.  
#numeric_cols = df.select_dtypes(include=['number']).columns
#print("Numeric columns:", numeric_cols.tolist())
#print(df[['aggregation_level', 'new_confirmed', 'new_deceased', 
# 'cumulative_confirmed', 'cumulative_deceased', 'cumulative_tested']])

query = """
        SELECT *
        FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
        WHERE country_name LIKE 'France' AND date = '2020-10-06' 
                AND new_deceased < 0
        ORDER BY location_key
"""

df = client.query_and_wait(query).to_dataframe()
print(df[['location_key', 'date', 'country_name', 'new_confirmed',
        'new_deceased']])

query = ('select * from '
        'bigquery-public-data.covid19_open_data.covid19_open_data limit 20')
query_job = client.query(query)
query_result = query_job.result()
df = query_result.to_dataframe()

numeric_dtypes = df.select_dtypes(include=["number"]).dtypes
print(numeric_dtypes)

# Zapis do pliku
with open("numeric_dtypes.txt", "w") as f:
    f.write(numeric_dtypes.to_string())

print("Typy danych kolumn liczbowych zapisane do numeric_dtypes.txt")

non_numeric_dtypes = df.select_dtypes(exclude=["number"]).dtypes

print(non_numeric_dtypes)


with open("non_numeric_dtypes.txt", "w") as f:
    f.write(non_numeric_dtypes.to_string())

print(df[['new_tested', 'population_largest_city',
        'population_clustered', 'human_capital_index', 'area_rural_sq_km',
        'area_urban_sq_km', 'life_expectancy']])
print(df[['adult_male_mortality_rate',
        'adult_female_mortality_rate', 'pollution_mortality_rate',
        'comorbidity_mortality_rate', 'mobility_retail_and_recreation']])
print(df[['mobility_grocery_and_pharmacy', 'mobility_parks', 
        'mobility_transit_stations', 'mobility_workplaces',
        'mobility_residential', 'age_bin_0', 'location_geometry']])

# Wnioski: Wartości liczbowe są na ogół przechowywane jako int64 lub
# float64. Wyjątkiem są kolumny, które są przechowywane jako obiekt.
# Niektóre z kolumn które przechowują obiekty zawierają dane na pozór
# liczbowe. Są też miejsca gdzie są podane wartości ujemne, które muszą
# mieć pewne specjalne znaczenie.

# 3.5. Sprawdź, jaki przedział czasowy jest uwzględniony w danych. 
# Dodatkowo porównaj przedziały czasowe dla przypadków nowych
# zachorowań, nowych śmierci oraz nowych zaszczepionych osób w danych.

# country_list = [row.country_name for row in result]

query = ('SELECT MIN(date) AS start_date, MAX(date) AS end_date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
result = client.query(query).result()
row = next(result)
start_date = row.start_date
end_date = row.end_date

print("Whole data period: " + str(start_date) + "/" + str(end_date))

query = ('SELECT MIN(date) AS start_date, MAX(date) AS end_date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
        'WHERE new_confirmed IS NOT NULL')
result = client.query(query).result()
row = next(result)
start_date = row.start_date
end_date = row.end_date

print("Data period of new confirmed cases: " + str(start_date) + "/" 
      + str(end_date))

query = ('SELECT MIN(date) AS start_date, MAX(date) AS end_date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
        'WHERE new_deceased IS NOT NULL')
result = client.query(query).result()
row = next(result)
start_date = row.start_date
end_date = row.end_date

print("Data period of new deceased cases: " + str(start_date) + "/" 
      + str(end_date))

query = ('SELECT MIN(date) AS start_date, MAX(date) AS end_date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
        'WHERE new_persons_vaccinated IS NOT NULL')
result = client.query(query).result()
row = next(result)
start_date = row.start_date
end_date = row.end_date

print("Data period of new vaccinated persons cases: " + str(start_date) + "/"
      + str(end_date))

query = ('SELECT new_confirmed, new_deceased, new_persons_vaccinated '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
        'WHERE date = "2022-09-17"')
result = client.query(query).result()
row = next(result)
print(str(row.new_confirmed) + ", " + str(row.new_deceased) + ", " 
      + str(row.new_persons_vaccinated))

# Wnioski: Od pierwszego dnia zbierania danych odnotowywane są
# przypadki zarażenia i zgonu, ale szczepienia zaczynają się z dopiero
# w marcu co prawdopobnie wynika z niepodawania jej wcześniej ludziom.
# Ostatniego dnia nie było żadnych danych na temat nowych zakazeń, 
# śmierci lub szczepień.

# 3.6. Sprawdź więcej informacji (co najmniej 5 różnych) o danych
# dotyczących COVID-19. W tym celu nie wykonuj żadnych dodatkowych
# obliczeń.

query = ('SELECT cumulative_confirmed, cumulative_deceased, '
        'cumulative_persons_vaccinated '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
)
result = client.query(query).result()
row = next(result)
print(str(row.cumulative_confirmed) + ", " + str(row.cumulative_deceased) + ", " 
      + str(row.cumulative_persons_vaccinated))

# 1. 

# 2.

# 3.

# 4.

# 5.

# 4. Przeanalizuj poniższe przypadki. Zastanów się, jakie dane 
# potrzebujesz do każdego z nich, a następnie zapisz je w osobnych, jak
# najprostszych obiektach DataFrame. Na tym etapie nie przetwarzaj ich.
# Zadbaj o czystość danych, m.in. usuń niepotrzebne puste wartości 
# i duplikaty, zunifikuj sposób podawania informacji, zaproponuj sposób
# naprawy błędnych danych. Gotowe dane z obiektów DataFrame zapisz 
# w osobnych plikach CSV.

# 4.1. Chcemy posiadać podstawowe dane o wszystkich krajach świata, 
# które będą zrozumiałe dla człowieka oraz uniwersalne i potencjalnie
# przyszłościowe do dalszego przetwarzania.

query = ('SELECT DISTINCT(country_name), '#iso_3166_1_alpha_2, iso_3166_1_alpha_3, ' 
        #'population, population_male, population_female, population_rural, '
        #'population_urban, population_density, human_development_index, '
        'gdp_usd, gdp_per_capita_usd, population_largest_city, '
        'life_expectancy, human_capital_index, area_sq_km '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
        'LIMIT 20'
)
query_job = client.query(query)
query_result = query_job.result()
df_1 = query_result.to_dataframe()

print(df_1)
df_1 = df_1.distinct

# Uzasadnienie: Dobieramy nazwę państwa, bo to jest jedyna postać 
# czytelna dla każdego człowieka. Zachowujemy obie formy ISO zapisu
# nazwy kraju, gdyż chcemy zachować uniwersalność identyfikatorów 
# państw. Zachowujemy całkowitą wielkość populacji wraz z jej podziałem
# na płeć, miejsce zamieszkania i gęstość zamieszkania, gdyż są to 
# czynniki, które mogą mieć znaczenie przy badaniu choroby. Zachowujemy
# HDI oraz pkb na osobę, gdyż daje nam to obraz standardu życiowego
# w skali odpowiednio lokalnej oraz międzynarodowej. 

# 4.2. Chcemy wygenerować statystyki dotyczące zachorowań na COVID-19 
# na całym świecie.

# query = ('SELECT new_confirmed, cumulative_confirmed, new_confirmed_age_0, '
#         'new_confirmed_age_1, new_confirmed_age_2, new_confirmed_age_3, '
#         'new_confirmed_age_4, new_confirmed_age_5, new_confirmed_age_6, '
#         'new_confirmed_age_7, new_confirmed_age_8, new_confirmed_age_9, '
#         'cumulative_confirmed_age_0, cumulative_confirmed_age_1, '
#         'cumulative_confirmed_age_2, cumulative_confirmed_age_3, '
#         'cumulative_confirmed_age_4, cumulative_confirmed_age_5, '
#         'cumulative_confirmed_age_6, cumulative_confirmed_age_7, '
#         'cumulative_confirmed_age_8, cumulative_confirmed_age_9 '
#         'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
# query_job = client.query(query)
# query_result = query_job.result()
# df_2 = query_result.to_dataframe()