# Autorzy: Wojciech Blyskal 247632, Magdalena Ozarek 247752
# 2.5
import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
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
# regiony- kolumna location_key jest zbudowana następująco: pierwsze
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
# Niektóre z kolumn, które przechowują obiekty, zawierają dane na pozór
# liczbowe. Są też miejsca, gdzie są podane wartości ujemne, które muszą
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
# w marcu, co prawdopodobnie wynika z niepodawania jej wcześniej ludziom.
# Ostatniego dnia nie było żadnych danych na temat nowych zakażeń,
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

query = ('SELECT population_age_00_09, population_age_10_19, population_age_20_29, '
         'population_age_30_39, population_age_40_49, population_age_50_59, population_age_60_69, '
         'population_age_70_79, population_age_80_and_older, life_expectancy FROM bigquery-public-data.covid19_open_data.covid19_open_data '
         'GROUP BY population_age_00_09, population_age_10_19, population_age_20_29, '
         'population_age_30_39, population_age_40_49, population_age_50_59, population_age_60_69, '
         'population_age_70_79, population_age_80_and_older, life_expectancy '
         'LIMIT 30')

query = ('SELECT country_name, age_bin_0, age_bin_1, age_bin_2, age_bin_3, age_bin_4, age_bin_5, age_bin_6, '
         'age_bin_7, age_bin_8, age_bin_9 FROM bigquery-public-data.covid19_open_data.covid19_open_data '
         'GROUP BY country_name, age_bin_0, age_bin_1, age_bin_2, age_bin_3, age_bin_4, age_bin_5, age_bin_6, '
         'age_bin_7, age_bin_8, age_bin_9 ORDER BY country_name LIMIT 30')



query = ('SELECT country_name, cumulative_confirmed_age_0, cumulative_confirmed_age_1, '
         'cumulative_confirmed_age_2, cumulative_confirmed_age_3 '
         'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
         'GROUP BY country_name, cumulative_confirmed_age_0, cumulative_confirmed_age_1, '
         'cumulative_confirmed_age_2, cumulative_confirmed_age_3 ORDER BY country_name LIMIT 30')


# query = ('SELECT country_name, nurses_per_1000, physicians_per_1000, hospital_beds_per_1000 '
#          'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
#          'WHERE hospital_beds_per_1000 > 0 '
#          'GROUP BY country_name, nurses_per_1000, physicians_per_1000, hospital_beds_per_1000 '
#          'ORDER BY hospital_beds_per_1000 DESC LIMIT 70')

query = ('SELECT country_name, nurses_per_1000, physicians_per_1000 '
         'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
         # 'WHERE hospital_beds_per_1000 > 0 '
         'GROUP BY country_name, nurses_per_1000, physicians_per_1000 '
         'ORDER BY physicians_per_1000 DESC LIMIT 70')



query = ('SELECT country_name, pollution_mortality_rate, comorbidity_mortality_rate '
         'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
         'GROUP BY country_name, pollution_mortality_rate, comorbidity_mortality_rate '
         'LIMIT 70')







query_job = client.query(query)
query_result = query_job.result()
df_1 = query_result.to_dataframe()

print(df_1)








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
        'life_expectancy, human_capital_index, area_sq_km, date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
)

# query = ('SELECT country_name, '#iso_3166_1_alpha_2, iso_3166_1_alpha_3, '
#         #'population, population_male, population_female, population_rural, '
#         #'population_urban, population_density, human_development_index, '
#         'gdp_usd, gdp_per_capita_usd, population_largest_city, '
#         'life_expectancy, human_capital_index, area_sq_km '
#         'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
#         'LIMIT 20'
# )

query_job = client.query(query)
query_result = query_job.result()
df_1 = query_result.to_dataframe()

print(df_1)

df_1 = df_1.replace({None: np.nan, pd.NA: np.nan})
# df_1["gdp_usd"] = df_1["gdp_usd"].replace(pd.NA, np.nan)


# print("Number of missing values in each column of the DataFrame:")
# print(df_1.isna().sum())

total_missing = df_1.isna().sum().sum()
if total_missing > 0:
        print("Number of missing values in each column of the DataFrame:")
        print(df_1.isna().sum())
        print(f"Total of missing values in the DataFrame: {total_missing}")
        df_missing = df_1[df_1.isna().any(axis=1)]
        print("Rows with missing values:")
        print(df_missing)

df_1 = df_1.dropna()

print("Done")

total_missing = df_1.isna().sum().sum()
if total_missing > 0:
        print("Number of missing values in each column of the DataFrame:")
        print(df_1.isna().sum())
        print(f"Total of missing values in the DataFrame: {total_missing}")
        df_missing = df_1[df_1.isna().any(axis=1)]
        print("Rows with missing values:")
        print(df_missing)

num_duplicates = df_1.duplicated().sum()
print(f"Number of duplicates: {num_duplicates}")

df_duplicates = df_1[df_1.duplicated()]
if num_duplicates > 0:
        print("Rows with duplicates:")
        print(df_duplicates)
        df_1 = df_1.drop_duplicates()
        df_1 = df_1.reset_index(drop=True)
        print(df_1)


# num_duplicates = df_1.duplicated().sum()
# print(f"Number of duplicates: {num_duplicates}")
#
# df_duplicates = df_1[df_1.duplicated()]
# if num_duplicates > 0:
#         print("Rows with duplicates:")
#         print(df_duplicates)

query = ('SELECT life_expectancy '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data '
         'WHERE life_expectancy NOT LIKE "__.%" OR life_expectancy NOT LIKE "___.%"'
        'GROUP BY life_expectancy '
)

query_job = client.query(query)
query_result = query_job.result()
df = query_result.to_dataframe()
print(df_1)

df_1['life_expectancy'] = df_1['life_expectancy'].astype(float)
print("Life expectancy type: ", df_1['life_expectancy'].dtype)
df_1.to_csv("4_1.csv", index=False)

# Uzasadnienie: Dobieramy nazwę państwa, bo to jest jedyna postać
# czytelna dla każdego człowieka. Zachowujemy obie formy ISO zapisu
# nazwy kraju, gdyż chcemy zachować uniwersalność identyfikatorów
# państw. Zachowujemy całkowitą wielkość populacji wraz z jej podziałem
# na płeć, miejsce zamieszkania i gęstość zamieszkania, gdyż są to
# czynniki, które mogą mieć znaczenie przy badaniu choroby. Zachowujemy
# HDI oraz pkb na osobę, gdyż daje nam to obraz standardu życiowego
# w skali odpowiednio lokalnej oraz międzynarodowej.


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

query = ('SELECT new_confirmed, cumulative_confirmed, '
        'cumulative_confirmed_age_0, cumulative_confirmed_age_1, '
        'cumulative_confirmed_age_2, cumulative_confirmed_age_3, '
        'cumulative_confirmed_age_4, cumulative_confirmed_age_5, '
        'cumulative_confirmed_age_6, cumulative_confirmed_age_7, '
        'cumulative_confirmed_age_8, cumulative_confirmed_age_9, ' 
        'date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
query_job = client.query(query)
query_result = query_job.result()
df_2 = query_result.to_dataframe()

num_duplicates = df_2.duplicated().sum()
print(f"Number of duplicates: {num_duplicates}")

df_duplicates = df_2[df_2.duplicated()]
if num_duplicates > 0:
        print("Rows with duplicates:")
        print(df_duplicates)
        df_2 = df_2.drop_duplicates()
        df_2 = df_2.reset_index(drop=True)
        print(df_2)

df_2.to_csv("4_2.csv", index=False)

# 4.3. Chcemy poznać efekty COVID-19 poprzez uwypuklenie problemu
# śmiertelności ludzi spowodowanej wirusem.

query = ('SELECT date, new_deceased, cumulative_deceased, new_deceased_male, '
        'new_deceased_female, cumulative_deceased_male, '
        'cumulative_deceased_female, '
        'cumulative_deceased_age_0, cumulative_deceased_age_1, '
        'cumulative_deceased_age_2, cumulative_deceased_age_3, '
        'cumulative_deceased_age_4, cumulative_deceased_age_5, '
        'cumulative_deceased_age_6, cumulative_deceased_age_7, '
        'cumulative_deceased_age_8, cumulative_deceased_age_9 '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
query_job = client.query(query)
query_result = query_job.result()
df_3 = query_result.to_dataframe()

num_duplicates = df_3.duplicated().sum()
print(f"Number of duplicates: {num_duplicates}")

df_duplicates = df_3[df_3.duplicated()]
if num_duplicates > 0:
        print("Rows with duplicates:")
        print(df_duplicates)
        df_3 = df_3.drop_duplicates()
        df_3 = df_3.reset_index(drop=True)
        print(df_3)

df_3.to_csv("4_3.csv", index=False)

# 4.4. Chcemy zaobserwować trendy i zależności dotyczące szczepień
# na COVID-19.

query = ('SELECT new_persons_vaccinated, cumulative_persons_vaccinated, '
        'new_persons_fully_vaccinated, cumulative_persons_fully_vaccinated, '
        'new_vaccine_doses_administered, '
        'cumulative_vaccine_doses_administered, date '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
query_job = client.query(query)
query_result = query_job.result()
df_4 = query_result.to_dataframe()

num_duplicates = df_4.duplicated().sum()
print(f"Number of duplicates: {num_duplicates}")

df_duplicates = df_4[df_4.duplicated()]
if num_duplicates > 0:
        print("Rows with duplicates:")
        print(df_duplicates)
        df_4 = df_4.drop_duplicates()
        df_4 = df_4.reset_index(drop=True)
        print(df_4)

df_4.to_csv("4_4.csv", index=False)

# Odrzucono uwzględnienie marki podanej szczepionki, bo były tak nieliczne w stosounku do całego zbioru, że nie miało to sensu.

# 4.5. Zdefiniuj własny dodatkowy przypadek.
# Przypadek: Chcemy wygenerować statystyki dotyczące ogólnej
# opieki zdrowotnej.

query = ('SELECT date, nurses_per_1000, physicians_per_1000, '
        'health_expenditure_usd, out_of_pocket_health_expenditure_usd '
        'FROM bigquery-public-data.covid19_open_data.covid19_open_data')
query_job = client.query(query)
query_result = query_job.result()
df_5 = query_result.to_dataframe()

num_duplicates = df_5.duplicated().sum()
print(f"Number of duplicates: {num_duplicates}")

df_duplicates = df_5[df_5.duplicated()]
if num_duplicates > 0:
        print("Rows with duplicates:")
        print(df_duplicates)
        df_5 = df_5.drop_duplicates()
        df_5 = df_5.reset_index(drop=True)
        print(df_5)

df_5.to_csv("4_5.csv", index=False)

# Zrezygnowano z łóżek na 1000 bo było mało, oraz z emergency_health_expenditure, bo prawie zawsze było to 0 lub nic.