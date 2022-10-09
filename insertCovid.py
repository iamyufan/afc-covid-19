import pandas as pd
import requests
import psycopg2
from scripts.pregressbar import ProgressBar


def create_tables():
    # ----------------- #
    # StateData
    # data_id, date, state_id, cases, deaths, People_at_least_one_dose, People_fully_vaccinated
    # ----------------- #
    table_name = 'StateData'
    # Execute the query: delete table if exists
    curs.execute("DROP TABLE IF EXISTS yfz.{}".format(table_name))
    print("Table dropped")
    # Execute the query: create table if not exists
    curs.execute(f'''
        CREATE TABLE IF NOT EXISTS yfz.{table_name} (
            data_id serial PRIMARY KEY,
            date DATE,
            state_id INTEGER REFERENCES yfz.states(state_id),
            cases INTEGER,
            deaths INTEGER,
            People_at_least_one_dose INTEGER,
            People_fully_vaccinated INTEGER
            )
        ''')
    print("Table created at yfz.StateData")


def insert_data():
    # # Scrape the data
    # header = {
    #     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    # }
    # # Request the covid data
    # csv = requests.get(
    #     'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv', headers=header
    # )
    # csv.encoding = 'utf-8'
    # # Save to pandas dataframe
    # covid_df = pd.read_csv(csv.text)

    # # Request the vaccination data
    # csv = requests.get(
    #     'https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/us_data/time_series/time_series_covid19_vaccine_us.csv',
    #     headers=header
    # )
    # csv.encoding = 'utf-8'
    # # Save to pandas dataframe
    # vacc_df = pd.read_csv(csv.text)


    # Read the covid cases and deaths data
    covid_df = pd.read_csv('csv/covid.csv')
    covid_df = covid_df[['date', 'state', 'cases', 'deaths']]
    # Read the covid vaccination data
    vacc_df = pd.read_csv('csv/vaccination.csv')
    vacc_df.rename(
        columns={'Date': 'date', 'Province_State': 'state'}, inplace=True)
    vacc_df = vacc_df[['date', 'state',
                       'People_at_least_one_dose', 'People_fully_vaccinated']]
    # Merge the two dataframes
    covid_df = covid_df.merge(vacc_df, on=['date', 'state'], how='left')
    # Fill the missing values with 0
    covid_df.fillna(0, inplace=True)
    # Insert the data into the database
    print("> Inserting data into yfz.StateData")
    progress = ProgressBar(len(covid_df), fmt=ProgressBar.FULL)
    for i in range(len(covid_df)):
        # Get the state_id from the database if the state exists else skip this row
        curs.execute("SELECT state_id FROM yfz.states WHERE state_name = '{}'".format(
            covid_df.loc[i, 'state']))
        # If curs does not return any value, skip this row
        if curs.rowcount == 0:
            continue
        state_id = curs.fetchone()[0]
        # Insert the data into the database
        curs.execute(f'''
            INSERT INTO yfz.StateData (date, state_id, cases, deaths, People_at_least_one_dose, People_fully_vaccinated)
            VALUES ('{covid_df.loc[i, 'date']}', {state_id}, {covid_df.loc[i, 'cases']}, {covid_df.loc[i, 'deaths']}, {covid_df.loc[i, 'People_at_least_one_dose']}, {covid_df.loc[i, 'People_fully_vaccinated']})
            ''')
        progress.current += 1
        progress()
    progress.done()
    print("Data inserted into yfz.states")
    print("-----------------\n")

    # Get 10 rows of data from the database
    curs.execute("SELECT * FROM yfz.StateData LIMIT 10")
    rows = curs.fetchall()
    [print(row) for row in rows]


if __name__ == '__main__':
    # Connect to an existing database yfz
    conn = psycopg2.connect(
        database='d6q3frh2587tv4',
        user='uyybfpcdrrshxa',
        password="b8a4132cad49583c6b4de0829feedd52fd28f3bd1ab5c38797add7f75d984036",
        host='ec2-35-170-146-54.compute-1.amazonaws.com',
        port='5432'
    )

    # Open a cursor to perform database operations
    curs = conn.cursor()
    print("Connected to database")

    # Create tables
    create_tables()

    # Insert data
    insert_data()

    # close the communication with the PostgreSQL
    curs.close()
    conn.close()
