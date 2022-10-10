import psycopg2
import requests
import pandas as pd
import json
import os
from uszipcode import SearchEngine


def fetch_data():
    '''
    Fetch the covid data from the github (cases, deaths, vaccinations)

    Returns:
        covid_df (pandas dataframe): the dataframe of the covid data
    '''
    # Fetch the cases and deaths data from CDC
    print("> Fetching covid cases and deaths data from CDC")
    csv = requests.get(
        'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
    )
    csv.encoding = 'utf-8'
    # Save the data to csv file
    print("> Saving data to csv file")
    with open('oriCSV/covidData.csv', 'w') as f:
        f.write(csv.text)

    # Fetch the vaccination data from JHU
    print("> Fetching covid vaccination data from JHU")
    csv = requests.get(
        'https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/us_data/time_series/time_series_covid19_vaccine_us.csv'
    )
    csv.encoding = 'utf-8'
    # Save the data to csv file
    print("> Saving data to csv file")
    with open('oriCSV/vaccinationData.csv', 'w') as f:
        f.write(csv.text)

    # Merge the two dataframes on date and state
    print("> Merging dataframes")
    covid_df = pd.read_csv('oriCSV/covidData.csv')
    vacc_df = pd.read_csv('oriCSV/vaccinationData.csv')
    vacc_df.rename(
        columns={'Date': 'date', 'Province_State': 'state'}, inplace=True)

    # Delete the two csv files
    print("> Deleting csv files")
    os.remove('oriCSV/covidData.csv')
    os.remove('oriCSV/vaccinationData.csv')

    covid_df = pd.merge(covid_df, vacc_df, on=['date', 'state'], how='outer')
    
    return covid_df


def preprocess(covid_df):
    '''
    1. Query the amazon fulfillment centers data from the PostgreSQL database
    2. Query the states data from the PostgreSQL database
    3. Add the covid data to the states data (merge with covid_df on state_name)
    4. Process the data to output a json containing the (1) covid data for each state and (2) the amazon fulfillment centers in each state

    Args:
        covid_df (pandas dataframe): the dataframe of the covid data

    Returns:
        {
            'state_id': {
                'state_name': str,
                'state_abbr': str,
                'population2020': int,
                'population2021': int,
                'centers': {
                    'center_id': {
                        'center_name': str,
                        'county_id': int,
                        'county_name': str,
                        'county_fips': int,
                        'zip_code': int,
                        'latitude': float,
                        'longitude': float,
                    }
                },
                'dates': {
                    'date': {
                        'cases': int,
                        'deaths': int,
                        'People_at_least_one_dose': int,
                        'People_fully_vaccinated': int,
                    }
                }
            }
        }
    '''
    # Connect to an existing database
    conn = psycopg2.connect(
        database='d6q3frh2587tv4',
        user='uyybfpcdrrshxa',
        password="b8a4132cad49583c6b4de0829feedd52fd28f3bd1ab5c38797add7f75d984036",
        host='ec2-35-170-146-54.compute-1.amazonaws.com',
        port='5432'
    )
    # Open a cursor to perform database operations
    curs = conn.cursor()
    print("> Connected to database")

    # Execute the query: select all centers from yfz.locations
    print("> Querying data from yfz.locations")
    curs.execute("SELECT * FROM yfz.locations")
    rows = curs.fetchall()
    # Save the data to pandas dataframe
    center_df = pd.DataFrame(
        rows, columns=['center_id', 'center_name', 'county_id', 'state_id', 'zip_code'])

    # Execute the query: select all states from yfz.states
    print("> Querying data from yfz.states")
    curs.execute("SELECT * FROM yfz.states")
    rows = curs.fetchall()
    # Save the data to pandas dataframe
    state_df = pd.DataFrame(rows, columns=[
                            'state_id', 'state_name', 'state_fips', 'state_abbr', 'population2020', 'population2021'])

    # Add the covid data to the states data (merge with covid_df on state_id)
    covid_df.rename(columns={'state': 'state_name'}, inplace=True)
    print("> Adding covid data to states data")
    state_df = pd.merge(state_df, covid_df, on='state_name', how='left')
    state_df = state_df.fillna(0)
    # keep only the columns we need
    state_df = state_df[['state_id', 'state_name', 'state_abbr', 'population2020', 'population2021',
                         'date', 'cases', 'deaths', 'People_at_least_one_dose', 'People_fully_vaccinated']]

    # Merge the two dataframes on state_id
    print("> Merging dataframes")
    state_df = pd.merge(state_df, center_df, on='state_id', how='left')

    # Save state_df to csv file
    print("> Saving data to csv file")
    state_df.to_csv('oriCSV/stateData.csv', index=False)

    # Process the data to output a json containing the (1) covid data for each state and (2) the amazon fulfillment centers in each state
    print("> Processing data to output a json")
    # Create a dictionary to store the data
    data = {}
    # Iterate through each row in the state_df
    for index, row in state_df.iterrows():
        # Get the state_id
        state_id = row['state_id']
        # If the state_id is not in the data dictionary, add it
        if state_id not in data:
            data[state_id] = {
                'state_name': row['state_name'],
                'state_abbr': row['state_abbr'],
                'population2020': int(row['population2020']),
                'population2021': int(row['population2021']),
                'centers': {},
                'dates': {},
            }
        # Get the date
        date = row['date']
        # If the date is not in the data dictionary, add it
        if date not in data[state_id]['dates']:
            data[state_id]['dates'][date] = {
                'cases': int(row['cases']),
                'deaths': int(row['deaths']),
                'People_at_least_one_dose': int(row['People_at_least_one_dose']),
                'People_fully_vaccinated': int(row['People_fully_vaccinated']),
            }
        # Add the center to the state if center_id is not nan
        if not pd.isna(row['center_id']):
            data[state_id]['centers'][str(int(row['center_id']))] = {
                'center_name': row['center_name'],
                'county_id': str(int(row['county_id'])),
                'zip_code': row['zip_code'],
            }
    
    # Close the cursor
    curs.close()
    # Close the connection
    conn.close()

    # Add a "new_cases" and "new_deaths" to make cases and deaths from cumulative to daily
    print("> Adding new_cases and new_deaths")
    for state_id in data:
        dates = list(data[state_id]['dates'].keys())
        dates.sort()
        for i in range(0, len(dates)):
            if i == 0:
                data[state_id]['dates'][dates[i]]['new_cases'] = data[state_id]['dates'][dates[i]]['cases']
                data[state_id]['dates'][dates[i]]['new_deaths'] = data[state_id]['dates'][dates[i]]['deaths']
            else:
                data[state_id]['dates'][dates[i]]['new_cases'] = data[state_id]['dates'][dates[i]]['cases'] - data[state_id]['dates'][dates[i-1]]['cases']
                data[state_id]['dates'][dates[i]]['new_deaths'] = data[state_id]['dates'][dates[i]]['deaths'] - data[state_id]['dates'][dates[i-1]]['deaths']

    return data



def main():
    # create a folder if it doesn't exist
    if not os.path.exists('oriCSV'):
        os.makedirs('oriCSV')

    # fetch covid data
    print('Fetching covid data...')
    covid_df = fetch_data()
    print()

    # preprocess the data
    print('Preprocessing data...')
    data = preprocess(covid_df)
    print()

    # save the data to json file
    print('Saving data to json file...')
    with open('data.json', 'w') as f:
        json.dump(data, f, indent = 4)
    print('Done!')

    # remove the csv file and folder
    os.remove('oriCSV/stateData.csv')
    os.rmdir('oriCSV')


if __name__ == '__main__':
    main()
