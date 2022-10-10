from audioop import avg
import json
import pandas as pd
import math

data = {}       # dictionary to store the json data
result = {}     # dictionary to store the result
dates = []      # list to store the dates


def read_data():
    '''
    Read in the latest data from the data.json file

    Returns:
        data (dict): The data from the data.json file
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
    with open('data.json', 'r') as f:
        data = json.load(f)
    return data


def calculate_features():
    # Calculate the daily new cases per 100,000 people
    print('> Calculating daily new cases per 100,000 people...')
    for state_id in data:
        # Get the state data
        state_data = data[state_id]
        # Get the dates
        dates = list(state_data['dates'].keys())
        # Sort the dates
        dates.sort()
        # Iterate through each date
        for date in dates:
            # Get the date data
            date_data = state_data['dates'][date]
            # Get the cases
            cases = date_data['cases']
            # Get the population
            population = state_data['population2021']
            # Calculate the daily new cases per 100,000 people
            daily_new_cases_per_100k = cases / population * 100000
            # Store the result
            if state_id not in result:
                result[state_id] = {}
            result[state_id][date] = {
                'daily_new_cases_per_100k': daily_new_cases_per_100k,
            }

    # Calculate the 7d rolling average of cases per 100,000 people
    print('> Calculating 7d rolling average of cases per 100,000 people...')
    for state_id in result:
        # Get the dates
        dates = list(result[state_id].keys())
        # Sort the dates
        dates.sort()
        # Iterate through each date
        for date in dates:
            # Get the daily new cases per 100,000 people
            try:
                daily_new_cases_per_100k = result[state_id][date]['daily_new_cases_per_100k']
            except KeyError:
                print(
                    f'No daily new cases per 100,000 people for {state_id} on {date}')
                return
            # For the first 6 days, the 7d rolling average is the same as the daily new cases per 100,000 people
            if dates.index(date) < 6:
                result[state_id][date]['7d_rolling_avg_cases_per_100k'] = daily_new_cases_per_100k
            # For the rest of the days, the 7d rolling average is the average of the daily new cases per 100,000 people of the last 7 days
            else:
                # Get the daily new cases per 100,000 people of the last 7 days
                last_7_days = dates[dates.index(
                    date) - 6:dates.index(date) + 1]
                last_7_days_daily_new_cases_per_100k = [
                    result[state_id][d]['daily_new_cases_per_100k'] for d in last_7_days]
                # Calculate the 7d rolling average of cases per 100,000 people
                seven_day_rolling_avg_cases_per_100k = sum(
                    last_7_days_daily_new_cases_per_100k) / len(last_7_days_daily_new_cases_per_100k)
                result[state_id][date]['7d_rolling_avg_cases_per_100k'] = seven_day_rolling_avg_cases_per_100k

    # Calculate the daily new deaths per 100,000 people
    print('> Calculating daily new deaths per 100,000 people...')
    for state_id in data:
        # Get the state data
        state_data = data[state_id]
        # Get the dates
        dates = list(state_data['dates'].keys())
        # Sort the dates
        dates.sort()
        # Iterate through each date
        for date in dates:
            # Get the date data
            date_data = state_data['dates'][date]
            # Get the deaths
            deaths = date_data['deaths']
            # Get the population
            population = state_data['population2021']
            # Calculate the daily new deaths per 100,000 people
            daily_new_deaths_per_100k = deaths / population * 100000
            # Store the result
            result[state_id][date]['daily_new_deaths_per_100k'] = daily_new_deaths_per_100k

    # Calculate the 7d rolling average of deaths per 100,000 people
    print('> Calculating 7d rolling average of deaths per 100,000 people...')
    for state_id in result:
        # Get the dates
        dates = list(result[state_id].keys())
        # Sort the dates
        dates.sort()
        # Iterate through each date
        for date in dates:
            # Get the daily new deaths per 100,000 people
            daily_new_deaths_per_100k = result[state_id][date]['daily_new_deaths_per_100k']
            # Get the previous 6 days
            prev_dates = dates[max(0, dates.index(date) - 6):dates.index(date)]
            # Get the previous 6 days' daily new deaths per 100,000 people
            prev_daily_new_deaths_per_100k = [
                result[state_id][prev_date]['daily_new_deaths_per_100k'] for prev_date in prev_dates]
            # Calculate the 7d rolling average
            # For the first 6 days, the 7d rolling average is the same as the daily new deaths per 100,000 people
            if dates.index(date) < 6:
                result[state_id][date]['7d_rolling_avg_deaths_per_100k'] = daily_new_deaths_per_100k
            # For the rest of the days, the 7d rolling average is the average of the daily new deaths per 100,000 people of the last 7 days
            else:
                # Calculate the 7d rolling average of deaths per 100,000 people
                seven_day_rolling_avg_deaths_per_100k = sum(
                    prev_daily_new_deaths_per_100k) / len(prev_daily_new_deaths_per_100k)
                result[state_id][date]['7d_rolling_avg_deaths_per_100k'] = seven_day_rolling_avg_deaths_per_100k

    # Calculate the daily percentage of people who received at least one dose
    print('> Calculating daily percentage of people who received at least one dose...')
    for state_id in data:
        # Get the state data
        state_data = data[state_id]
        # Get the dates
        dates = list(state_data['dates'].keys())
        # Sort the dates
        dates.sort()
        # Iterate through each date
        for date in dates:
            # Get the date data
            date_data = state_data['dates'][date]
            # Get the people who received at least one dose
            people_at_least_one_dose = date_data['People_at_least_one_dose']
            # Get the population
            population = state_data['population2021']
            # Calculate the daily percentage of people who received at least one dose
            daily_percentage_of_people_who_received_at_least_one_dose = people_at_least_one_dose / population * 100
            # Store the result
            result[state_id][date]['daily_percentage_of_people_who_received_at_least_one_dose'] = daily_percentage_of_people_who_received_at_least_one_dose

    # Calculate the daily percentage of people who are fully vaccinated
    print('> Calculating daily percentage of people who are fully vaccinated...')
    for state_id in data:
        # Get the state data
        state_data = data[state_id]
        # Get the dates
        dates = list(state_data['dates'].keys())
        # Sort the dates
        dates.sort()
        # Iterate through each date
        for date in dates:
            # Get the date data
            date_data = state_data['dates'][date]
            # Get the people who are fully vaccinated
            people_fully_vaccinated = date_data['People_fully_vaccinated']
            # Get the population
            population = state_data['population2021']
            # Calculate the daily percentage of people who are fully vaccinated
            daily_percentage_of_people_who_are_fully_vaccinated = people_fully_vaccinated / population * 100
            # Store the result
            result[state_id][date]['daily_percentage_of_people_who_are_fully_vaccinated'] = daily_percentage_of_people_who_are_fully_vaccinated

    return result, dates

def calculate_risk_level(state_data):
    '''
    Calculating the risk level by metrics

    Input: 
        state_data: the calculated features for a state at a certain date

    Returns:
        risk_level (1-4)
    '''
    # Get the features
    daily_new_cases_per_100k = state_data['daily_new_cases_per_100k']
    seven_day_rolling_avg_cases_per_100k = state_data['7d_rolling_avg_cases_per_100k']
    daily_new_deaths_per_100k = state_data['daily_new_deaths_per_100k']
    seven_day_rolling_avg_deaths_per_100k = state_data['7d_rolling_avg_deaths_per_100k']
    daily_percentage_of_people_who_received_at_least_one_dose = state_data[
        'daily_percentage_of_people_who_received_at_least_one_dose']
    daily_percentage_of_people_who_are_fully_vaccinated = state_data[
        'daily_percentage_of_people_who_are_fully_vaccinated']

    # Calculate the risk level by 7d rolling average of cases per 100,000 people
    if seven_day_rolling_avg_cases_per_100k < 3:
        risk_level_by_cases = 1
    elif seven_day_rolling_avg_cases_per_100k < 10:
        risk_level_by_cases = 2
    elif seven_day_rolling_avg_cases_per_100k < 20:
        risk_level_by_cases = 3
    else:
        risk_level_by_cases = 4

    # Calculate the risk level by 7d rolling average of deaths per 100,000 people
    if seven_day_rolling_avg_deaths_per_100k < 0.1:
        risk_level_by_deaths = 1
    elif seven_day_rolling_avg_deaths_per_100k < 0.3:
        risk_level_by_deaths = 2
    elif seven_day_rolling_avg_deaths_per_100k < 0.6:
        risk_level_by_deaths = 3
    else:
        risk_level_by_deaths = 4

    # Calculate the risk level by daily percentage of people who received at least one dose
    if daily_percentage_of_people_who_received_at_least_one_dose < 40:
        risk_level_by_vaccination = 2
    elif daily_percentage_of_people_who_received_at_least_one_dose < 60:
        risk_level_by_vaccination = 1.5
    elif daily_percentage_of_people_who_received_at_least_one_dose < 90:
        risk_level_by_vaccination = 1
    else:
        risk_level_by_vaccination = 0.5

    # Calculate the risk level by daily percentage of people who are fully vaccinated
    if daily_percentage_of_people_who_are_fully_vaccinated < 20:
        risk_level_by_vaccination += 2
    elif daily_percentage_of_people_who_are_fully_vaccinated < 40:
        risk_level_by_vaccination += 1.5
    elif daily_percentage_of_people_who_are_fully_vaccinated < 60:
        risk_level_by_vaccination += 1
    else:
        risk_level_by_vaccination += 0.5

    # Calculate the risk level
    # weights for each metric
    w_case = 0.6
    w_death = 0.3
    w_vaccination = 0.2
    risk_level = w_case * risk_level_by_cases + w_death * risk_level_by_deaths + w_vaccination * risk_level_by_vaccination

    return int(risk_level)



def main():
    # Calculate the features
    print('Calculating features...')
    result, dates = calculate_features()

    # Convert the result to pandas DataFrame for each date, where each row is a state, and each column is a feature
    print('Saving result...')
    for date in dates:
        # Get the data for each state
        try:
            result_date = {state_id: result[state_id][date]
                           for state_id in result}
        except:
            # print('Date not found:', date)
            continue
        # Convert the data to pandas DataFrame
        df = pd.DataFrame.from_dict(result_date, orient='index')
        # Add the state information
        df['state_id'] = df.index
        df['state_name'] = df['state_id'].apply(
            lambda state_id: data[state_id]['state_name'])
        df['state_abbr'] = df['state_id'].apply(
            lambda state_id: data[state_id]['state_abbr'])
        df['population2021'] = df['state_id'].apply(
            lambda state_id: data[state_id]['population2021'])
        # Calculate the risk level
        df['risk_level'] = df.apply(calculate_risk_level, axis=1)
        # Reorder the columns
        df = df[['state_id', 'state_name', 'state_abbr', 'risk_level', 'population2021', 'daily_new_cases_per_100k', '7d_rolling_avg_cases_per_100k', 'daily_new_deaths_per_100k',
                 '7d_rolling_avg_deaths_per_100k', 'daily_percentage_of_people_who_received_at_least_one_dose', 'daily_percentage_of_people_who_are_fully_vaccinated']]
        # Save the DataFrame to a csv file
        df.to_csv(f'./results/{date}.csv', index=False)

    print('Done!')


if __name__ == '__main__':
    data = read_data()
    main()
