import json
import datetime
import pandas as pd

def calculate_risk_level(state_data):
    '''
    Calculating the risk level by metrics

    Input: 
        state_data: the calculated features for a state at a certain date

    Returns:
        risk_level (1-4)
    '''
    # Get the features
    seven_day_rolling_avg_new_cases_per_100k = state_data['7d_rolling_avg_new_cases_per_100k']
    seven_day_rolling_avg_new_deaths_per_100k = state_data['7d_rolling_avg_new_deaths_per_100k']
    daily_percentage_of_people_who_received_at_least_one_dose = state_data[
        'daily_percentage_of_people_who_received_at_least_one_dose']
    daily_percentage_of_people_who_are_fully_vaccinated = state_data[
        'daily_percentage_of_people_who_are_fully_vaccinated']

    # Calculate the risk level by 7d rolling average of new_cases per 100,000 people
    if seven_day_rolling_avg_new_cases_per_100k < 3:
        risk_level_by_new_cases = 1
    elif seven_day_rolling_avg_new_cases_per_100k < 10:
        risk_level_by_new_cases = 2
    elif seven_day_rolling_avg_new_cases_per_100k < 20:
        risk_level_by_new_cases = 3
    else:
        risk_level_by_new_cases = 4

    # Calculate the risk level by 7d rolling average of new_deaths per 100,000 people
    if seven_day_rolling_avg_new_deaths_per_100k < 0.1:
        risk_level_by_new_deaths = 1
    elif seven_day_rolling_avg_new_deaths_per_100k < 0.3:
        risk_level_by_new_deaths = 2
    elif seven_day_rolling_avg_new_deaths_per_100k < 0.6:
        risk_level_by_new_deaths = 3
    else:
        risk_level_by_new_deaths = 4

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
    risk_level = w_case * risk_level_by_new_cases + w_death * \
        risk_level_by_new_deaths + w_vaccination * risk_level_by_vaccination

    return int(risk_level)

def calculate_risk_by_date(date):
    '''
    Calculate the risk level csv for a given date 

    Input:
        date: 'YYYY-MM-DD'
    '''

    # Read the newly updated data.json
    with open('data.json', 'r') as f:
        data = json.load(f)

    # Create a dictionary to store the risk level for each state
    result = {}

    # Calculate the daily new cases and new deaths per 100,000 people for each state
    # and save them as daily_new_new_cases_per_100k and daily_new_new_deaths_per_100k
    print('> Calculating the daily new cases and new deaths per 100,000 people for each state...')
    for state_id in data:
        # Get the population of the state
        population = data[state_id]['population2021']
        # Get the number of new cases
        new_cases = data[state_id]['dates'][date]['new_cases']
        # Get the number of new deaths
        new_deaths = data[state_id]['dates'][date]['new_deaths']
        # Calculate the daily new new_cases per 100,000 people
        new_cases_per_100k = new_cases / population * 100000
        # Calculate the daily new new_deaths per 100,000 people
        new_deaths_per_100k = new_deaths / population * 100000
        # Add the new_cases_per_100k to the result dictionary
        result[state_id] = {
            'daily_new_cases_per_100k': new_cases_per_100k,
            'daily_new_deaths_per_100k': new_deaths_per_100k,
        }

    # Calculate the 7d rolling average of new_cases and new_deaths per 100,000 people as record as 
    # 7d_rolling_avg_new_cases_per_100k and 7d_rolling_avg_new_deaths_per_100k
    # Read in the results from the previous seven days in the results folder
    print('> Calculating the 7d rolling average of new_cases and new_deaths per 100,000 people...')
    for state_id in result:
        result[state_id]['7d_rolling_avg_new_cases_per_100k'] = result[state_id]['daily_new_cases_per_100k']
        result[state_id]['7d_rolling_avg_new_deaths_per_100k'] = result[state_id]['daily_new_deaths_per_100k']

    for i in range(1, 7):
        # Calculate the date of the previous day
        prev_date = (datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        # Read in the risk level csv from the previous day
        prev_df = pd.read_csv(f'results/{prev_date}.csv')
        # Iterate through each row in the risk level csv
        for index, row in prev_df.iterrows():
            # Get the state_id
            state_id = str(row['state_id'])
            # Add the new_cases_per_100k to the result dictionary
            result[state_id]['7d_rolling_avg_new_cases_per_100k'] += row['daily_new_cases_per_100k']
            # Add the new_deaths_per_100k to the result dictionary
            result[state_id]['7d_rolling_avg_new_deaths_per_100k'] += row['daily_new_deaths_per_100k']
    
    # average the 7d_rolling_avg_new_deaths_per_100k
    for state_id in result:
        result[state_id]['7d_rolling_avg_new_cases_per_100k'] /= 7
        result[state_id]['7d_rolling_avg_new_deaths_per_100k'] /= 7

    # Calculate the daily percentage of people who received at least one dose and the daily percentage of people who are fully vaccinated
    # and save them as daily_percentage_of_people_who_received_at_least_one_dose and daily_percentage_of_people_who_are_fully_vaccinated
    print('> Calculating the daily percentage of people who received at least one dose and the daily percentage of people who are fully vaccinated...')
    for state_id in data:
        # Get the population of the state
        population = data[state_id]['population2021']
        # Get the number of people who received at least one dose
        people_who_received_at_least_one_dose = data[state_id]['dates'][date]['People_at_least_one_dose']
        # Get the number of people who are fully vaccinated
        people_who_are_fully_vaccinated = data[state_id]['dates'][date]['People_fully_vaccinated']
        # Calculate the daily percentage of people who received at least one dose
        percentage_of_people_who_received_at_least_one_dose = people_who_received_at_least_one_dose / population * 100
        # Calculate the daily percentage of people who are fully vaccinated
        percentage_of_people_who_are_fully_vaccinated = people_who_are_fully_vaccinated / population * 100
        # Add the percentage_of_people_who_received_at_least_one_dose to the result dictionary
        result[state_id]['daily_percentage_of_people_who_received_at_least_one_dose'] = percentage_of_people_who_received_at_least_one_dose
        # Add the percentage_of_people_who_are_fully_vaccinated to the result dictionary
        result[state_id]['daily_percentage_of_people_who_are_fully_vaccinated'] = percentage_of_people_who_are_fully_vaccinated

    # Convert the result to pandas DataFrame, where each row is a state, and each column is a feature
    df = pd.DataFrame.from_dict(result, orient='index')
    # Add the state information
    df['state_id'] = df.index
    df['state_name'] = df['state_id'].apply(
        lambda state_id: data[state_id]['state_name'])
    df['state_abbr'] = df['state_id'].apply(
        lambda state_id: data[state_id]['state_abbr'])
    df['population2021'] = df['state_id'].apply(
        lambda state_id: data[state_id]['population2021'])
    # Calculate the risk level
    print('> Calculating the risk level...')
    df['risk_level'] = df.apply(calculate_risk_level, axis=1)
    # Reorder the columns
    df = df[['state_id', 'state_name', 'state_abbr', 'risk_level', 'population2021', 'daily_new_cases_per_100k', '7d_rolling_avg_new_cases_per_100k', 'daily_new_deaths_per_100k',
                '7d_rolling_avg_new_deaths_per_100k', 'daily_percentage_of_people_who_received_at_least_one_dose', 'daily_percentage_of_people_who_are_fully_vaccinated']]

    # State_id to risk level mapping
    state_id_to_risk_level = df[['state_id', 'risk_level']].set_index('state_id').to_dict()['risk_level']
    # Give another table containing the risk level for centers given the risk level of the state
    print('> Calculating the risk level for centers...')
    # read in the centers data at center.csv
    centers_df = pd.read_csv('centers.csv')
    centers_df['risk_level'] = centers_df['state_id'].apply(lambda x: state_id_to_risk_level.get(str(int(x))))
    centers_df['state_abbr'] = centers_df['state_id'].apply(lambda x: data[str(int(x))]['state_abbr'])
    centers_df['7d_rolling_avg_new_cases_per_100k'] = centers_df['state_id'].apply(lambda x: result[str(int(x))]['7d_rolling_avg_new_cases_per_100k'])
    centers_df['7d_rolling_avg_new_deaths_per_100k'] = centers_df['state_id'].apply(lambda x: result[str(int(x))]['7d_rolling_avg_new_deaths_per_100k'])
    centers_df['daily_percentage_of_people_who_received_at_least_one_dose'] = centers_df['state_id'].apply(lambda x: result[str(int(x))]['daily_percentage_of_people_who_received_at_least_one_dose'])
    centers_df['daily_percentage_of_people_who_are_fully_vaccinated'] = centers_df['state_id'].apply(lambda x: result[str(int(x))]['daily_percentage_of_people_who_are_fully_vaccinated'])

    return df, centers_df


    