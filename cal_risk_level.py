import os
import datetime
from calculate_risk_daily import calculate_risk_by_date


def get_last_date():
    '''
    Get the last date in the results folder

    Returns:
        last_date (str): the last date in the results folder
    '''
    # Get the list of files in the results folder
    files = os.listdir('results')
    # Get the last date
    last_date = sorted(files)[-1][:-4]
    return last_date


def main():
    '''
    Update the csv in the results folder
    - Add yesterday's result
    - Update the day before yesterday's result

    CSV files in the results folder are named as <YYYY-MM-DD>.csv
    '''
    # Get the risk dataframe of yesterday
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    print(f'Calculating the risk level for {yesterday}')
    df_yesterday, centers_df = calculate_risk_by_date(yesterday)

    # Save the result to the results folder
    df_yesterday.to_csv(f'./results/{yesterday}.csv', index=False)
    # if the folder does not exist, create it
    if not os.path.exists('./results/centers'):
        os.makedirs('./results/centers')
    centers_df.to_csv(f'./results/centers/{yesterday}.csv', index=False)


if __name__ == '__main__':
    main()
