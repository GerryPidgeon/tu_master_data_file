import numpy as np
import pandas as pd
import os

def process_lieferando_data():
    # Change to Data Directory
    folder_path = r'H:\Shared drives\99 - Data\01 - Source Data\04 - Lieferando\CSV Files'
    os.chdir(folder_path)

    # Create an empty list to store data frames
    dataframe = []

    # Loop through all files in the directory
    for filename in os.listdir(folder_path):
        # Check if file is a CSV file
        if filename.endswith('.csv'):
            # Load CSV file into a data frame with ';' separator
            filepath = os.path.join(folder_path, filename)
            df = pd.read_csv(filepath, sep=';')
            # Add column to data frame with file name
            df['file'] = filename
            df['file'] = df['file'].apply(lambda x: x.split('-')[-1].strip())
            df['file'] = df['file'].replace('.csv', '', regex=True)
            # Append data frame to list
            dataframe.append(df)

    # Concatenate all data frames in the list
    df = pd.concat(dataframe)

    # Create Standard Format, with Correct Naming Convention
    # Date / Time Metrics
    df['Date'] = pd.to_datetime(df['Date'])
    df['OrderDate'] = df['Date'].dt.date
    df['OrderTime'] = df['Date'].dt.time

    # Clean Restaurant Names
    df['Location'] = df['file']
    cleaned_names = pd.read_csv(r'H:\Shared drives\99 - Data\03 - Rx Name List\Full Rx List, with Cleaned Names.csv')
    df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
    df = pd.merge(df, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')
    df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
    df['Location'] = df['Cleaned Location']
    df = df.drop(columns=['Cleaned Location'])

    # Clean Channel Partner Names
    df['Channel'] = 'Lieferando'

    # Clean Order ID
    df['OrderID'] = '#' + df['Order'].astype(str)

    # Clean Other Entries
    df['OrderType'] = np.where(df['Pickup'] == 'no', "Delivery", 'Pickup')
    df['GrossAOV'] = df['Total amount']

    # Create Primary Key
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderDate'].dt.strftime('%Y-%m-%d')

    # Create Delivery Type
    df['DeliveryType'] = np.where(df['Pickup'] == 'no', 'Delivery', 'Pickup')

    df = df[['PrimaryKey', 'Location', 'OrderID',  'OrderDate', 'OrderTime', 'Channel', 'DeliveryType', 'GrossAOV']]

    # Sort The DataFrame
    df = df.sort_values(['OrderDate', 'OrderTime'])

    # Filter Lieferando Test Order
    df = df.loc[df['OrderID'] != '#63H1WT']

    # Save The DataFrame
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    df = df.to_csv('Lieferando Data.csv', index=False)