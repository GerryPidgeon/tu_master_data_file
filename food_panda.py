import numpy as np
import pandas as pd
import datetime as dt
import openpyxl as xl
import os

def process_food_panda_data():
    # Change to Data Directory
    folder_path = r'H:\Shared drives\99 - Data\01 - Source Data\05 - Food Panda\CSV Files'
    os.chdir(folder_path)

    # Create an empty list to store data frames
    df_list = []

    # Loop through all files in the directory
    for filename in os.listdir(folder_path):
        # Check if file is a CSV file
        if filename.endswith('.xls'):
            # Load CSV file into a data frame with ';' separator
            filepath = os.path.join(folder_path, filename)
            df = pd.read_excel(filepath)
            # Append data frame to list
            df_list.append(df)

    # Concatenate all data frames in the list
    food_panda_data = pd.concat(df_list)
    food_panda_data = food_panda_data.loc[food_panda_data['Bestellnummer'].notnull()]

    # Create Standard Format, with Correct Naming Convention
    # Date / Time Metrics
    food_panda_data['Lieferdatum'] = pd.to_datetime(food_panda_data['Lieferdatum'])
    food_panda_data['OrderDate'] = food_panda_data['Lieferdatum']

    # Clean Restaurant Names
    food_panda_data['Location'] = 'Mitte'

    # Clean Channel Partner Names
    food_panda_data['Channel'] = 'Food Panda'

    # Clean Order ID
    food_panda_data['OrderID'] = '#' + food_panda_data['Bestellnummer'].astype(str)

    # Clean Other Entries
    food_panda_data['DeliveryType'] = np.where(food_panda_data['Lieferart'] == 'Foodpanda', "Delivery", 'Pickup')
    food_panda_data['GrossAOV'] = food_panda_data['Brutto Bestellwert']
    food_panda_data['MarketplaceFee'] = food_panda_data['Provisions brutto (a)']
    food_panda_data['TotalPayout'] = food_panda_data['Saldo    (b+c)-a']

    # Create Primary Key
    food_panda_data['OrderDate'] = pd.to_datetime(food_panda_data['OrderDate'])
    food_panda_data['PrimaryKey'] = food_panda_data['OrderID'] + ' - ' + food_panda_data['Location'] + ' - ' + food_panda_data['OrderDate'].dt.strftime('%Y-%m-%d')

    food_panda_data = food_panda_data[['PrimaryKey', 'Location', 'OrderID', 'OrderDate', 'Channel', 'DeliveryType', 'GrossAOV', 'MarketplaceFee', 'TotalPayout']]

    # Sort The DataFrame
    food_panda_data = food_panda_data.sort_values(['OrderDate'])

    # Filter Order ID
    food_panda_data = food_panda_data.loc[~food_panda_data['PrimaryKey'].str.startswith('#Lief')]

    # Reset Index
    food_panda_data = food_panda_data.reset_index(drop=True)

    # Remove Duplicate Primary Keys
    cleaned_food_panda_data = food_panda_data['PrimaryKey']
    cleaned_food_panda_data = cleaned_food_panda_data.drop_duplicates()

    # Create new DataFrame
    food_panda_data = food_panda_data[food_panda_data.index.isin(cleaned_food_panda_data.index)]

    # Save The DataFrame
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    food_panda_data = food_panda_data.to_csv('Food Panda Data.csv', index=False)