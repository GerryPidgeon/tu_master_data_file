import pandas as pd
import os

def process_wolt_order_data():
    # Change to Data Directory
    folder_path = r'H:\Shared drives\99 - Data\01 - Source Data\03 - Wolt\CSV Files'
    os.chdir(folder_path)
    
    # Create an empty list to store data frames
    dataframe = []
    
    # Loop through all files in the directory
    for filename in os.listdir(folder_path):
        # Check if file is a CSV file
        if filename.endswith('.csv'):
            # Load CSV file into a data frame
            filepath = os.path.join(folder_path, filename)
            df = pd.read_csv(filepath)
            # Append data frame to list
            dataframe.append(df)
    
    # Concatenate all data frames in the list
    df = pd.concat(dataframe)
    
    # Convert 'Order placed' to datetime
    df['Order placed'] = pd.to_datetime(df['Order placed'], format='%m/%d/%y, %I:%M %p')
    
    # Extract date and time components
    df['OrderDate'] = df['Order placed'].dt.date
    df['OrderTime'] = df['Order placed'].dt.time
    
    # Convert 'Delivery time' to datetime
    df['Delivery time'] = pd.to_datetime(df['Delivery time'], format='%m/%d/%y, %I:%M %p')
    
    # Extract time component from 'Delivery time'
    df['OrderEndTime'] = df['Delivery time'].dt.time
    
    # Add Order Duration
    df['OrderDuration'] = ''
    
    # Combine date and time columns to create datetime objects
    df['OrderStartTime'] = pd.to_datetime(df['OrderDate'].astype(str) + ' ' + df['OrderTime'].astype(str))
    
    # Clean Restaurant Names
    cleaned_names = pd.read_csv(r'H:\Shared drives\99 - Data\03 - Rx Name List\Full Rx List, with Cleaned Names.csv')
    df['Location'] = df['Venue']
    df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
    df = pd.merge(df, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')
    df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
    df['Location'] = df['Cleaned Location']
    df = df.drop(columns=['Cleaned Location'])
    
    # Clean Channel Partner Names
    df['Channel'] = 'Wolt'
    
    # Clean Order ID
    df['OrderID'] = df['Order number']
    
    # Clean Statuses
    df['OrderStatus'] = df['Delivery status'].str.title()
    
    # Clean Other Entries
    df['DeliveryType'] = df['Delivery type'].str.title().str.replace('Takeaway', 'Pickup').str.replace('Homedelivery', 'Delivery')
    df['GrossAOV'] = df['Price']
    df['RatingScore'] = df['Review score']
    df['OrderStartTime'] = df['OrderTime']
    
    # Sort The DataFrame
    df = df.sort_values(['OrderDate', 'OrderTime'])
    
    # Create Primary Key
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderDate'].dt.strftime('%Y-%m-%d')
    
    # Order the DataFrame
    wolt_order_list = ['PrimaryKey', 'Location', 'OrderID',  'OrderDate', 'OrderTime', 'Channel', 'OrderStatus', 'DeliveryType', 'GrossAOV', 'OrderDuration', 'RatingScore']
    df = df[wolt_order_list]
    
    # Save The DataFrame
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    df = df.to_csv('Wolt Data.csv', index=False)