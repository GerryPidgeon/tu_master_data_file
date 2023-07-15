import pandas as pd
import datetime as dt
import numpy as np
import os

def process_deliverect_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\01 - Source Data\01 - Deliverect\CSV Files\Orders')
    
    # Create Deliverect Consolidated File
    dataframe = []
    
    for file_path in [
        '23.01 - Jan 23 Orders.csv',
        '23.02 - Feb 23 Orders.csv',
        '23.03 - Mar 23 Orders.csv',
        '23.04 - Apr 23 Orders.csv',
        '23.05 - May 23 Orders.csv',
        '23.06 - Jun 23 Orders.csv',
        '23.07 - Jul 23 Orders.csv',
        '23.08 - Aug 23 Orders.csv',
        '23.09 - Sep 23 Orders.csv',
        '23.10 - Oct 23 Orders.csv',
        '23.11 - Nov 23 Orders.csv',
        '23.12 - Dec 23 Orders.csv'
    ]:
        if os.path.exists(file_path):
            # If the file path exists, load the corresponding CSV file and append it to the list
            df = pd.read_csv(file_path, dtype={'OrderID': str}, encoding='utf-8')
            df['OrderID'] = '#' + df['OrderID'].astype(str)  # Add '#' in front of OrderID
            dataframe.append(df)
        else:
            # If the file path does not exist, append None to the list
            dataframe.append(None)
    
    # Concatenate the loaded data frames
    df = pd.concat([df for df in dataframe if df is not None])
    
    # Drop Cancelled Duplicates
    df['Status'] = df['Status'].replace('CANCELED', 'CANCEL')
    df = df.drop_duplicates()
    
    # Clean Created Time
    df['AdjustedCreatedTime'] = pd.to_datetime(df['CreatedTimeUTC']).dt.tz_localize('UTC')
    df['AdjustedCreatedTime'] = df['AdjustedCreatedTime'].dt.tz_convert('Europe/Berlin')
    df['OrderDate'] = df['AdjustedCreatedTime'].dt.date
    df['OrderTime'] = df['AdjustedCreatedTime'].dt.time
    
    # Clean Pickup Time
    df['AdjustedPickupTime'] = pd.to_datetime(df['PickupTimeUTC']).dt.tz_localize('UTC')
    df['AdjustedPickupTime'] = df['AdjustedPickupTime'].dt.tz_convert('Europe/Berlin')
    df['PickupTime'] = df['AdjustedPickupTime'].dt.time
    
    # Clean Scheduled Time
    try:
        df['AdjustedScheduledTime'] = pd.to_datetime(df['ScheduledTimeUTC']).dt.tz_localize('UTC')
        df['AdjustedScheduledTime'] = df['AdjustedScheduledTime'].dt.tz_convert('Europe/Berlin')
        df['ScheduledTime'] = df['AdjustedScheduledTime'].dt.time
    except Exception:
        df['ScheduledTime'] = np.nan
    
    # Clean Location
    cleaned_names = pd.read_csv(r'H:\Shared drives\99 - Data\03 - Rx Name List\Full Rx List, with Cleaned Names.csv')
    df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
    df = pd.merge(df, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')
    df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
    df['Location'] = df['Cleaned Location']
    df = df.drop(columns=['Cleaned Location'])
    
    # Clean Other Columns
    df['Channel'] = df['Channel'].str.replace('TakeAway Com', 'Lieferando')
    df['OrderStatus'] = df['Status'].str.replace('_', ' ').str.title()
    df['DeliveryType'] = df['Type'].str.title()
    df['PaymentType'] = df['Payment'].str.title()
    df['DeliveryFee'] = df['DeliveryCost']
    df['Discounts'] = df['DiscountTotal']
    df['GrossAOV'] = df['SubTotal']
    df['NetAOV'] = df['PaymentAmount']
    df['Tips'] = df['Tip']
    
    # Clean Food Panda OrderID
    df['OrderID'] = np.where(df['Channel'] == 'Food Panda', df['OrderID'].str.split(' ', n=1).str[0], df['OrderID'])
    
    # Create New Columns
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderDate'].astype(str)
    
    # Drop Columns that aren't needed
    columns_to_drop = ['CreatedTimeUTC', 'PickupTimeUTC', 'ScheduledTimeUTC', 'ReceiptID', 'Note', 'ChannelLink', 'Tax', 'FailureMessage', 'LocationID', 'PosLocationID', 'ChannelLinkID',
                       'OrderTotalAmount', 'CustomerAuthenticatedUserId', 'DeliveryTimeInMinutes', 'PreparationTimeInMinutes', 'AdjustedCreatedTime', 'AdjustedPickupTime', 'AdjustedScheduledTime', 'Type',
                       'Voucher', 'Payment', 'PaymentAmount', 'DiscountTotal', 'SubTotal', 'PaymentAmount', 'Tip', 'Status', 'DeliveryCost', 'CreatedTime']
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Sort Columns
    cols = df.columns.tolist()
    first_cols = ['OrderDate', 'OrderTime', 'PickupTime', 'ScheduledTime', 'Location', 'Brands', 'OrderID', 'Channel', 'OrderStatus', 'DeliveryType', 'PaymentType', 'NetAOV', 'Rebate', 'ServiceCharge', 'DeliveryFee', 'Discounts', 'Tips', 'GrossAOV', 'VAT', 'IsTestOrder', 'ProductPLUs', 'ProductNames', 'PrimaryKey']
    remaining_cols = [col for col in cols if col not in first_cols]
    df = df[first_cols + remaining_cols]
    
    # Sort Rows
    df = df.sort_values(by=['OrderDate', 'OrderTime'])
    
    # Align Columns Names
    df = df.rename(columns={'Discounts': 'PromotionsOnItems'})
    
    # Standardise Brands
    df['Brands'] = np.where (df['Brands'].isnull(), 'Birdie Birdie', df['Brands'])
    df['Loc with Brand'] = df['Location'] + ' - ' + df['Brands'].str.split(n=1).str[0]
    
    # Standardise Columns
    column_order = ['PrimaryKey', 'Location', 'Loc with Brand', 'Brands', 'OrderID', 'OrderDate', 'OrderTime', 'Channel', 'OrderStatus', 'DeliveryType', 'GrossAOV', 'PromotionsOnItems', 'DeliveryFee', 'Tips', 'ProductPLUs', 'ProductNames', 'IsTestOrder', 'PaymentType', 'PickupTime']
    df = df[column_order]
    
    # Reset Index
    df = df.reset_index(drop=True)
    
    # Remove Duplicate Primary Keys
    cleaned_df = df['PrimaryKey']
    cleaned_df = cleaned_df.drop_duplicates()
    
    # Create new DataFrame
    df = df[df.index.isin(cleaned_df.index)]
    
    # Filter Lieferando Test Order
    df = df.loc[df['OrderID'] != '#63H1WT']
    
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    
    # Export File
    df.to_csv('Deliverect Data.csv', index=False)