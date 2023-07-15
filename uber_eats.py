import pandas as pd
import datetime as dt
import numpy as np
import os
import itertools

def clean_location_names(df):
    # Clean Location
    cleaned_names = pd.read_csv(r'H:\Shared drives\99 - Data\03 - Rx Name List\Full Rx List, with Cleaned Names.csv')
    df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
    df = pd.merge(df, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')
    df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
    df['Location'] = df['Cleaned Location']
    df = df.drop(columns=['Cleaned Location'])
    return df

def convert_time_format(time_decimal): # This converts for decimal time to hh:mm:ss
    if pd.notnull(time_decimal):
        total_seconds = int(time_decimal * 60)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = (total_seconds % 60)
        time_obj = dt.time(hours, minutes, seconds)
        return time_obj.strftime('%H:%M:%S')
    else:
        return ''

def format_timedelta(td):
    if pd.notnull(td):
        hours, remainder = divmod(td.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    else:
        return ''

def week_start(date): # This converts date to week date
    date = pd.to_datetime(date)
    days_since_monday = date.weekday()
    week_start = date - dt.timedelta(days=days_since_monday)
    return week_start

def process_uber_eats_order_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files')

    # Create UberEats Order Consolidated File
    dataframe = []

    for file_path in [
        '22.11 - UE - Orders.csv',
        '22.12 - UE - Orders.csv',
        '23.01 - UE - Orders.csv',
        '23.02 - UE - Orders.csv',
        '23.03 - UE - Orders.csv',
        '23.04 - UE - Orders.csv',
        '23.05 - UE - Orders.csv',
        '23.06 - UE - Orders.csv',
        '23.07 - UE - Orders.csv',
        '23.08 - UE - Orders.csv',
        '23.09 - UE - Orders.csv',
        '23.10 - UE - Orders.csv',
        '23.11 - UE - Orders.csv',
        '23.12 - UE - Orders.csv',
    ]:
        if os.path.exists(file_path):
            # If the file path exists, load the corresponding CSV file and append it to the list
            df = pd.read_csv(file_path, dtype={'Order ID': str}, encoding='utf-8')
            df['Order ID'] = '#' + df['Order ID'].astype(str)  # Add '#' in front of OrderID
            dataframe.append(df)
        else:
            # If the file path does not exist, append None to the list
            dataframe.append(None)

    # Concatenate the loaded data frames
    df = pd.concat([df for df in dataframe if df is not None])

    # Remove blank columns
    df = df.drop(columns={'External restaurant ID', 'Cancelled by', 'Cancellation time', 'Online order?', 'Eats Brand'})

    # Rename to Cleaned Names
    df.columns = df.columns.str.title().str.replace(' ', '')
    df = df.rename(columns={'Restaurant': 'Location', 'OrderId': 'OrderID', 'OrderUuid': 'OrderUUID', 'Scheduled?': 'IsScheduled', 'Completed?': 'IsCompleted', 'TicketSize': 'GrossAOV', 'DateOrdered': 'OrderDate',
                                                    'TimeCustomerOrdered': 'OrderTime', 'TimeMerchantAccepted': 'OrderAcceptedTime', 'TimeCourierStartedTrip': 'CourierStartedTime', 'TimeCourierDelivered': 'CourierEndedTime', 'CourierWaitingTime(Restaurant)': 'CourierRxWaitTime',
                                                    'CourierWaitingTime(Eater)': 'CourierCxWaitTime', 'TotalPrep&Hand-OffTime': 'TotalRxTime', 'DeliveryBatchType': 'StackingType', 'FulfilmentType': 'DeliveryType', 'OrderChannel': 'OrderDeviceType', 'WorkflowUuid': 'WorkflowUUID',
                                                    'PrepTimeIncreased?': 'IsPrepTimeIncreased'})

    # Change relevant columns to Boolean
    df['IsScheduled'] = np.where(df['IsScheduled'] == 1, True, False)
    df['IsCompleted'] = np.where(df['IsCompleted'] == 1, True, False)
    df['IsPrepTimeIncreased'] = np.where(df['IsPrepTimeIncreased'] == 1, True, False)
    df['IsSubscription'] = np.where(df['SubscriptionPass'].notnull(), True, False)

    # Clean certain columns
    df['StackingType'] = df['StackingType'].str.replace('_', ' ', regex=True)
    df['StackingType'] = df['StackingType'].str.title()
    df['OrderStatus'] = df['OrderStatus'].str.title()
    df['DeliveryStatus'] = df['DeliveryStatus'].str.title()

    # Clean Location Names
    df = clean_location_names(df)

    # Clean Channel Partner Names
    df['Channel'] = 'UberEats'

    # Convert from DateTime to Time
    df['OrderTime'] = pd.to_datetime(df['OrderTime']).dt.time
    df['OrderAcceptedTime'] = pd.to_datetime(df['OrderAcceptedTime']).dt.time
    df['CourierArrivalTime'] = pd.to_datetime(df['CourierArrivalTime']).dt.time
    df['CourierStartedTime'] = pd.to_datetime(df['CourierStartedTime']).dt.time
    df['CourierEndedTime'] = pd.to_datetime(df['CourierEndedTime']).dt.time

    # Adjust for Correct Time
    df['OrderTime'] = df['OrderTime'].apply(lambda x: (dt.datetime.combine(dt.date.today(), x)).time())
    df['OrderAcceptedTime'] = df['OrderAcceptedTime'].apply(lambda x: (dt.datetime.combine(dt.date.today(), x)).time() if pd.notnull(x) else pd.NaT)
    df['CourierArrivalTime'] = df['CourierArrivalTime'].apply(lambda x: (dt.datetime.combine(dt.date.today(), x)).time() if pd.notnull(x) else pd.NaT)
    df['CourierStartedTime'] = df['CourierStartedTime'].apply(lambda x: (dt.datetime.combine(dt.date.today(), x)).time() if pd.notnull(x) else pd.NaT)
    df['CourierEndedTime'] = df['CourierEndedTime'].apply(lambda x: (dt.datetime.combine(dt.date.today(), x)).time() if pd.notnull(x) else pd.NaT)

    # Convert from Decimal to Time
    df['TimeToConfirm'] = df['TimeToConfirm'].apply(lambda x: convert_time_format(x))
    df['OriginalPrepTime'] = df['OriginalPrepTime'].apply(lambda x: convert_time_format(x))
    df['CourierRxWaitTime'] = df['CourierRxWaitTime'].apply(lambda x: convert_time_format(x))
    df['CourierCxWaitTime'] = df['CourierCxWaitTime'].apply(lambda x: convert_time_format(x))
    df['TotalRxTime'] = df['TotalRxTime'].apply(lambda x: convert_time_format(x))
    df['OrderDuration'] = df['OrderDuration'].apply(lambda x: convert_time_format(x))

    # Create Additional Date & Time Columns
    df['OrderStartTime'] = np.where(df['IsScheduled'] == True, df['OrderAcceptedTime'], df['OrderTime'])

    # Calculate Order Times
    # TimeToAccept
    df['OrderStartTime'] = pd.to_datetime(df['OrderStartTime'].astype(str), format='%H:%M:%S')
    df['OrderAcceptedTime'] = pd.to_datetime(df['OrderAcceptedTime'].astype(str), format='%H:%M:%S')

    time_diff = df['OrderAcceptedTime'] - df['OrderStartTime']
    df['TimeToAccept'] = time_diff.apply(lambda x: format_timedelta(x))


    # TimeToCourierArrivedAtRx
    df['CourierArrivalTime'] = pd.to_datetime(df['CourierArrivalTime'].astype(str), format='%H:%M:%S')
    df['TimeToCourierArrivedAtRx'] = df['CourierArrivalTime'] - df['OrderAcceptedTime']

    time_diff = df['CourierArrivalTime'] - df['OrderAcceptedTime']
    df['TimeToCourierArrivedAtRx'] = (df['CourierArrivalTime'] - df['OrderAcceptedTime']).fillna(pd.Timedelta(0)).astype(str).str[-8:]


    # TimeCourierAtRx
    df['CourierStartedTime'] = pd.to_datetime(df['CourierStartedTime'].astype(str), format='%H:%M:%S')

    time_diff = df['CourierStartedTime'] - df['CourierArrivalTime']
    df['TimeCourierAtRx'] = (df['CourierStartedTime'] - df['CourierArrivalTime']).fillna(pd.Timedelta(0)).astype(str).str[-8:]

    # TimeToDeliver
    df['CourierEndedTime'] = pd.to_datetime(df['CourierEndedTime'].astype(str), format='%H:%M:%S')
    df['CourierCxWaitTime'] = pd.to_timedelta(df['CourierCxWaitTime'])

    time_diff = df['CourierEndedTime'] - df['CourierStartedTime'] - df['CourierCxWaitTime']
    df['TimeToDeliver'] = (df['CourierEndedTime'] - df['CourierStartedTime'] - df['CourierCxWaitTime']).fillna(pd.Timedelta(0)).astype(str).str[-8:]

    # TimeCourierAtCx
    df['TimeCourierAtCx'] = df['CourierCxWaitTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]

    # Clean Dates
    df['OrderStartTime'] = df['OrderStartTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]
    df['OrderAcceptedTime'] = df['OrderAcceptedTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]
    df['CourierArrivalTime'] = df['CourierArrivalTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]
    df['CourierStartedTime'] = df['CourierStartedTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]
    df['CourierArrivalTime'] = df['CourierArrivalTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]
    df['CourierEndedTime'] = df['CourierEndedTime'].fillna(pd.Timedelta(0)).astype(str).str[-8:]

    # Remove not needed columns
    df = df.drop(columns={'SubscriptionPass', 'CourierCxWaitTime'})

    # Arrange Columns
    cols = df.columns.tolist()
    first_cols = ['OrderID', 'Location', 'DeliveryType', 'Channel']
    date_cols = ['OrderDate', 'OrderTime']
    second_cols = ['OrderUUID', 'WorkflowUUID']
    boolean_cols = ['IsScheduled', 'IsCompleted', 'IsPrepTimeIncreased', 'IsSubscription']
    time_cols = ['OrderStartTime', 'OrderAcceptedTime',  'CourierArrivalTime', 'CourierStartedTime', 'CourierArrivalTime', 'CourierEndedTime']
    timevalue_cols = ['TimeToAccept', 'TimeToCourierArrivedAtRx', 'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx', 'OrderDuration']
    remaining_cols = [col for col in cols if col not in (first_cols + date_cols + second_cols + time_cols + timevalue_cols)]
    df = df[first_cols + date_cols + second_cols + time_cols + timevalue_cols + remaining_cols]

    # Create Primary Key
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderDate']

    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')

    # Export to CSV file
    df.to_csv('UE Order Data.csv', index=False)

def process_uber_eats_payment_details_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files')

    # Create UberEats Order Consolidated File
    dataframe = []

    for file_path in [
        '22.11 - UE - Payment Details.csv',
        '22.12 - UE - Payment Details.csv',
        '23.01 - UE - Payment Details.csv',
        '23.02 - UE - Payment Details.csv',
        '23.03 - UE - Payment Details.csv',
        '23.04 - UE - Payment Details.csv',
        '23.05 - UE - Payment Details.csv',
        '23.06 - UE - Payment Details.csv',
        '23.07 - UE - Payment Details.csv',
        '23.08 - UE - Payment Details.csv',
        '23.09 - UE - Payment Details.csv',
        '23.10 - UE - Payment Details.csv',
        '23.11 - UE - Payment Details.csv',
        '23.12 - UE - Payment Details.csv',
    ]:
        if os.path.exists(file_path):
            # If the file path exists, load the corresponding CSV file and append it to the list
            df = pd.read_csv(file_path, dtype={'Order ID': str}, encoding='utf-8')
            new_columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            df.columns = new_columns
            dataframe.append(df)
        else:
            # If the file path does not exist, append None to the list
            dataframe.append(None)

    # Concatenate the loaded data frames
    df = pd.concat([df for df in dataframe if df is not None])

    # Clean Location
    df = df.rename(columns={'Shop name': 'Location'})

    # Remove Refunds (This is captured in the refunds DataFrame
    df = df.loc[df['Order status'] != 'Refund']

    # Import Location Cleaner
    df = clean_location_names(df)

    # Create Two Different Data Frames
    orders_df = df.loc[df['Other payments description'].isnull()]
    ue_pd_non_orders_data = df.loc[df['Other payments description'].notnull()]

    # Adjust and Clean ue_pd_non_orders_data DataFrame
    ue_pd_non_orders_data = ue_pd_non_orders_data[
        ['Location', 'Payout date', 'Other payments (incl. VAT)', 'Other payments description']]
    ue_pd_non_orders_data = ue_pd_non_orders_data.rename(
        columns={'Payout date': 'PayoutDate', 'Other payments (incl. VAT)': 'OtherPayments',
                 'Other payments description': 'PaymentType'})
    ue_pd_non_orders_data['PaymentType'] = ue_pd_non_orders_data['PaymentType'].str.title()
    ue_pd_non_orders_data['PaymentType'] = ue_pd_non_orders_data['PaymentType'].str.replace('Vat', 'VAT')
    ue_pd_non_orders_data.loc[:, 'OtherPayments'] = pd.to_numeric(ue_pd_non_orders_data['OtherPayments'],
                                                                  errors='coerce')
    ue_pd_non_orders_data.loc[:, 'OtherPayments'] = ue_pd_non_orders_data['OtherPayments'].astype('float64')

    # Adjust Payout Date to Prior Week
    ue_pd_non_orders_data = ue_pd_non_orders_data.copy()
    ue_pd_non_orders_data.loc[:, 'PayoutDate'] = pd.to_datetime(ue_pd_non_orders_data['PayoutDate'],
                                                                dayfirst=True) - pd.DateOffset(days=7)

    # Group Data
    ue_pd_non_orders_data = ue_pd_non_orders_data.groupby(['Location', 'PayoutDate', 'PaymentType'])[
        'OtherPayments'].sum().reset_index()

    # Create Unique Lists
    unique_payment_descriptions = ue_pd_non_orders_data['PaymentType'].unique()
    unique_locations = ue_pd_non_orders_data['Location'].unique()
    unique_entries = len(ue_pd_non_orders_data)
    total_combinations = unique_entries * len(unique_locations) * len(unique_payment_descriptions)

    # Find Min and Max Dates
    orders_df.loc[:, 'Order date'] = pd.to_datetime(orders_df['Order date'], dayfirst=True)
    valid_order_dates = orders_df['Order date'].dropna()
    max_order_date = valid_order_dates.max()
    valid_non_order_dates = ue_pd_non_orders_data['PayoutDate'].dropna()
    max_non_order_date = valid_non_order_dates.max()
    min_non_order_date = valid_non_order_dates.min()

    # Create New Table
    date_range = pd.date_range(start=min_non_order_date, end=max_order_date, freq='D')
    new_df = pd.DataFrame({'PayoutDate': date_range})
    new_df['OrderWeek'] = new_df['PayoutDate'].apply(week_start)

    # Create Day Count
    day_count = new_df.groupby('OrderWeek')['PayoutDate'].nunique().reset_index()
    day_count = day_count.rename(columns={'PayoutDate': 'Unique Days Count'})
    new_df = new_df.merge(day_count, on='OrderWeek', how='left')

    # Create all combinations of Location and Payment Descriptions
    combinations = list(itertools.product(unique_locations, unique_payment_descriptions))

    # Repeat each row for each combination
    new_df = pd.DataFrame(np.repeat(new_df.values, len(combinations), axis=0), columns=new_df.columns)
    new_df[['Location', 'Payment Descriptions']] = pd.DataFrame(combinations * len(new_df))

    # Add Spend Amount
    new_df['OtherPayments'] = pd.merge(new_df[['Location', 'OrderWeek', 'Payment Descriptions']],
                                       ue_pd_non_orders_data[
                                           ['Location', 'PayoutDate', 'PaymentType', 'OtherPayments']],
                                       left_on=['Location', 'OrderWeek', 'Payment Descriptions'],
                                       right_on=['Location', 'PayoutDate', 'PaymentType'],
                                       how='left')['OtherPayments']

    # Remove Blank Amount Columns
    new_df = new_df.loc[new_df['OtherPayments'].notnull()]

    # Change to Daily
    new_df['OtherPayments'] = new_df['OtherPayments'] / new_df['Unique Days Count']

    # Remove Columns That Are Not Needed
    new_df = new_df.drop(columns=['Unique Days Count'])

    # Clean orders_df DataFrame
    orders_df = orders_df[
        ['Order ID', 'Workflow ID', 'Location', 'Order date', 'Sales (incl. VAT)', 'Promotions on items (incl. VAT)',
         'Price adjustments (including VAT)', 'Delivery fee (incl. VAT)',
         'Promotions on delivery (incl. VAT)', 'Tips', 'Marketplace fee after discount (incl. VAT)', 'Total payout ',
         'Payout date']]
    orders_df.columns = orders_df.columns.str.title().str.replace(' ', '')
    orders_df = orders_df.rename(
        columns={'OrderId': 'OrderID', 'WorkflowId': 'WorkflowUUID', 'Sales(Incl.Vat)': 'GrossAOV',
                 'PromotionsOnItems(Incl.Vat)': 'PromotionsOnItems',
                 'PriceAdjustments(IncludingVat)': 'PriceAdjustments',
                 'DeliveryFee(Incl.Vat)': 'DeliveryFee', 'MarketplaceFeeAfterDiscount(Incl.Vat)': 'MarketplaceFee',
                 'PromotionsOnDelivery(Incl.Vat)': 'PromotionsOnDelivery'})

    selected_columns = ['GrossAOV', 'PromotionsOnItems', 'PriceAdjustments', 'DeliveryFee', 'MarketplaceFee',
                        'PromotionsOnDelivery', 'Tips', 'TotalPayout']
    orders_df[selected_columns] = orders_df[selected_columns].astype(float)
    orders_df['OrderDate'] = pd.to_datetime(orders_df['OrderDate'])

    # Create Primary Key
    orders_df['PrimaryKey'] = orders_df['OrderID'] + ' - ' + orders_df['Location'] + ' - ' + \
                                      orders_df['OrderDate'].dt.strftime('%Y-%m-%d')

    # # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    new_df.to_csv('UE Non Order Payment Data.csv', index=False)
    orders_df.to_csv('UE Order Payment Data.csv', index=False)

def process_uber_eats_refund_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files')

    # Create UberEats Refunds Consolidated File
    dataframe = []

    for file_path in [
        '22.11 - UE - Inaccurate Orders.csv',
        '22.12 - UE - Inaccurate Orders.csv',
        '23.01 - UE - Inaccurate Orders.csv',
        '23.02 - UE - Inaccurate Orders.csv',
        '23.03 - UE - Inaccurate Orders.csv',
        '23.04 - UE - Inaccurate Orders.csv',
        '23.05 - UE - Inaccurate Orders.csv',
        '23.06 - UE - Inaccurate Orders.csv',
        '23.07 - UE - Inaccurate Orders.csv',
        '23.08 - UE - Inaccurate Orders.csv',
        '23.09 - UE - Inaccurate Orders.csv',
        '23.10 - UE - Inaccurate Orders.csv',
        '23.11 - UE - Inaccurate Orders.csv',
        '23.12 - UE - Inaccurate Orders.csv',
    ]:
        if os.path.exists(file_path):
            # If the file path exists, load the corresponding CSV file and append it to the list
            df = pd.read_csv(file_path, dtype={'Order ID': str}, encoding='utf-8')
            df['Order ID'] = '#' + df['Order ID'].astype(str)  # Add '#' in front of OrderID
            dataframe.append(df)
        else:
            # If the file path does not exist, append None to the list
            dataframe.append(None)

    # Concatenate the loaded data frames
    df = pd.concat([df for df in dataframe if df is not None])

    # Clean DataFrame
    df = df[['Restaurant', 'Order ID', 'Workflow UUID', 'Time customer ordered', 'Order issue', 'Ticket size', 'Customer refunded', 'Refund Covered by Merchant', 'Refund Not Covered by Merchant']]
    df.columns = df.columns.str.title().str.replace(' ', '').str.replace('Uuid', 'UUID').str.replace('Id', 'ID')
    df['OrderIssue'] = df['OrderIssue'].str.title().str.replace('_', ' ')
    df['TimeCustomerOrdered'] = pd.to_datetime(df['TimeCustomerOrdered'])
    df = df.rename(columns={'Restaurant': 'Location', 'TimeCustomerOrdered' : 'OrderDate', 'TicketSize': 'GrossAOV', 'CustomerRefunded': 'TotalRefund'})
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])

    # Import Location Cleaner
    df = clean_location_names(df)

    # Create Primary Key
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderDate'].dt.strftime('%Y-%m-%d')

    # # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    df.to_csv('UE Refund Data.csv', index=False)

def process_uber_eats_review_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files')

    # Create UberEats Refunds Consolidated File
    dataframe = []

    for file_path in [
        '22.11 - UE - Customer Reviews.csv',
        '22.12 - UE - Customer Reviews.csv',
        '23.01 - UE - Customer Reviews.csv',
        '23.02 - UE - Customer Reviews.csv',
        '23.03 - UE - Customer Reviews.csv',
        '23.04 - UE - Customer Reviews.csv',
        '23.05 - UE - Customer Reviews.csv',
        '23.06 - UE - Customer Reviews.csv',
        '23.07 - UE - Customer Reviews.csv',
        '23.08 - UE - Customer Reviews.csv',
        '23.09 - UE - Customer Reviews.csv',
        '23.10 - UE - Customer Reviews.csv',
        '23.11 - UE - Customer Reviews.csv',
        '23.12 - UE - Customer Reviews.csv',
    ]:
        if os.path.exists(file_path):
            # If the file path exists, load the corresponding CSV file and append it to the list
            df = pd.read_csv(file_path, dtype={'Order ID': str}, encoding='utf-8')
            df['Order ID'] = '#' + df['Order ID'].astype(str)  # Add '#' in front of OrderID
            dataframe.append(df)
        else:
            # If the file path does not exist, append None to the list
            dataframe.append(None)

    # Concatenate the loaded data frames
    df = pd.concat([df for df in dataframe if df is not None])

    # Clean DataFrame
    df = df[
        ['Restaurant', 'Order ID', 'Order UUID', 'Date ordered', 'Rating type', 'Rating value']]
    df = df.rename(
        columns={'Restaurant': 'Location', 'Order ID': 'OrderID', 'Order UUID': 'OrderUUID',
                 'Date ordered': 'OrderDate', 'Rating type': 'RatingType', 'Rating value': 'RatingScore'})
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df['RatingType'] = df['RatingType'].str.title().str.replace('_', ' ')
    df = df.loc[df['RatingType'] == 'Consumer To Merchant']
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])

    # Import Location Cleaner
    from ue_orders import clean_location_names
    df = clean_location_names(df)

    # Create Primary Key
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + \
                                   df['OrderDate'].dt.strftime('%Y-%m-%d')

    # # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    df.to_csv('UE Review Data.csv', index=False)
    
def process_uber_eats_review_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files')

    # Create UberEats Refunds Consolidated File
    dataframe = []

    for file_path in [
        '22.11 - UE - Customer Reviews.csv',
        '22.12 - UE - Customer Reviews.csv',
        '23.01 - UE - Customer Reviews.csv',
        '23.02 - UE - Customer Reviews.csv',
        '23.03 - UE - Customer Reviews.csv',
        '23.04 - UE - Customer Reviews.csv',
        '23.05 - UE - Customer Reviews.csv',
        '23.06 - UE - Customer Reviews.csv',
        '23.07 - UE - Customer Reviews.csv',
        '23.08 - UE - Customer Reviews.csv',
        '23.09 - UE - Customer Reviews.csv',
        '23.10 - UE - Customer Reviews.csv',
        '23.11 - UE - Customer Reviews.csv',
        '23.12 - UE - Customer Reviews.csv',
    ]:
        if os.path.exists(file_path):
            # If the file path exists, load the corresponding CSV file and append it to the list
            df = pd.read_csv(file_path, dtype={'Order ID': str}, encoding='utf-8')
            df['Order ID'] = '#' + df['Order ID'].astype(str)  # Add '#' in front of OrderID
            dataframe.append(df)
        else:
            # If the file path does not exist, append None to the list
            dataframe.append(None)

    # Concatenate the loaded data frames
    df = pd.concat([df for df in dataframe if df is not None])

    # Clean DataFrame
    df = df[
        ['Restaurant', 'Order ID', 'Order UUID', 'Date ordered', 'Rating type', 'Rating value']]
    df = df.rename(
        columns={'Restaurant': 'Location', 'Order ID': 'OrderID', 'Order UUID': 'OrderUUID',
                 'Date ordered': 'OrderDate', 'Rating type': 'RatingType', 'Rating value': 'RatingScore'})
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df['RatingType'] = df['RatingType'].str.title().str.replace('_', ' ')
    df = df.loc[df['RatingType'] == 'Consumer To Merchant']
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])

    # Import Location Cleaner
    df = clean_location_names(df)

    # Create Primary Key
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + \
                                   df['OrderDate'].dt.strftime('%Y-%m-%d')

    # # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    df.to_csv('UE Review Data.csv', index=False)

def process_uber_eats_consolidtated_data():
    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')

    # Load Data
    orders_df = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\UE Order Data.csv')
    payment_details_df = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\UE Order Payment Data.csv')
    refund_df = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\UE Refund Data.csv')
    review_df = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\UE Review Data.csv')

    # Clean Dates Columns
    orders_df['OrderDate'] = pd.to_datetime(orders_df['OrderDate'], format='mixed', dayfirst=True).dt.date
    refund_df['OrderDate'] = pd.to_datetime(refund_df['OrderDate'], format='mixed', dayfirst=True).dt.date
    review_df['OrderDate'] = pd.to_datetime(review_df['OrderDate'], format='mixed', dayfirst=True).dt.date

    # Combine the "Primary Keys" column from the four DataFrames
    consolidated_df = pd.concat([orders_df[['PrimaryKey']], payment_details_df[['PrimaryKey']], refund_df[['PrimaryKey']], review_df[['PrimaryKey']]], ignore_index=True)

    # Remove Dupes
    consolidated_df = consolidated_df.drop_duplicates()

    # Create Lists To Import
    orders_list = ['PrimaryKey', 'Location', 'OrderID', 'OrderDate', 'OrderTime', 'City', 'OrderStatus',
                      'DeliveryStatus', 'DeliveryType', 'GrossAOV', 'StackingType', 'WorkflowUUID', 'OrderUUID',
                      'IsScheduled', 'IsSubscription', 'OrderStartTime', 'TimeToAccept', 'TimeToCourierArrivedAtRx',
                      'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx', 'OrderDuration']
    payments_list_1 = ['PrimaryKey', 'Location', 'OrderID', 'OrderDate', 'GrossAOV', 'WorkflowUUID']
    payments_list_2 = ['PrimaryKey', 'PromotionsOnItems', 'PriceAdjustments', 'DeliveryFee', 'PromotionsOnDelivery',
                          'Tips', 'MarketplaceFee', 'TotalPayout', 'PayoutDate']
    refund_list = ['PrimaryKey', 'OrderIssue', 'TotalRefund', 'RefundCoveredByMerchant',
                      'RefundNotCoveredByMerchant']
    review_list = ['PrimaryKey', 'RatingScore']
    consolidated_list_1 = ['PrimaryKey', 'Location', 'OrderID', 'OrderDate', 'OrderTime', 'City', 'OrderStatus',
                              'DeliveryStatus', 'DeliveryType', 'GrossAOV', 'StackingType', 'WorkflowUUID', 'OrderUUID',
                              'IsScheduled', 'IsSubscription', 'OrderStartTime', 'TimeToAccept',
                              'TimeToCourierArrivedAtRx', 'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx',
                              'OrderDuration']
    consolidated_list_2 = ['PrimaryKey', 'Location', 'OrderID', 'OrderDate', 'OrderTime', 'Channel', 'City',
                              'OrderStatus', 'DeliveryStatus', 'DeliveryType', 'GrossAOV', 'StackingType',
                              'WorkflowUUID', 'OrderUUID',
                              'IsScheduled', 'IsSubscription', 'OrderStartTime', 'TimeToAccept',
                              'TimeToCourierArrivedAtRx', 'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx',
                              'OrderDuration', 'PromotionsOnItems',
                              'PriceAdjustments', 'DeliveryFee', 'PromotionsOnDelivery', 'Tips', 'MarketplaceFee',
                              'TotalPayout', 'PayoutDate', 'OrderIssue', 'TotalRefund', 'RefundCoveredByMerchant',
                              'RefundNotCoveredByMerchant', 'RatingScore']

    # Merge DataFrames - Step 1
    consolidated_df = pd.merge(consolidated_df, orders_df[orders_list], on='PrimaryKey', how='left')
    consolidated_df = pd.merge(consolidated_df, orders_df[payments_list_1], on='PrimaryKey', how='left')

    # Populate All Rows
    consolidated_df['OrderID'] = np.where(consolidated_df['OrderID_x'].isnull(), consolidated_df['OrderID_y'], consolidated_df['OrderID_x'])
    consolidated_df['OrderDate'] = np.where(consolidated_df['OrderDate_x'].isnull(), consolidated_df['OrderDate_y'], consolidated_df['OrderDate_x'])
    consolidated_df['Location'] = np.where(consolidated_df['Location_x'].isnull(), consolidated_df['Location_y'], consolidated_df['Location_x'])
    consolidated_df['GrossAOV'] = np.where(consolidated_df['GrossAOV_x'].isnull(), consolidated_df['GrossAOV_y'], consolidated_df['GrossAOV_x'])
    consolidated_df['WorkflowUUID'] = np.where(consolidated_df['WorkflowUUID_x'].isnull(), consolidated_df['WorkflowUUID_y'], consolidated_df['WorkflowUUID_x'])

    # Remove Not Needed Columns From DataFrame
    consolidated_df = consolidated_df[consolidated_list_1]

    # Merge DataFrames - Step 2
    consolidated_df = pd.merge(consolidated_df, payment_details_df[payments_list_2], on='PrimaryKey', how='left')
    consolidated_df = pd.merge(consolidated_df, refund_df[refund_list], on='PrimaryKey', how='left')
    consolidated_df = pd.merge(consolidated_df, review_df[review_list], on='PrimaryKey', how='left')

    # Re Order DataFrame
    consolidated_df['Channel'] = 'Uber Eats'

    # Re Order DataFrame
    consolidated_df = consolidated_df[consolidated_list_2]

    # Change to Data Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')

    # Export to CSV file
    consolidated_df.to_csv('UE Consolidated Data.csv', index=False)