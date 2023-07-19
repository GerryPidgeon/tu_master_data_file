import shutil
import os
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime

# Setup Time Functions
def week_start(date):
    date = pd.to_datetime(date)
    days_since_monday = date.weekday()
    week_start = date - dt.timedelta(days=days_since_monday)
    return week_start

def month_start(date):
    return pd.to_datetime(date).to_period('M').to_timestamp()

def get_period_string(date_str):
    # Convert date string to datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Determine period based on day of the month
    period_num = 1 if date_obj.day <= 15 else 2

    # Format period string as "P{period_num} mmm yy"
    period_str = f"P{period_num} {date_obj.strftime('%b %y')}"
    return period_str

def process_consolidated_data():
    # Read CSV Files
    deliverect_data = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\Deliverect Data.csv')
    uber_eats_data = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\UE Consolidated Data.csv')
    wolt_data = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\Wolt Data.csv')
    lieferando_data = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\Lieferando Data.csv')
    food_panda_data = pd.read_csv(r'H:\Shared drives\99 - Data\00 - Cleaned Data\Food Panda Data.csv')

    # Combine the "Primary Keys" column from all DataFrames
    consolidated_data = pd.concat([deliverect_data[['PrimaryKey']], uber_eats_data[['PrimaryKey']], wolt_data[['PrimaryKey']], lieferando_data[['PrimaryKey']], food_panda_data[['PrimaryKey']]], ignore_index=True)

    # Drop Duplicates
    consolidated_data = consolidated_data.drop_duplicates()

    # Export Primary Key List
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    consolidated_data.to_csv('Primary Key List.csv', index=False)

    # Does it exist in Deliverect
    consolidated_data['ExistsInDeliverect'] = np.where(consolidated_data['PrimaryKey'].isin(deliverect_data['PrimaryKey']), 'Yes', 'No')

    # Populate Deliverect Data
    consolidated_data = pd.merge(consolidated_data, deliverect_data[['PrimaryKey', 'Location', 'Loc with Brand', 'Brands', 'OrderID', 'OrderDate', 'Channel', 'OrderTime', 'OrderStatus', 'DeliveryType', 'GrossAOV', 'PromotionsOnItems', 'DeliveryFee', 'Tips',
                                                                     'ProductPLUs', 'ProductNames','IsTestOrder', 'PaymentType', 'PickupTime']], on='PrimaryKey', how='left')

    # Create Cleaned Status
    consolidated_data['CleanedOrderStatus'] = consolidated_data['OrderStatus'].str.replace("_", " ").str.title()
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Delivered', 'Ready For Pickup', 'Auto Finalized', 'In Delivery', 'Preparing'], 'Delivered')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Cancel', 'Canceled'], 'Cancelled')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Failed', 'Failed Cancel', 'Failed Resolved'], 'Failed')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Unfulfilled'], 'Unfulfilled')

    # Create All Columns
    column_order = ['PrimaryKey', 'ExistsInDeliverect', 'Location', 'Loc with Brand', 'Brands', 'OrderID', 'OrderDate', 'OrderTime', 'OrderWeek', 'OrderMonth', 'Period', 'Channel', 'ChannelCopy', 'City', 'OrderStatus', 'CleanedOrderStatus', 'DeliveryStatus', 'DeliveryType', 'GrossAOV', 'StackingType', 'WorkflowUUID', 'OrderUUID', 'IsScheduled',
                    'IsSubscription', 'OrderStartTime', 'TimeToAccept', 'TimeToCourierArrivedAtRx', 'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx', 'OrderDuration', 'PromotionsOnItems', 'PriceAdjustments', 'DeliveryFee',
                    'PromotionsOnDelivery', 'Tips', 'MarketplaceFee', 'TotalPayout', 'PayoutDate', 'OrderIssue', 'TotalRefund', 'RefundCoveredByMerchant', 'RefundNotCoveredByMerchant', 'RatingScore', 'ProductPLUs', 'ProductNames',
                    'IsTestOrder', 'PaymentType', 'PickupTime']

    # Check if each column already exists in the DataFrame
    missing_columns = [col for col in column_order if col not in consolidated_data.columns]

    # Create the missing columns with NaN values
    for col in missing_columns:
        consolidated_data[col] = np.nan

    # List of Master (Primary Key) Columns
    master_columns_list = ['PrimaryKey', 'Location', 'OrderID', 'OrderDate', 'Channel']

    # Populate Master (Primary Key) Columns

    # Uber Eats
    consolidated_data = pd.merge(consolidated_data, uber_eats_data[master_columns_list], on='PrimaryKey', how='left', suffixes=('', '_uber'))
    consolidated_data['Location'] = np.where(pd.isna(consolidated_data['Location']), consolidated_data['Location_uber'], consolidated_data['Location'])
    consolidated_data['OrderID'] = np.where(pd.isna(consolidated_data['OrderID']), consolidated_data['OrderID_uber'], consolidated_data['OrderID'])
    consolidated_data['OrderDate'] = np.where(pd.isna(consolidated_data['OrderDate']), consolidated_data['OrderDate_uber'], consolidated_data['OrderDate'])
    consolidated_data['Channel'] = np.where(pd.isna(consolidated_data['Channel']), consolidated_data['Channel_uber'], consolidated_data['Channel'])
    consolidated_data = consolidated_data.drop(['Location_uber', 'OrderID_uber', 'OrderDate_uber', 'Channel_uber'], axis=1)

    # Wolt
    consolidated_data = pd.merge(consolidated_data, wolt_data[master_columns_list], on='PrimaryKey', how='left', suffixes=('', '_wolt'))
    consolidated_data['Location'] = np.where(pd.isna(consolidated_data['Location']), consolidated_data['Location_wolt'], consolidated_data['Location'])
    consolidated_data['OrderID'] = np.where(pd.isna(consolidated_data['OrderID']), consolidated_data['OrderID_wolt'], consolidated_data['OrderID'])
    consolidated_data['OrderDate'] = np.where(pd.isna(consolidated_data['OrderDate']), consolidated_data['OrderDate_wolt'], consolidated_data['OrderDate'])
    consolidated_data['Channel'] = np.where(pd.isna(consolidated_data['Channel']), consolidated_data['Channel_wolt'], consolidated_data['Channel'])
    consolidated_data = consolidated_data.drop(['Location_wolt', 'OrderID_wolt', 'OrderDate_wolt', 'Channel_wolt'], axis=1)

    # Lieferando
    consolidated_data = pd.merge(consolidated_data, lieferando_data[master_columns_list], on='PrimaryKey', how='left', suffixes=('', '_lieferando'))
    consolidated_data['Location'] = np.where(pd.isna(consolidated_data['Location']), consolidated_data['Location_lieferando'], consolidated_data['Location'])
    consolidated_data['OrderID'] = np.where(pd.isna(consolidated_data['OrderID']), consolidated_data['OrderID_lieferando'], consolidated_data['OrderID'])
    consolidated_data['OrderDate'] = np.where(pd.isna(consolidated_data['OrderDate']), consolidated_data['OrderDate_lieferando'], consolidated_data['OrderDate'])
    consolidated_data['Channel'] = np.where(pd.isna(consolidated_data['Channel']), consolidated_data['Channel_lieferando'], consolidated_data['Channel'])
    consolidated_data = consolidated_data.drop(['Location_lieferando', 'OrderID_lieferando', 'OrderDate_lieferando', 'Channel_lieferando'], axis=1)

    # Food Panda
    consolidated_data = pd.merge(consolidated_data, food_panda_data[master_columns_list], on='PrimaryKey', how='left', suffixes=('', '_food_panda'))
    consolidated_data['Location'] = np.where(pd.isna(consolidated_data['Location']), consolidated_data['Location_food_panda'], consolidated_data['Location'])
    consolidated_data['OrderID'] = np.where(pd.isna(consolidated_data['OrderID']), consolidated_data['OrderID_food_panda'], consolidated_data['OrderID'])
    consolidated_data['OrderDate'] = np.where(pd.isna(consolidated_data['OrderDate']), consolidated_data['OrderDate_food_panda'], consolidated_data['OrderDate'])
    consolidated_data['Channel'] = np.where(pd.isna(consolidated_data['Channel']), consolidated_data['Channel_food_panda'], consolidated_data['Channel'])
    consolidated_data = consolidated_data.drop(['Location_food_panda', 'OrderID_food_panda', 'OrderDate_food_panda', 'Channel_food_panda'], axis=1)

    # List of Uber Eats Columns (excludes master_columns_list)
    uber_eats_columns_list = ['OrderTime', 'City', 'OrderStatus', 'DeliveryStatus', 'DeliveryType', 'GrossAOV', 'StackingType', 'WorkflowUUID', 'OrderUUID', 'IsScheduled', 'IsSubscription', 'OrderStartTime',
                              'TimeToAccept', 'TimeToCourierArrivedAtRx', 'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx', 'OrderDuration', 'PromotionsOnItems', 'PriceAdjustments', 'DeliveryFee', 'PromotionsOnDelivery',
                              'Tips', 'MarketplaceFee', 'TotalPayout', 'PayoutDate', 'OrderIssue', 'TotalRefund', 'RefundCoveredByMerchant', 'RefundNotCoveredByMerchant', 'RatingScore']

    uber_eats_list = ['PrimaryKey'] + uber_eats_columns_list

    consolidated_data = pd.merge(consolidated_data, uber_eats_data[uber_eats_list], on='PrimaryKey', how='left', suffixes=('', '_uber'))

    for column in uber_eats_columns_list:
        condition = (consolidated_data[column].isnull()) & (consolidated_data['Channel'] == 'Uber Eats')
        consolidated_data[column] = np.where(condition, consolidated_data[f'{column}_uber'], consolidated_data[column])

    # List of Wolt Columns (excludes master_columns_list)
    wolt_columns_list = ['OrderTime', 'OrderStatus', 'DeliveryType', 'GrossAOV', 'OrderDuration', 'RatingScore']

    wolt_list = ['PrimaryKey'] + wolt_columns_list

    consolidated_data = pd.merge(consolidated_data, wolt_data[wolt_list], on='PrimaryKey', how='left', suffixes=('', '_wolt'))

    for column in wolt_columns_list:
        condition = (consolidated_data[column].isnull()) & (consolidated_data['Channel'] == 'Wolt')
        consolidated_data[column] = np.where(condition, consolidated_data[f'{column}_wolt'], consolidated_data[column])

    # List of Lieferando Columns (excludes master_columns_list)
    lieferando_columns_list = ['OrderTime', 'DeliveryType', 'GrossAOV']

    lieferando_list = ['PrimaryKey'] + lieferando_columns_list

    consolidated_data = pd.merge(consolidated_data, lieferando_data[lieferando_list], on='PrimaryKey', how='left', suffixes=('', '_lieferando'))

    for column in lieferando_columns_list:
        condition = (consolidated_data[column].isnull()) & (consolidated_data['Channel'] == 'Lieferando')
        consolidated_data[column] = np.where(condition, consolidated_data[f'{column}_lieferando'], consolidated_data[column])

    # List of Food Panda Columns (excludes master_columns_list)
    food_panda_columns_list = ['DeliveryType', 'GrossAOV', 'MarketplaceFee', 'TotalPayout']

    food_panda_list = ['PrimaryKey'] + food_panda_columns_list

    consolidated_data = pd.merge(consolidated_data, food_panda_data[food_panda_list], on='PrimaryKey', how='left', suffixes=('', '_food_panda'))

    for column in food_panda_columns_list:
        condition = (consolidated_data[column].isnull()) & (consolidated_data['Channel'] == 'Food Panda')
        consolidated_data[column] = np.where(condition, consolidated_data[f'{column}_food_panda'], consolidated_data[column])

    # Clean up Statuses
    consolidated_data['CleanedOrderStatus'] = np.where(consolidated_data['CleanedOrderStatus'].isnull(), consolidated_data['OrderStatus'], consolidated_data['CleanedOrderStatus'])
    consolidated_data['CleanedOrderStatus'] = np.where((consolidated_data['CleanedOrderStatus'] == 'Deliverect Parsed') & (consolidated_data['DeliveryStatus'].str.contains('Cancel')), 'Cancelled', consolidated_data['CleanedOrderStatus'])
    consolidated_data['CleanedOrderStatus'] = np.where(consolidated_data['Channel'] == 'Lieferando', 'Delivered', consolidated_data['CleanedOrderStatus']) # Due to Deliverect Issues, assumes all are Delivered
    consolidated_data['CleanedOrderStatus'] = np.where((consolidated_data['Channel'] == 'Food Panda') & (consolidated_data['ExistsInDeliverect'] == 'No'), 'Delivered', consolidated_data['CleanedOrderStatus'])
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].str.replace('Canceled', 'Cancelled')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].str.replace('Deliverect Parsed', 'Delivered')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Accepted', 'Completed', 'Delivered', 'Duplicate', 'Ready For Pickup', 'Auto Finalized', 'In Delivery', 'Preparing'], 'Delivered')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Cancel', 'Canceled'], 'Cancelled')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Failed', 'Failed Cancel', 'Failed Resolved'], 'Failed')
    consolidated_data['CleanedOrderStatus'] = consolidated_data['CleanedOrderStatus'].replace(['Unfulfilled'], 'Unfulfilled')

    # Sort DataFrame
    consolidated_data = consolidated_data[column_order]
    consolidated_data = consolidated_data.sort_values(by=('OrderDate'))

    # Exclude Records without Order Date
    consolidated_data = consolidated_data[consolidated_data['OrderDate'].notnull()]

    # Add Additional Columns
    consolidated_data['OrderWeek'] = consolidated_data['OrderDate'].apply(week_start)
    consolidated_data['OrderMonth'] = consolidated_data['OrderDate'].apply(month_start)
    consolidated_data['Period'] = consolidated_data['OrderDate'].apply(get_period_string)

    # Correct Brands
    consolidated_data['Brands'] = np.where(consolidated_data['Brands'].isnull(), 'Birdie Birdie', consolidated_data['Brands'])
    consolidated_data['Loc with Brand'] = np.where(consolidated_data['Loc with Brand'].isnull(), consolidated_data['Location'] + ' - ' + consolidated_data['Brands'].str.split(n=1).str[0], consolidated_data['Loc with Brand'])

    # Provide Count For Master Order Index
    filter_condition = consolidated_data['CleanedOrderStatus'] == 'Delivered'
    consolidated_data['MOIRunningCount'] = consolidated_data[filter_condition].groupby(['Period', 'Loc with Brand', 'Channel']).cumcount() + 1
    consolidated_data['MOIRunningCount'] = consolidated_data['MOIRunningCount'].astype(str)
    consolidated_data['MasterOrderIndex'] = np.where(consolidated_data['MOIRunningCount'] == 'nan', '', consolidated_data['Period'] + ' - ' + consolidated_data['Loc with Brand'] + ' - ' + consolidated_data['Channel'] + ' ' + consolidated_data['MOIRunningCount'])
    consolidated_data['MasterOrderIndex'] = consolidated_data['MasterOrderIndex'].str.split('.').str[0]
    consolidated_data = consolidated_data.drop(columns=['MOIRunningCount'])

    # Provide Count For PDF Index - Half Month
    filter_condition = consolidated_data['CleanedOrderStatus'] == 'Delivered'
    consolidated_data['PDFRunningCount'] = consolidated_data[filter_condition].groupby(['Period', 'Location']).cumcount() + 1
    consolidated_data['PDFRunningCount'] = consolidated_data['PDFRunningCount'].astype(str)
    consolidated_data['PDFIndexHalfMonth'] = np.where(consolidated_data['PDFRunningCount'] == 'nan', '', consolidated_data['Period'] + ' - ' + consolidated_data['Location'] + ' ' + consolidated_data['PDFRunningCount'])
    consolidated_data['PDFIndexHalfMonth'] = consolidated_data['PDFIndexHalfMonth'].str.split('.').str[0]
    consolidated_data = consolidated_data.drop(columns=['PDFRunningCount'])

    # Provide Count For PDF Index - Full Month
    filter_condition = consolidated_data['CleanedOrderStatus'] == 'Delivered'
    consolidated_data['PDFRunningCount'] = consolidated_data[filter_condition].groupby(['OrderMonth', 'Location']).cumcount() + 1
    consolidated_data['PDFRunningCount'] = consolidated_data['PDFRunningCount'].astype(str)
    consolidated_data['TextDate'] = pd.to_datetime(consolidated_data['OrderMonth'], format="%d-%b-%Y")
    consolidated_data['TextDate'] = consolidated_data['TextDate'].dt.strftime("%b-%y")
    consolidated_data['PDFIndexFullMonth'] = np.where(consolidated_data['PDFRunningCount'] == 'nan', '', consolidated_data['TextDate'] + ' - ' + consolidated_data['Location'] + ' ' + consolidated_data['PDFRunningCount'])
    consolidated_data['PDFIndexFullMonth'] = consolidated_data['PDFIndexFullMonth'].str.split('.').str[0]
    consolidated_data = consolidated_data.drop(columns=['PDFRunningCount', 'TextDate'])

    # Filter Lieferando Test Order
    consolidated_data = consolidated_data.loc[consolidated_data['OrderID'] != '#63H1WT']

    # Add Discount and Promo Filters
    consolidated_data['Discount Flag'] = np.where((consolidated_data['PromotionsOnItems'] != 0), 'Yes', 'No')
    consolidated_data['Promo Flag'] = np.where((consolidated_data['ProductNames'].str.contains('2 x 1 Birdie Birdie Sandwich Wolt Promotion')) | (consolidated_data['ProductNames'].str.contains('2 x 1 Classic Tender Combo Wolt Promotion')), 'Yes', 'No')
    tolerance = 1e-6  # Set the tolerance value
    consolidated_data['Promo Flag'] = np.where((consolidated_data['Channel'] == 'Uber Eats') & (consolidated_data['ProductNames'].str.contains('Tender Combo')) & (consolidated_data['PromotionsOnItems'] != 0) & (np.isclose(consolidated_data['PromotionsOnItems'] % 9.80, 0, atol=tolerance)), 'Yes', consolidated_data['Promo Flag'])
    consolidated_data['Discount Flag'] = np.where((consolidated_data['Channel'] == 'Uber Eats') & (consolidated_data['ProductNames'].str.contains('Tender Combo')) & (consolidated_data['PromotionsOnItems'] != 0) & (np.isclose(consolidated_data['PromotionsOnItems'] % 9.80, 0, atol=tolerance)), 'No', consolidated_data['Discount Flag'])

    # Adjust UberEats AOV for BOGOF
    consolidated_data['GrossAOV Adj'] = np.where((consolidated_data['Channel'] == 'Uber Eats') & (consolidated_data['Promo Flag'] == 'Yes'), consolidated_data['GrossAOV'] + consolidated_data['PromotionsOnItems'], consolidated_data['GrossAOV'])

    # Re Order Columns
    consolidated_data = consolidated_data[['PrimaryKey', 'ExistsInDeliverect', 'Location', 'Loc with Brand', 'Brands', 'OrderID', 'OrderDate', 'OrderTime', 'OrderWeek', 'OrderMonth', 'Period', 'Channel', 'ChannelCopy', 'City', 'OrderStatus', 'CleanedOrderStatus', 'DeliveryStatus', 'DeliveryType', 'Discount Flag', 'Promo Flag', 'GrossAOV', 'GrossAOV Adj', 'StackingType', 'WorkflowUUID', 'OrderUUID', 'IsScheduled', 'IsSubscription', 'OrderStartTime', 'TimeToAccept', 'TimeToCourierArrivedAtRx', 'TimeCourierAtRx', 'TimeToDeliver', 'TimeCourierAtCx', 'OrderDuration', 'PromotionsOnItems', 'PriceAdjustments', 'DeliveryFee', 'PromotionsOnDelivery', 'Tips', 'MarketplaceFee', 'TotalPayout', 'PayoutDate', 'OrderIssue', 'TotalRefund', 'RefundCoveredByMerchant', 'RefundNotCoveredByMerchant', 'RatingScore', 'ProductPLUs', 'ProductNames', 'IsTestOrder', 'PaymentType', 'PickupTime', 'MasterOrderIndex', 'PDFIndexHalfMonth', 'PDFIndexFullMonth']]

    # Save CSV
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')
    consolidated_data.to_csv('Consolidated Data.csv', index=False)

process_consolidated_data()