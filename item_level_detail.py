import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def process_item_level_data():
    # Change Directory
    os.chdir(r'H:\Shared drives\99 - Data\00 - Cleaned Data')

    # Load CSV
    deliverect_data = pd.read_csv('Consolidated Data.csv')

    # Filter for Deliverect Only Orders
    deliverect_data = deliverect_data.loc[deliverect_data['ExistsInDeliverect'] == 'Yes']

    # Filter for required columns
    deliverect_data = deliverect_data[['PrimaryKey', 'OrderDate', 'OrderTime', 'OrderWeek', 'OrderMonth', 'Period', 'Location', 'Brands', 'OrderID', 'Channel', 'CleanedOrderStatus', 'ProductPLUs', 'ProductNames']]

    # Split Name and PLU
    deliverect_data = deliverect_data.rename(columns={'ProductPLUs': 'ProductPLU', 'ProductNames': 'ProductName'})
    deliverect_data = deliverect_data[deliverect_data['ProductName'].notnull()]
    deliverect_data['ProductName'] = deliverect_data['ProductName'].apply(lambda x: x.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore'))
    deliverect_data['ProductName'] = deliverect_data['ProductName'].str.replace('Stck', 'Stack')
    deliverect_data['ProductName'] = deliverect_data['ProductName'].str.replace('Kse', 'Cheese')

    # Split both ProductPLU and ProductName columns on commas
    deliverect_data['ProductPLU'] = deliverect_data['ProductPLU'].str.split(', ')
    deliverect_data['ProductName'] = deliverect_data['ProductName'].str.split(', ')

    # Combine ProductPLU and ProductName into a tuple in a new column
    deliverect_data['ProductPLU_Names'] = deliverect_data.apply(lambda x: list(zip(x['ProductPLU'], x['ProductName'])), axis=1)

    # Explode the tuples column
    deliverect_data = deliverect_data.explode('ProductPLU_Names')

    # Split the tuple back into separate columns
    deliverect_data[['ProductPLU', 'ProductName']] = pd.DataFrame(deliverect_data['ProductPLU_Names'].tolist(), index=deliverect_data.index)

    # Drop the tuples column
    deliverect_data = deliverect_data.drop(columns='ProductPLU_Names')

    # Add the quantity
    deliverect_data[['Quantity', 'Product_Name']] = deliverect_data['ProductName'].str.split(' ', n=1, expand=True)
    deliverect_data[['PLU_Name', 'Quantity']] = deliverect_data['ProductPLU'].str.split(': ', n=1, expand=True)
    deliverect_data = deliverect_data.drop(columns=['ProductPLU', 'ProductName'])
    deliverect_data = deliverect_data.rename(columns={'PLU_Name': 'ProductPLU', 'Product_Name': 'ProductName'})
    deliverect_data = deliverect_data[['PrimaryKey', 'OrderDate', 'OrderTime', 'OrderWeek', 'OrderMonth', 'Period', 'Location', 'Brands', 'OrderID', 'Channel', 'CleanedOrderStatus', 'ProductPLU', 'ProductName', 'Quantity']]

    # Classify Product or Modifier
    deliverect_data['Item Class'] = np.where(deliverect_data['ProductPLU'].str.startswith('P'), 'Product', 'Modifier')

    # Classify Product or Modifier
    deliverect_data['Item Class'] = np.where(deliverect_data['ProductPLU'].str.startswith('P'), 'Product', 'Modifier')

    # Create Check Columns
    deliverect_data['Offset OrderID'] = deliverect_data['OrderID'].shift(-1)
    deliverect_data['Offset Item Class'] = deliverect_data['Item Class'].shift(-1)
    deliverect_data['Offset ProductName'] = deliverect_data['ProductName'].shift(-1)
    deliverect_data['Offset ProductPLU'] = deliverect_data['ProductPLU'].shift(-1)

    # Order ID Check
    deliverect_data['Order Check'] = np.where(deliverect_data['OrderID'] == deliverect_data['Offset OrderID'], 'No', 'Yes')

    # Clear Last Entry for Each Order
    deliverect_data['Offset Item Class'] = np.where(deliverect_data['Order Check'] == 'Yes', '', deliverect_data['Offset Item Class'])
    deliverect_data['Offset ProductName'] = np.where(deliverect_data['Order Check'] == 'Yes', '', deliverect_data['Offset ProductName'])
    deliverect_data['Offset ProductPLU'] = np.where(deliverect_data['Order Check'] == 'Yes', '', deliverect_data['Offset ProductPLU'])

    # Populate Last Row
    deliverect_data.iloc[-1, deliverect_data.columns.get_loc('Offset OrderID')] = 'Last Row'
    deliverect_data.iloc[-1, deliverect_data.columns.get_loc('Offset Item Class')] = 'Last Row'
    deliverect_data.iloc[-1, deliverect_data.columns.get_loc('Offset ProductName')] = 'Last Row'
    deliverect_data.iloc[-1, deliverect_data.columns.get_loc('Offset ProductPLU')] = 'Last Row'

    # Create Cleaned Names column
    deliverect_data['CleanedName'] = deliverect_data['ProductName']

    # Clean up Lieferando
    deliverect_data['Channel'] = np.where(deliverect_data['Channel'] == 'TakeAway Com', 'Lieferando', deliverect_data['Channel'])

    # Clean up Vegan Name
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == 'planted.chicken. Plant-based Tenders', 'Vegan Tenders', deliverect_data['CleanedName'])
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == 'planted.chicken. Vegane Tenders', 'Vegan Tenders', deliverect_data['CleanedName'])
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == 'planted.chicken Combo', 'Vegan Tenders Combo', deliverect_data['CleanedName'])

    # Clean up Regular and Large
    deliverect_data['CleanedName'] = np.where(deliverect_data['Offset ProductName'] == 'Regular', deliverect_data['CleanedName'] + ' [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['ProductName'] = np.where(deliverect_data['ProductName'] == 'Regular', np.nan, deliverect_data['ProductName']) # Filter

    deliverect_data['CleanedName'] = np.where(deliverect_data['Offset ProductName'] == 'Large', deliverect_data['CleanedName'] + ' [Large]', deliverect_data['CleanedName']) # Clean
    deliverect_data['ProductName'] = np.where(deliverect_data['ProductName'] == 'Large', np.nan, deliverect_data['ProductName']) # Filter

    # Clean up Lieferando
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Classic Tenders'), 'Classic Tenders [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Classic Tenders [4 Stack]'), 'Classic Tenders [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Classic Tenders [6 Stack]'), 'Classic Tenders [Large]', deliverect_data['CleanedName']) # Clean
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Classic Wings [7 Stack]'), 'Classic Wings [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Classic Wings [10 Stack]'), 'Classic Wings [Large]', deliverect_data['CleanedName']) # Clean
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Classic Wings'), 'Classic Wings [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] == 'Lieferando') & (deliverect_data['CleanedName'] == 'Vegan Tenders'), 'Vegan Tenders [Regular]', deliverect_data['CleanedName']) # Clean

    # Clean up Other Tenders
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['Offset ProductPLU'] == 'M-4S-Z35I-17') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), 'Classic Tenders [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['ProductName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['ProductPLU'] == 'M-4S-Z35I-17') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), np.nan, deliverect_data['ProductName']) # Filter

    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['Offset ProductPLU'] == 'M-6S-rWEN-17') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), 'Classic Tenders [Large]', deliverect_data['CleanedName']) # Clean
    deliverect_data['ProductName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['ProductPLU'] == 'M-6S-rWEN-17') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), np.nan, deliverect_data['ProductName']) # Filter

    # Clean up Other Wings
    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['Offset ProductPLU'] == 'M-6S-rJJT-4') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), 'Classic Wings [Regular]', deliverect_data['CleanedName']) # Clean
    deliverect_data['ProductName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['ProductPLU'] == 'M-6S-rJJT-4') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), np.nan, deliverect_data['ProductName']) # Filter

    deliverect_data['CleanedName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['Offset ProductPLU'] == 'M-9S-kHgW-4') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), 'Classic Wings [Large]', deliverect_data['CleanedName']) # Clean
    deliverect_data['ProductName'] = np.where((deliverect_data['Channel'] != 'Lieferando') & (deliverect_data['ProductPLU'] == 'M-9S-kHgW-4') & (deliverect_data['CleanedName'] == deliverect_data['ProductName']), np.nan, deliverect_data['ProductName']) # Filter

    # Clean up Sauces
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == 'mit Birdiesauce', 'Birdie Sauce', deliverect_data['CleanedName'])
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == 'Chipotle-Jalapeo Mayo', 'Chipotle Mayo', deliverect_data['CleanedName'])
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == 'Mayo', 'Original Heinz Mayo', deliverect_data['CleanedName'])

    # Clean up Promo Offers
    deliverect_data['CleanedName'] = np.where(deliverect_data['CleanedName'] == ' - 2 x 1 Classic Tender Combo Wolt Promotion', '2 x 1 Classic Tender Combo Wolt Promotion', deliverect_data['CleanedName'])

    # Filter Blank ProductName
    deliverect_data = deliverect_data.loc[deliverect_data['ProductName'].notnull()]
    deliverect_data = deliverect_data.loc[deliverect_data['ProductName'] != 'ohne Sauce']

    # Dropped unneeded columns
    deliverect_data['ProductName'] = deliverect_data['CleanedName']
    deliverect_data = deliverect_data.drop(columns={'Offset Item Class', 'Offset ProductName', 'Offset ProductPLU', 'CleanedName'})

    # Change Directory
    os.chdir(r'H:\Shared drives\99 - Data\20 - Orders as an Item Level\Item Level Detail')

    # Populate Birria Item Level Data
    birria_order_detail = pd.read_csv('Prices and Costs by Item - Birria.csv')
    deliverect_data = pd.merge(deliverect_data, birria_order_detail[['ProductName', 'CleanedName']], on='ProductName', how='left')
    deliverect_data['ProductName'] = np.where(deliverect_data['Brands'] == 'Birria & the Beast', deliverect_data['CleanedName'], deliverect_data['ProductName'])

    # Re-Order Output
    deliverect_data = deliverect_data[['OrderDate', 'OrderTime', 'OrderWeek', 'OrderMonth', 'Location', 'Brands', 'OrderID', 'Channel', 'CleanedOrderStatus', 'ProductPLU', 'ProductName', 'Quantity']]

    # Reset Index
    deliverect_data = deliverect_data.reset_index()

    # Sort on date and index
    deliverect_data['Index'] = deliverect_data.index
    deliverect_data = deliverect_data.sort_values(by=['OrderDate', 'OrderTime', 'Index'])
    deliverect_data = deliverect_data.drop(columns={'index', 'Index'})

    # Save as a CSV
    deliverect_data.to_csv('Combined Sales Output.csv', index=False)

def process_item_level_analysis():
    # Change Directory
    os.chdir(r'H:\Shared drives\99 - Data\20 - Orders as an Item Level\Item Level Detail')

    # Load CSV's
    deliverect_data = pd.read_csv('Combined Sales Output.csv')
    birdie_order_detail = pd.read_csv('Prices and Costs by Item - Birdie.csv')
    birria_order_detail = pd.read_csv('Prices and Costs by Item - Birria.csv')

    # Convert the 'OrderDate' column to Timestamp type
    deliverect_data['OrderDate'] = pd.to_datetime(deliverect_data['OrderDate'])

    # Remove Duplicates
    birdie_order_detail = birdie_order_detail.drop_duplicates(keep='first').reset_index(drop=True)

    # Split Brands
    deliverect_data_birdie_short = deliverect_data.loc[deliverect_data['Brands'] == 'Birdie Birdie']
    deliverect_data_birdie_long = deliverect_data.loc[deliverect_data['Brands'] == 'Birdie Birdie']
    deliverect_data_birria_short = deliverect_data.loc[deliverect_data['Brands'] == 'Birria & the Beast']
    deliverect_data_birria_long = deliverect_data.loc[deliverect_data['Brands'] == 'Birria & the Beast']

    # Filter for the past 36 days
    current_date = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    past_date = current_date - timedelta(days=36)

    # Filter the DataFrame to show only the past 36 days
    deliverect_data_birdie_short = deliverect_data_birdie_short[deliverect_data_birdie_short['OrderDate'] >= past_date]
    deliverect_data_birria_short = deliverect_data_birria_short[deliverect_data_birria_short['OrderDate'] >= past_date]

    # Filter the DataFrame to show only the past 36 days
    deliverect_data_birdie_short = pd.merge(deliverect_data_birdie_short, birdie_order_detail[
        ['ProductName', 'Index', 'Category', 'Retail Price', 'VAT Rate', 'Food Costs', 'Packaging Costs', 'Waste Costs',
         'Production Costs', 'Total Costs']], on=['ProductName'], how='left')
    deliverect_data_birria_short = pd.merge(deliverect_data_birria_short, birria_order_detail[
        ['ProductPLU', 'Index', 'Category', 'Retail Price', 'VAT Rate', 'Food Costs', 'Packaging Costs', 'Waste Costs',
         'Production Costs', 'Total Costs']], on=['ProductPLU'], how='left')

    # Create a pivot table with OrderDate as columns and sum of Quantity as values - Birdie
    birdie_pivot_table = deliverect_data_birdie_short.pivot_table(index=['Index', 'ProductName', 'Category'],
                                                                  columns='OrderDate', values='Quantity',
                                                                  aggfunc=np.sum)
    birdie_pivot_table = birdie_pivot_table.sort_values(by=['Index'])

    # Create a pivot table with OrderDate as columns and sum of Quantity as values - Birria
    birria_pivot_table = deliverect_data_birria_short.pivot_table(index=['Index', 'ProductName', 'Category'],
                                                                  columns='OrderDate', values='Quantity',
                                                                  aggfunc=np.sum)
    birria_pivot_table = birria_pivot_table.sort_values(by=['Index'])
    birria_pivot_table = birria_pivot_table.groupby(level=['Index', 'ProductName', 'Category']).sum()

    # Export CSV
    birdie_pivot_table.to_csv('Last 36 Day Results - Birdie.csv')
    birria_pivot_table.to_csv('Last 36 Day Results - Birria.csv')

    # Filter for the past 62 days
    current_date = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    past_date = current_date - timedelta(days=62)

    # Filter the DataFrame to show only the past 62 days
    deliverect_data_birdie_long = deliverect_data_birdie_long[deliverect_data_birdie_long['OrderDate'] >= past_date]
    deliverect_data_birria_long = deliverect_data_birria_long[deliverect_data_birria_long['OrderDate'] >= past_date]

    # Filter the DataFrame to show only the past 62 days
    deliverect_data_birdie_long = pd.merge(deliverect_data_birdie_long, birdie_order_detail[
        ['ProductName', 'Index', 'Category', 'Retail Price', 'VAT Rate', 'Food Costs', 'Packaging Costs', 'Waste Costs',
         'Production Costs', 'Total Costs']], on=['ProductName'], how='left')
    deliverect_data_birria_long = pd.merge(deliverect_data_birria_long, birria_order_detail[
        ['ProductName', 'Index', 'Category', 'Retail Price', 'VAT Rate', 'Food Costs', 'Packaging Costs', 'Waste Costs',
         'Production Costs', 'Total Costs']], on=['ProductName'], how='left')

    # Bring Quantity Into Account
    deliverect_data_birdie_long['Retail Price'] = deliverect_data_birdie_long['Retail Price'] * \
                                                  deliverect_data_birdie_long['Quantity']
    deliverect_data_birdie_long['Food Costs'] = deliverect_data_birdie_long['Food Costs'] * deliverect_data_birdie_long[
        'Quantity']
    deliverect_data_birdie_long['Packaging Costs'] = deliverect_data_birdie_long['Packaging Costs'] * \
                                                     deliverect_data_birdie_long['Quantity']
    deliverect_data_birdie_long['Waste Costs'] = deliverect_data_birdie_long['Waste Costs'] * \
                                                 deliverect_data_birdie_long['Quantity']
    deliverect_data_birdie_long['Production Costs'] = deliverect_data_birdie_long['Production Costs'] * \
                                                      deliverect_data_birdie_long['Quantity']
    deliverect_data_birria_long['Retail Price'] = deliverect_data_birria_long['Retail Price'] * \
                                                  deliverect_data_birria_long['Quantity']
    deliverect_data_birria_long['Food Costs'] = deliverect_data_birria_long['Food Costs'] * deliverect_data_birria_long[
        'Quantity']
    deliverect_data_birria_long['Packaging Costs'] = deliverect_data_birria_long['Packaging Costs'] * \
                                                     deliverect_data_birria_long['Quantity']
    deliverect_data_birria_long['Waste Costs'] = deliverect_data_birria_long['Waste Costs'] * \
                                                 deliverect_data_birria_long['Quantity']
    deliverect_data_birria_long['Production Costs'] = deliverect_data_birria_long['Production Costs'] * \
                                                      deliverect_data_birria_long['Quantity']

    # Bring Quantity Into Account
    deliverect_data_birdie_long['Total Costs'] = deliverect_data_birdie_long['Food Costs'] + \
                                                 deliverect_data_birdie_long['Packaging Costs'] + \
                                                 deliverect_data_birdie_long['Waste Costs'] + \
                                                 deliverect_data_birdie_long['Production Costs']
    deliverect_data_birria_long['Total Costs'] = deliverect_data_birria_long['Food Costs'] + \
                                                 deliverect_data_birria_long['Packaging Costs'] + \
                                                 deliverect_data_birria_long['Waste Costs'] + \
                                                 deliverect_data_birria_long['Production Costs']

    # Clean Lieferando Chicken Tenders
    # deliverect_data_birdie_long['Retail Price'] = np.where((deliverect_data_birdie_long['Channel'] == 'Lieferando') & (
    #             deliverect_data_birdie_long['ProductName'] == 'Classic Tenders [Regular]'), 7.50,
    #                                                        deliverect_data_birdie_long['Retail Price'])

    # Export CSV
    deliverect_data_birdie_long.to_csv('Order Detail with Prices - Birdie.csv', index=False)
    deliverect_data_birria_long.to_csv('Order Detail with Prices - Birria.csv', index=False)

process_item_level_data()