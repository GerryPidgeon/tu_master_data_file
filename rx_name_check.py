import os
import pandas as pd
import numpy as np

def process_rx_name_data():
    # Set the directory path
    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\01 - Deliverect\CSV Files\Orders'

    # Create an empty list to store the data frames
    data_frames = []

    # Iterate over all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            # Read the CSV file into a data frame
            df = pd.read_csv(file_path)
            # Append the data frame to the list
            data_frames.append(df)

    # Merge all data frames into a single data frame
    deliverect_df = pd.concat(data_frames)

    # Extract the 'Location' column and drop duplicates
    deliverect_locations = deliverect_df['Location'].drop_duplicates()
    deliverect_locations = pd.DataFrame({'Location': deliverect_locations, 'Source': 'Deliverect'})

    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files'
    data_frames = []

    for filename in os.listdir(directory_path):
        if "Orders" in filename:
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            data_frames.append(df)

    ue_orders_df = pd.concat(data_frames)
    ue_orders_df = ue_orders_df.rename(columns={'Restaurant': 'Location'})

    # Extract the 'Location' column and drop duplicates
    ue_orders_locations = ue_orders_df['Location'].drop_duplicates()
    ue_orders_locations = pd.DataFrame({'Location': ue_orders_locations, 'Source': 'Uber Eats'})

    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files'
    data_frames = []

    for filename in os.listdir(directory_path):
        if "Payment" in filename:
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            new_columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            df.columns = new_columns
            data_frames.append(df)

    ue_payment_details_df = pd.concat(data_frames)
    ue_payment_details_df = ue_payment_details_df.rename(columns={'Shop name': 'Location'})

    # Extract the 'Location' column and drop duplicates
    ue_payment_details_locations = ue_payment_details_df['Location'].drop_duplicates()
    ue_payment_details_locations = pd.DataFrame({'Location': ue_payment_details_locations, 'Source': 'Uber Eats'})

    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files'
    data_frames = []

    for filename in os.listdir(directory_path):
        if "Inaccurate" in filename:
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            data_frames.append(df)

    ue_refunds_df = pd.concat(data_frames)
    ue_refunds_df = ue_refunds_df.rename(columns={'Restaurant': 'Location'})

    # Extract the 'Location' column and drop duplicates
    ue_refunds_locations = ue_refunds_df['Location'].drop_duplicates()
    ue_refunds_locations = pd.DataFrame({'Location': ue_refunds_locations, 'Source': 'Uber Eats'})

    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\02 - UberEats\CSV Files'
    data_frames = []

    for filename in os.listdir(directory_path):
        if "Reviews" in filename:
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            data_frames.append(df)

    ue_reviews_df = pd.concat(data_frames)
    ue_reviews_df = ue_reviews_df.rename(columns={'Restaurant': 'Location'})

    # Extract the 'Location' column and drop duplicates
    ue_reviews_locations = ue_reviews_df['Location'].drop_duplicates()
    ue_reviews_locations = pd.DataFrame({'Location': ue_reviews_locations, 'Source': 'Uber Eats'})

    # Set the directory path
    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\03 - Wolt\CSV Files'

    # Create an empty list to store the data frames
    data_frames = []

    # Iterate over all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            # Read the CSV file into a data frame
            df = pd.read_csv(file_path)
            # Append the data frame to the list
            data_frames.append(df)

    # Merge all data frames into a single data frame
    wolt_df = pd.concat(data_frames)
    wolt_df = wolt_df.rename(columns={'Venue': 'Location'})

    # Extract the 'Location' column and drop duplicates
    wolt_locations = wolt_df['Location'].drop_duplicates()
    wolt_locations = pd.DataFrame({'Location': wolt_locations, 'Source': 'Wolt'})

    # Set the directory path
    directory_path = r'H:\Shared drives\99 - Data\01 - Source Data\04 - Lieferando\CSV Files'

    # Create an empty list to store the data frames
    data_frames = []

    # Iterate over all files in the directory
    for filename in os.listdir(directory_path):
        # Check if file is a CSV file
        if filename.endswith('.csv'):
            # Load CSV file into a data frame with ';' separator
            filepath = os.path.join(directory_path, filename)
            df = pd.read_csv(filepath, sep=';')
            # Add column to data frame with file name
            df['file'] = filename
            df['file'] = df['file'].apply(lambda x: x.split('-')[-1].strip())
            df['file'] = df['file'].replace('.csv', '', regex=True)
            # Append data frame to list
            data_frames.append(df)

    # Merge all data frames into a single data frame
    lieferando_df = pd.concat(data_frames)
    lieferando_df = lieferando_df.rename(columns={'file': 'Location'})

    # Extract the 'Location' column and drop duplicates
    lieferando_locations = lieferando_df['Location'].drop_duplicates()
    lieferando_locations = pd.DataFrame({'Location': lieferando_locations, 'Source': 'Lieferando'})

    # Merge Lists Together
    all_locations = pd.concat([deliverect_locations, ue_orders_locations, ue_payment_details_locations, ue_reviews_locations, ue_refunds_locations, wolt_locations, lieferando_locations])
    all_locations = all_locations.drop_duplicates()
    all_locations = all_locations.sort_values(by='Location')

    # Remove Special Characters
    all_locations['Location'] = all_locations['Location'].str.replace('ö', 'o').str.replace('ü', 'u')

    # Merge Lists Together alt view
    all_locations_alt = all_locations
    all_locations_alt = all_locations_alt.drop(columns=['Source'])
    all_locations_alt = all_locations_alt.drop_duplicates()
    all_locations_alt = all_locations_alt.sort_values(by='Location')

    # Merge Lists Together alt view
    all_locations_alt['Deliverect'] = np.where(all_locations_alt['Location'].isin(deliverect_locations['Location']), 'Yes', 'No')
    all_locations_alt['UE Orders'] = np.where(all_locations_alt['Location'].isin(ue_orders_locations['Location']), 'Yes', 'No')
    all_locations_alt['UE Payment Details'] = np.where(all_locations_alt['Location'].isin(ue_payment_details_locations['Location']), 'Yes', 'No')
    all_locations_alt['UE Refunds'] = np.where(all_locations_alt['Location'].isin(ue_refunds_locations['Location']), 'Yes', 'No')
    all_locations_alt['UE Reviews'] = np.where(all_locations_alt['Location'].isin(ue_reviews_locations['Location']), 'Yes', 'No')
    all_locations_alt['Wolt'] = np.where(all_locations_alt['Location'].isin(wolt_locations['Location']), 'Yes', 'No')
    all_locations_alt['Lieferando'] = np.where(all_locations_alt['Location'].isin(lieferando_locations['Location']), 'Yes', 'No')

    # Export to CSV
    os.chdir(r'H:\Shared drives\99 - Data\03 - Rx Name List')
    all_locations_alt.to_csv('Full Rx List.csv', index=False)

    # Import Cleaned Names
    cleaned_names = pd.read_csv(r'Full Rx List, with Cleaned Names.csv')
    all_locations_alt = pd.merge(all_locations_alt, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')