from src.clean import clean_data

def get_top_handsets(df, n=10):
    """
    Get the top N handsets used by customers.
    
    Parameters:
        df (pandas.DataFrame): Dataframe containing handset data.
        n (int): Number of top handsets to return.
    
    Returns:
        pandas.Series: Top N handsets with their counts.
    """
    df_cleaned = clean_data(df)  # Clean the data first
    return df_cleaned['Handset Type'].value_counts().head(n)

def get_top_manufacturers(df, n=3):
    """
    Get the top N handset manufacturers.
    
    Parameters:
        df (pandas.DataFrame): Dataframe containing manufacturer data.
        n (int): Number of top manufacturers to return.
    
    Returns:
        pandas.Series: Top N manufacturers with their counts.
    """
    # Clean the data first
    df_cleaned = clean_data(df)  
    return df_cleaned['Handset Manufacturer'].value_counts().head(n)

def get_top_handsets_by_top_manufacturers(df):
    """
    Identify the top 5 handsets for each of the top 3 handset manufacturers
    

    Parameters:
        df (pandas.DataFrame): The dataset containing handset and manufacturer information.
    
    Returns:
        dict: A dictionary with manufacturers as keys and the top 5 handsets as values,
              represented by a pandas Series.

    """
    df_cleaned = clean_data(df)  # Clean the data first
    top_manufacturers = df_cleaned['Handset Manufacturer'].value_counts().nlargest(3).index
    top_handsets_by_manufacturer = {}
    for manufacturer in top_manufacturers:
        filtered_df = df_cleaned[df_cleaned['Handset Manufacturer'] == manufacturer]
        top_handsets = filtered_df['Handset Type'].value_counts().nlargest(5)
        top_handsets_by_manufacturer[manufacturer] = top_handsets
    return top_handsets_by_manufacturer



import pandas as pd

def calculate_total_usage(df):
    """
    Calculate the total data usage for each application category by summing the
    download and upload bytes.

    Parameters:
        df (pandas.DataFrame): DataFrame containing the xDR data.

    Returns:
        pandas.DataFrame: DataFrame with each application's total data usage added as new columns.
    """
    apps = ['Social Media', 'Google', 'Email', 'Youtube', 'Netflix', 'Gaming', 'Other']
    for app in apps:
        dl_col = f'{app} DL (Bytes)'
        ul_col = f'{app} UL (Bytes)'
        total_col = f'Total {app} (Bytes)'
        df[dl_col] = pd.to_numeric(df[dl_col], errors='coerce').fillna(0)
        df[ul_col] = pd.to_numeric(df[ul_col], errors='coerce').fillna(0)
        df[total_col] = df[dl_col] + df[ul_col]

    return df

def aggregate_user_data(df):
    """
    Aggregate user data to analyze behavior on telecommunications applications.
    Calculates totals first and then aggregates this data per user.

    Parameters:
        df (pandas.DataFrame): DataFrame that has been preprocessed to include total usage columns.

    Returns:
        pandas.DataFrame: Aggregated DataFrame by user with counts and sums of usage and sessions.
    """
    # Calculate totals first
    df = calculate_total_usage(df)
    
    # Define the columns to aggregate
    aggregation_dict = {
        'Bearer Id': ('number_of_xdr_sessions', 'count'),
        'Dur. (ms)': ('total_duration', 'sum'),
        'Total DL (Bytes)': ('total_download', 'sum'),
        'Total UL (Bytes)': ('total_upload', 'sum')
    }
    
    apps = ['Social Media', 'Google', 'Email', 'Youtube', 'Netflix', 'Gaming', 'Other']
    for app in apps:
        aggregation_dict[f'Total {app} (Bytes)'] = (f'total_{app.lower()}', 'sum')

    # Aggregate the data per user
    aggregated_df = df.groupby('MSISDN/Number').agg(**{v[0]: (k, v[1]) for k, v in aggregation_dict.items()})
    
    return aggregated_df

# Example of usage in a script where 'df' is your loaded and possibly initially cleaned DataFrame
# df = load_data(query)  # Your function to load data
# aggregated_user_df = aggregate_user_data(df)
# print(aggregated_user_df.head())



def calculate_average_usage(df):
    """
    Calculate the average data usage for each application.
    
    Parameters:
        df (pandas.DataFrame): Dataframe with total usage data.
    
    Returns:
        pandas.Series: Series with average usage per application.
    """
    application_columns = ['Total Social Media (Bytes)', 'Total Google (Bytes)', 'Total Email (Bytes)',
                           'Total Youtube (Bytes)', 'Total Netflix (Bytes)', 'Total Gaming (Bytes)', 'Total Other (Bytes)']
    averages = df[application_columns].mean()
    return averages

import pandas as pd

def aggregate_user_data(df):
    """Aggregate user data to analyze behavior on telecommunications applications."""
    # Calculate totals first
    df = calculate_total_usage(df)
    
    # Aggregate the data per user
    aggregated_df = df.groupby('MSISDN/Number').agg(
        number_of_xdr_sessions=pd.NamedAgg(column='Bearer Id', aggfunc='count'),
        total_duration=pd.NamedAgg(column='Dur. (ms)', aggfunc='sum'),
        total_download=pd.NamedAgg(column='Total DL (Bytes)', aggfunc='sum'),
        total_upload=pd.NamedAgg(column='Total UL (Bytes)', aggfunc='sum'),
        total_social_media=pd.NamedAgg(column='Total Social Media (Bytes)', aggfunc='sum'),
        total_google=pd.NamedAgg(column='Total Google (Bytes)', aggfunc='sum'),
        total_email=pd.NamedAgg(column='Total Email (Bytes)', aggfunc='sum'),
        total_youtube=pd.NamedAgg(column='Total Youtube (Bytes)', aggfunc='sum'),
        total_netflix=pd.NamedAgg(column='Total Netflix (Bytes)', aggfunc='sum'),
        total_gaming=pd.NamedAgg(column='Total Gaming (Bytes)', aggfunc='sum'),
        total_other=pd.NamedAgg(column='Total Other (Bytes)', aggfunc='sum')
    )
    return aggregated_df
