import pandas as pd
import logging
import numpy as np
# Setup logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def replace_outliers_with_median(df, column):
    """Replace outliers in a dataframe column with the column's median."""
    try:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        median = df[column].median()
        condition = ~df[column].between(Q1 - 1.5 * IQR, Q3 + 1.5 * IQR)
        df.loc[condition, column] = median
        # logging.info(f"Outliers replaced in {column}")
    except KeyError as e:
        logging.error(f"{column} not found in DataFrame: {e}")
        raise

def clean_data(df):
    """
    Clean the DataFrame by handling missing values and normalizing entries.
    
    Parameters:
        df (pandas.DataFrame): Dataframe to be cleaned.
    
    Returns:
        pandas.DataFrame: Cleaned dataframe.
    """
    try:
        # Numerical columns: Replace missing values with the median of each column
        numerical_cols = ['Dur. (ms)', 'Avg RTT DL (ms)', 'Avg RTT UL (ms)',
                          'Avg Bearer TP DL (kbps)', 'Avg Bearer TP UL (kbps)',
                          'TCP DL Retrans. Vol (Bytes)', 'TCP UL Retrans. Vol (Bytes)',
                          'Activity Duration DL (ms)', 'Activity Duration UL (ms)',
                          'Dur. (ms).1', 'Total UL (Bytes)', 'Total DL (Bytes)']
        
        for col in numerical_cols:
            if col in df.columns:
                replace_outliers_with_median(df, col)
                median_value = df[col].median(skipna=True)
                df[col] = df[col].fillna(median_value)

        # Categorical columns: Replace 'undefined' or missing values with the mode for 'Handset Type'
        if 'Handset Type' in df.columns:
            df['Handset Type'] = df['Handset Type'].replace('undefined', pd.NA)
            # Compute mode for each manufacturer and fill in missing handset types
            for manufacturer in df['Handset Manufacturer'].unique():
                manufacturer_mode = df.loc[df['Handset Manufacturer'] == manufacturer, 'Handset Type'].mode(dropna=True)
                if not manufacturer_mode.empty:
                    # Replace NaN with the mode for the specific manufacturer
                    df.loc[(df['Handset Manufacturer'] == manufacturer) & (df['Handset Type'].isna()), 'Handset Type'] = manufacturer_mode[0]
        
        categorical_cols = ['Handset Type', 'Handset Manufacturer', 'Last Location Name']
        for col in categorical_cols:
            if col in df.columns:
                mode_value = df[col].mode()[0]
                df[col] = df[col].fillna(mode_value)
                df[col] = df[col].str.title()
        # Normalize string entries to ensure consistent capitalization
        string_cols = ['Handset Manufacturer', 'Handset Type', 'Last Location Name']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].str.title()

        return df

    except KeyError as e:
        logging.error(f"Column not found in DataFrame: {e}")
        raise  # Re-raise the exception after logging
    except Exception as e:
        logging.error(f"An error occurred during data cleaning: {e}")
        raise  # Re-raise the exception after logging
