import pandas as pd
import numpy as np
import json
from datetime import datetime


def clean_data(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        
        
        missing_values = df.isnull().sum()
        
        optional_text_cols = ['description', 'neighborhood_overview', 'host_about', 
                            'host_response_time', 'host_neighbourhood']
        for col in optional_text_cols:
            df[col].fillna('', inplace=True)
        
        numeric_cols = ['bathrooms', 'bedrooms', 'beds', 'price', 
                        'review_scores_rating', 'review_scores_accuracy']
        for col in numeric_cols:
            df[col].fillna(0, inplace=True)
        

        date_cols = ['last_scraped', 'host_since', 'first_review', 'last_review',
                    'calendar_last_scraped', 'calendar_updated']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        bool_cols = ['host_is_superhost', 'host_has_profile_pic', 
                    'host_identity_verified', 'instant_bookable', 'has_availability']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].map({'t': True, 'f': False})
        
        if 'price' in df.columns:
            df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)
        
        if 'amenities' in df.columns:
            df['amenities'] = df['amenities'].apply(lambda x: json.loads(x.replace('"{', '{').replace('}"', '}')))
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = (df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))
            df = df[valid_coords].copy()
        
        if 'host_response_rate' in df.columns:
            df['host_response_rate'] = df['host_response_rate'].str.replace('%', '').astype(float) / 100
        
        if 'neighbourhood_cleansed' in df.columns:
            df['neighborhood_cleansed'] = df['neighbourhood_cleansed']
        if 'neighbourhood_group_cleansed' in df.columns:
            df['neighborhood_group'] = df['neighbourhood_group_cleansed']
        
        df.to_csv(output_file, index=False)
        
        return df
    
    except Exception as e:
        print(f"Error during data cleaning: {str(e)}")
        raise

if __name__ == "__main__":
    clean_data('listings_Paris.csv', 'listings_Paris_clean.csv')