import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import json
import logging

logging.basicConfig(filename='data_migration.log', level=logging.INFO)

def migrate_to_mongodb(csv_file, db_name, collection_name, host='localhost', port=27017):
    try:
        # Connect to MongoDB
        client = MongoClient(host, port)
        db = client[db_name]
        collection = db[collection_name]
        hosts_collection = db['hosts']
        
        logging.info("Connected to MongoDB successfully")
        
        # Load cleaned CSV
        df = pd.read_csv(csv_file)
        logging.info(f"Loaded {len(df)} records from {csv_file}")
        
        # Prepare for batch insert
        listings_batch = []
        hosts_batch = []
        host_ids_processed = set()
        
        for _, row in df.iterrows():
            try:
                # Prepare listing document
                listing_doc = {
                    '_id': row['id'],
                    'listing_url': row.get('listing_url', ''),
                    'last_scraped': row.get('last_scraped'),
                    'source': row.get('source', ''),
                    'name': row.get('name', ''),
                    'description': row.get('description', ''),
                    'neighborhood_overview': row.get('neighborhood_overview', ''),
                    'picture_url': row.get('picture_url', ''),
                    'property_type': row.get('property_type', ''),
                    'room_type': row.get('room_type', ''),
                    'accommodates': row.get('accommodates', 0),
                    'bathrooms': row.get('bathrooms', 0),
                    'bathrooms_text': row.get('bathrooms_text', ''),
                    'bedrooms': row.get('bedrooms', 0),
                    'beds': row.get('beds', 0),
                    'amenities': json.loads(row.get('amenities', '[]')),
                    'price': float(row.get('price', 0)),
                    'minimum_nights': row.get('minimum_nights', 1),
                    'maximum_nights': row.get('maximum_nights', 1125),
                    'availability': {
                        'last_updated': row.get('calendar_last_scraped'),
                        'days_30': row.get('availability_30', 0),
                        'days_60': row.get('availability_60', 0),
                        'days_90': row.get('availability_90', 0),
                        'days_365': row.get('availability_365', 0)
                    },
                    'location': {
                        'neighborhood': row.get('neighbourhood', ''),
                        'neighborhood_cleansed': row.get('neighborhood_cleansed', ''),
                        'neighborhood_group': row.get('neighborhood_group', ''),
                        'coordinates': {
                            'type': 'Point',
                            'coordinates': [float(row.get('longitude', 0)), 
                                          float(row.get('latitude', 0))]
                        }
                    },
                    'reviews': {
                        'total_count': row.get('number_of_reviews', 0),
                        'last_12_months': row.get('number_of_reviews_ltm', 0),
                        'last_30_days': row.get('number_of_reviews_l30d', 0),
                        'first_review': row.get('first_review'),
                        'last_review': row.get('last_review'),
                        'scores': {
                            'rating': row.get('review_scores_rating', 0),
                            'accuracy': row.get('review_scores_accuracy', 0),
                            'cleanliness': row.get('review_scores_cleanliness', 0),
                            'checkin': row.get('review_scores_checkin', 0),
                            'communication': row.get('review_scores_communication', 0),
                            'location': row.get('review_scores_location', 0),
                            'value': row.get('review_scores_value', 0)
                        },
                        'per_month': row.get('reviews_per_month', 0)
                    },
                    'host_id': row.get('host_id'),
                    'instant_bookable': row.get('instant_bookable', False),
                    'license': row.get('license', ''),
                    'scrape_info': {
                        'scrape_id': row.get('scrape_id'),
                        'calendar_last_scraped': row.get('calendar_last_scraped')
                    }
                }
                
                listings_batch.append(listing_doc)
                
                # Prepare host document if not already processed
                host_id = row.get('host_id')
                if host_id and host_id not in host_ids_processed:
                    host_doc = {
                        '_id': host_id,
                        'host_url': row.get('host_url', ''),
                        'name': row.get('host_name', ''),
                        'since': row.get('host_since'),
                        'location': row.get('host_location', ''),
                        'about': row.get('host_about', ''),
                        'response_info': {
                            'time': row.get('host_response_time', ''),
                            'rate': row.get('host_response_rate', 0),
                            'acceptance_rate': row.get('host_acceptance_rate', 0)
                        },
                        'is_superhost': row.get('host_is_superhost', False),
                        'verifications': json.loads(row.get('host_verifications', '[]')),
                        'profile_info': {
                            'thumbnail_url': row.get('host_thumbnail_url', ''),
                            'picture_url': row.get('host_picture_url', ''),
                            'has_profile_pic': row.get('host_has_profile_pic', False),
                            'identity_verified': row.get('host_identity_verified', False)
                        },
                        'neighborhood': row.get('host_neighbourhood', ''),
                        'listings_count': {
                            'total': row.get('host_total_listings_count', 0),
                            'entire_homes': row.get('calculated_host_listings_count_entire_homes', 0),
                            'private_rooms': row.get('calculated_host_listings_count_private_rooms', 0),
                            'shared_rooms': row.get('calculated_host_listings_count_shared_rooms', 0)
                        }
                    }
                    hosts_batch.append(host_doc)
                    host_ids_processed.add(host_id)
                
                # Insert in batches of 1000
                if len(listings_batch) >= 1000:
                    collection.insert_many(listings_batch)
                    listings_batch = []
                    logging.info("Inserted 1000 listing documents")
                
                if len(hosts_batch) >= 1000:
                    hosts_collection.insert_many(hosts_batch)
                    hosts_batch = []
                    logging.info("Inserted 1000 host documents")
                    
            except Exception as e:
                logging.warning(f"Error processing row {_}: {str(e)}")
                continue
        
        # Insert remaining documents
        if listings_batch:
            collection.insert_many(listings_batch)
            logging.info(f"Inserted final batch of {len(listings_batch)} listing documents")
        
        if hosts_batch:
            hosts_collection.insert_many(hosts_batch)
            logging.info(f"Inserted final batch of {len(hosts_batch)} host documents")
        
        # Create indexes
        collection.create_index([('location.coordinates', '2dsphere')])
        collection.create_index([('price', 1)])
        collection.create_index([('host_id', 1)])
        collection.create_index([('room_type', 1)])
        hosts_collection.create_index([('is_superhost', 1)])
        
        logging.info("Created necessary indexes")
        
        client.close()
        logging.info("Migration completed successfully")
        
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    migrate_to_mongodb(
        csv_file='listings_Paris_clean.csv',
        db_name='paris_listing',
        collection_name='listings',
        host='mongodb',
        port=27017
    )