## Justification for choosing MongoDB:
* The dataset has a complex, nested structure, hierarchical relationships and some json data 
* Document stores scale well horizontally
* Flexible schema allows for evolving data requirements
*  Good performance for read-heavy workloads typical of listing platforms


##  NoSQL Schema Design
we will have 2 seperate collections Listings and hosts

### Listings
```js
    {
    _id: ObjectId, 
    listing_url: String,
    last_scraped: ISODate,
    source: String,
    name: String,
    description: String,
    neighborhood_overview: String,
    picture_url: String,
    property_type: String,
    room_type: String,
    accommodates: Integer,
    bathrooms: Number,
    bathrooms_text: String,
    bedrooms: Integer,
    beds: Integer,
    amenities: Array, 
    price: Number,
    minimum_nights: Integer,
    maximum_nights: Integer,
    availability: 
    {
        last_updated: ISODate,
        days_30: Integer,
        days_60: Integer,
        days_90: Integer,
        days_365: Integer
    },
    location: 
    {
        neighborhood: String,
        neighborhood_cleansed: String,
        neighborhood_group: String,
        coordinates: 
        { 
            type: "Point",
            coordinates: [longitude, latitude]
        }
    },
    reviews: 
    {
        total_count: Integer,
        last_12_months: Integer,
        last_30_days: Integer,
        first_review: ISODate,
        last_review: ISODate,
        scores: {
            rating: Number,
            accuracy: Number,
            cleanliness: Number,
            checkin: Number,
            communication: Number,
            location: Number,
            value: Number
        },
        per_month: Number
    },
    host_id: Integer, 
    instant_bookable: Boolean,
    license: String,
    scrape_info: 
    {
        scrape_id: Number,
        calendar_last_scraped: ISODate
    }
}
```
### hosts

```js
{
    _id: Integer, 
    host_url: String,
    name: String,
    since: ISODate,
    location: String,
    about: String,
    response_info: 
    {
        time: String,
        rate: Number,
        acceptance_rate: Number
    },
    is_superhost: Boolean,
    verifications: Array,
    profile_info: 
    {
        thumbnail_url: String,
        picture_url: String,
        has_profile_pic: Boolean,
        identity_verified: Boolean
    },
    neighborhood: String,
    listings_count: 
    {
        total: Integer,
        entire_homes: Integer,
        private_rooms: Integer,
        shared_rooms: Integer
    }
}
```

### Denormalization Decisions:
1. Embedded Availability Data: The availability metricsare embedded within the listing document for fast access.
2. Separate Hosts Collection: Hosts are separated since they have many listings, preventing duplication of host data.
4. Review Scores: Embedded in listings despite being related to reviews, since they're frequently accessed with listings
