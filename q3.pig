-- Load the state, county, mcd, place and zip
RUN  /vol/automed/data/uscensus1990/load_tables.pig

-- Join state and place
state_place =
    JOIN state BY code LEFT,
         place BY state_code;

-- Group by state name
state_place_grouped =
    GROUP state_place
    BY state::name;

-- Project the columns necessary for the query
state_place_info = 
    FOREACH state_place_grouped {
        no_town =
            FILTER state_place
            BY type == 'town';
        no_city = 
            FILTER state_place
            BY type == 'city';
        no_village =
            FILTER state_place
            BY type == 'village';
        GENERATE group AS state_name,
             COUNT(no_city) AS no_city,
             COUNT(no_town) AS no_town,
			 COUNT(no_village) AS no_village;
	}
    
-- Sort by state name
state_place_info_ordered = 
    ORDER state_place_info
    BY state_name;

STORE state_place_info_ordered INTO 'q3' USING PigStorage(',');
