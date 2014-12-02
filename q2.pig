-- Load the state, county, mcd, place and zip
RUN  /vol/automed/data/uscensus1990/load_tables.pig

-- Join state and code 
state_county =
    JOIN state BY code,
         county BY state_code;

-- Group by state name
state_county_grouped =
    GROUP state_county
    BY state::name;

-- Project state name, sum of population of the county and sum of land area of the county
state_county_info = 
    FOREACH state_county_grouped
    GENERATE group AS state_name,
             SUM(state_county.county::population) AS population,
             SUM(state_county.county::land_area) AS land_area;
    
-- Sort by state_name    
state_county_info_ordered = 
    ORDER state_county_info
    BY state_name;

STORE state_county_info_ordered INTO 'q2' USING PigStorage(',');
