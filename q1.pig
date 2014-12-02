
-- Load the state, county, mcd, place and zip
RUN  /vol/automed/data/uscensus1990/load_tables.pig


-- Doing difference between state and code

state_and_county = 
    JOIN state BY code LEFT,
    county BY state_code;

state_without_county = 
    FILTER state_and_county
    BY county::state_code IS NULL;

state_name_without_county = 
    FOREACH state_without_county
    GENERATE state::name AS state_name;

-- Sort by state_name
state_name_without_county_ordered = 
	ORDER state_name_without_county
	BY state_name;

STORE state_name_without_county_ordered INTO 'q1' USING PigStorage(',');
