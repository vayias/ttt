RUN /vol/automed/data/uscensus1990/load_tables.pig

-- Get the cities from place
place_cities = 
	FILTER place
	BY type =='city';

-- Project just the columns we will need later
state_bag = FOREACH state GENERATE code, name;

-- Project just the columns we will need later
place_cities_bag = FOREACH place_cities GENERATE state_code, name, population;


state_cities = JOIN state_bag BY code, place_cities_bag BY state_code;

state_cities_info = FOREACH state_cities GENERATE state_bag::name AS state_name,
place_cities_bag::name AS city, place_cities_bag::population AS population;


state_cities_info_grouped = GROUP state_cities_info BY state_name;

state_cities_top5 = FOREACH state_cities_info_grouped {
	city_population = FOREACH state_cities_info GENERATE city, population;
	city_population_ordered = ORDER city_population BY population DESC;
	top_five = LIMIT city_population_ordered 5;
	GENERATE group AS state_name, FLATTEN(top_five);
}

--Sort By state name
state_cities_top5_ordered = ORDER state_cities_top5 BY state_name;

STORE state_cities_top5_ordered INTO 'q4' USING PigStorage(',');

