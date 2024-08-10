-- Load the dataset from HDFS
gas = LOAD 'dataset124.csv' USING PigStorage(',') AS (
    CityName:chararray, 
    Location:chararray, 
    Year:int, 
    Temperature:double, 
    State:chararray, 
    GasPresent:chararray, 
    QuantityOfGas:double
);

-- Filter the data for the last 3 years
last_3_years = FILTER gas BY Year >= 2015;

-- Group the data by State and Year
grouped_data = GROUP last_3_years BY (State, Year);

-- Calculate the average temperature for each state and year
average_temp = FOREACH grouped_data GENERATE 
    group.State AS State,
    group.Year AS Year,
    AVG(last_3_years.Temperature) AS avg_temperature;

-- Store the results in HDFS directly
STORE average_temp INTO '/avg_temp' USING PigStorage(',');
