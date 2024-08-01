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

-- Find the maximum temperature
max_temp = FOREACH (GROUP gas ALL) GENERATE MAX(gas.Temperature) AS max_temperature;

-- Dump the results to the console
DUMP max_temp;
