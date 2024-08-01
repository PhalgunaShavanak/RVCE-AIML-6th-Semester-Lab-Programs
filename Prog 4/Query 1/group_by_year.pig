-- Load the dataset
gas = LOAD 'dataset124.csv' USING PigStorage(',') AS (
    CityName:chararray, 
    Location:chararray, 
    Year:int, 
    Temperature:double, 
    State:chararray, 
    GasPresent:chararray, 
    QuantityOfGas:double
);

-- Group the data by Year
grouped_data = GROUP gas BY Year;

-- Dump the grouped data to see the results
DUMP grouped_data;
