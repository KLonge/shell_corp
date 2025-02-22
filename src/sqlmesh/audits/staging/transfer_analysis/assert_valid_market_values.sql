audit (
    name assert_valid_market_values
);

-- Check for invalid market values
SELECT * FROM @this_model 
WHERE market_value_euro < 1000000 
   OR market_value_euro > 100000000; 