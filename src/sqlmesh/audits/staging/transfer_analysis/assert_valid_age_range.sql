audit (
    name assert_valid_age_range
);

-- Check for invalid age ranges
SELECT * FROM @this_model 
WHERE age < 15 OR age > 100; 