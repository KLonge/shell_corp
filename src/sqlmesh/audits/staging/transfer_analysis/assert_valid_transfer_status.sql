audit (
    name assert_valid_transfer_status
);

-- Check for invalid transfer status values
SELECT * FROM @this_model 
WHERE transfer_status NOT IN ('available', 'unavailable'); 