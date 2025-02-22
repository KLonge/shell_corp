audit (
    name assert_valid_contract_dates
);

-- Check for invalid contract dates
SELECT * FROM @this_model 
WHERE DATE(contract_end_date) < DATE(CURRENT_DATE); 