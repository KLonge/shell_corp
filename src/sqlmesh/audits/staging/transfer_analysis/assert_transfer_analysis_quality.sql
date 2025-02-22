audit (
    NAME assert_transfer_analysis_quality
);
-- RETURN rows that violate our quality rules
SELECT
    *
FROM
    @this_model
WHERE
    -- VALUE tier validation
    (
        market_value_euro < 1000000
        OR market_value_euro > 100000000
    ) -- age validation
    OR (
        age < 15
        OR age > 45
    ) -- contract DATE validation
    OR DATE(contract_end_date) < DATE(CURRENT_DATE) -- status validation
    OR transfer_status NOT IN (
        'available',
        'unavailable'
    );
