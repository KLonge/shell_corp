model (
    NAME staging.transfer_analysis,
    kind full,
    cron '@daily',
    dialect duckdb
);
SELECT
    player_name,
    current_club,
    POSITION,
    market_value_euro,
    contract_end_date,
    age,
    transfer_status,
    CASE
        WHEN market_value_euro >= 20000000 THEN 'Premium'
        WHEN market_value_euro >= 10000000 THEN 'High Value'
        WHEN market_value_euro >= 5000000 THEN 'Mid Range'
        ELSE 'Development'
    END AS value_tier,
    CASE
        WHEN contract_end_date <= CURRENT_DATE + INTERVAL '6 months' THEN 'Urgent'
        WHEN contract_end_date <= CURRENT_DATE + INTERVAL '1 year' THEN 'Watch List'
        ELSE 'Long Term'
    END AS contract_status,--
ADD
    availability flag for filtering transfer_status = 'available' AS is_available
FROM
    raw.transfer_listings
