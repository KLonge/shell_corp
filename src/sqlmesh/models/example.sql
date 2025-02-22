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
    CASE
        WHEN market_value_euro >= 20000000 THEN 'Premium'
        WHEN market_value_euro >= 10000000 THEN 'High Value'
        WHEN market_value_euro >= 5000000 THEN 'Mid Range'
        ELSE 'Development'
    END AS value_tier,
    CASE
        WHEN DATE(contract_end_date) <= CURRENT_DATE + INTERVAL '6 months' THEN 'Urgent'
        WHEN DATE(contract_end_date) <= CURRENT_DATE + INTERVAL '1 year' THEN 'Watch List'
        ELSE 'Long Term'
    END AS contract_status
FROM
    raw.transfer_listings
