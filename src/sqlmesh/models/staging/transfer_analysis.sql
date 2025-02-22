model (
    NAME staging.transfer_analysis,
    kind full,
    cron '@daily',
    dialect duckdb,
    audits (
        not_null(
            COLUMNS:= (
                player_name,
                current_club,
                POSITION,
                market_value_euro,
                contract_end_date,
                age,
                transfer_status
            )
        ),
        number_of_rows(
            threshold:= 1
        ),
        assert_transfer_analysis_quality
    )
);
SELECT
    player_name,
    current_club,
    POSITION,
    market_value_euro,
    DATE(contract_end_date) AS contract_end_date,
    age,
    transfer_status,
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
    END AS contract_status,
    CASE
        WHEN transfer_status = 'available' THEN TRUE
        ELSE FALSE
    END AS is_available
FROM
    raw.transfer_listings
