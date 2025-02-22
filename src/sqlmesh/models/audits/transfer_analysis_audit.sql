model (
    NAME audits.transfer_analysis_audit,
    kind full,
    cron '@daily',
    dialect duckdb
);
WITH value_tier_stats AS (
    SELECT
        value_tier,
        COUNT(*) AS player_count,
        AVG(market_value_euro) AS avg_value,
        MIN(market_value_euro) AS min_value,
        MAX(market_value_euro) AS max_value
    FROM
        staging.transfer_analysis
    GROUP BY
        value_tier
),
contract_status_stats AS (
    SELECT
        contract_status,
        COUNT(*) AS player_count,
        MIN(contract_end_date) AS earliest_contract,
        MAX(contract_end_date) AS latest_contract
    FROM
        staging.transfer_analysis
    GROUP BY
        contract_status
),
data_quality_checks AS (
    SELECT
        'Null Check' AS check_name,
        CASE
            WHEN COUNT(*) = COUNT(*) FILTER (
                WHERE
                    player_name IS NOT NULL
                    AND current_club IS NOT NULL
                    AND POSITION IS NOT NULL
                    AND market_value_euro IS NOT NULL
                    AND contract_end_date IS NOT NULL
                    AND age IS NOT NULL
                    AND transfer_status IS NOT NULL
            ) THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (
            WHERE
                player_name IS NULL
        ) AS null_player_names,
        COUNT(*) FILTER (
            WHERE
                current_club IS NULL
        ) AS null_clubs,
        COUNT(*) FILTER (
            WHERE
                POSITION IS NULL
        ) AS null_positions
    FROM
        staging.transfer_analysis
    UNION ALL
    SELECT
        'Value Range Check' AS check_name,
        CASE
            WHEN COUNT(*) FILTER (
                WHERE
                    market_value_euro < 1000000
                    OR market_value_euro > 100000000
            ) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (
            WHERE
                market_value_euro < 1000000
        ) AS below_min_value,
        COUNT(*) FILTER (
            WHERE
                market_value_euro > 100000000
        ) AS above_max_value,
        NULL AS unused
    FROM
        staging.transfer_analysis
    UNION ALL
    SELECT
        'Age Range Check' AS check_name,
        CASE
            WHEN COUNT(*) FILTER (
                WHERE
                    age < 15
                    OR age > 45
            ) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (
            WHERE
                age < 15
        ) AS under_15,
        COUNT(*) FILTER (
            WHERE
                age > 45
        ) AS over_45,
        NULL AS unused
    FROM
        staging.transfer_analysis
)
SELECT
    'Value Tier Distribution' AS metric_type,
    value_tier AS category,
    player_count,
    avg_value,
    min_value,
    max_value,
    NULL AS date_metric,
    NULL AS check_status
FROM
    value_tier_stats
UNION ALL
SELECT
    'Contract Status Distribution' AS metric_type,
    contract_status AS category,
    player_count,
    NULL AS avg_value,
    NULL AS min_value,
    NULL AS max_value,
    earliest_contract AS date_metric,
    NULL AS check_status
FROM
    contract_status_stats
UNION ALL
SELECT
    'Data Quality Check' AS metric_type,
    check_name AS category,
    total_records AS player_count,
    null_player_names AS avg_value,
    null_clubs AS min_value,
    null_positions AS max_value,
    NULL AS date_metric,
    status AS check_status
FROM
    data_quality_checks
