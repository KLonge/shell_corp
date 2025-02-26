model (
    NAME prod.derived_b,
    kind full,
    dialect duckdb,
    description "High-value transfers - migrated from legacy derived table B",
    COLUMNS (
        transfer_id VARCHAR,
        player_id VARCHAR,
        player_name VARCHAR,
        POSITION VARCHAR,
        nationality VARCHAR,
        market_value_millions DOUBLE,
        selling_club_id VARCHAR,
        selling_club VARCHAR,
        selling_league VARCHAR,
        buying_club_id VARCHAR,
        buying_club VARCHAR,
        buying_league VARCHAR,
        transfer_fee_millions DOUBLE,
        transfer_type VARCHAR,
        transfer_window VARCHAR,
        transfer_date DATE,
        contract_length_years DOUBLE,
        salary_thousands_weekly DOUBLE
    ),
    audits (
        not_null(
            COLUMNS:= (
                transfer_id,
                player_id,
                selling_club_id,
                buying_club_id,
                transfer_fee_millions
            )
        ),
        number_of_rows(
            threshold:= 1
        )
    )
);
WITH player_clubs AS (
    SELECT
        p.player_id,
        p.name AS player_name,
        p.position,
        p.nationality,
        p.market_value_millions,
        C.club_id,
        C.name AS club_name,
        C.league,
        C.country
    FROM
        raw.app_a p
        LEFT JOIN raw.app_b C
        ON p.current_club = C.name
)
SELECT
    t.transfer_id,
    t.player_id,
    p.player_name,
    p.position,
    p.nationality,
    p.market_value_millions,
    t.selling_club_id,
    sell.name AS selling_club,
    sell.league AS selling_league,
    t.buying_club_id,
    buy.name AS buying_club,
    buy.league AS buying_league,
    t.transfer_fee_millions,
    t.transfer_type,
    t.transfer_window,
    t.transfer_date,
    t.contract_length_years,
    t.salary_thousands_weekly
FROM
    raw.app_c t
    LEFT JOIN player_clubs p
    ON t.player_id = p.player_id
    LEFT JOIN raw.app_b sell
    ON t.selling_club_id = sell.club_id
    LEFT JOIN raw.app_b buy
    ON t.buying_club_id = buy.club_id
WHERE
    t.transfer_fee_millions >= 5.0
    AND t.transfer_type IN (
        'Permanent',
        'Loan with Option to Buy'
    )
    AND CAST(
        t.transfer_date AS DATE
    ) >= '2022-01-01'
    AND CAST(
        t.transfer_date AS DATE
    ) <= CURRENT_DATE
    AND t.status = 'Completed'
ORDER BY
    t.transfer_fee_millions DESC
LIMIT
    200
