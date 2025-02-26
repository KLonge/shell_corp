model (
    NAME prod.derived_a,
    kind full,
    dialect duckdb,
    description "Top players by market value - migrated from legacy derived table A",
    audits (
        not_null(
            COLUMNS:= (
                player_id,
                NAME,
                POSITION,
                age,
                nationality,
                current_club,
                market_value_millions
            )
        ),
        number_of_rows(
            threshold:= 1
        )
    )
);-- this IS A straightforward SQL transformation that creates A VIEW OF top players BY market VALUE
SELECT
    p.player_id,
    p.name,
    p.position,
    p.age,
    p.nationality,
    p.current_club,
    p.market_value_millions,
    p.contract_end_date,
    'Unknown' AS league,
    'Unknown' AS country
FROM
    legacy_prod.app_a p
WHERE
    p.market_value_millions > 5
ORDER BY
    p.market_value_millions DESC
