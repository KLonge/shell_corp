-- derived TABLE A: top players BY market VALUE -- this IS A straightforward SQL transformation that creates A VIEW OF top players BY market VALUE
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
    prod.app_a p
WHERE
    p.market_value_millions > 5 -- lowered threshold
ORDER BY
    p.market_value_millions DESC
