SELECT
    us_state,
    argMax(toString(cat_id), amount) AS category,
    max(amount) AS max_amount
FROM
    fraud.transactions
GROUP BY
    us_state
ORDER BY
    us_state
FORMAT CSVWithNames
;
