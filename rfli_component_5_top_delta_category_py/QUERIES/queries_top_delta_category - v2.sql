/*Query para generar el Top de las categorias.*/
SELECT prices_consult.category_id,
    prices_consult.tir_variation,
    prices_consult.abs_tir_variation,
    category_info.class,
    category_info.currency_group,
    category_info.rate_group,
    category_info.rating_group,
    category_info.maturity_range
FROM (
        SELECT today_prices.category_id AS category_id,
            ROUND(
                today_prices.promedio_valor - yesterday_prices.promedio_valor,
                3
            ) AS tir_variation,
            ROUND(
                ABS(
                    today_prices.promedio_valor - yesterday_prices.promedio_valor
                ),
                3
            ) AS abs_tir_variation
        FROM (
                SELECT category_id,
                    AVG(yield) AS promedio_valor
                FROM precia_published.pub_rfl_prices
                WHERE valuation_date = DATE(NOW())
                GROUP BY category_id
            ) AS today_prices
            JOIN (
                SELECT category_id,
                    AVG(yield) AS promedio_valor
                FROM precia_published.pub_rfl_prices
                WHERE valuation_date = DATE(NOW()) -1
                GROUP BY category_id
            ) AS yesterday_prices ON today_prices.category_id = yesterday_prices.category_id
        ORDER BY abs_tir_variation DESC
    ) AS prices_consult
    RIGHT JOIN precia_sources.src_rfl_category AS category_info ON prices_consult.category_id = category_info.category_id
WHERE category_info.class NOT IN(
        '005',
        '008',
        '013',
        '015',
        '016',
        '029',
        '039',
        '040'
    )
ORDER BY abs_tir_variation DESC
LIMIT 30;
/*Query para el detalle de las categorias. */
SELECT category_id,
    margin_type
FROM prc_rfl_category_margin
WHERE margin_date = DATE(NOW())
    AND category_id IN (
        1129019,
        111127017,
        32127019,
        11135920,
        222905,
        11127021,
        11112604,
        37112704,
        32112704,
        11112704,
        111126022,
        111125822,
        321127022,
        111125815,
        371127022,
        111127022,
        111127015,
        321127015,
        371127015,
        111126015,
        1013304,
        1013504,
        11112705,
        1112705,
        3213584,
        1213594,
        12136022,
        111135821,
        38137022,
        10137022
    );
/*Query para el detalle de la curva. */
SELECT today_yield.cc_curve,
    AVG(today_yield.rate - yesterday_yield.rate) * 100 AS pbs_diff
from precia_published.pub_rfl_yield today_yield
    INNER JOIN precia_published.pub_rfl_yield yesterday_yield ON today_yield.cc_curve = yesterday_yield.cc_curve
    AND today_yield.term = yesterday_yield.term
WHERE today_yield.rate_date = DATE(NOW())
    and yesterday_yield.rate_date = DATE(NOW()) -1
GROUP BY cc_curve;
/*Query para el detalle de las operaciones. */
SELECT instrument AS nemotecnico,
    folio,
    yield,
    amount,
    category_id,
    maturity_date,
    timestamp_operation
FROM prc_rfl_operations
WHERE category_id IN (
        1129019,
        111127017,
        32127019,
        11135920,
        222905,
        11127021,
        11112604,
        37112704,
        32112704,
        11112704,
        111126022,
        111125822,
        321127022,
        111125815,
        371127022,
        111127022,
        111127015,
        321127015,
        371127015,
        111126015,
        1013304,
        1013504,
        11112705,
        1112705,
        3213584,
        1213594,
        12136022,
        111135821,
        38137022,
        10137022
    )
    AND operation_date = DATE(NOW());