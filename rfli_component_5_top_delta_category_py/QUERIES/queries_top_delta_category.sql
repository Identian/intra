SELECT today_prices.category_id,
    (
        ROUND(today_prices.promedio_valor, 2) - ROUND(yesterday_prices.promedio_valor, 2)
    ) AS tir_variation,
    ABS(
        (
            ROUND(today_prices.promedio_valor, 2) - ROUND(yesterday_prices.promedio_valor, 2)
        )
    ) AS abs_tir_variation,
    CONVERT(category_info.class, DECIMAL(3, 0)) AS class,
    category_info.currency_group,
    category_info.rate_group,
    category_info.rating_group,
    category_info.maturity_range
FROM precia_sources.src_rfl_category AS category_info,
    (
        SELECT category_id,
            COUNT(*) AS cantidad_filas,
            AVG(yield) AS promedio_valor
        FROM precia_published.pub_rfl_prices
        WHERE valuation_date = DATE(NOW())
        GROUP BY category_id
    ) AS today_prices
    LEFT OUTER JOIN (
        SELECT category_id,
            COUNT(*) AS cantidad_filas,
            AVG(yield) AS promedio_valor
        FROM precia_published.pub_rfl_prices
        WHERE valuation_date = DATE(NOW())-1
        GROUP BY category_id
    ) AS yesterday_prices ON today_prices.category_id = yesterday_prices.category_id
WHERE category_info.category_id = today_prices.category_id
    AND category_info.class NOT IN("005", "008", "013", "015", "016", "029", "039", "040")
GROUP BY category_id
ORDER BY abs_tir_variation DESC
LIMIT 30;
/*MARGIN QUERY*/
SELECT category_id,
    margin_type
FROM prc_rfl_category_margin
WHERE margin_date = DATE(NOW())
    AND category_id IN (
        111126016,
        11136015,
        12131015,
        10227025,
        111125815,
        1129027,
        1129026,
        1129012,
        1129023,
        11137022,
        112909,
        111127011,
        112908,
        37112708,
        12113598,
        112907,
        111126015,
        1129017,
        2229013,
        112904,
        37112706,
        222905,
        1129018,
        1129015,
        11112585,
        10137011,
        111125916,
        11112705,
        32112705,
        32112706
    );
/*MOVEMENT CURVE DIFF*/
SELECT today_yield.cc_curve,
    today_yield.rate_date,
    avg(today_yield.rate - yesterday_yield.rate) * 100 AS pbs_diff
from precia_published.pub_rfl_yield today_yield
    INNER JOIN precia_published.pub_rfl_yield yesterday_yield ON today_yield.cc_curve = yesterday_yield.cc_curve
    AND today_yield.term = yesterday_yield.term
where today_yield.rate_date = DATE(NOW())
    and yesterday_yield.rate_date = DATE(NOW())-1
GROUP BY cc_curve;
/*OPERATIONS*/
SELECT instrument AS nemotecnico,
    folio,
    yield,
    amount,
    category_id,
    maturity_date,
    timestamp_operation
FROM prc_rfl_operations
WHERE category_id IN (
        111126016,
        11136015,
        12131015,
        10227025,
        111125815,
        1129027,
        1129026,
        1129012,
        1129023,
        11137022,
        112909,
        111127011,
        112908,
        37112708,
        12113598,
        112907,
        111126015,
        1129017,
        2229013,
        112904,
        37112706,
        222905,
        1129018,
        1129015,
        11112585,
        10137011,
        111125916,
        11112705,
        32112705,
        32112706
    )
    AND operation_date = DATE(NOW())