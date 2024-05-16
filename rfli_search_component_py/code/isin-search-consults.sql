/*
SELECT rfl_prices.isin_code as isin,
    rfl_prices.instrument as nemo,
    cast(rfl_prices.issue_date As CHAR) AS issue_date,
    cast(rfl_prices.maturity_date As CHAR) AS maturity_date,
    COALESCE(rfl_issuer.name, 'NA') AS issuer_name,
    rfl_category.maturity_range,
    rfl_prices.maturity_days,
    rfl_prices.margin_value as margin,
    rfl_prices.equivalent_margin,
    rfl_prices.mean_price,
    rfl_prices.clean_price,
    rfl_prices.accrued_interest,
    rfl_prices.convexity,
    rfl_prices.duration,
    rfl_prices.modified_duration,
    rfl_prices.rate_type,
    rfl_prices.category_id as category_id,
    COALESCE(
        NULLIF(rfl_prices_yesterday.real_rating, ''),
        'NA'
    ) AS real_rating,
    rfl_prices.currency_type,
    rfl_prices.yield
FROM precia_published.pub_rfl_prices rfl_prices
    LEFT JOIN precia_sources.src_rfl_instrument rfl_instrument ON rfl_prices.isin_code = rfl_instrument.isin_code
    LEFT JOIN precia_sources.src_rfl_category rfl_category ON rfl_prices.category_id = rfl_category.category_id
    LEFT JOIN precia_sources.src_rfl_issuer rfl_issuer ON rfl_instrument.issuer = rfl_issuer.issuer
    LEFT JOIN precia_published.pub_rfl_prices rfl_prices_yesterday ON rfl_prices_yesterday.isin_code = rfl_prices.isin_code
WHERE rfl_prices.valuation_date = '2024-03-05'
    AND rfl_prices.isin_code != ''
    AND rfl_prices_yesterday.valuation_date = '2024-03-04';

 precia_sources.src_rfl_category -> maturity_range con CATEGORY_ID
 precia_sources.src_rfl_issuer -> issuer using instrument CON INSTRUMENT
 precia_sources.src_rfl_instrument -> real_rating CON ISIN CODE
 */
 /*ISIN SEARCH*/
 /*     consulta de categorias*/
SELECT
    category_id,
    rating_group,
    maturity_range
FROM
    precia_sources.src_rfl_category;
 /*     consulta de issuers*/
SELECT
    DISTINCT instrument_table.isin_code,
    COALESCE(issuer_table.name, 'NA') AS issuer_name
FROM
    precia_sources.src_rfl_instrument AS instrument_table
    LEFT JOIN
    precia_sources.src_rfl_issuer AS issuer_table
    ON instrument_table.issuer=issuer_table.issuer;
 /*     consulta de prices*/
SELECT today_prices.isin_code as isin,
    today_prices.instrument as nemo,
    cast(today_prices.issue_date As CHAR) AS issue_date,
    cast(today_prices.maturity_date As CHAR) AS maturity_date,
    today_prices.maturity_days,
    today_prices.margin_value as margin,
    today_prices.equivalent_margin,
    today_prices.mean_price,
    today_prices.clean_price,
    today_prices.accrued_interest,
    today_prices.convexity,
    today_prices.duration,
    today_prices.modified_duration,
    today_prices.rate_type,
    today_prices.category_id,
    today_prices.currency_type,
    today_prices.yield,
    COALESCE(today_prices.real_rating, yesterday_prices.yesterday_real_rating, 'NA') AS real_rating,
    FROM precia_published.pub_today_prices AS today_prices
    LEFT JOIN (
        SELECT isin_code AS yesterday_isin_code,
            real_rating AS yesterday_real_rating
        FROM precia_published.pub_today_prices
        WHERE valuation_date = ''
            AND isin_code != ''
            AND instrument NOT IN('TIDISDVL', 'CERTS')
    ) AS yesterday_prices ON today_prices.isin_code = yesterday_prices.yesterday_isin_code
WHERE today_prices.valuation_date = ''
    AND today_prices.isin_code != ''
    AND today_prices.instrument NOT IN('TIDISDVL', 'CERTS');


/*ISIN TRACK*/
SELECT today_prices.isin_code,
    today_prices.instrument,
    today_prices.yield,
    today_prices.equivalent_margin,
    round(today_prices.margin_value, 4) as margin,
    today_prices.spread,
    today_prices.mean_price,
    today_prices.clean_price,
    cast(today_prices.issue_date As CHAR) AS issue_date,
    cast(today_prices.maturity_date As CHAR) AS maturity_date,
    today_prices.category_id,
    yesterday_prices.yesterday_yield,
    yesterday_prices.yesterday_mean_price
FROM precia_published.pub_rfl_prices AS today_prices
    LEFT JOIN (
        SELECT isin_code AS yesterday_isin_code,
            yield as yesterday_yield,
            mean_price as yesterday_mean_price
        FROM precia_published.pub_rfl_prices
        WHERE valuation_date = '2024-03-05'
            AND isin_code != ''
            AND instrument NOT IN('TIDISDVL', 'CERTS')
    ) AS yesterday_prices ON today_prices.isin_code = yesterday_prices.yesterday_isin_code
WHERE today_prices.valuation_date = '2024-03-04'
    AND today_prices.isin_code != ''
    AND today_prices.instrument NOT IN('TIDISDVL', 'CERTS');