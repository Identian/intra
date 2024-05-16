SELECT today_yield.cc_curve,
    ROUND(
        AVG(today_yield.rate - yesterday_yield.rate) * 100,
        2
    ) AS pbs_diff
FROM precia_published.pub_rfl_yield AS today_yield
    LEFT JOIN precia_published.pub_rfl_yield AS yesterday_yield ON today_yield.cc_curve = yesterday_yield.cc_curve
    AND today_yield.term = yesterday_yield.term
WHERE today_yield.rate_date = 'today'
    AND yesterday_yield.rate_date = 'yesterday'
    AND today_yield.term <= TERM
GROUP BY today_yield.cc_curve;