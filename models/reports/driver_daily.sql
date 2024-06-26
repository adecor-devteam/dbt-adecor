SELECT
    t.orderid AS tracking_number,
    LOWER(u.id) as driver_id,
    d1.companyname AS pickup_location,
    d2.companyname AS dropoff_location,
    ps.urgency AS price_set,
    TO_CHAR(ROUND(SUM(t.mileage),2), 'FM999,999,999.00') AS total_km_driven,
    TO_CHAR(ROUND(SUM(t.totalcost),2), 'FM$999,999,999.00') AS total_cost,
    TO_CHAR(ROUND(SUM(
        CASE 
            WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
            THEN ROUND(t.totalcost * 0.79,2) 
            ELSE ROUND(t.totalcost * 0.69,2) 
        END),2), 'FM$999,999,999.00') AS total_compensation,
    TO_CHAR((t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST'), 'Month DD, YYYY') AS delivery_date,
    (t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date AS entry_date_mst
FROM {{ ref('ontime_tracking') }} AS t
INNER JOIN {{ ref('ontime_users') }} AS u ON t.droppedoffbydriverid = u.id
INNER JOIN {{ ref('ontime_destinations') }} AS d1 ON t.dfrom = d1.id
INNER JOIN {{ ref('ontime_destinations') }} AS d2 ON t.dto = d2.id
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON t.priceset = ps.id
WHERE u.email ILIKE '%@%\.%'
GROUP BY
    t.orderid, d1.companyname, d2.companyname, ps.urgency, (t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date, LOWER(u.id), TO_CHAR((t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST'), 'Month DD, YYYY')
ORDER BY t.orderid DESC
