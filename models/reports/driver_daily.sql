SELECT
    t.orderid AS tracking_number,
    d1.companyname AS pickup_location,
    d2.companyname AS dropoff_location,
    ps.urgency AS price_set,
    TO_CHAR(SUM(t.mileage), 'FM999999999.00') AS total_km_driven,
    TO_CHAR(SUM(t.totalcost), 'FM$999,999,999.00') AS total_cost,
    TO_CHAR(SUM(
        CASE 
            WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
            THEN t.totalcost * 0.79 
            ELSE t.totalcost * 0.69 
        END), 'FM$999,999,999.00') AS total_compensation,
    TO_CHAR((t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST'), 'Month DD, YYYY, HH12:MI:SS AM') AS delivery_date,
    (t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date AS entry_date_mst
FROM {{ ref('ontime_tracking') }} AS t
INNER JOIN {{ ref('ontime_users') }} AS u ON t.droppedoffbydriverid = u.id
INNER JOIN {{ ref('ontime_destinations') }} AS d1 ON t.dfrom = d1.id
INNER JOIN {{ ref('ontime_destinations') }} AS d2 ON t.dto = d2.id
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON t.priceset = ps.id
WHERE u.email ILIKE '%@%\.%'
GROUP BY
    t.orderid, d1.companyname, d2.companyname, ps.urgency, t.whendroppedoff, u.firstname, u.lastname
ORDER BY t.orderid DESC;
