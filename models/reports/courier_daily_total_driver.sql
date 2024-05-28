SELECT
    u.firstname || ' ' || u.lastname AS driver_name,
    LOWER(u.id) as driver_id,
    TO_CHAR(COUNT(*), 'FM999,999,999') AS total_delivery_count,
    TO_CHAR(ROUND(SUM(t.mileage),2), 'FM999,999,999.00') AS total_km_driven,
    TO_CHAR(ROUND(SUM(t.totalcost),2), 'FM$999,999,999.00') AS total_cost,
    TO_CHAR(ROUND(SUM(CASE 
                WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
                THEN ROUND(t.totalcost * 0.79,2)  
                ELSE ROUND(t.totalcost * 0.69,2)  
              END),2), 'FM$999,999,999.00') AS total_compensation,
    (t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date AS entry_date_mst,
    TO_CHAR((t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST'), 'Month DD, YYYY') AS delivery_date
FROM {{ ref('ontime_tracking') }} AS t
INNER JOIN {{ ref('ontime_users') }} AS u ON t.droppedoffbydriverid = u.id
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON t.priceset = ps.id
WHERE u.firstname ~ '^(0[1-9]|[1-9][0-9]|1[0-9]{2}|2[0-9]{2}) '
GROUP BY u.firstname, u.lastname, LOWER(u.id), (t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date, TO_CHAR((t.whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST'), 'Month DD, YYYY')
