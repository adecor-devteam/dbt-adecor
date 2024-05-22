SELECT
    u.firstname || ' ' || u.lastname AS driver_name,
    u.id as driver_id,
    u.email AS driver_email,
    TO_CHAR(COUNT(*), 'FM999999') AS delivery_count,
    TO_CHAR(SUM(t.mileage), 'FM999999999.00') AS total_km_driven,
    TO_CHAR(SUM(t.totalcost), 'FM$999,999,999.00') AS total_cost,
    TO_CHAR(SUM(CASE 
                WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
                THEN t.totalcost * 0.79 
                ELSE t.totalcost * 0.69 
              END), 'FM$999,999,999.00') AS total_days_compensation,
    (t.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date AS entry_date_mst
FROM {{ ref('ontime_tracking') }} AS t
INNER JOIN {{ ref('ontime_users') }} AS u ON t.droppedOffByDriverID = u.ID
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON t.PriceSet = ps.ID
WHERE u.email ILIKE '%@%\.%'
GROUP BY driver_name, driver_id, driver_email, (t.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date
ORDER BY
    entry_date_mst