SELECT
    u.firstname || ' ' || u.lastname AS driver_name,
    u.id as driver_id,
    u.email AS driver_email,
    COUNT(*) AS delivery_count,
    SUM(t.mileage) AS total_km_driven,
    SUM(t.totalcost) AS total_daily_cost,
    SUM(CASE 
                WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
                THEN t.totalcost * 0.79 
                ELSE t.totalcost * 0.69 
              END) AS total_days_compensation,
    (t.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date AS entry_date_mst
FROM {{ ref('ontime_tracking') }} AS t
INNER JOIN {{ ref('ontime_users') }} AS u ON t.droppedOffByDriverID = u.ID
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON t.PriceSet = ps.ID
GROUP BY driver_name, driver_id, driver_email, (t.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date
ORDER BY
    entry_date_mst