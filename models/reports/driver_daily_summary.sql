SELECT
    u.firstname || ' ' || u.lastname AS driver_name,
    LOWER(u.id) as driver_id,
    u.email AS driver_email,
    TO_CHAR(COUNT(*), 'FM999999') AS delivery_count,
    TO_CHAR(ROUND(SUM(t.mileage),2), 'FM999999999.00') AS total_km_driven,
    TO_CHAR(ROUND(SUM(t.totalcost),2), 'FM$999,999,999.00') AS total_cost,
    TO_CHAR(ROUND(SUM(CASE 
                WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
                THEN t.totalcost * 0.79 
                ELSE t.totalcost * 0.69 
              END),2), 'FM$999,999,999.00') AS total_days_compensation,
    ROUND(SUM(CASE 
            WHEN ps.urgency ILIKE '%Hotshot%' OR ps.urgency ILIKE '%Hot shot%' 
            THEN t.totalcost * 0.79 
            ELSE t.totalcost * 0.69 
            END),2) AS total_compensation_amount,
    (t.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date AS entry_date_mst
FROM {{ ref('ontime_tracking') }} AS t
INNER JOIN {{ ref('ontime_users') }} AS u ON t.droppedOffByDriverID = u.ID
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON t.PriceSet = ps.ID
WHERE u.email ILIKE '%@%\.%'
GROUP BY driver_name, driver_id, driver_email, (t.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST')::date
ORDER BY
    entry_date_mst