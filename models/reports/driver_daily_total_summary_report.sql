SELECT
    date_trunc('day', i.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST') AS "date",
    COUNT(*) AS "total_delivery_count",
    round(SUM(i.Mileage), 2) AS "total_km_driven",
    SUM(i.TotalCost)::text::money AS "total_cost",
    SUM(CASE
                WHEN ps.Urgency LIKE '%Hotshot%' OR ps.Urgency LIKE '%Hot shot%'
                THEN i.TotalCost * 0.79
                ELSE i.TotalCost * 0.69
              END)::text::money AS "total_compensation",
    (SUM(i.TotalCost) - SUM(CASE
                WHEN ps.Urgency LIKE '%Hotshot%' OR ps.Urgency LIKE '%Hot shot%'
                THEN i.TotalCost * 0.79
                ELSE i.TotalCost * 0.69
              END))::text::money AS Net
FROM {{ ref('ontime_tracking') }} AS i
INNER JOIN {{ ref('ontime_users') }} AS u ON i.DroppedOffByDriverID = u.ID
INNER JOIN {{ ref('ontime_pricesets') }} AS ps ON i.PriceSet = ps.ID
WHERE u.FirstName ~ '^[0-9][0-9] '
GROUP BY 1
