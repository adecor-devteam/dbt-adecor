SELECT
    date_trunc('day', i.WhenDroppedOff AT TIME ZONE 'UTC' AT TIME ZONE 'MST') AS "Date",
    COUNT(*) AS "Total Delivery Count",
    round(SUM(i.Mileage), 2) AS "Total KM's Driven",
    SUM(i.TotalCost)::text::money AS "Total Cost",
    SUM(CASE
                WHEN ps.Urgency LIKE '%Hotshot%' OR ps.Urgency LIKE '%Hot shot%'
                THEN i.TotalCost * 0.79
                ELSE i.TotalCost * 0.69
              END)::text::money AS "Total Compensation",
    (SUM(i.TotalCost) - SUM(CASE
                WHEN ps.Urgency LIKE '%Hotshot%' OR ps.Urgency LIKE '%Hot shot%'
                THEN i.TotalCost * 0.79
                ELSE i.TotalCost * 0.69
              END))::text::money AS Net
FROM ontime_tracking AS i
INNER JOIN ontime_users AS u ON i.DroppedOffByDriverID = u.ID
INNER JOIN ontime_pricesets AS ps ON i.PriceSet = ps.ID
WHERE u.FirstName ~ '^[0-9][0-9] '
GROUP BY 1;
