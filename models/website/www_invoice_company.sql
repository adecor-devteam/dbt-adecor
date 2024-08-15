-- Invoices Table (Company View)
-- Account Number - (ARMS Company ID)
-- Company Name - (ontime_customers)
-- Unpaid Invoices - (quickbooks_invoice) If Balance > 0
-- Under 30 Days - (quickbooks_invoice) If Balance > 0 & Created Date < 30 days
-- 31-60 Days - (quickbooks_invoice) If Balance > 0 & Created Date > 30 days & < 60 days
-- 61-90 Days - (quickbooks_invoice) If Balance > 0 & Created Date > 60 days and < 90 days
-- Over 90 Days - (quickbooks_invoice) If Balance > 0 & Created Date > 90 days


SELECT 	oc.company as company_name,
		cd.id as account_number,
		(SELECT count(qi.*) FROM quickbooks_invoice qi WHERE qi.customer_id = cd.qb_customerid and qi.balance>0) AS unpaid_invoices,
		(SELECT count(qi.*) FROM quickbooks_invoice qi WHERE qi.customer_id = cd.qb_customerid and qi.balance>0 and EXTRACT(DAY FROM NOW() - qi.created_at ) <= 30 ) AS under_30_days,
		(SELECT count(qi.*) FROM quickbooks_invoice qi WHERE qi.customer_id = cd.qb_customerid and qi.balance>0 and EXTRACT(DAY FROM NOW() - qi.created_at ) > 30 and EXTRACT(DAY FROM NOW() - qi.created_at ) <= 60 ) AS in_31_60_days,
		(SELECT count(qi.*) FROM quickbooks_invoice qi WHERE qi.customer_id = cd.qb_customerid and qi.balance>0 and EXTRACT(DAY FROM NOW() - qi.created_at ) > 60 and EXTRACT(DAY FROM NOW() - qi.created_at ) <= 90 ) AS in_61_90_days,
		(SELECT count(qi.*) FROM quickbooks_invoice qi WHERE qi.customer_id = cd.qb_customerid and qi.balance>0 and EXTRACT(DAY FROM NOW() - qi.created_at ) > 90 ) AS over_90_days
		FROM {{ ref('ontime_customers') }} oc
left join {{ source('arms','customer_details') }} cd on (oc.id = cd.ontime_customerid)
