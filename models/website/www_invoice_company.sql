-- Invoices Table (Company View)
-- Account Number - (ARMS Company ID)
-- Company Name - (ontime_customers)
-- Unpaid Invoices - (quickbooks_invoice) If Balance > 0
-- Under 30 Days - (quickbooks_invoice) If Balance > 0 & Created Date < 30 days
-- 31-60 Days - (quickbooks_invoice) If Balance > 0 & Created Date > 30 days & < 60 days
-- 61-90 Days - (quickbooks_invoice) If Balance > 0 & Created Date > 60 days and < 90 days
-- Over 90 Days - (quickbooks_invoice) If Balance > 0 & Created Date > 90 days

{{
config(
    materialized = 'materialized_view',
	on_configuration_change="apply"
)
}}


SELECT 	oc.company as company_name,
		ROW_NUMBER() OVER (ORDER BY oc.creationdate) as account_number,
		count(qi.doc_number) AS unpaid_invoices,
		count(qi30.doc_number) as under_30_days,
		count(qi31_60.doc_number) as in_31_60_days,
		count(qi61_90.doc_number) as in_61_90_days,
		count(qi90.doc_number) as over_90_days
FROM {{ ref('ontime_customers') }} oc
left join {{ ref('ontime_invoices') }} oi on (oi.customerid = oc.id)
left join {{ ref('quickbooks_invoice') }} qi on ( qi.doc_number = oi.invoicenumber::TEXT and qi.balance>0)
left join {{ ref('quickbooks_invoice') }} qi30 on ( qi30.doc_number = oi.invoicenumber::TEXT and qi30.balance>0 and EXTRACT(DAY FROM NOW() - qi30.created_at ) <= 30 )
left join {{ ref('quickbooks_invoice') }} qi31_60 on ( qi31_60.doc_number = oi.invoicenumber::TEXT and qi31_60.balance>0 and  EXTRACT(DAY FROM NOW() - qi31_60.created_at ) > 30 and EXTRACT(DAY FROM NOW() - qi31_60.created_at ) <= 60 ) 
left join {{ ref('quickbooks_invoice') }} qi61_90 on ( qi61_90.doc_number = oi.invoicenumber::TEXT and qi61_90.balance>0 and  EXTRACT(DAY FROM NOW() - qi61_90.created_at ) > 60 and EXTRACT(DAY FROM NOW() - qi61_90.created_at ) <= 90 )
left join {{ ref('quickbooks_invoice') }} qi90 on ( qi90.doc_number = oi.invoicenumber::TEXT and qi90.balance>0 and EXTRACT(DAY FROM NOW() - qi90.created_at ) > 90 )
group by oc.id, oc.company, oc.creationdate