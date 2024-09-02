-- Company Table
-- Account Number - (ARMS Company ID)
-- Company Name - (ontime_customers)
-- GST - (customer_details)
-- Convenience Fee - (customer_details)
-- Rewards - (customer_details)
-- Send Emails - (customer_details)
-- Discount - (customer_details)
-- Unpaid Invoices (Total) - Total Number of Unpaid Invoices (whole number)
-- Balance Due - (quickbooks_invoice) Sum of all unpaid invoices

{{
config(
    materialized = 'materialized_view',
	on_configuration_change="apply"
)
}}


SELECT 	oc.company as account_name,
		cd.id as account_number,
		COALESCE(cd.gst,cdd.gst) as gst,
		coalesce(cd.convenience_fee, cdd.convenience_fee) as convenience_fee,
		cd.total_rewards as rewards,
		coalesce(cd.email_invoices,cdd.email_invoices) as send_emails,
		coalesce(cd.discount,cdd.discount)  as discount,
		count(qi.doc_number) as upaid_invoices,
		sum(qi.balance) as balance_due
FROM {{ ref('ontime_customers') }} oc
left join {{ source('arms','customer_details') }} cd on (oc.id = cd.ontime_customerid)
left join {{ ref('ontime_invoices') }} oi on (oi.customerid = oc.id)
left join {{ ref('quickbooks_invoice') }} qi on ( qi.doc_number = oi.invoicenumber::TEXT and qi.balance>0)
cross join {{ source('arms','customer_details_defaults') }} cdd 
where cdd.ontime_customerid = 'default'
group by cd.id, oc.company, cdd.id, oc.id, oi.customerid