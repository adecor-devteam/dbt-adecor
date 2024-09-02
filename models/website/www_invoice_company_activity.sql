-- Invoice Number - (ontime_invoices)
-- Payment Status - (quickbooks_invoice) If Balance = 0 then Paid, Otherwise Unpaid
-- Emails Sent - (invoice_tracking)
-- Invoice Status - (quickbooks_invoice) If Balance > 0 then Calculate days from Created Date (i.e. 5 days)
-- Invoice Amount - (invoice_tracking)
-- Rewards - (invoice_tracking) If Invoice is Paid and Rewards in (customer_details) = True Calculates total rewards from Subtotal * 0.025 and then rounds down to nearest ($40). In short the rewards is awarded 1 mile for every $40.
-- Discount - (customer_details)
-- GST - (customer_details)
-- Convenience Fee - (customer_details)
-- Total Owing - (quickbooks_invoice)
-- Last Updated - This is the last time the invoice was processed so I believe you can use the qb_invoice_created_at column


{{
config(
    materialized = 'materialized_view',
	on_configuration_change="apply"
)
}}



SELECT
	iit.invoicenumber as invoice_number,
    iit.company as Company,
    CASE    
        WHEN iit.quickbooks_balance > 0 THEN 'Unpaid'
        ELSE 'Paid'
    END AS payment_status,
    iit.delivered_email_count as emails_sent,
    case 
    	when iit.quickbooks_balance > 0 then iit.days_since_creation
    	else 0
    end as invoice_status,
    ROUND((iit.orders_total - iit.orders_total * iit.discount/100),2) + ROUND(ROUND((iit.orders_total - iit.orders_total * iit.discount/100),2) * iit.gst / 100,2) as invoice_amount,
    case 
    	when (iit.quickbooks_balance = 0 and iit.reward_air_miles = true) then TRUNC((ROUND((iit.orders_total - iit.orders_total * iit.discount/100),2) + ROUND(ROUND((iit.orders_total - iit.orders_total * iit.discount/100),2) * iit.gst / 100,2)) * 0.025)
    	else 0
    end as rewards,
    iit.discount as discount,
    iit.gst as gst,
    iit.convenience_fee as convenience_fee,
    iit.quickbooks_balance as total_owing,
    iit.qb_invoice_created_at as last_updated
FROM {{ ref('interim_invoice_total') }} iit 
