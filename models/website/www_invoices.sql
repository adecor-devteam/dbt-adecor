SELECT
    iit.invoicenumber as invoice_number,
    iit.ontime_invoice_date as invoice_date,
    iit.qb_invoice_created_at as creation_date,
    iit.company as company_name,
    ROUND((iit.orders_total - iit.orders_total * iit.discount/100),2) + ROUND(ROUND((iit.orders_total - iit.orders_total * iit.discount/100),2) * iit.gst / 100,2) as ontime_total,
    iit.quickbooks_total as quickbooks_total,
    iit.qb_invoice_status as processed_in_qb,
    CASE    
        WHEN iit.quickbooks_balance > 0 THEN iit.days_since_creation
        ELSE 0
    END AS invoice_status,
    iit.delivered_email_count as email_status
FROM {{ ref('interim_invoice_total') }} iit