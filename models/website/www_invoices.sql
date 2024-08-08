SELECT
    oit.invoicenumber as invoice_number,
    oit.ontime_invoice_date as invoice_date,
    oit.qb_invoice_created_at as creation_date,
    oit.company as company_name,
    oit.orders_total - ROUND((oit.orders_total - oit.orders_total * oit.discount) * oit.gst / 100,2) as ontime_total,
    oit.qb_invoice_status as processed_in_qb
FROM {{ ref('ontime_invoice_total') }} oit