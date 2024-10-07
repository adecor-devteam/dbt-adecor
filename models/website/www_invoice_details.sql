

{{
config(
    materialized = 'materialized_view',
	on_configuration_change="apply"
)
}}


SELECT
    oi.id as ontime_invoice_id,
    oi.invoicenumber as invoicenumber,
    oi.invoicedate as invoicedate,
    oi.creationdate as creationdate,
    sum(ROUND(ot.totalcost,2)) AS subtotal,
    oi.customerid as ontime_customerid,
    oc.company as company,
    oc.email as billing_email,
    it.json_storage_path as json_storage_path,
    it.pdf_storage_path as pdf_storage_path,
    it.pdf_generated_status as pdf_generated_status,
    it.pdf_generated_at as pdf_generated_at,
    it.qb_invoicenumber as qb_invoicenumber,
    it.qb_invoice_status as qb_invoice_status,
    it.qb_invoice_created_at as qb_invoice_created_at,
    it.edi_template_status as edi_template_status,
    it.edi_template_created_at as edi_template_created_at,
    cd.id as customer_details_id,
    cd.qb_customerid as quickbooks_customer_id,
    qc.display_name as quickbooks_customer_display_name,
    qi.id as quickbooks_invoice_id,
    qi.doc_number as quickbooks_invoice_doc_number,
    qi.customer_id as quickbooks_invoice_customer_id,
    qi.created_at as quickbooks_invoice_created_at,
    qi.updated_at as quickbooks_invoice_updated_at,
    qi.total_amount as quickbooks_invoice_total_amount,
    qi.total_tax as quickbooks_invoice_total_tax,
    qi.balance as quickbooks_invoice_balance,
    CASE    
        WHEN qi.balance > 0 THEN 'Unpaid'
        ELSE 'Paid'
    END AS payment_status
FROM {{ ref('ontime_invoices_orders') }}  oi
left join  {{ source('arms','invoice_tracking')}} it on (oi.invoicenumber = it.invoicenumber)
left join {{ ref('ontime_customers') }} oc on (oc.id = oi.customerid)
left join {{ ref('ontime_tracking') }} ot on (oi.tracking_id = ot.id )
left join {{ source('arms','customer_details')}} cd on (oi.customerid = cd.ontime_customerid)
left join {{ ref('quickbooks_invoice') }} qi on ( oi.invoicenumber::TEXT = qi.doc_number)
left join {{ ref('quickbooks_customer')  }} qc on ( cd.qb_customerid = qc.id )
group by 
            oi.id, 
            oi.invoicenumber, 
            oi.invoicedate, 
            oi.creationdate, 
            oi.customerid,
            oc.company,
            oc.email,
            it.json_storage_path,
            it.pdf_storage_path,
            it.pdf_generated_status,
            it.pdf_generated_at,
            it.qb_invoicenumber,
            it.qb_invoice_status,
            it.qb_invoice_created_at,
            it.edi_template_status,
            it.edi_template_created_at,
            cd.id,
            cd.qb_customerid,
            qc.display_name,
            qi.id,
            qi.doc_number,
            qi.customer_id,
            qi.created_at,
            qi.updated_at,
            qi.total_amount,
            qi.total_tax,
            qi.balance


