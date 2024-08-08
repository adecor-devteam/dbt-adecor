SELECT 	oi.invoicenumber as invoicenumber,
        oi.invoicedate as ontime_invoice_date,
        it.qb_invoice_created_at as qb_invoice_created_at,
        oc.company as company,
		it.qb_invoice_status as qb_invoice_status,
		ROUND(qi.total_amount,2) as quickbooks_total,
		EXTRACT(DAY FROM NOW() - it.qb_invoice_created_at) AS days_since_creation,
		qi.balance as quickbooks_balance,
		COALESCE(cd.gst, 5.0) AS gst,
		COALESCE(cd.discount, 0) AS discount,
        sum(ROUND(ot.totalcost,2)) AS orders_total
from  {{ ref('ontime_invoices_orders') }}  oi
		left join  {{ source('arms','invoice_tracking')}} it on (oi.invoicenumber = it.invoicenumber)
		left join {{ ref('ontime_customers') }} oc on (oc.id = oi.customerid)
		left join {{ ref('ontime_tracking') }} ot on (oi.tracking_id = ot.id )
		left join {{ source('arms','customer_details')}} cd on (cd.ontime_customerid = oi.customerid)
		left join {{ ref('quickbooks_invoice') }} qi on ( oi.invoicenumber::TEXT = qi.doc_number)
group by oi.invoicenumber, oi.invoicedate, it.qb_invoice_created_at, oc.company, cd.gst, cd.discount, it.qb_invoice_status, qi.total_amount, qi.balance