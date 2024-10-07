SELECT DISTINCT ON	(oi.invoicenumber)
		oi.invoicenumber as invoicenumber,
        oi.invoicedate as ontime_invoice_date,
        it.qb_invoice_created_at as qb_invoice_created_at,
        oc.company as company,
		it.qb_invoice_status as qb_invoice_status,
		ROUND(qi.total_amount,2) as quickbooks_total,
		EXTRACT(DAY FROM NOW() - it.qb_invoice_created_at) AS days_since_creation,
		qi.customer_id as quickbooks_customer_id,
		qi.balance as quickbooks_balance,
		qi.transaction_date as transaction_date,
		count(distinct(iet.pdf_sendgrid_messageid)) as delivered_email_count,
		COALESCE(cd.gst, cdd.gst) AS gst,
		COALESCE(cd.discount, cdd.discount) AS discount,
		COALESCE(cd.convenience_fee, cdd.convenience_fee) AS convenience_fee,
		COALESCE(cd.reward_air_miles, cdd.reward_air_miles) as reward_air_miles,
        sum(ROUND(ot.totalcost,2)) AS orders_total,
		ROUND(sum(ROUND(ot.totalcost,2)) * COALESCE(cd.discount, cdd.discount) / 100,2) as discount_amount
from  {{ ref('ontime_invoices_orders') }}  oi
		left join  {{ source('arms','invoice_tracking')}} it on (oi.invoicenumber = it.invoicenumber)
		left join {{ ref('ontime_customers') }} oc on (oc.id = oi.customerid)
		left join {{ ref('ontime_tracking') }} ot on (oi.tracking_id = ot.id )
		left join {{ source('arms','customer_details')}} cd on (cd.ontime_customerid = oi.customerid)
		left join {{ ref('quickbooks_invoice') }} qi on ( oi.invoicenumber::TEXT = qi.doc_number)
		left join {{ source('arms','invoice_email_tracking') }} iet on ( iet.invoiceid = oi.invoicenumber )
		left join {{ source('arms','arms_email_activity_feed') }} aeaf on ( ( aeaf.batch_message_id = iet.pdf_sendgrid_messageid) and aeaf.status='delivered' )
		cross join {{ source('arms','customer_details_defaults') }} cdd 
		where cdd.ontime_customerid = 'default'
group by 	oi.invoicenumber, 
			oi.invoicedate, 
			it.qb_invoice_created_at, 
			oc.company, 
			cd.id, 
			cdd.id,
			it.qb_invoice_status,
			qi.customer_id,
			qi.total_amount, 
			qi.balance,
			qi.transaction_date,
			iet.pdf_sendgrid_messageid
order by 
			oi.invoicenumber
