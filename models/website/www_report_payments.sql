
{{
config(
    materialized = 'materialized_view',
	on_configuration_change="apply"
)
}}



SELECT 
qp.created_at as payment_date,
TO_CHAR(qp.created_at, 'YYYY-MM-DD HH24:MI:SS') as payment_date_text,
qc.id as account_number,
qc.company_name as company_name,
string_agg(qi.doc_number, ', ') as invoices,
qpm.name as payment_methods,
ROUND(qp.total_amount,2) as conv_fee,
ROUND(qp.total_amount,2) as payment_amount,
sum(ROUND(qi.total_amount,2)) as total_amount,
qp.reference_number as payment_reference,
qp.id as qb_payment_id,
ROUND(sc.amount,2) as stripe_amount
FROM {{ ref('quickbooks_payment') }}  qp 
left join {{ ref('quickbooks_customer') }} qc on (qc.id = qp.customer_id)
left join {{ ref('quickbooks_invoice_linked_txn') }} qilt on (qilt.payment_id = qp.id) 
left join {{ ref('quickbooks_invoice') }} qi on (qi.id = qilt.invoice_id)
left join {{ ref('quickbooks_payment_method') }} qpm on (qpm.id = qp.payment_method_id)
left join {{ ref('stripe_charge') }} sc on ( LEFT(sc.id, 20) = qp.reference_number)
group by qp.created_at, qc.id, qc.company_name, qpm.name, qp.total_amount, qp.reference_number, qp.id, sc.amount
