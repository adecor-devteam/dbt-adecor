SELECT
    qp.id as quickbooks_payment_id,
    qp.customer_id as quickbooks_customer_id,
    qc.display_name as quickbooks_customer_display_name,
    qp.created_at as  quickbooks_payment_created_at,
    qp.total_amount as quickbooks_payment_total_amount,
    qp.reference_number as quickbooks_payment_reference_number,
    qp.payment_method_id as quickbooks_payment_payment_method_id,
    qp.deposit_to_account_id as quickbooks_payment_deposit_to_account_id,
    qp.private_note as quickbooks_payment_private_not,
    qpm.id as quickbooks_payment_method_id,
    qpm.name as quickbooks_payment_method_name,
    sc.id as stripe_charge_id,
    sc.customer_id as stripe_charge_customer_id,
    sc.payment_intent_id as stripe_charge_payment_intent_id,
    sc.payment_method_id as stripe_charge_payment_method_id,
    sc.status as stripe_charge_status,
    sc.paid as stripe_charge_paid,
    sc.amount as stripe_charge_amount,
    sc.created as stripe_charge_created,
    sc.outcome_type as stripe_charge_outcome_type,
    sc.failure_code as stripe_charge_failure_code,
    sc.failure_message as stripe_charge_failure_message,
    sc.billing_detail_name as stripe_charge_billing_detail_name,
    sc.description as stripe_charge_description,
    sc.receipt_email as stripe_charge_receipt_email,
    sc.receipt_number as stripe_charge_receipt_number,
    sc.receipt_url as stripe_charge_receipt_url,
    sc.metadata as stripe_charge_metadata,
    string_agg(qi.doc_number, ', ') as invoices,
    ROUND(qp.total_amount,2) as conv_fee,
    sum(iit.discount_amount) as discount,
    sum(iit.gst_amount) as gst
FROM {{ ref('quickbooks_payment') }}  qp
left join {{ ref('quickbooks_customer') }} qc on (qc.id = qp.customer_id)
left join {{ ref('quickbooks_payment_method') }} qpm on (qpm.id = qp.payment_method_id)
left join {{ ref('stripe_charge') }} sc on ( LEFT(sc.id, 20) = qp.reference_number)
left join {{ ref('quickbooks_invoice_linked_txn') }} qilt on (qp.id = qilt.payment_id) 
left join {{ ref('quickbooks_invoice') }} qi on (qilt.invoice_id = qi.id)
left join {{ ref('interim_invoice_total') }} iit on ( qi.doc_number = iit.invoicenumber::TEXT)
group by qp.id, qp.customer_id, qc.id, qc.display_name, qp.created_at, qp.total_amount, qp.reference_number,
            qp.payment_method_id, qp.deposit_to_account_id, qp.private_note,
            qpm.id, qpm.name, sc.id, sc.customer_id, sc.payment_intent_id, sc.payment_method_id,
            sc.status, sc.paid, sc.amount, sc.created, sc.outcome_type, sc.failure_code, sc.failure_message,
            sc.billing_detail_name, sc.description, sc.receipt_email, sc.receipt_number, sc.receipt_url, sc.metadata

